import json
import time

import settings

from framework.base_worker import BaseWorker
from framework import reports
from elasticsearch_dsl.connections import connections

from record_cleaner import parse_record
from utils.exceptions import UnsupportedFeed
from utils.dup_detect import (
    build_job_id,
    better_job,
)
from utils.clean_es import (
    bulk_update_actions,
    bulk_execute,
)
from settings import (
    listinghash_db,
    r_db,
    r_new_job_key,
    r_old_job_key,
    r_total_job_key,
    KEY_PROCESS_SEQ,
)


class CleanerWorker(BaseWorker):

    def __init__(self, PreTopic, NextTopic):  # , NextTopicOldJob):
        super(CleanerWorker, self).__init__(__name__, PreTopic, NextTopic)

        # self.next_topic_oldjob = NextTopicOldJob()
        self.jobs_buffer = []

    def build_msg_key(self, record, feed_name, seq):
        return '%s-%s' % (feed_name, seq)

    def process(self, record, feed_name, seq):
        try:
            error, data = parse_record(record, feed_name, seq)
        except UnsupportedFeed:
            # error logged in parse_record method
            return

        # import json
        # self.logger.info('record %s' % json.dumps(data, indent=4))

        if error:
            reports.incr('clean-error', feed_name)
            self.logger.debug("discarded - clean error - " + json.dumps(
                {
                    "jobId": data and data.get("id",'unknown') or 'unknown',
                    "error": error,
                    "feed": feed_name,
                }))
        else:
            job_id = build_job_id(**data)

            self.jobs_buffer.append(
                {
                    'job_id': job_id,
                    'job_data': data,
                    'feed_name': feed_name,
                    'seq': seq,
                }
            )

            if len(self.jobs_buffer) > 1000:
                self.process_batch(self.jobs_buffer)
                self.jobs_buffer = []

    def process_batch(self, jobs):
        # query redis in a pipeline
        pipe = listinghash_db.pipeline()
        for job in jobs:
            pipe.get(job['job_data']['listingHash'])
        job_info_xs = pipe.execute()
        # deserializing jobs
        job_info_xs = [json.loads(job) if job else job for job in job_info_xs]
        # dup detect by listinghash cache
        _to_update = []
        _to_norm = []
        for job_info, job in zip(job_info_xs, jobs):
            if job_info:
                _to_update.append((job['job_data'], job_info))
            else:
                _to_norm.append(job)
        # bulk update old jobs
        # TODO: log old jobs to a topic for destination reasoning
        #       logger slows down the processor, don't use logger
        process_seq = r_db.get(KEY_PROCESS_SEQ)
        if not process_seq:
            self.logger.error("process_seq is empty")
            is_ok = False
        else:
            _better_job = []
            for job, job_info in _to_update:
                if better_job(job, job_info) >= 0:
                    # job = job.copy()
                    job['_id'] = job_info['_id']
                    _better_job.append(job)
            es_actions = bulk_update_actions(_better_job, process_seq)
            conn = connections.get_connection()
            is_ok = bulk_execute(self.logger, conn, es_actions)

        # if failed, norm old jobs as well, so
        # [_to_update] ++ [_to_norm] <- jobs
        # update: just discard them
        if not is_ok:
            # _to_norm = jobs
            self.logger.warn("Failed to bulk update existing jobs")

        # reporting
        count_all_job = len(jobs)
        count_old_job = len(_to_update)
        count_new_job = len(_to_norm)
        r_db.incr(r_total_job_key, count_all_job)
        r_db.incr(r_new_job_key, count_new_job)
        r_db.incr(r_old_job_key, count_old_job)

        # send to normalizer
        for job in _to_norm:
            self.produce_msg(**job)
