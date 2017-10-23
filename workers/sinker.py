import time

from elasticsearch_dsl.connections import connections
from elasticsearch.helpers import bulk

from framework.base_worker import BaseWorker
from models.joblisting import JobPosting
from models.basic import Basic

import settings
from settings import (
    global_db,
)


_jobs_es_batch = []


# IN_PROGRESS | IDLE
R_PRECOMPUTE_LOCK_KEY = "precompute"
MAX_WAIT_TIME = 30 * 60


def block_for_precompute(logger):
    if global_db.get(R_PRECOMPUTE_LOCK_KEY) == "IN_PROGRESS":
        logger.info("waiting for precomputing")
        start_time = time.time()
        while True:
            time.sleep(20)
            # check timeout
            if time.time() - start_time > MAX_WAIT_TIME:
                logger.warn("MAX_WAIT_TIME exceeded, force break")
                global_db.set(R_PRECOMPUTE_LOCK_KEY, "EXCEEDED")
                break
            # break if finished
            if global_db.get(R_PRECOMPUTE_LOCK_KEY) != "IN_PROGRESS":
                logger.info("precompute finished. continue working")
                break
            # continue waiting


class SinkerWorker(BaseWorker):

    def __init__(self, PreTopic, NextTopic):
        super(SinkerWorker, self).__init__(__name__, PreTopic, NextTopic)
        # Since origin JLM use default schema
        # we use that too, for the auto keyword field generation
        # JobPosting.init()

        # We define only the basic mappings,
        # reason listed in Basic.__doc__
        # update: moved to master, since elasticsearch is used in multiple 
        # componets now
        Basic.init()

    def build_msg_key(self, job_id, seq, *args, **kwargs):
        return "%s-%s" % (job_id, str(seq))

    def process(self, job_id, job_data, seq):
        # block in favor of precompute
        block_for_precompute(self.logger)

        job_data.update({
            "_op_type": "index",
            "_id": job_id,
            "_index": settings.JOB_POSTING_INDEX,
            "_type": settings.JOB_POSTING_DOC_TYPE,
            "createDate": long(time.time() * 1000),
        })

        # TODO: fix this in our datasource
        if 'company' in job_data.keys():
            del job_data['company']
        if 'companyDisplay' in job_data.keys():
            del job_data['companyDisplay']

        _jobs_es_batch.append(job_data)
        es_jobs_number = len(_jobs_es_batch)

        if es_jobs_number >= settings.ES_BATCH_SIZE:
            self.logger.info('saving to ES. job_id: %s' % job_id)
            bulk(connections.get_connection(), _jobs_es_batch)
            del _jobs_es_batch[:]
