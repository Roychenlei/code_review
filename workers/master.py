import time
import json
import base64
import uuid
import kazoo

import settings

from settings import (
    r_db,
    global_db,
    r_total_job_key,
    r_old_job_key,
    r_new_job_key,
    r_error_job_key,
    TASK_DEFINITION_PATH,
    JOB_SOURCES,
    KAFKA_SERVER,
    KAFKA_POSTFIX_PATH,
    NUM_CONCURRENCY,
)
from framework.base_worker import BaseWorker
from utils.clean_es import clean_es, build_listinghash_cache
from utils import cache
from topics.job_source import JobSourceTopic
from settings import MASTER_INTERVAL, KAFKA_WAIT_TIME, TOPIC_COUNT_MAX_IDLE_TIME
from cluster import tasks, topics, kafka_utils
from models.basic import Basic
from kafka.client_async import KafkaClient

ENOTFIN = 1
SUCC = 0

TASK_RERUN_WAIT_TIME = 15


def get_jobs_statistic():
    return "statistics info of jobs. new/old/error/total %s/%s/%s/%s" % (
        r_db.get(r_new_job_key) or 0,
        r_db.get(r_old_job_key) or 0,
        r_db.get(r_error_job_key) or 0,
        r_db.get(r_total_job_key) or 0,
    )


def init_task_definitions(logger, zk_cli):
    logger.info("initializing task definitions in zookeeper")
    tasks.setup_task_definition(zk_cli, TASK_DEFINITION_PATH, JOB_SOURCES)


def send_task(logger, zk_cli):
    # get next task
    task = tasks.next_task(zk_cli, TASK_DEFINITION_PATH)

    topic = JobSourceTopic()
    # get postfix
    val, stats = zk_cli.get(KAFKA_POSTFIX_PATH)
    producer = topic.init_producer()
    producer.change_postfix(val)

    kfk_cli = KafkaClient(bootstrap_servers=settings.KAFKA_SERVER)
    # current topics (error_code, topic, is_internal, partitions)
    is_fetch_metadata_success = False
    try:
        topics_res = kafka_utils.get_metadata(kfk_cli, 10000)
        if topics_res is not None:
            topics = (x[1] for x in topics_res.topics)
            logger.debug("Current topics in kafka: %s" % str(list(topics)))
            is_fetch_metadata_success = True
    except Exception:
        pass
    if not is_fetch_metadata_success:
        logger.warn("Failed to fetch metadata from kafka")

    producer.produce(task)
    logger.info('sent to Topic %s, feed=%2d. %s' % (
        producer._topic_name,
        task['order'],
        task['name']))
    return task


def switch_topics(logger, zk_cli, topic_list, postfix):
    topic_configs = {
        topic.topic_name: {
            "partitions": NUM_CONCURRENCY,
        } for topic in topic_list}
    logger.info(str(topic_configs))
    try:
        val, state = zk_cli.get(KAFKA_POSTFIX_PATH)
    except kazoo.exceptions.NoNodeError:
        val = None
    logger.info("switching postfix from %s to %s" % (
        str(val),
        postfix,
    ))
    topics.master_switch_topics(
        zk_cli,
        logger,
        KAFKA_SERVER,
        10000,
        topic_configs,
        KAFKA_POSTFIX_PATH,
        postfix,
    )


class Master(BaseWorker):

    def __init__(self, workers_info, topics):
        super(Master, self).__init__(__name__)
        # TODO: fix workers for different purposes refactoring
        # need setup_zk because base_worker is not clean
        self._setup_zk()

        self._workers_info = workers_info
        self._topics = topics

        # states
        # !IMPORTANT
        # if new state are added, make sure they are reset in handle_setup
        self._topic_check_idx = 0
        self._last_topic_counts = None
        self._topic_idle_start_time = None
        self._task = None
        self._task_start_time = None
        self._task_finish_time = None

    @staticmethod
    def _run(meth, sleep_interval=None):
        while meth() != SUCC:
            if sleep_interval:
                time.sleep(sleep_interval)

    def run_loop(self):
        # init
        self.logger.info("minimal status " + json.dumps(self._workers_info))
        self._run(self.handle_init, MASTER_INTERVAL)
        while True:
            self.logger.warning('starting new process...')
            # setup
            self.handle_setup()

            # monitor
            self.logger.info('start monitoring workers. interval: %ss' % MASTER_INTERVAL)
            self._run(self.handle_monitor, MASTER_INTERVAL)

            self.logger.info('go to FINISH state')
            self.handle_finish()

            self.logger.info('go to IDLE state')
            self.handle_idle()

    def handle_init(self):
        """
        cluster initialization work
        1. checks all workers are up
        """
        # init elasticsearch schema
        Basic.init()

        # check workers
        worker_status = {
            worker: global_db.get(settings.KEY_WORKER_READY % worker) or 0
            for worker in self._workers_info.keys()
        }

        for worker in self._workers_info.keys():
            cnt_got = int(worker_status[worker])
            cnt_required = int(self._workers_info[worker])
            if cnt_got < cnt_required:
                self.logger.debug('count of %s. got: %s, required: %s' % (
                    worker, cnt_got, cnt_required))
                return ENOTFIN

        self.logger.info('workers ready. %s' % json.dumps(worker_status))

        # send task definitions to zookeeper
        init_task_definitions(self.logger, self.get_zk_client())

        return SUCC

    def handle_setup(self):
        """
        Setup environment for a task
        1. clear redis
        2. generate new process_seq id
        3. send init message to kafka
        4. reset state
        """
        self.logger.info("setting up env for new task")

        self.logger.info("clearing redis...")
        cache.init_redis()

        # generate new process_seq
        process_seq = time.strftime("%Y%m%d%H%M", time.gmtime())
        self._process_seq = process_seq
        r_db.set(settings.KEY_PROCESS_SEQ, process_seq)
        self.logger.info("generated process_seq: %s" % process_seq)

        postfix = base64.urlsafe_b64encode(uuid.uuid4().bytes).strip("=")
        switch_topics(self.logger, self.get_zk_client(), self._topics, postfix)

        # populating listinghash cache
        self.logger.info("populating listinghash cache...")
        if not build_listinghash_cache(self.logger):
            self.logger.warn("failed to populate listinghash cache")

        # send init message
        self.logger.info("sending tasks for process-%s" % process_seq)
        task = send_task(self.logger, self.get_zk_client())
        self._task = task

        # reset state used by Master worker only
        self._topic_check_idx = 0
        self._last_topic_counts = None
        self._topic_idle_start_time = None
        self._task_start_time = time.time()
        self._task_finish_time = None

        self.logger.info("process-%s started successfully" % process_seq)

    def handle_monitor(self):
        """
        Monitoring worker status and task progress
        1. checks if all worker are working properly
           1. the last respond time not exceeds max_res_time
        2. checks task progress
           1. how many steps have finished
           2. the number of message in, message out, error message
        """
        # runtime status report
        self.logger.debug(get_jobs_statistic())

        # dump current topic counts
        topic_counts = [(
            topic.get_cnt_produced() or 0,
            topic.get_cnt_consumed() or 0,
            topic.get_cnt_cached() or 0,
        ) for topic in self._topics]

        # if topic counts didn't change for more than TOPIC_COUNT_MAX_IDLE_TIME
        # consider the system has finished and log a warning
        # >> shared states involved <<
        # * _last_topic_counts
        # * _topic_idle_start_time
        # * TOPIC_COUNT_MAX_IDLE_TIME
        if self._last_topic_counts and self._last_topic_counts == topic_counts:
            if not self._topic_idle_start_time:
                self.logger.debug("start counting IDLE time. none of the topics changed since last check")
                self._topic_idle_start_time = time.time()
            else:
                idle_time = time.time() - self._topic_idle_start_time

                if idle_time > TOPIC_COUNT_MAX_IDLE_TIME:
                    self.logger.warning("workers IDLE in last %ss. considering as finished" % idle_time)
                    self.logger.warning(get_jobs_statistic())
                    for t, c in zip(self._topics, topic_counts):
                        n_produced, n_consumed, n_cached = c
                        self.logger.warning("stopping %s, produced/consumed/cached: %s/%s/%s" %
                                            (t.topic_name, n_produced, n_consumed, n_cached))

                    n_produced, n_consumed, n_cached = topic_counts[self._topic_check_idx]
                    return SUCC
                if idle_time > 0.7 * TOPIC_COUNT_MAX_IDLE_TIME:
                    self.logger.info("workers IDLE in last %ss. TOPIC_COUNT_MAX_IDLE_TIME: %s" % (idle_time, TOPIC_COUNT_MAX_IDLE_TIME))
        else:
            # topic counts changed, so set _topic_idle_start_time to None
            self._topic_idle_start_time = None

        # prepare for next check
        self._last_topic_counts = topic_counts

        # check topic finish state
        while self._topic_check_idx < len(self._topics):
            # all topics before are finished while this topic is not at last check
            # so check it
            topic = self._topics[self._topic_check_idx]
            n_produced, n_consumed, n_cached = topic_counts[self._topic_check_idx]

            # do not check n_produced > 0, for feed error may result in 0 message produced in some workerss
            if n_consumed < n_produced:  # n_msg_out >= n_msg_in
                self.logger.debug("processing %s, produced/consumed/cached: %s/%s/%s" %
                                  (topic.topic_name, n_produced, n_consumed, n_cached))
                return ENOTFIN  # unfinished
            else:
                self._topic_check_idx += 1
                self.logger.warning("done %s, produced/consumed/cached: %s/%s/%s" %
                                    (topic.topic_name, n_produced, n_consumed, n_cached))

        self.logger.info("considered success, go to finish state.")
        return SUCC

    def handle_finish(self):
        """
        After all data has been processed.
        1. _delete_by_query { query: { bool: { must_not: { term { processSeq: $SEQ } } } } }
        """
        process_seq_id = self._process_seq
        if process_seq_id:
            try:
                is_succ, msg, rate = clean_es(process_seq_id, self._task['abbr'])
                if is_succ:
                    self.logger.info("%s, del rate: %.2f%%" % (msg, rate))
                else:
                    self.logger.warn("%s, del rate: %.2f%%" % (msg, rate))
            except Exception as e:
                self.logger.exception(e)
        else:
            self.logger.error("process seq (%s) not valid, skip es clean up" % process_seq_id)

        # mark finish time
        self._task_finish_time = time.time()

    def handle_idle(self):
        """
        Checks if it's ok to start task of next day
        Then switch master status to S_SETUP
        """
        time_remain = TASK_RERUN_WAIT_TIME + self._task_finish_time - time.time()
        while time_remain > 0:
            self.logger.debug("IDLE between two process... time remaining: %ds" % time_remain)
            time.sleep(15)
            time_remain = TASK_RERUN_WAIT_TIME + self._task_finish_time - time.time()
