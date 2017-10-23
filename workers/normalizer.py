import logging
import requests
import uuid
import base64
import time
import json

from framework.base_worker import BaseWorker
from framework import reports

import settings
from settings._redis import r_db


MAJOR_IN_DESC_AND_PREFERRED_TITLE = 60
MAJOR_SYNONYM_IN_DESC_AND_PREFERRED_TITLE = 60
MAJOR_IN_DESC_AND_TITLE = 45
MAJOR_SYNONYM_IN_DESC_AND_TITLE = 45
MAJOR_IN_DESC = 35
MAJOR_SYNONYM_IN_DESC = 35

MAJOR_BUCKET_1_CHOICES = {
    MAJOR_IN_DESC_AND_PREFERRED_TITLE,
    MAJOR_SYNONYM_IN_DESC_AND_PREFERRED_TITLE,
    MAJOR_IN_DESC_AND_TITLE,
    MAJOR_SYNONYM_IN_DESC_AND_TITLE,
    MAJOR_IN_DESC,
    MAJOR_SYNONYM_IN_DESC,
}

RETRY_PRESET = [(3, 0), (10, 0), (60, 10), (600, 30), (1800, 120)]
RETRY_PRESET_LEN = len(RETRY_PRESET)

logger = logging.getLogger(__name__)


LOG_INTERVAL = 10

LOCATION_EDIT_DISTANCE = 2

_job_batch = []
_job_ids = []
_job_seqs = []
_job_feeds = []


def timeperf(f):
    def wrapper(*args, **kwargs):
        t1 = time.time()
        r = f(*args, **kwargs)
        t = time.time() - t1
        logger.info('timecost: %.4f' % t)
        return r
    return wrapper


# @timeperf
def request_norm(jobs, batch_id):
    data = {
        'batch_id': batch_id,
        'classify_job_level': True,
        'classify_major': True,
        'classify_education_degree': True,
        'extract_benefits': True,
        'extract_visa_status': True,
        'extract_job_type': True,
        'jobs': [{
            'title': r['title'],
            'description': r['desc'] or '',
            'soc_hint': r.get('socCodeHint') or 'Undefined',
            'organization': r['company'] or '',
            'city': r['city'] or '',
            'state': r['state'] or '',
            'edit_distance_threshold': settings.LOCATION_EDIT_DISTANCE,
        } for r in jobs],
    }
    start_time = time.time()
    try:
        r = requests.post(settings.URL_NORM_JOB, json=data,
                          timeout=settings.NORM_REQ_TIMEOUT)
    except requests.Timeout:
        return None
    delta = time.time() - start_time
    if delta > settings.NORM_REQ_ALERT_WATERMARK_S:
        logger.warn("norm request: %.6fs" % delta)

    assert r is not None
    return r


def request_with_retry(logger, jobs, batch_id):
    retry_times = int(round(settings.NUM_CONCURRENCY * 1.3))
    retry_wait_times = 3
    resp_succ = False
    resp_err = False
    resp_err_reason = None

    # break on either resp_succ or resp_succ
    for j in range(retry_wait_times):
        resp = request_norm(jobs, batch_id)
        # when timeout, no further retry
        if resp is None:
            resp_err = True
            resp_err_reason = "timed out"
            break

        status_code = resp.status_code
        if status_code == 200:
            resp_succ = True
            break
        # fast retry for status code: 429, 502
        elif status_code == 429 or status_code == 502:
            logger.info("batch %d fast retry" % batch_id)
            fast_retry_count = 0
            for i in range(retry_times):
                resp = request_norm(_job_batch, batch_id)
                if resp is None:
                    resp_err = True
                    resp_err_reason = "timed out"
                    break
                status_code = resp.status_code
                fast_retry_count += 1
                if status_code == 200:
                    resp_succ = True
                    break
                elif status_code == 429 or status_code == 502:
                    continue
                else:
                    resp_err = True
                    resp_err_reason = (
                        "unexpected status code %d" % status_code)
                    break
            # check status
            if resp_succ:
                logger.info("batch %d success in %d fast retries" %
                            (batch_id, fast_retry_count))
                # break the outter most loop
                break
            if resp_err:
                break
        else:
            # unknown status code
            resp_err = True
            resp_err_reason = (
                "unexpected status code %d" % status_code)
            break

        # handle succ or err break from fast retry loop
        if resp_succ or resp_err:
            break

        # else wait for another retry loop
        logger.info("batch %d status code %d wait 3s and retry" %
                    (batch_id, status_code))
        time.sleep(3)
    else:
        # retry times exceeded
        resp_err = True
        resp_err_reason = "max retry times exceeded"

    # we have a final result now
    if resp_succ:
        try:
            return resp.json()
        except ValueError:
            resp_err = True
            resp_err_reason = "resp decode error"

    # all we have is resp_err == True now
    logger.error("batch %d %s" % (batch_id, resp_err_reason))
    for jobid, feed in zip(_job_ids, _job_feeds):
        logger.debug('discarded - norm req failed - ' + json.dumps(
            {"jobId": jobid, "feed": feed, "reason": resp_err_reason}))
    return None


class NormalizerWorker(BaseWorker):

    def __init__(self, PreTopic, NextTopic):
        super(NormalizerWorker, self).__init__(__name__, PreTopic, NextTopic)
        self.processed = False

    def build_msg_key(self, job_id, seq, *args, **kwargs):
        return "%s-%s" % (job_id, str(seq))

    def process(self, job_id, job_data, feed_name, seq):

        _job_batch.append(job_data)
        _job_ids.append(job_id)
        _job_feeds.append(feed_name)
        _job_seqs.append(seq)

        job_number = len(_job_batch)
        if job_number < settings.NORM_BATCH_SIZE:
            self.processed = False
            return

        self.processed = True

        # batch full, send norm requests
        batch_id = r_db.incr(settings.r_batch_num_key)

        # handle retry and error handling all in one function
        rsp_json = request_with_retry(self.logger, _job_batch, batch_id)
        if not rsp_json:
            return

        # handle response length mismatch
        if len(rsp_json.get('normalized_jobs', [])) != job_number:
            reports.incr_by(job_number, 'norm-error')
            logger.error('unexpected norm error. length_neq_sent_size. batch_id: %s' % (batch_id, ))
            for jobid, feed in zip(_job_ids, _job_feeds):
                self.logger.debug('discarded - norm resp invalid - ' + json.dumps({"jobId": jobid, "feed": feed}))
            return

        # handle every single normed job
        for job_id, norm_rsp, i_job_data, job_seq, job_feed in zip(_job_ids, rsp_json['normalized_jobs'], _job_batch, _job_seqs, _job_feeds):
            if 'error_code' in norm_rsp:
                reports.incr('norm-error')
                self.logger.debug('discarded - norm failed - ' + json.dumps({"jobId": job_id, "feed": job_feed}))
            else:
                kwargs = {
                    'job_id': job_id,
                    'norm_rsp': norm_rsp,
                    'job_data': i_job_data,
                    'seq': job_seq,
                }
                self.produce_msg(**kwargs)

    def post_process(self, *args, **kwargs):
        if not self.processed:
            return

        del _job_batch[:]
        del _job_ids[:]
        del _job_seqs[:]
        del _job_feeds[:]
