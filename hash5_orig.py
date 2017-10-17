import hashlib
from hashlib import sha1
from binascii import hexlify
from elasticsearch import Elasticsearch, helpers
import time



ESURL = 'datascience2.dev.zippia.com:9211'
undefined = "undefined"

es = Elasticsearch(hosts=[ESURL])

def getin(path, d):
    path = path[::-1]

    while len(path) != 0:
        k = path.pop()
        if not isinstance(d, dict):
            return None
        d = d.get(k)

    return d

def build_listing_hash(desc):
    return hexlify(sha1((desc).encode("UTF-8")).digest())
desc_result = []


def AC():
    desc = 'body'
    res = helpers.scan(client=es,
                       scroll='2m',
                       size=10,
                       query={"query": {"match_all": {}}, "_source": [desc]},
                       index='dump2_parser',
                       doc_type='APPCAST')
    for val in res:
        result = build_listing_hash(val['_source'].get(desc, None) or undefined)
        job_id = val['_id']
        desc_result.append((result, job_id))



    

def CR():
    desc = 'body'
    res = helpers.scan(client=es,
                       scroll='2m',
                       size=10,
                       query={"query": {"match_all": {}}, "_source": [desc]},
                       index='dump2_parser',
                       doc_type='COLLEGE_RECRUITER')
    for val in res:
        result = build_listing_hash(val['_source'].get(desc, None) or undefined)
        job_id = val['_id']
        desc_result.append((result, job_id))



def DE():
    desc = 'PositionProfile.PositionFormattedDescription.Content'
 
    res = helpers.scan(client=es,
                       scroll='2m',
                       size=10,
                       query={"query": {"match_all": {}}, "_source": [desc]},
                       index='dump2_parser',
                       doc_type='DIRECT_EMPLOYERS')

    for val in res:
        tmp=getin([
            'PositionProfile',
            'PositionFormattedDescription',
            'Content'], val) or ''
    for val in res:
        result = build_listing_hash(val['_source'].get(desc, None) or undefined)
        job_id = val['_id']
        desc_result.append((result, job_id))


def J2C_AGG():
    desc = 'description'
    res = helpers.scan(client=es,
                       scroll='2m',
                       size=10,
                       query={"query": {"match_all": {}}, "_source": [desc]},
                       index='dump2_parser',
                       doc_type='JOBS2CAREERS_AGGREGATE')
    for val in res:
        result = build_listing_hash(val['_source'].get(desc, None) or undefined)
        job_id = val['_id']
        desc_result.append((result, job_id))



def J2C_CPA():
    desc = 'description'
    res = helpers.scan(client=es,
                       scroll='2m',
                       size=10,
                       query={"query": {"match_all": {}}, "_source": [desc]},
                       index='dump2_parser',
                       doc_type='JOBS2CAREERS_CPA')
    for val in res:
        result = build_listing_hash(val['_source'].get(desc, None) or undefined)
        job_id = val['_id']
        desc_result.append((result, job_id))


def J2C_CPC():
    desc = 'description'
    res = helpers.scan(client=es,
                       scroll='2m',
                       size=10,
                       query={"query": {"match_all": {}}, "_source": [desc]},
                       index='dump2_parser',
                       doc_type='JOBS2CAREERS_CPC')
    for val in res:
        result = build_listing_hash(val['_source'].get(desc, None) or undefined)
        job_id = val['_id']
        desc_result.append((result, job_id))



def RJ():
    desc = 'description'
    res = helpers.scan(client=es,
                       scroll='2m',
                       size=10,
                       query={"query": {"match_all": {}}, "_source": [desc]},
                       index='dump2_parser',
                       doc_type='JOVEO')
    for val in res:
        result = build_listing_hash(val['_source'].get(desc, None) or undefined)
        job_id = val['_id']
        desc_result.append((result, job_id))


def JJ():
    desc = 'description'
    res = helpers.scan(client=es,
                       scroll='2m',
                       size=10,
                       query={"query": {"match_all": {}}, "_source": [desc]},
                       index='dump2_parser',
                       doc_type='JUJU')
    for val in res:
        result = build_listing_hash(val['_source'].get(desc, None) or undefined)
        job_id = val['_id']
        desc_result.append((result, job_id))


def LN():
    desc = 'body'
    res = helpers.scan(client=es,
                       scroll='2m',
                       size=10,
                       query={"query": {"match_all": {}}, "_source": [desc]},
                       index='dump2_parser',
                       doc_type='LENSA')
    for val in res:
        result = build_listing_hash(val['_source'].get(desc, None) or undefined)
        job_id = val['_id']
        desc_result.append((result, job_id))


def TUJ():
    desc = 'JobDescription'
    res = helpers.scan(client=es,
                       scroll='2m',
                       size=10,
                       query={"query": {"match_all": {}}, "_source": [desc]},
                       index='dump2_parser',
                       doc_type='TOPUSAJOBS')
    for val in res:
        result = build_listing_hash(val['_source'].get(desc, None) or undefined)
        job_id = val['_id']
        desc_result.append((result, job_id))


if __name__ == '__main__':
   
    starttime = time.time()
    CR()
    print('till CR: %6f' % (time.time() - starttime))  # noqa; 501
    AC()
    print('till AC: %6f' % (time.time() - starttime))  # noqa; 501
    DE()
    print('till DE: %6f' % (time.time() - starttime))  # noqa; 501
    J2C_AGG()
    print('till J2C_AGG: %6f' % (time.time() - starttime))  # noqa; 501
    J2C_CPA()
    print('till J2C_CPA: %6f' % (time.time() - starttime))  # noqa; 501
    J2C_CPC()
    print('till J2C_CPC: %6f' % (time.time() - starttime))  # noqa; 501
    RJ()
    print('till RJ: %6f' % (time.time() - starttime))  # noqa; 501
    JJ()
    print('till JJ: %6f' % (time.time() - starttime))  # noqa; 501
    LN()
    print('till LN: %6f' % (time.time() - starttime))  # noqa; 501
    TUJ()
    print('till TUJ: %6f' % (time.time() - starttime))  # noqa; 501

    with open('hash_original.csv', 'w') as o: 
        for i in desc_result:
            o.write(i)
            o.write('\n')