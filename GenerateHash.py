import hashlib
from hashlib import sha1
from binascii import hexlify
from elasticsearch import Elasticsearch, helpers

ESURL = 'datascience2.dev.zippia.com:9211'
undefined = "undefined"

es = Elasticsearch(hosts =[ESURL])

res = helpers.scan( client=es, 
                    scroll='2m', 
                    size=10,
                    query={"query": {"match_all": {}}}, 
                    index='dump_cleaner')


def build_listing_hash(titleDisplay, companyDisplay, city, state, desc):
    return hexlify(sha1((
        titleDisplay +
        companyDisplay +
        city +
        state +
        desc).encode("UTF-8")).digest())


if __name__ == '__main__':
    with open('tmp.csv', 'w') as o:  
        for val in res:
            result = build_listing_hash(
                        val['_source'].get("title", None) or undefined, 
                        val['_source'].get("company", None) or undefined,
                        val['_source'].get("city", None) or undefined,
                        val['_source'].get("state", None) or undefined,
                        val['_source'].get("desc", None) or undefined)
            jobid = val['_source'].get('source', None) + val['_source'].get('jobId', None)  # noqa; 501
            final_result = '%s,%s' % (result, jobid) 
            o.write(final_result)
            o.write('\n')

