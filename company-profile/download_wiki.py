import requests
from pymongo import MongoClient

from cache import cache

from config import (
    MONGO_HOST,
    MONGO_DB_NAME,
    WIKI_COLLECTION_ORIGINAL,
    WIKI_COLLECTION_INPUT,
)





filter = {'company name': {'$ne': ''}, 'web address': {'$ne': ''}, "headquarters/branch": "Headquarter"}



# @cache
def search(keyword):
    url = 'https://en.wikipedia.org/w/api.php?action=opensearch&format=json&formatversion=2&search={}&namespace=0&limit=10&suggest=true'.format(keyword)
    rsp = requests.get(url)
    return rsp.json()


# @cache
def query_profile(url):
    rsp = requests.get(url)
    return rsp.content


def extract_domain(url):
    domain_name = url
    if domain_name.startswith('http://'):
        domain_name = domain_name[7:]
    if domain_name.startswith('https://'):
        domain_name = domain_name[8:]
    if domain_name.startswith('www.'):
        domain_name = domain_name[4:]
    return domain_name.rstrip('/')


def search_wiki():

    cnt = 0
    done = set()

    client = MongoClient(host=MONGO_HOST)
    collection = client[MONGO_DB_NAME][WIKI_COLLECTION_INPUT]

    wiki_coll = client[MONGO_DB_NAME][WIKI_COLLECTION_ORIGINAL]

    # for r in wiki_coll.find({}, {'in_company_name': 1}):
    #    done.add(r.get('in_company_name'))

    print '%s to search' % (collection.count(filter) - len(done))

    for r in collection.find(filter, {'id': 1, 'company name': 1, 'web address': 1}):
        id = r.get('id')
        c = r.get('company name')
        web_address = r.get('web address')

        key = id
        if key not in done:
            rsp = search(c)
            wiki_coll.insert({
                '_id': key,
                'id': id,
                'in_company_name': c,
                'in_web_address': web_address,
                'in_domain_name': extract_domain(web_address),
                'search_results': [{
                    'name': name,
                    'desc': desc,
                    'wiki_url': url,
                    'html_content': query_profile(url),
                    } for name, desc, url in zip(rsp[1], rsp[2], rsp[3])],
            })

            cnt += 1
            # done.add(key)

    print '%s done' % cnt


if __name__ == '__main__':
    search_wiki()
