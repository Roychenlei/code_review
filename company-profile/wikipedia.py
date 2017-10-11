import requests
import csv
import re
from lxml import etree


from cache import cache


ptn = re.compile('[A-Z][a-z]+|[A-Z]+')
ptn_2 = re.compile('\W+')


@cache
def get_companies_name_website_from_mongo():
    return ''


def get_companies_name_website_from_csv(filename):
    idx_company_name = 0
    idx_company_website = 10
    data = []
    with open(filename, 'rb') as csvfile:
        fr = csv.reader(csvfile)
        for row in fr:
            data.append((row[idx_company_name], row[idx_company_website]))
    return data[1:]


@cache
def search(keyword):
    url = 'https://en.wikipedia.org/w/api.php?action=opensearch&format=json&formatversion=2&search={}&namespace=0&limit=10&suggest=true'.format(keyword)
    rsp = requests.get(url)
    return rsp.json()


@cache
def query_profile(url):
    rsp = requests.get(url)
    return rsp.content


def extract_words(text):
    words = ptn.findall(text)
    return {w for w in words}


def extract_words_2(text):
    words = [w.strip() for w in ptn_2.sub(' ', text).split() if w.strip()]
    return {w for w in words}


def find_best_match(input, matches):

    splited = []

    base = extract_words(input)
    for idx, t in enumerate(matches):
        words = extract_words(t)

        res = words - base - {'corpor', 'incorpor', 'compani', 'the'}
        if res:
            splited.append((t, res))
        else:
            return idx, splited

    base = extract_words_2(input)
    for idx, t in enumerate(matches):
        words = extract_words_2(t)

        res = words - base - {'corpor', 'incorpor', 'compani', 'the'}
        if not res:
            return idx, splited

    return -1, splited


def get_info(url):
    text = query_profile(url)
    html = etree.HTML(text)
    results = html.xpath('//table[@class="infobox vcard"]')
    if not results:
        pass
        # print 'not found: %s' % url
    elif len(results) > 1:
        pass
        # print url
    else:
        info_text = etree.tostring(results[0])
        ptn = re.compile('Website</th>\s*<td.*?href="(.*?)">.*?</td>', re.DOTALL)
        return ptn.findall(info_text)


def extract_domain(url):
    domain_name = url
    if domain_name.startswith('http://'):
        domain_name = domain_name[7:]
    if domain_name.startswith('https://'):
        domain_name = domain_name[8:]
    if domain_name.startswith('www.'):
        domain_name = domain_name[4:]
    return domain_name.rstrip('/')


def main(input_data):

    for c, url in input_data:
        input, matches, descs, urls = search(c)


if __name__ == '__main__':
    input_data = set(get_companies_name_website_from_csv('data_1000.csv'))
    print '%s unique companies got from csv file.' % len(input_data)

    main(input_data)
