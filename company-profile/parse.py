# -*- coding: utf-8 -*-
from pymongo import MongoClient
import time
import re
from lxml import etree
from cache import cache

from download_wiki import extract_domain
from config import (
    MONGO_HOST,
    MONGO_DB_NAME,
    WIKI_COLLECTION_PARSED,
    WIKI_COLLECTION_CLEAN,
    WIKI_COLLECTION_ORIGINAL,
    CRITERIA,
)

table_xpath_first = '//*[@id="mw-content-text"]/div/table[@class="infobox vcard"]'  # noqa, 501
table_xpath_second = '//*[@id="mw-content-text"]/div/div/table[@class="infobox vcard"]'  # noqa, 501


ptn_clean = re.compile('\W+')
stop_words = {
    'co',
    'corp',
    'corpor',
    'corporation',
    'inc',
    'incorpor',
    'incorporated',
    'company',
    'companies',
    'production',
    'ltd',
}


hard_coded = {
    'News Corp': 'https://en.wikipedia.org/wiki/News_Corp',
}


meta_key_map = {
    'Status': 'ignore',
    'National Adjutant': 'ignore',
    'Publication': 'ignore',
    'Profit': 'ignore',
    'No. of attorneys': 'ignore',
    'Interest on reserves': 'ignore',
    'Current status': 'ignore',
    'Coordinates': 'ignore',
    'Net income': 'Net Income',
    'Total assets': 'Total Assets',
    'Access requirements': 'ignore',
    'Subsidiaries': 'Subsidiaries',
    'Official language': 'ignore',
    'Predecessor': 'ignore',
    'Volunteers': 'ignore',
    'Frequent-flyer program': 'ignore',
    'Parent': 'ignore',
    'Volunteers (2014)': 'ignore',
    'Areas served': 'Areas Served',
    'Expenses (2013)': 'ignore',
    'Director': 'ignore',
    'Visitors': 'ignore',
    'Office location': 'ignore',
    'Membership (2016)': 'ignore',
    'Services': 'ignore',
    'Production output': 'ignore',
    'Academic affiliations': 'ignore',
    'Full name': 'ignore',
    'Successor': 'ignore',
    'Owners': 'ignore',
    'Branches': 'ignore',
    'Access and use': 'ignore',
    'OCLC number': 'ignore',
    'Headquarters': 'Headquarters Location',
    'United States Botanic Gardens': 'ignore',
    'Postgraduates': 'ignore',
    'Products': 'Products',
    'Founder(s)': 'Founders',
    'Focus cities': 'ignore',
    'Newspaper': 'ignore',
    'Organization': 'ignore',
    'Athletics': 'ignore',
    'Successors': 'ignore',
    'Ceased operations': 'ignore',
    'Interest paid on excess reserves?': 'ignore',
    'Secondary hubs': 'ignore',
    'Rating': 'ignore',
    'Alliance': 'ignore',
    'Chairwoman': 'ignore',
    'Links': 'ignore',
    'Affiliated university': 'ignore',
    'Abbreviation': 'ignore',
    'Affiliation': 'ignore',
    'Company type': 'Organization Type',
    'Commander in Chief': 'ignore',
    'Registration': 'ignore',
    'Traded as': 'ignore',
    'Type of business': 'Organization Type',
    'Destinations': 'ignore',
    'Established': 'ignore',
    'Publisher': 'ignore',
    'Main organ': 'ignore',
    'Sports': 'ignore',
    'National Commander': 'ignore',
    'Editor': 'ignore',
    'Founders': 'Founders',
    'Other information': 'ignore',
    'Type': 'Organization Type',
    'Religious affiliation': 'ignore',
    'Revenue (2013)': 'Revenue',
    'Former names': 'ignore',
    'CEO': 'ignore',
    'Method': 'ignore',
    'Academic staff': 'ignore',
    'Chief Executive Officer': 'ignore',
    'Created by': 'ignore',
    'Members': 'ignore',
    'Fields': 'ignore',
    'National Vice Commanders': 'ignore',
    'Mascot': 'ignore',
    'Website': 'Website URL',
    'Slogan(s)': 'ignore',
    'Origins': 'ignore',
    'Created': 'ignore',
    'Key people': 'Key people',
    'President and CEO': 'ignore',
    'Budget': 'ignore',
    'Foundation President': 'ignore',
    'Dissolved': 'ignore',
    'Reserve requirements': 'ignore',
    'Care system': 'ignore',
    'Hubs': 'ignore',
    'AOC #': 'ignore',
    'Legal status': 'ignore',
    'Location': 'city',
    'Operating bases': 'ignore',
    'Employees (2013)': 'ignore',
    'Founder': 'Founders',
    'Revenue': 'Revenue',
    'Items collected': 'ignore',
    'Owner(s)': 'ignore',
    'Focus': 'ignore',
    'Region served': 'ignore',
    'Founded': 'Founded Date',
    'Currency': 'ignore',
    'Profit per equity partner': 'ignore',
    'Emergency department': 'ignore',
    'Owner': 'ignore',
    'Chair': 'ignore',
    'Interest rate target': 'ignore',
    'Endowment': 'ignore',
    'Campus': 'ignore',
    'Board of directors': 'ignore',
    'Staff': 'ignore',
    'Number of locations': 'ignore',
    'Sporting affiliations': 'ignore',
    'Parent company': 'ignore',
    'Board Chair': 'ignore',
    'Area served': 'ignore',
    'Secessions': 'ignore',
    'Collection': 'ignore',
    'Population served': 'ignore',
    'Volunteers (2013)': 'ignore',
    'Trading name': 'ignore',
    'Central bank of': 'ignore',
    'President & CEO': 'ignore',
    'Major practice areas': 'ignore',
    'Students': 'ignore',
    'Revenue (2015)': 'Revenue',
    'Commercial': 'ignore',
    'Supreme Knight': 'ignore',
    'Lists': 'ignore',
    'Membership': 'ignore',
    'Executive director': 'ignore',
    'Number of employees': 'Number of Employees',
    'ISIN': 'ignore',
    'President/CEO': 'ignore',
    'Administrative staff': 'ignore',
    'Available in': 'ignore',
    'Predecessors': 'ignore',
    'Divisions': 'ignore',
    'Circulation': 'ignore',
    'Principal': 'ignore',
    'AUM': 'AUM',
    'Supreme Chaplain': 'ignore',
    'Beds': 'ignore',
    'Brands': 'ignore',
    'Executive Director and CEO': 'ignore',
    'Capital ratio': 'ignore',
    'History': 'ignore',
    'Revenue (2014)': 'Revenue',
    'Nickname': 'ignore',
    'Date founded': 'ignore',
    'Fleet size': 'ignore',
    'Undergraduates': 'ignore',
    'Employees (2014)': 'ignore',
    'AARP Services, Inc President': 'ignore',
    'Colors': 'ignore',
    'Formation': 'ignore',
    'Region': 'ignore',
    'Native name': 'ignore',
    'Operating income': 'Operating Income',
    'Expenses (2014)': 'ignore',
    'Size': 'ignore',
    'Bank rate': 'ignore',
    'Area served': 'ignore',
    'Total equity': 'Total Equity',
    'Junior Vice Commander in Chief': 'ignore',
    'Affiliations': 'ignore',
    'No. of offices': 'ignore',
    'Senior Vice Commander in Chief': 'ignore',
    'Users': 'Users Count',
    'Fate': 'ignore',
    'Format': 'ignore',
    'ISSN': 'ignore',
    'Administered by': 'ignore',
    'Chairman of the Board of Directors': 'ignore',
    'Purpose': 'ignore',
    'National Treasurer': 'ignore',
    'Genre': 'ignore',
    'Tax ID no.': 'ignore',
    'Motto': 'ignore',
    'Expenses (2015)': 'ignore',
    'Type of site': 'ignore',
    'Industry': 'Industry',
    'Political alignment': 'ignore',
    'Membership (2014)': 'ignore',
    'Defunct': 'ignore',
    'Slogan': 'ignore',
    'Country': 'Country',
    'Employees': 'ignore',
    'IATA': 'ignore',
    'Formerly called': 'ignore',
    'Motto in English': 'ignore',
    'Advertising': 'ignore',
    'Company slogan': 'ignore',
    'Launched': 'ignore',
    'President': 'ignore',
    'Alexa rank': 'Alexa Rank',
    'Commenced operations': 'ignore',
    'Merger of': 'ignore',
    'Mission': 'ignore',
    'Lahey Hospital & Medical Center': 'ignore',
    'Former type': 'ignore',
    'Founded at': 'Founded Date',
    'No. of employees': 'ignore',
    'Hospital type': 'ignore',
    'Geography': 'ignore',
}


def copy_coll_original_to_parsed():
    client = MongoClient(host=MONGO_HOST)

    wiki_coll_original = client[MONGO_DB_NAME][WIKI_COLLECTION_ORIGINAL]
    wiki_coll_parsed = client[MONGO_DB_NAME][WIKI_COLLECTION_PARSED]

    # wiki_coll_parsed.remove({})
    pipeline = [{"$match": {}},
                {"$out": WIKI_COLLECTION_PARSED},
                ]
    wiki_coll_original.aggregate(pipeline)


def init_skip():

    client = MongoClient(host=MONGO_HOST)
    wiki_coll = client[MONGO_DB_NAME][WIKI_COLLECTION_PARSED]

    wiki_coll.update({'search_results': {'$ne': []}},
                     {'$set': {
                         'skip_code': 0,
                         'skip_reason': 'to be done',
                     }},
                     multi=True,
                     )

    wiki_coll.update({'search_results': []},
                     {'$set': {
                         'skip_code': 1,
                         'skip_reason': 'empty search results',
                     }},
                     multi=True,
                     )


def skip_no_rightbox():
    client = MongoClient(host=MONGO_HOST)
    wiki_coll = client[MONGO_DB_NAME][WIKI_COLLECTION_PARSED]

    for r in wiki_coll.find({'skip_code': 0}, {'search_results': 1}):
        skip = True
        for idx, s in enumerate(r.get('search_results')):
            if 'table class="infobox vcard"' in s.get('html_content'):
                skip = False
                has_rightbox = True
            else:
                has_rightbox = False
            wiki_coll.update(
                {'_id': r.get('_id')},
                {'$set': {'search_results.%s.has_rightbox' % idx: has_rightbox}})

        if skip:
            wiki_coll.update(
                {'_id': r.get('_id')},
                {'$set': {
                    'skip_code': 2,
                    'skip_reason': 'none of the results contain rightbox',
                }})


def find_websites_rightbox(text):
    results = []

    tmp = []
    html = etree.HTML(text)
    tables = html.xpath('//table[@class="infobox vcard"]')
    if not tables:
        return []

    for table in tables:
        info_text = etree.tostring(table)
        ptn = re.compile('Website</th>\s*<td(.*)</td>', re.DOTALL)
        ptn2 = re.compile('href="([^"]*)"')

        sr = ptn.search(info_text)
        if sr:
            grps = sr.groups()
            if grps:
                tmp = ptn2.findall(grps[0])
        results.extend(tmp)

    return [extract_domain(i) for i in results]


def build_name_string(text):
    clean_t = ptn_clean.sub(' ', text).lower()
    words = [w for w in clean_t.split() if w not in stop_words]
    return ''.join(words)


def match_by_domain_name():

    filter_1 = {'search_results': {'$ne': []}, 'matched': {'$ne': 1}}

    client = MongoClient(host=MONGO_HOST)
    wiki_coll = client[MONGO_DB_NAME][WIKI_COLLECTION_PARSED]
    print '%s to match' % wiki_coll.count(filter_1)

    cnt_ok = 0
    for r in wiki_coll.find(filter_1, {'in_company_name': 1,
                                       'in_domain_name': 1,
                                       'search_results': 1,
                                       }):
        domain_name = r.get('in_domain_name')
        # company name could be None
        input_string = build_name_string(r.get('in_company_name', ''))

        matched = set()

        for idx, s in enumerate(r.get('search_results')):

            # has_rightbox = s.get('has_rightbox') or False
            match_rightbox_domain = s.get('match_rightbox_domain') or False
            match_exact_name = s.get('match_exact_name') or False

            html_content = s.get('html_content')
            wiki_url = s.get('wiki_url')

            # match rightbox domain
            domain_list = find_websites_rightbox(html_content)
            if not match_rightbox_domain and domain_list and domain_list[0] == domain_name:  # noqa; 501
                match_rightbox_domain = True
                wiki_coll.update(
                    {'_id': r.get('_id')},
                    {'$set': {'search_results.%s.match_rightbox_domain' % idx: 1 + domain_list.index(domain_name)}})  # noqa; 501
                if domain_list.index(domain_name) > 0:
                    print("WARN: non first domain name matched: %s in %s" % (domain_name, str(domain_list)))  # noqa; 501
            # elif domain_name in html_content:
            #   print domain_name

            # name exactly match
            if not match_exact_name and input_string == build_name_string(s.get('name')):  # noqa; 501
                match_exact_name = True
                wiki_coll.update(
                    {'_id': r.get('_id')},
                    {'$set': {'search_results.%s.match_exact_name' % idx: 1}})

            if match_rightbox_domain and match_exact_name:
                matched.add(wiki_url)

            # if has_rightbox and match_exact_name:
            #    print '%s. %s' % (r.get('_id'), wiki_url)

        if len(matched) > 1:
            print 'more than 1 one matched. %s. %s' % (r.get('_id'), ' | '.join(matched))  # noqa; 501
        elif len(matched) == 1:
            wiki_coll.update(
                {'_id': r.get('_id')},
                {'$set': {
                    'matched': 1,
                    '_matched_by': 'match_by_domain_name',
                    'wiki_url': matched.pop(),
                }})
            cnt_ok += 1
    print 'matched by domain name:', cnt_ok


def extract_children_text(xpath_node):
    if not xpath_node:
        return '-'

    return ' '.join([t.strip() for t in xpath_node[0].itertext()])


def clean_no_right_box():
    client = MongoClient(host=MONGO_HOST)
    wiki_coll = client[MONGO_DB_NAME][WIKI_COLLECTION_PARSED]

    print wiki_coll.update(
        {'skip_code': 0},
        {'$pull': {'search_results': {'has_rightbox': False}}},
        multi=True,
        )


def extract_domian_from_rightbox(url):
    domain_name = url
    if domain_name.startswith('http://'):
        domain_name = domain_name[7:]
    if domain_name.startswith('https://'):
        domain_name = domain_name[8:]
    return domain_name.rstrip('/')


def extract_website(text):
    results = []

    tmp = []
    html = etree.HTML(text)
    tables = html.xpath('//table[@class="infobox vcard"]')
    if not tables:
        return []

    for table in tables:
        info_text = etree.tostring(table)
        ptn = re.compile('Website</th>\s*<td(.*)</td>', re.DOTALL)
        ptn2 = re.compile('href="([^"]*)"')

        sr = ptn.search(info_text)
        if sr:
            grps = sr.groups()
            if grps:
                tmp = ptn2.findall(grps[0])
        results.extend(tmp)

    return [extract_domian_from_rightbox(i) for i in results]


def parse_rightbox_texts():

    client = MongoClient(host=MONGO_HOST)
    wiki_coll = client[MONGO_DB_NAME][WIKI_COLLECTION_PARSED]

    for r in wiki_coll.find({'skip_code': 0}, {'search_results': 1}):
        for idx, s in enumerate(r.get('search_results')):
            wiki_url = s.get('wiki_url')
            metas = []

            html = etree.HTML(s.get('html_content'))
            tables = html.xpath(table_xpath_first)
            if not tables:
                tables = html.xpath(table_xpath_second)
            for table in tables:

                captions = [c.text for c in table.xpath('./caption')]
                if len(captions) > 1:
                    print 'ERROR. %s caption in %s' % (len(captions), wiki_url)

                meta = [{
                    'key': extract_children_text(item.xpath('./th')).strip(),
                    'value': extract_children_text(item.xpath('./td')).strip(),
                } for item in table.xpath('.//tr')]

                url_from_wiki = extract_website(s.get('html_content'))
                if url_from_wiki:
                    find_urls = url_from_wiki[0]
                    for item in meta:
                        if item['key'] == 'Website':
                            item['value'] = find_urls
                            break
                if not meta:
                    print 'ERROR. no meta in %s' % (wiki_url,)

                metas.append({
                    'captions': captions,
                    'meta': meta,
                })

            wiki_coll.update(
                {'_id': r.get('_id')},
                {'$set': {
                    'search_results.%s.metas' % idx: metas,
                }})


def parse_brief_intro():

    client = MongoClient(host=MONGO_HOST)
    wiki_coll = client[MONGO_DB_NAME][WIKI_COLLECTION_PARSED]

    for r in wiki_coll.find({'skip_code': 0}, {'search_results': 1}):
        for idx, s in enumerate(r.get('search_results')):
            wiki_url = s.get('wiki_url')

            html = etree.HTML(s.get('html_content'))
            res = html.xpath('//*[@id="mw-content-text"]/div/p[1]')
            if not res:
                brief = ''
                print 'ERROR. empty brief in %s' % (wiki_url, )
            else:
                brief = etree.tostring(res[0])

            wiki_coll.update(
                {'_id': r.get('_id')},
                {'$set': {
                    'search_results.%s.Description' % idx: brief,
                }})


def create_clean_db():
    filter_1 = {'matched': 1}

    client = MongoClient(host=MONGO_HOST)
    wiki_coll = client[MONGO_DB_NAME][WIKI_COLLECTION_PARSED]

    print '%s to copy' % wiki_coll.count(filter_1)

    # keys = set()
    cnt_domain = 0
    cnt_more_company = 0

    for r in wiki_coll.find({'matched': 1}):
        wiki_url = r.get('wiki_url')
        domain_is_ok = False
        find_more_company_is_ok = False
        for idx, s in enumerate(r.get('search_results')):
            metas = s.get('metas')
            meta_idx = (s.get('match_rightbox_domain') or s.get('match_criteria') or 1) - 1  # noqa; 501
            meta = metas and metas[meta_idx]
            if wiki_url == s.get('wiki_url') and meta:
                if s.get('match_criteria'):
                    find_more_company_is_ok = True
                try:
                    # keys.update({i['key'] for i in meta['meta'] if i['key'].strip('-') and i['value']})  # noqa; 501
                    info = {meta_key_map.get(i['key'], i['key']): i['value'] for i in meta['meta'] if i['key'].strip('-') and i['value']}  # noqa; 501
                    info['Description'] = s.get('Description')
                    info['Legal Name'] = (meta.get('captions') or [None])[0]
                    info['Name'] = r.get('in_company_name')

                    wiki_coll.update({'_id': r.get('_id')}, {'$set': info})
                    domain_is_ok = True
                except Exception as e:
                    import json
                    print r.get('_id')
                    print e
                    print json.dumps({i['key']: i['value'] for i in meta['meta'] if i['key'].strip('-') and i['value']}, indent=4)  # noqa; 501
    # print u'\n'.join(map(unicode, keys))
        if domain_is_ok:
            cnt_domain += 1
        else:
            print'failed to save companyID: %d' % r.get('_id')
        if find_more_company_is_ok:
            cnt_more_company += 1
    print "copied company:%d, more company:%d" % (cnt_domain, cnt_more_company)  # noqa; 501


@cache
def find_topn_meta_key(host, db_name, collection_name):
    filter_matched = {'matched': 1}
    client = MongoClient(host=host)
    wiki_coll = client[db_name][collection_name]

    print '%s matched' % wiki_coll.count(filter_matched)

    count_key = {}
    ccc = 0
    for r in wiki_coll.find(filter_matched):
        ccc += 1
        print ccc
        for s in r.get('search_results'):
            for i in s.get('metas', []):
                for ele in i.get('meta', {}):
                    if ele['key'] not in count_key:
                        count_key[ele['key']] = 0
                    count_key[ele['key']] += 1

    return sorted(count_key.items(), key=lambda item: item[1], reverse=True)


def find_more_company():

    filter_not_matched = {'matched': {'$ne': 1}, 'skip_code': 0}

    meta_keys = find_topn_meta_key(MONGO_HOST, MONGO_DB_NAME, WIKI_COLLECTION_PARSED)  # noqa; 501

    # for k, v in meta_keys:
    #     print '%s: %s' % (k, v)

    client = MongoClient(host=MONGO_HOST)
    wiki_coll = client[MONGO_DB_NAME][WIKI_COLLECTION_PARSED]

    print '%s not matched' % wiki_coll.count(filter_not_matched)
    cnt_ok = 0
    cnt_error = 1
    for r in wiki_coll.find(filter_not_matched, {'in_company_name': 1,
                                                 'in_domain_name': 1,
                                                 'search_results': 1,
                                                 }):
        is_ok = False
        for idx, s in enumerate(r.get('search_results')):
            match_criteria = s.get('match_criteria') or False
            wiki_url = s.get('wiki_url')
            company_name = r.get('in_company_name')

            html_content = s.get('html_content')
            if s.get('match_exact_name'):
                for meta_idx, i in enumerate(s['metas']):

                    data_key = set()
                    for ele in i['meta']:
                        data_key.add(ele['key'])

                    if (CRITERIA - data_key):
                        pass
                    else:
                        is_ok = True
                        wiki_coll.update(
                            {'_id': r.get('_id')},
                            {'$set': {
                                'matched': 1,
                                '_matched_by': 'find_more_company',
                                'wiki_url': wiki_url,
                                'search_results.%s.match_criteria' % idx: meta_idx + 1,  # noqa; 501
                                }})
        if is_ok:
            cnt_ok += 1
        else:
            cnt_error += 1
    print("found: %d/%d" % (cnt_ok, cnt_error))

if __name__ == '__main__':
    starttime = time.time()
    copy_coll_original_to_parsed()
    print('till cop_coll_original_to_parsed: %6f' % (time.time() - starttime))  # noqa; 501
    init_skip()
    print('till init_skip: %6f' % (time.time() - starttime))
    skip_no_rightbox()
    print('till skip_no_rightbox: %6f' % (time.time() - starttime))
    clean_no_right_box()
    print('till clean_no_right_box: %6f' % (time.time() - starttime))
    parse_brief_intro()
    print('till parse_brief_intro: %6f' % (time.time() - starttime))
    parse_rightbox_texts()
    print('till parse_rightbox_texts: %6f' % (time.time() - starttime))
    match_by_domain_name()
    print('till match_by_domain_name: %6f' % (time.time() - starttime))
    find_more_company()
    print('till find_more_company: %6f' % (time.time() - starttime))
    create_clean_db()
    print('till create_clean_db: %6f' % (time.time() - starttime))

    # copy_coll_original_to_parsed()
    # init_skip()
    # skip_no_rightbox()
    # clean_no_right_box()
    # parse_brief_intro()
    # parse_rightbox_texts()
    # gen_meta_key_list()
    # match_by_domain_name() 
    # find_more_company()
    # create_clean_db()

