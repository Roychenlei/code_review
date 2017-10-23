import time
import dateutil
import datetime

import settings

from utils.dup_detect import (
    build_listing_hash,
)
from utils.text import build_desc_cleaner, collapse_whitespace

desc_clean = build_desc_cleaner(settings.JOB_DESC_MAX_LEN,
                                settings.FAST_MODE,
                                settings.JOB_DESC_ALLOWED_TAGS,
                                settings.JOB_DESC_ALLOWED_ATTRS)


def to_timestamp(d):
    """
    Convert d to milliseconds timestamp if d is str or unicode.
    And milliseconds timestamp comform to that in origin JLM.
    """
    if isinstance(d, (str, unicode)):
        return int(time.mktime(
            (dateutil.parser.parse(d) - datetime.timedelta(0, time.timezone))
            .timetuple()))
    elif isinstance(d, (int, long, float)):
        return d


class BaseParser(object):

    source_name = None
    desc_tag_name = 'description'

    target_attrs = (
        'source',
        'id',
        'title',
        'desc',
        'company',
        'industry',
        'zipcode',
        'city',
        'state',
        'country',
        'price',
        'postingDate',
        'url',
        # 'jobLevel',
        # 'educationDegree',
    )

    required_fields = (
        'id',
        'title',
        'url',
        # 'city',
        # 'state',
    )

    def __init__(self, record):
        self.orig_data = record
        self.error = []

        if not self.source_name:
            raise "source_name field is required for a RecordParser."

    def build_source(self):
        return self.source_name

    def get_desc_raw(self):
        return (self.orig_data.get(self.desc_tag_name) or '')

    def build_desc(self):
        return desc_clean(self.get_desc_raw())

    def build_country(self):
        return 'USA'

    def build_zipcode(self):
        return self.orig_data.get('zip')

    def build_postingDate(self):
        return self.orig_data.get('posted_at')

    def build_value(self, tag):
        meth = getattr(self, 'build_%s' % tag, None)  # slow here
        return meth() if meth else self.orig_data.get(tag)

    def validate_data(self, data):
        for field in self.required_fields:
            if not data[field]:
                self.error.append('missing %s' % field)
                return False
        # validate city and state separately
        if not data['city'] and not data['state']:
            self.error.append('missing city')
            self.error.append('missing state')
            return False
        return True

    def run(self):
        data = {
            'source': '',
            'id': '',
            'title': '',
            'desc': '',
            'company': '',
            'industry': [],
            'zipcode': '',
            'city': '',
            'state': '',
            'country': '',
            'price': '',
            'postingDate': '',
            'url': '',
            'jobLevel': '',
            'educationDegree': '',
        }
        data.update({t: self.build_value(t) for t in self.target_attrs})
        self.validate_data(data)
        if self.error:
            return self.error, data


        # __ listingHash __
        # using 'undefined' to be compatible with the origin js version
        undefined = "undefined"
        if "listingHash" not in data.keys():
            listing_hash = build_listing_hash(
                data.get("title", None) or undefined,
                data.get("company", None) or undefined,
                data.get("city", None) or undefined,
                data.get("state", None) or undefined,
                self.get_desc_raw() or undefined,
            )
            data["listingHash"] = listing_hash


        # lstrip `$' from title company city state
        # and collapse whitespaces
        _title = data['title']
        if _title:
            _title = _title.lstrip('$')
            _title = collapse_whitespace(_title)

        _company = data['company']
        if _company:
            _company = _company.lstrip('$')
            _company = collapse_whitespace(_company)

        _city = data['city']
        if _city:
            _city = _city.lstrip('$')
            _city = collapse_whitespace(_city)

        _state = data['state']
        if _state:
            _state = _state.lstrip('$')
            _state = collapse_whitespace(_state)

        # handling left $ char in description
        _desc = data['desc']
        if _desc:
            _desc = data['desc'].lstrip('$')

        data.update({
            "title": _title,
            "company": _company,
            "city": _city,
            "state": _state,
            'desc': _desc,
        })

        data.update({
            'jobId': data['id'],
            'titleDisplay': data['title'],
            'companyDisplay': data['company'],
        })

        # __ postingDate __
        _postingDate = data['postingDate']
        _postingDate = to_timestamp(_postingDate)
        data['postingDate'] = long(_postingDate * 1000)

        return self.error, data
