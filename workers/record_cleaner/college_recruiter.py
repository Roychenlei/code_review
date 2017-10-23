from .base_record_parser import BaseParser
from utils.pay_price import college_recruiter


class Parser(BaseParser):
    source_name = 'CR'
    desc_tag_name = 'body'

    def build_id(self):
        return self.orig_data.get('job_reference')

    def build_industry(self):
        # category may exist in orig data and the value is null
        # the output is a list with at least 1 industry
        return (self.orig_data.get('category') or '').split(',')

    def build_price(self):
        return college_recruiter(self.orig_data.get('cpc'))
