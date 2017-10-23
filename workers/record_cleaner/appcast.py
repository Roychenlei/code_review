from .base_record_parser import BaseParser
from utils.pay_price import appcast


class Parser(BaseParser):
    source_name = 'AC'
    desc_tag_name = 'body'

    def build_id(self):
        return self.orig_data.get('job_reference')

    def build_zipcode(self):
        return self.orig_data.get('zip')

    def build_industry(self):
        # category may exist in orig data and the value is null
        # the output is a list with at least 1 industry
        return [self.orig_data.get('category')]

    def build_price(self):
        return appcast(self.orig_data.get('ecpc'))
