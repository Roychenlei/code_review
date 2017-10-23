from .base_record_parser import BaseParser
from utils.pay_price import jobs2careers_cpa


class Parser(BaseParser):
    source_name = 'J2C_CPA'
    desc_tag_name = 'description'

    def build_id(self):
        return self.orig_data.get('referencenumber')

    def build_industry(self):
        return [self.orig_data.get('industry0') or '']

    def build_postingDate(self):
        return self.orig_data.get('date')

    def build_price(self):
        return jobs2careers_cpa(self.orig_data.get('ecpc'))
