from .base_record_parser import BaseParser


class Parser(BaseParser):
    source_name = 'RM '
    desc_tag_name = 'description'

    def build_id(self):
        return self.orig_data.get('referencenumber')

    def build_industry(self):
        # category may exist in orig data and the value is null
        # the output is a list with at least 1 industry
        return (self.orig_data.get('category') or '').split()

    def build_zipcode(self):
        return self.orig_data.get('postalcode')

    def build_postingDate(self):
        return self.orig_data.get('date')

    def build_price(self):
        return 1                  # waiting the pm decide the algorithms
