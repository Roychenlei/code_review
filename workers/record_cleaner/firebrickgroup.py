from .base_record_parser import BaseParser


class Parser(BaseParser):
    source_name = 'FBG'
    desc_tag_name = 'description'

    def build_postingDate(self):
        return self.orig_data.get('postingdate')

    def build_price(self):
        return 1                  # waiting the pm decide the algorithms
