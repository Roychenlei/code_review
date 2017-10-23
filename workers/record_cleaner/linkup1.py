from .base_record_parser import BaseParser


class Parser(BaseParser):
    source_name = 'LU_1'
    desc_tag_name = 'job_description'

    def build_id(self):
        return self.orig_data.get('jobcode')

    def build_title(self):
        return self.orig_data.get('job_title')

    def build_company(self):
        return self.orig_data.get('company_name')

    def build_url(self):
        return self.orig_data.get('job_url')

    def build_postingDate(self):
        return 0        # no posting date

    def build_price(self):
        return 1                  # waiting the pm decide the algorithms