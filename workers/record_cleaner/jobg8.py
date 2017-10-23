from .base_record_parser import BaseParser


class Parser(BaseParser):
    source_name = 'JB8'
    desc_tag_name = 'Description'

    def build_id(self):
        return self.orig_data.get('SenderReference')
    
    def build_title(self):
        return self.orig_data.get('Position')

    def build_company(self):
        return self.orig_data.get('AdvertiserName')

    def build_industry(self):
        return self.orig_data.get('Classification')

    def build_zipcode(self):
        return self.orig_data.get('PostalCode')

    # <Country ValueID="247">United States</Country>
    # <Location ValueID="15351">New Jersey</Location>
    # <Area ValueID="15625">Newark</Area>
    # <Country ValueID="247">United States</Country>
    # <Location ValueID="15356">Ohio</Location>
    # <Area ValueID="15665">Cleveland</Area>
    def build_state(self):
        return self.orig_data.get('Location')

    def build_city(self):
        return self.orig_data.get('Area')
    
    def build_url(self):
        return self.orig_data.get('ApplicationURL')

    def build_price(self):
        return 1                  # waiting the pm decide the algorithms


