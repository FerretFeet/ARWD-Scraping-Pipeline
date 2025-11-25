import re

from bs4 import BeautifulSoup

from src.models.SelectorTemplate import SelectorTemplate
from src.utils.logger import logger


class LegislatorSelector(SelectorTemplate):

    def __init__(self, url: str):
        super().__init__(
            url=url,
            selectors={
                'title': ('div h1'),
                'phone': _LegislatorParsers.parse_phone,
                'email': _LegislatorParsers.parse_email,
                'address': ('h1 + p'),
                'district': _LegislatorParsers.parse_district,
                'seniority': _LegislatorParsers.parse_seniority,
                'public_service': _LegislatorParsers.parse_public_service,
                'committees': ('div#meetingBodyWrapper a'),  # NEED TO SCRAPE COMMITTEES TOO
                'committee_links': ('div#meetingBodyWrapper a', 'href'),  # NEED TO SCRAPE COMMITTEES TOO
            }
        )

class _LegislatorParsers:
    @staticmethod
    def _parse_table_val(soup: BeautifulSoup, label_str: str):
        label = re.compile(rf"^{label_str}\s*")
        result = []
        table = soup.find('div', attrs={'id': 'tableDataWrapper'})
        if not table:
            logger.warning('table not found')
            return None
        label_tags = table.select('div.row div.d-lg-block b')
        label_tag = ''
        for tag in label_tags:
            if label.match(tag.get_text().strip()):
                label_tag = tag
                break

        if not label_tag or label_tag == '':
            logger.warning('label tag not found')
            return None
        target_parent = label_tag.parent
        if not target_parent: return None
        target = target_parent.find_next_sibling()
        if not target:
            logger.warning('target not found')
            return None
        result.append(target.get_text().strip())
        return result

    @staticmethod
    def parse_phone(soup: BeautifulSoup):
        return _LegislatorParsers._parse_table_val(soup, 'Phone:')

    @staticmethod
    def parse_email(soup: BeautifulSoup):
        return _LegislatorParsers._parse_table_val(soup, 'Email:')

    @staticmethod
    def parse_district(soup: BeautifulSoup):
        return _LegislatorParsers._parse_table_val(soup, 'District:')

    @staticmethod
    def parse_seniority(soup: BeautifulSoup):
        return _LegislatorParsers._parse_table_val(soup, 'Seniority:')

    @staticmethod
    def parse_public_service(soup: BeautifulSoup):
        return _LegislatorParsers._parse_table_val(soup, 'Public Service:')
