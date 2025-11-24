"""Selector template for arkleg.state.ar.us/Bills/Detail?id=####"""
import re

from bs4 import BeautifulSoup

from src.models.SelectorTemplate import SelectorTemplate


class BillSelector(SelectorTemplate):

    def __init__(self, url: str):
        super().__init__(
            url=url,
            selectors={
                'title': ('div h1'),
                'bill_no': _BillParsers.parse_bill_no,
                'bill_no_dwnld': _BillParsers.parse_bill_no_dwnld,
                'act_no': _BillParsers.parse_act_no,
                'act_no_dwnld': _BillParsers.parse_act_no_dwnld,
                'orig_chamber': _BillParsers.parse_orig_chamber,
                'lead_sponsor': _BillParsers.parse_lead_sponsor,
                'lead_sponsor_link': _BillParsers.parse_lead_sponsor_link,
                'other_primary_sponsor': _BillParsers.parse_other_primary_sponsor,
                'other_primary_sponsor_link': _BillParsers.parse_other_primary_sponsor_link,
                'cosponsors': _BillParsers.parse_cosponsors,
                'cosponsors_link': _BillParsers.parse_cosponsors_link,
                'intro_date': _BillParsers.parse_intro_date,
                'act_date': _BillParsers.parse_act_date,
                'vote_links': _BillParsers.parse_vote_links,
            },
        )
class _BillParsers:

    @staticmethod
    def parse_vote_links(soup: BeautifulSoup):
        vote_links = soup.find_all("a", string=re.compile(r"vote", re.I))
        print('vote links')

        print(vote_links)
        return [link.get("href") for link in vote_links]

    @staticmethod
    def _parse_bill_detail_table(soup: BeautifulSoup, label_text: str, target_attr: str | None = None):
        """Find the div that *contains* the label text"""
        label_el = soup.find("div", string=re.compile(re.escape(label_text), re.I))
        if label_el is None:
            return None

        target_el = label_el.find_next_sibling("div")
        if target_el is None:
            return None
        if target_attr is None:
            return target_el.get_text(strip=True)
        elif target_attr == 'href':
            return target_el.find('a').get('href')
        return target_el.get(target_attr)

    @staticmethod
    def parse_bill_no(soup: BeautifulSoup):
        label = "Bill Number"
        return _BillParsers._parse_bill_detail_table(soup, label)

    @staticmethod
    def parse_bill_no_dwnld(soup: BeautifulSoup):
        label = "Bill Number"
        return _BillParsers._parse_bill_detail_table(soup, label, 'href')


    @staticmethod
    def parse_act_no(soup: BeautifulSoup):
        label = "Act Number"
        return _BillParsers._parse_bill_detail_table(soup, label)

    @staticmethod
    def parse_act_no_dwnld(soup: BeautifulSoup):
        label = "Act Number"
        return _BillParsers._parse_bill_detail_table(soup, label, 'href')

    @staticmethod
    def parse_orig_chamber(soup: BeautifulSoup):
        label = "Originating Chamber"
        return _BillParsers._parse_bill_detail_table(soup, label)

    @staticmethod
    def parse_lead_sponsor(soup: BeautifulSoup):
        label = "Lead Sponsor"
        return _BillParsers._parse_bill_detail_table(soup, label)

    @staticmethod
    def parse_lead_sponsor_link(soup: BeautifulSoup):
        label = "Lead Sponsor"
        return _BillParsers._parse_bill_detail_table(soup, label, 'href')

    @staticmethod
    def parse_other_primary_sponsor(soup: BeautifulSoup):
        label = "Other Primary Sponsor"
        return _BillParsers._parse_bill_detail_table(soup, label)

    @staticmethod
    def parse_other_primary_sponsor_link(soup: BeautifulSoup):
        label = "Other Primary Sponsor"
        return _BillParsers._parse_bill_detail_table(soup, label, 'href')    @staticmethod

    def parse_cosponsors(soup: BeautifulSoup):
        label = "Cosponsor"
        return _BillParsers._parse_bill_detail_table(soup, label)

    @staticmethod
    def parse_cosponsors_link(soup: BeautifulSoup):
        label = "Cosponsor"
        return _BillParsers._parse_bill_detail_table(soup, label, 'href')

    def parse_intro_date(soup: BeautifulSoup):
        label = "Introduction Date"
        return _BillParsers._parse_bill_detail_table(soup, label)

    def parse_act_date(soup: BeautifulSoup):
        label = "Act Date"
        return _BillParsers._parse_bill_detail_table(soup, label)

