"""Selector template for arkleg.state.ar.us/Bills/Detail?id=####."""

import html
import re

from bs4 import BeautifulSoup

from src.models.selector_template import SelectorTemplate


class BillSelector(SelectorTemplate):
    """Selector for Arkleg bill page."""

    def __init__(self, url: str) -> None:
        """Initialize the selector template."""
        super().__init__(
            url=url,
            selectors={
                "title": ("div h1"),
                "bill_no": _BillParsers.parse_bill_no,
                "bill_no_dwnld": _BillParsers.parse_bill_no_dwnld,
                "act_no": _BillParsers.parse_act_no,
                "act_no_dwnld": _BillParsers.parse_act_no_dwnld,
                "orig_chamber": _BillParsers.parse_orig_chamber,
                "lead_sponsor": _BillParsers.parse_lead_sponsor,
                "lead_sponsor_link": _BillParsers.parse_lead_sponsor_link,
                "other_primary_sponsor": _BillParsers.parse_other_primary_sponsor,
                "other_primary_sponsor_link": _BillParsers.parse_other_primary_sponsor_link,
                "cosponsors": _BillParsers.parse_cosponsors,
                "cosponsors_link": _BillParsers.parse_cosponsors_link,
                "intro_date": _BillParsers.parse_intro_date,
                "act_date": _BillParsers.parse_act_date,
                "vote_links": _BillParsers.parse_vote_links,
            },
        )


class _BillParsers:
    @staticmethod
    def parse_vote_links(soup: BeautifulSoup) -> list[str] | None:
        vote_links = soup.find_all("a", string=re.compile(r"vote", re.IGNORECASE))  # type: ignore  # noqa: PGH003

        return [html.unescape(link.get("href")) for link in vote_links]

    @staticmethod
    def _parse_bill_detail_table(
        soup: BeautifulSoup,
        label_text: str,
        target_attr: str | None = None,
        nested_tag: str | None = None,
    ) -> list[str] | str | None:
        """Find the div that *contains* the label text."""
        label_el = soup.find(name="div", string=re.compile(re.escape(label_text), re.IGNORECASE))  # type: ignore  # noqa: PGH003
        if label_el is None:
            return None

        target_el = label_el.find_next_sibling("div")
        if target_el is None:
            return None
        if nested_tag is not None:
            target_el = target_el.find_all(nested_tag)

        if target_attr is None:
            if isinstance(target_el, list):
                result = [el.get_text(strip=True) for el in target_el]
            else:
                result = target_el.get_text(strip=True)
        else:  # noqa: PLR5501
            if isinstance(target_el, list):
                result = [x.get(target_attr) for x in target_el]
            else:
                result = target_el.get(target_attr)
        if isinstance(result, list):
            result = [html.unescape(x) for x in result]
        else:
            result = html.unescape(result)
        return result

    @staticmethod
    def parse_bill_no(soup: BeautifulSoup) -> str | None:
        label = "Bill Number"
        return _BillParsers._parse_bill_detail_table(soup, label)

    @staticmethod
    def parse_bill_no_dwnld(soup: BeautifulSoup) -> str | None:
        label = "Bill Number"
        return _BillParsers._parse_bill_detail_table(soup, label, "href", "a")

    @staticmethod
    def parse_act_no(soup: BeautifulSoup) -> str | None:
        label = "Act Number"
        return _BillParsers._parse_bill_detail_table(soup, label)

    @staticmethod
    def parse_act_no_dwnld(soup: BeautifulSoup) -> str | None:
        label = "Act Number"
        return _BillParsers._parse_bill_detail_table(soup, label, "href", "a")

    @staticmethod
    def parse_orig_chamber(soup: BeautifulSoup) -> str | None:
        label = "Originating Chamber"
        return _BillParsers._parse_bill_detail_table(soup, label)

    @staticmethod
    def parse_lead_sponsor(soup: BeautifulSoup) -> list[str] | str | None:
        label = "Lead Sponsor"
        return _BillParsers._parse_bill_detail_table(soup, label, nested_tag="a")

    @staticmethod
    def parse_lead_sponsor_link(soup: BeautifulSoup) -> list[str] | str | None:
        label = "Lead Sponsor"
        return _BillParsers._parse_bill_detail_table(soup, label, "href", "a")

    @staticmethod
    def parse_other_primary_sponsor(soup: BeautifulSoup) -> list[str] | None:
        label = "Other Primary Sponsor"
        return _BillParsers._parse_bill_detail_table(soup, label, nested_tag="a")

    @staticmethod
    def parse_other_primary_sponsor_link(soup: BeautifulSoup) -> list[str] | None:
        label = "Other Primary Sponsor"
        return _BillParsers._parse_bill_detail_table(soup, label, "href", "a")

    @staticmethod
    def parse_cosponsors(soup: BeautifulSoup) -> list[str] | None:
        label = "Cosponsors"
        return _BillParsers._parse_bill_detail_table(soup, label, nested_tag="a")

    @staticmethod
    def parse_cosponsors_link(soup: BeautifulSoup) -> list[str] | None:
        label = "Cosponsors"
        return _BillParsers._parse_bill_detail_table(soup, label, "href", "a")

    @staticmethod
    def parse_intro_date(soup: BeautifulSoup) -> str | None:
        label = "Introduction Date"
        return _BillParsers._parse_bill_detail_table(soup, label)

    @staticmethod
    def parse_act_date(soup: BeautifulSoup) -> str | None:
        label = "Act Date"
        return _BillParsers._parse_bill_detail_table(soup, label)
