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
                "other_primary_sponsor": _BillParsers.parse_other_primary_sponsor,
                "cosponsors": _BillParsers.parse_cosponsors,
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
    def _parse_bill_detail_table(  # noqa: D417
        soup: BeautifulSoup,
        label_text: str,
        target_attr: str,
        *,
        nested_tag: str | None = None,
        additional_attrs: list[str] | None = [None],  # noqa: B006
    ) -> list[str] | str | None | tuple:
        """
        Find the corrosponding element.attr for the tabel label.

        Args:
         - soup: Beautiful Soup object
         - label_text: text label for table
         - target_attr: attribute to return. 'text' or None for text
         - nested_tag: If target el is buried in label_text element's sibling, drill this
         - additional_attrs: List of additional attrs to return in a Tuple

        """
        label_el = soup.find(name="div", string=re.compile(re.escape(label_text), re.IGNORECASE))  # type: ignore  # noqa: PGH003
        if label_el is None:
            return None
        target_el = label_el.find_next_sibling("div")
        if target_el is None:
            return None
        if nested_tag is not None:
            target_el = target_el.find_all(nested_tag)
        # Element successfully selected, extract data
        result = []
        for el in target_el:
            el_attrs = {}
            for attr in [*additional_attrs, target_attr]:
                if attr is None:
                    continue
                if attr == "text":
                    text = el.get_text(strip=True)
                    el_attrs[attr] = text
                else:
                    el_attrs[attr] = el.get(attr)
            result.append(el_attrs)

        attr_order = ["text"] + [
            a for a in [*additional_attrs, target_attr] if a != "text" and a is not None
        ]

        if all(len(result) == 1 for result in result):
            # Flatten dict if only 1 key
            result = [next(iter(x.values())) for x in result]
        else:
            result = [tuple(x[attr] for attr in attr_order) for x in result]

        if len(result) == 1:
            # return list entry if only item in result.
            result = result[0]

        return result

    @staticmethod
    def parse_bill_no(soup: BeautifulSoup) -> str | None:
        label = "Bill Number"
        result = _BillParsers._parse_bill_detail_table(soup, label, "text")
        return result[2]

    @staticmethod
    def parse_bill_no_dwnld(soup: BeautifulSoup) -> str | None:
        label = "Bill Number"
        return _BillParsers._parse_bill_detail_table(soup, label, "href", nested_tag="a")

    @staticmethod
    def parse_act_no(soup: BeautifulSoup) -> str | None:
        label = "Act Number"
        result = _BillParsers._parse_bill_detail_table(soup, label, "text")
        return result[2]

    @staticmethod
    def parse_act_no_dwnld(soup: BeautifulSoup) -> str | None:
        label = "Act Number"
        return _BillParsers._parse_bill_detail_table(soup, label, "href", nested_tag="a")

    @staticmethod
    def parse_orig_chamber(soup: BeautifulSoup) -> str | None:
        label = "Originating Chamber"
        return _BillParsers._parse_bill_detail_table(soup, label, "text")

    @staticmethod
    def parse_lead_sponsor(soup: BeautifulSoup) -> list[tuple[str, str]] | None:
        label = "Lead Sponsor"
        return _BillParsers._parse_bill_detail_table(
            soup,
            label,
            "text",
            nested_tag="a",
            additional_attrs=["href"],
        )

    @staticmethod
    def __parse_lead_sponsor_link(soup: BeautifulSoup) -> list[str] | str | None:
        label = "Lead Sponsor"
        return _BillParsers._parse_bill_detail_table(soup, label, "href", nested_tag="a")

    @staticmethod
    def parse_other_primary_sponsor(soup: BeautifulSoup) -> list[tuple[str, str]] | None:
        label = "Other Primary Sponsor"
        return _BillParsers._parse_bill_detail_table(
            soup,
            label,
            "text",
            nested_tag="a",
            additional_attrs=["href"],
        )

    @staticmethod
    def __parse_other_primary_sponsor_link(soup: BeautifulSoup) -> list[str] | None:
        label = "Other Primary Sponsor"
        return _BillParsers._parse_bill_detail_table(soup, label, "href", nested_tag="a")

    @staticmethod
    def parse_cosponsors(soup: BeautifulSoup) -> list[tuple[str, str]] | None:
        label = "Cosponsors"
        return _BillParsers._parse_bill_detail_table(
            soup,
            label,
            "text",
            nested_tag="a",
            additional_attrs=["href"],
        )

    @staticmethod
    def __parse_cosponsors_link(soup: BeautifulSoup) -> list[str] | None:
        label = "Cosponsors"
        return _BillParsers._parse_bill_detail_table(soup, label, "href", nested_tag="a")

    @staticmethod
    def parse_intro_date(soup: BeautifulSoup) -> str | None:
        label = "Introduction Date"
        return _BillParsers._parse_bill_detail_table(soup, label, "text")

    @staticmethod
    def parse_act_date(soup: BeautifulSoup) -> str | None:
        label = "Act Date"
        return _BillParsers._parse_bill_detail_table(soup, label, "text")
