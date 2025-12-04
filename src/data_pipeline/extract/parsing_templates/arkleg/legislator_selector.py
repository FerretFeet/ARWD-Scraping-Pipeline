"""Selector template for arkleg.state.ar.us/Legislators/Detail?."""

import re

from bs4 import BeautifulSoup

from src.data_pipeline.transform.utils.cast_to_int import cast_to_int
from src.data_pipeline.transform.utils.normalize_list_of_str_link import normalize_list_of_str_link
from src.data_pipeline.transform.utils.normalize_str import normalize_str
from src.data_pipeline.transform.utils.transform_leg_title import transform_leg_title
from src.data_pipeline.transform.utils.transform_phone import transform_phone
from src.models.selector_template import SelectorTemplate
from src.utils.logger import logger


class LegislatorSelector(SelectorTemplate):
    """Selector template for Arkleg legislator page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "title": ("div h1", transform_leg_title),
                "phone": (_LegislatorParsers.parse_phone, transform_phone),
                "email": (_LegislatorParsers.parse_email, normalize_str),
                "address": ("h1 + p", normalize_str),
                "district": (_LegislatorParsers.parse_district, cast_to_int),
                "seniority": (_LegislatorParsers.parse_seniority, cast_to_int),
                "public_service": (_LegislatorParsers.parse_public_service, normalize_str),
                "committees": (
                    _LegislatorParsers.get_committees_names_links,
                    normalize_list_of_str_link,
                ),
            },
        )


class _LegislatorParsers:
    @staticmethod
    def get_committees_names_links(soup: BeautifulSoup) -> list[tuple[str, str]]:
        els = soup.select("div#meetingBodyWrapper a")
        result = []
        for el in els:
            el_text = el.get_text(strip=True)
            el_link = el.get("href")
            result.append((el_text, el_link))
        return result

    @staticmethod
    def _parse_table_val(soup: BeautifulSoup, label_str: str) -> list[str] | None:
        label = re.compile(rf"^{label_str}\s*")
        result = []
        table = soup.find("div", attrs={"id": "tableDataWrapper"})
        if not table:
            logger.warning("table not found")
            return None
        label_tags = table.select("div.row div.d-lg-block b")
        label_tag = ""
        for tag in label_tags:
            if label.match(tag.get_text().strip()):
                label_tag = tag
                break

        if not label_tag or label_tag == "":
            logger.warning("label tag not found")
            return None
        target_parent = label_tag.parent
        if not target_parent:
            return None
        target = target_parent.find_next_sibling()
        if not target:
            logger.warning("target not found")
            return None
        result.append(target.get_text().strip())
        return result

    @staticmethod
    def parse_phone(soup: BeautifulSoup) -> list[str] | None:
        return _LegislatorParsers._parse_table_val(soup, "Phone:")

    @staticmethod
    def parse_email(soup: BeautifulSoup) -> list[str] | None:
        return _LegislatorParsers._parse_table_val(soup, "Email:")

    @staticmethod
    def parse_district(soup: BeautifulSoup) -> list[str] | None:
        return _LegislatorParsers._parse_table_val(soup, "District:")

    @staticmethod
    def parse_seniority(soup: BeautifulSoup) -> list[str] | None:
        return _LegislatorParsers._parse_table_val(soup, "Seniority:")

    @staticmethod
    def parse_public_service(soup: BeautifulSoup) -> list[str] | None:
        return _LegislatorParsers._parse_table_val(soup, "Public Service:")
