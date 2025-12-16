"""Selector template for arkleg.state.ar.us/Legislators/Detail?."""

import html
import re

from bs4 import BeautifulSoup

from src.data_pipeline.transform.utils.cast_to_int import cast_to_int
from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.data_pipeline.transform.utils.normalize_str import normalize_str
from src.data_pipeline.transform.utils.transform_leg_title import transform_leg_title
from src.data_pipeline.transform.utils.transform_phone import transform_phone
from src.models.selector_template import SelectorTemplate
from src.structures.directed_graph import DirectionalGraph, Node
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
                "district": (_LegislatorParsers.parse_district, normalize_str),
                "seniority": (_LegislatorParsers.parse_seniority, cast_to_int),
                "public_service": (_LegislatorParsers.parse_public_service, normalize_str),
                "committee_ids": (
                    _LegislatorParsers.get_committees_links,
                    empty_transform,
                ),
                "state_committee_ids": (self.committee_ids_lookup, empty_transform),
            },
        )

    def committee_ids_lookup(
        self,
        node: Node,
        state_tree: DirectionalGraph,
        parsed_data: dict,
    ) -> dict[str, list[str]]:
        """Search through state for committee_ids."""
        urls = parsed_data.get("committee_ids")
        if not urls:
            return {"committee_ids": []}
        result = []
        for url in urls:
            found_node = self.get_dynamic_state(
                state_tree,
                {"committee_id": None},
                {"url": html.unescape(url)},
            )
            if found_node:
                result.append(found_node.data.get("committee_id"))
        return {"committee_ids": result}


class _LegislatorParsers:
    @staticmethod
    def get_committees_links(soup: BeautifulSoup) -> list[str]:
        els = soup.select("div#meetingBodyWrapper a")
        result = []
        for el in els:
            el_link = el.get("href")
            result.append(el_link)
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
