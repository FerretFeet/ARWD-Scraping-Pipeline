"""Fetcher templates for Arkleg."""

import re

from bs4 import BeautifulSoup

from src.models.selector_template import SelectorTemplate
from src.structures.registries import PipelineRegistries, PipelineRegistryKeys, register_processor


@register_processor(PipelineRegistryKeys.ARK_LEG_SEEDER, PipelineRegistries.FETCH)
class ArkLegSeederLinkSelector(SelectorTemplate):
    """Selector template for ArkLeg BillCategory Page."""

    def __init__(self, url: str) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "legislator_list": ("div.container ul li div.container a:first-child", "href"),
                "bill_section": self.parse_bill_section,
            },
        )

    @staticmethod
    def parse_bill_section(soup: BeautifulSoup) -> list[str] | None:
        """Select bills section links."""
        for a in soup.select("li a:first-child"):
            if a.get_text(strip=True).lower() == "bill":
                return a.get("href")
        return None


@register_processor(PipelineRegistryKeys.BILLS_SECTION, PipelineRegistries.FETCH)
class ArkLegSeederLinkSelector(SelectorTemplate):
    """Selector template for ArkLeg BillCategory Page."""

    def __init__(self, url: str) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "bill_categories": ("main div#bodyContent div.container a:first-child", "href"),
            },
        )


@register_processor(PipelineRegistryKeys.BILL_CATEGORIES, PipelineRegistries.FETCH)
class BillCategoryLinkSelector(SelectorTemplate):
    """Selector template for ArkLeg BillCategory Page."""

    def __init__(self, url: str) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "bill_categories": ("div#billTypesListWrapper a", "href"),
            },
        )


@register_processor(PipelineRegistryKeys.BILL_LIST, PipelineRegistries.FETCH)
class BillListLinkSelector(SelectorTemplate):
    """Selector for Arkleg BillList Page."""

    def __init__(self, url: str) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "bill": ("div.measureTitle b a", "href"),
                "bill_list": ("div.tableSectionFooter div b + a", "href"),
            },
        )


@register_processor(PipelineRegistryKeys.BILL, PipelineRegistries.FETCH)
class BillLinkSelector(SelectorTemplate):
    """Selector for Arkleg bill page."""

    def __init__(self, url: str) -> None:
        """Initialize the selector template."""
        super().__init__(
            url=url,
            selectors={
                "bill_vote": self.parse_vote_links,
            },
        )

    @staticmethod
    def parse_vote_links(soup: BeautifulSoup) -> list[str] | None:
        """Select vote page links."""
        return soup.find_all("a", string=re.compile(r"vote", re.IGNORECASE))  # type: ignore  # noqa: PGH003


@register_processor(PipelineRegistryKeys.LEGISLATOR_LIST, PipelineRegistries.FETCH)
class LegislatorListLinkSelector(SelectorTemplate):
    """Selector for Arkleg Legislator List Page."""

    def __init__(self, url: str) -> None:
        """Initialize the selector template."""
        super().__init__(
            url=url,
            selectors={
                "legislator": (
                    "div#tableDataWrapper div.row.tableRow a:first-child, div#tableDataWrapper "
                    "div.row.tableRowAlt a:first-child",
                    "href",
                ),
            },
        )
