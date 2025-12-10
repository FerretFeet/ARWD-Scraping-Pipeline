import re

from bs4 import BeautifulSoup

# Assuming these imports are already part of the code
from src.models.selector_template import SelectorTemplate
from src.structures.registries import (
    ProcessorRegistry,  # Assuming you import this
)

# Initialize the registry
registry = ProcessorRegistry()


class ArkLegSeederLinkSelector(SelectorTemplate):
    """Selector template for ArkLeg BillCategory Page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "legislator_list": ("div.container ul li div.container a:first-child", "href"),
                "bill_section": self.parse_bill_section,
            },
        )

    @staticmethod
    def parse_section(soup: BeautifulSoup, matchstr: str) -> list[str] | None:
        """Select bills section links."""
        for a in soup.select("li a:first-child"):
            if a.get_text(strip=True).lower() == "bill":
                return a.get("href")
        return None
    @staticmethod
    def parse_bill_section(soup: BeautifulSoup) -> list[str] | None:
        """Select bills section links."""
        return ArkLegSeederLinkSelector.parse_section(soup, "bill")
    @staticmethod
    def parse_committee_section(soup: BeautifulSoup) -> list[str] | None:
        """Select bills section links."""
        return ArkLegSeederLinkSelector.parse_section(soup, "committees")



class BillSectionLinkSelector(SelectorTemplate):
    """Selector template for ArkLeg BillCategory Page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "bill_categories": ("main div#bodyContent div.container a:first-child", "href"),
            },
        )


class BillCategoryLinkSelector(SelectorTemplate):
    """Selector template for ArkLeg BillCategory Page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "bill_categories": ("div#billTypesListWrapper a", "href"),
            },
        )


class BillListLinkSelector(SelectorTemplate):
    """Selector for Arkleg BillList Page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "bill": ("div.measureTitle b a", "href"),
                "bill_list": ("div.tableSectionFooter div b + a", "href"),
            },
        )


class BillLinkSelector(SelectorTemplate):
    """Selector for Arkleg bill page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "bill_vote": self.parse_vote_links,
            },
        )

    @staticmethod
    def parse_vote_links(soup: BeautifulSoup) -> list[str] | None:
        """Select vote page links."""
        return soup.find_all("a", string=re.compile(r"vote", re.IGNORECASE))  # type: ignore  # noqa: PGH003


class LegislatorListLinkSelector(SelectorTemplate):
    """Selector for Arkleg Legislator List Page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "legislator": (
                    "div#tableDataWrapper div.row.tableRow a:first-child, div#tableDataWrapper "
                    "div.row.tableRowAlt a:first-child",
                    "href",
                ),
            },
        )

class CommitteeCategories(SelectorTemplate):
    """Selector template for ArkLeg BillCategory Page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "committee_categories": ("", "href"),
            },
        )
    @staticmethod
    def parse_committee_categories(soup: BeautifulSoup) -> list[str] | None:
        for a in soup.select("div#content div.container a"):
            if a.get_text(strip=True).lower() in ["joint", "senate", "house", "task force"]:
                return a.get("href")
        return None


class CommitteeListLinkSelector(SelectorTemplate):
    """Selector for Arkleg Legislator List Page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "committee": (
                    "div#content div.container div.row a",
                    "href",
                ),
                "next_page": ("div#content div.container > p a", "href"),
            },
        )
