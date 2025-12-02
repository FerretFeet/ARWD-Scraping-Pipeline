import re

from bs4 import BeautifulSoup

# Assuming these imports are already part of the code
from src.models.selector_template import SelectorTemplate
from src.structures.registries import (
    PipelineRegistries,
    PipelineRegistryKeys,
    ProcessorRegistry,  # Assuming you import this
)

# Initialize the registry
registry = ProcessorRegistry()


@registry.register(PipelineRegistryKeys.ARK_LEG_SEEDER, PipelineRegistries.FETCH)
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
    def parse_bill_section(soup: BeautifulSoup) -> list[str] | None:
        """Select bills section links."""
        for a in soup.select("li a:first-child"):
            if a.get_text(strip=True).lower() == "bill":
                return a.get("href")
        return None


@registry.register(PipelineRegistryKeys.BILLS_SECTION, PipelineRegistries.FETCH)
class ArkLegSeederLinkSelectorForBillSection(SelectorTemplate):
    """Selector template for ArkLeg BillCategory Page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "bill_categories": ("main div#bodyContent div.container a:first-child", "href"),
            },
        )


@registry.register(PipelineRegistryKeys.BILL_CATEGORIES, PipelineRegistries.FETCH)
class BillCategoryLinkSelector(SelectorTemplate):
    """Selector template for ArkLeg BillCategory Page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "bill_categories": ("div#billTypesListWrapper a", "href"),
            },
        )


@registry.register(PipelineRegistryKeys.BILL_LIST, PipelineRegistries.FETCH)
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


@registry.register(PipelineRegistryKeys.BILL, PipelineRegistries.FETCH)
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


@registry.register(PipelineRegistryKeys.LEGISLATOR_LIST, PipelineRegistries.FETCH)
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
