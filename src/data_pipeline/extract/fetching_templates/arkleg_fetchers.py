import re

from bs4 import BeautifulSoup

from src.data_pipeline.transform.utils.normalize_str import normalize_str

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
                "bill_section": self.parse_bill_section,
                "legislator_list": self.parse_legislator_list,
                "committees_cat": self.parse_committee_cats,
            },
        )

    @staticmethod
    def parse_legislator_list(soup: BeautifulSoup) -> list[str] | None:
        """Select legislators list."""
        a = soup.find("a", attrs={"id": "dropdownMenuLegislators"})
        new_a = a.find_next("a")
        return new_a.get("href")
    @staticmethod
    def parse_committee_cats(soup: BeautifulSoup) -> list[str] | None:
        """Select bills section links."""
        a = soup.find("a", attrs={"id": "dropdownMenuCommittees"})
        return a.get("href")
    @staticmethod
    def parse_bill_section(soup: BeautifulSoup) -> list[str] | None:
        """Select bills section links."""
        a = soup.find("a", attrs={"id": "dropdownMenuBills"})
        return a.get("href")



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
                "committee_categories": self.parse_committee_categories,
            },
        )
    @staticmethod
    def parse_committee_categories(soup: BeautifulSoup) -> list[str] | None:
        """Select committee category links."""
        valid_categories = ["joint", "senate", "house", "task force"]
        result = []
        a = soup.select("div#content div.container a")
        for a in soup.select("div#content div.container a"):
            if normalize_str(a.get_text(strip=True)) in valid_categories:
                result.append(a.get("href"))
        return result


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
#############33
###### NEEDE TO CREATE NODES FOR COMMITTEES AND ASSOCIATE THEM WITH DB IDS
