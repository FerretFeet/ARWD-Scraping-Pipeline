"""Selector template for Arkleg.state.ar.us/Legislators/List."""

from src.models.SelectorTemplate import SelectorTemplate


class LegislatorListSelector(SelectorTemplate):
    """Selector for Arkleg Legislator List Page."""

    def __init__(self, url: str):
        """Initialize the selector template."""
        super().__init__(
            url=url,
            selectors={
                "legislator_link": (
                    "div#tableDataWrapper div.row.tableRow a:first-child, div#tableDataWrapper "
                    "div.row.tableRowAlt a:first-child",
                    "href",
                ),
            },
        )
