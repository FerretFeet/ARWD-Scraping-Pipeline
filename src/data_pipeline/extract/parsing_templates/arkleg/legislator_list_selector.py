"""Selector template for Arkleg.state.ar.us/Legislators/List."""
from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.data_pipeline.transform.utils.strip_session_from_string import strip_session_from_link
from src.models.selector_template import SelectorTemplate


class LegislatorListSelector(SelectorTemplate):
    """Selector for Arkleg Legislator List Page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "active_urls": ((
                    "div#tableDataWrapper div.row.tableRow a:first-child, div#tableDataWrapper "
                    "div.row.tableRowAlt a:first-child",
                    "href",
                ), empty_transform),
            },
        )

    def strip_all_session_from_links(self, linklist: list[str], *, strict:bool = False) -> list[str]:
        """Strip all sessions from linklist."""
        return [strip_session_from_link(link, getSession=False) for link in linklist]

