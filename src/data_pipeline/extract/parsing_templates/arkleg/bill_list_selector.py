"""Selector template for Arkleg.state.ar.us/Bills/ViewBills."""

from src.models.selector_template import SelectorTemplate


class BillListSelector(SelectorTemplate):
    """Selector for Arkleg BillList Page."""

    next_page: str

    def __init__(self, url: str) -> None:
        """Initialize the selector template."""
        super().__init__(
            url=url,
            selectors={
                "chamber": ("div h1"),
                "session": ("option[selected]"),
                "bill_url": ("div.measureTitle b a", "href"),
                "next_page": ("div.tableSectionFooter div b + a", "href"),
            },
        )
