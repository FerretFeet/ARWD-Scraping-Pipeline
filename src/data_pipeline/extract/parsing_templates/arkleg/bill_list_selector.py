"""Selector template for Arkleg.state.ar.us/Bills/ViewBills."""

from src.models.selector_template import SelectorTemplate


class BillListSelector(SelectorTemplate):
    """Selector for Arkleg BillList Page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "chamber": ("div h1"),
                "session": ("option[selected]"),
                "bill_url": ("div.measureTitle b a", "href"),
                "next_page": ("div.tableSectionFooter div b + a", "href"),
            },
        )
