"""Selector template for Arkleg.state.ar.us/Bills/ViewBills."""

from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.data_pipeline.transform.utils.normalize_str import normalize_str
from src.models.selector_template import SelectorTemplate


class BillListSelector(SelectorTemplate):
    """Selector for Arkleg BillList Page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "chamber": (
                    ("div h1"),
                    lambda chamber, *, strict: (
                        normalize_str(chamber, strict=strict, remove_substr="bills")
                    ),
                ),
                "session": (("option[selected]"), empty_transform),
                "bill_url": (("div.measureTitle b a", "href"), empty_transform),
                "next_page": (("div.tableSectionFooter div b + a", "href"), empty_transform),
            },
        )
