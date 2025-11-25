"""Selector template for Arkleg.state.ar.us/Bills/SearchByRange."""

from src.models.SelectorTemplate import SelectorTemplate


class BillCategorySelector(SelectorTemplate):
    """Selector template for ArkLeg BillCategory Page."""

    def __init__(self, url: str):
        """Initialize the selector template."""
        super().__init__(
            url=url,
            selectors={
                "bill_cat_link": ("div#billTypesListWrapper a", "href"),
            },
        )
