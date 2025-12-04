"""Selector template for Arkleg.state.ar.us/Bills/SearchByRange."""
from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.models.selector_template import SelectorTemplate


class BillCategorySelector(SelectorTemplate):
    """Selector template for ArkLeg BillCategory Page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "bill_cat_link": (("div#billTypesListWrapper a", "href"), empty_transform),
            },
        )
