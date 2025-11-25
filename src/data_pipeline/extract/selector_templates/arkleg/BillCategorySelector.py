"""Selector template for Arkleg.state.ar.us/Bills/SearchByRange"""

from src.models.SelectorTemplate import SelectorTemplate


class BillCategorySelector(SelectorTemplate):
    def __init__(self, url: str):
        super().__init__(
            url=url,
            selectors={
                "bill_cat_link": ("div#billTypesListWrapper a", "href"),
            },
        )
