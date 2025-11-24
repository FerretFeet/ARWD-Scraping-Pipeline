"""Selector template for Arkleg.state.ar.us/Bills/ViewBills"""


from src.models.SelectorTemplate import SelectorTemplate


class BillListSelector(SelectorTemplate):
    next_page: str

    def __init__(self, url: str):
        super().__init__(
            url=url,
            selectors={
                'chamber': ('div h1'),
                'session': ('option[selected]'),
                'bill_url': ('div.measureTitle b a', 'href'),
                'next_page': ('div.tableSectionFooter div b + a', 'href')
            },
        )


