"""Params for auto-download of html fixtures
Each url will be downloaded if not already found locally
and saved into fixtures/{name}.{variant}.html

Inputs
- name: str (rel path from /fixtures/ and file name)
- url: str (url to be downloaded)
- variant: str (filename differentiator)

"""

BILL_CATEGORY_FIXTURE_PARAMS = [
    (
        "bill_cat/page",
        "https://arkleg.state.ar.us/Bills/SearchByRange?ddBienniumSession=2017%2F2017R",
        "v1",
    ),
    (
        "bill_cat/page",
        "https://arkleg.state.ar.us/Bills/SearchByRange?ddBienniumSession=2025%2F2025R",
        "v2",
    ),
    (
        "bill_cat/page",
        "https://arkleg.state.ar.us/Bills/SearchByRange?ddBienniumSession=2003%2FR",
        "v3",
    ),
    (
        "bill_cat/page",
        "https://arkleg.state.ar.us/Bills/SearchByRange?ddBienniumSession=2003%2FR",
        "known",
    ),
]
BILL_FIXTURE_PARAMS = [
    (
        "bill_page/bill",
        "https://www.arkleg.state.ar.us/Bills/Detail?id=HB1001&ddBienniumSession=2025%2F2025R",
        "v1",
    ),
    (
        "bill_page/bill",
        "https://www.arkleg.state.ar.us/Bills/Detail?id=HB1001&ddBienniumSession=2017%2F2017R",
        "v2",
    ),
    (
        "bill_page/bill",
        "https://www.arkleg.state.ar.us/Bills/Detail?id=HB1001&ddBienniumSession=2003%2FR",
        "v3",
    ),
    (
        "bill_page/bill",
        "https://www.arkleg.state.ar.us/Bills/Detail?id=HB1001&ddBienniumSession=2003%2FR",
        "known",
    ),
]

BILL_LIST_FIXTURE_PARAMS = [
    (
        "bill_list_page/bill_list",
        "https://www.arkleg.state.ar.us/Bills/ViewBills?&type=HB&ddBienniumSession=2025%2F2025R",
        "v1",
    ),
    (
        "bill_list_page/bill_list",
        "view-source:https://arkleg.state.ar.us/Bills/ViewBills?start=40&type=HB&ddBienniumSession=2025%2F2025R#SearchResults",
        "v2",
    ),
    (
        "bill_list_page/bill_list",
        "https://www.arkleg.state.ar.us/Bills/ViewBills?&type=HB&ddBienniumSession=2003%2FR",
        "v3",
    ),
    (
        "bill_list_page/bill_list",
        "https://www.arkleg.state.ar.us/Bills/ViewBills?&type=HB&ddBienniumSession=2003%2FR",
        "known",
    ),
]

VOTE_PAGE_FIXTURE_PARAMS = [
    (
        "vote_page/vote",
        "https://arkleg.state.ar.us/Bills/Votes?id=HB1002&rcs=108&chamber=House&ddBienniumSession=2013%2F2013R",
        "v1",
    ),
    (
        "vote_page/vote",
        "https://arkleg.state.ar.us/Bills/Votes?id=HB1002&rcs=254&chamber=Senate&ddBienniumSession=2013%2F2013R",
        "v2",
    ),
    (
        "vote_page/vote",
        "https://arkleg.state.ar.us/Bills/Votes?id=HB1002&rcs=650&chamber=House&ddBienniumSession=2017%2F2017R",
        "v3",
    ),
    (
        "vote_page/vote",
        "https://arkleg.state.ar.us/Bills/Votes?id=HB1001&rcs=10&chamber=Senate&ddBienniumSession=2003%2FR",
        "v4",
    ),
    (
        "vote_page/vote",
        "https://arkleg.state.ar.us/Bills/Votes?id=HB1001&rcs=10&chamber=Senate&ddBienniumSession=2003%2FR",
        "known",
    ),
]

LEGISLATOR_FIXTURE_PARAMS = [
    (
        "legislator/legislator",
        "https://arkleg.state.ar.us/Legislators/Detail?member=J.+Boyd&ddBienniumSession=2025%2F2025R",
        "v1",
    ),
    (
        "legislator/legislator",
        "https://arkleg.state.ar.us/Legislators/Detail?member=Mcelroy&ddBienniumSession=2017%2F2017R",
        "v2",
    ),
    (
        "legislator/legislator",
        "https://arkleg.state.ar.us/Legislators/Detail?member=Bledsoe&ddBienniumSession=2017%2F2017R",
        "v3",
    ),
    (
        "legislator/legislator",
        "https://arkleg.state.ar.us/Legislators/Detail?member=Bledsoe&ddBienniumSession=2017%2F2017R",
        "known",
    ),
]
LEGISLATOR_LIST_PARAMS = [
    (
        "legislator/list",
        "https://arkleg.state.ar.us/Legislators/List",
        "v1",
    ),
    (
        "legislator/list",
        "https://arkleg.state.ar.us/Legislators/List",
        "known",
    ),
]
