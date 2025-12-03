"""Configuration for automatic selector testing
Used in integration_test/*/test_selector.py
Inputs:
- selector_class
- fixture_params (located in ./html_fixture_params.py)
- required_keys (list of required successful selector attributes)
"""
from tests.configs.html_fixture_params import (
    BILL_CATEGORY_FIXTURE_PARAMS,
    BILL_FIXTURE_PARAMS,
    BILL_LIST_FIXTURE_PARAMS,
    LEGISLATOR_FIXTURE_PARAMS,
    LEGISLATOR_LIST_PARAMS,
    VOTE_PAGE_FIXTURE_PARAMS,
)

SELECTOR_TESTS = [
    {
        "fixture_params": LEGISLATOR_FIXTURE_PARAMS,
        "required_keys": ["title", "district", "seniority"],
    },
    {
        "fixture_params": LEGISLATOR_LIST_PARAMS,
        "required_keys": ["legislator_link"],
    },
    {
        "fixture_params": BILL_CATEGORY_FIXTURE_PARAMS,
        "required_keys": ["bill_cat_link"],
    },
    {
        "fixture_params": BILL_LIST_FIXTURE_PARAMS,
        "required_keys": ["chamber", "session", "bill_url"],
    },
    {
        "fixture_params": BILL_FIXTURE_PARAMS,
        "required_keys": [
            "title",
            "bill_no",
            "bill_no_dwnld",
            "orig_chamber",
            "lead_sponsor",
            "intro_date",
        ],
    },
    {
        "fixture_params": VOTE_PAGE_FIXTURE_PARAMS,
        "required_keys": ["title"],
    },
    # Add other selectors here...
]
