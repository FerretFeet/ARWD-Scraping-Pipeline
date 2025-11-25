"""Configuration for automatic selector testing
Used in integration_test/*/test_selector.py
Inputs:
- selector_class
- fixture_params (located in ./html_fixture_params.py))
- required_keys (list of required successful selector attributes)
"""

from src.data_pipeline.extract.selector_templates.arkleg.BillCategorySelector import (
    BillCategorySelector,
)
from src.data_pipeline.extract.selector_templates.arkleg.BillListSelector import (
    BillListSelector,
)
from src.data_pipeline.extract.selector_templates.arkleg.BillSelector import (
    BillSelector,
)
from src.data_pipeline.extract.selector_templates.arkleg.BillVoteSelector import (
    BillVoteSelector,
)
from src.data_pipeline.extract.selector_templates.arkleg.LegislatorListSelector import (
    LegislatorListSelector,
)
from src.data_pipeline.extract.selector_templates.arkleg.LegislatorSelector import (
    LegislatorSelector,
)
from src.data_pipeline.transform.templates.arkleg.LegislatorTransformer import (
    LegislatorTransformer,
)
from tests.fixture_params import (
    BILL_CATEGORY_FIXTURE_PARAMS,
    BILL_FIXTURE_PARAMS,
    BILL_LIST_FIXTURE_PARAMS,
    LEGISLATOR_FIXTURE_PARAMS,
    LEGISLATOR_LIST_PARAMS,
    VOTE_PAGE_FIXTURE_PARAMS,
)

SELECTOR_TESTS = [
    {
        "selector_class": LegislatorSelector,
        "transformer_class": LegislatorTransformer,
        "fixture_params": LEGISLATOR_FIXTURE_PARAMS,
        "required_keys": ["title", "district", "seniority"],
    },
    {
        "selector_class": LegislatorListSelector,
        "fixture_params": LEGISLATOR_LIST_PARAMS,
        "required_keys": ["legislator_link"],
    },
    {
        "selector_class": BillCategorySelector,
        "fixture_params": BILL_CATEGORY_FIXTURE_PARAMS,
        "required_keys": ["bill_cat_link"],
    },
    {
        "selector_class": BillListSelector,
        "fixture_params": BILL_LIST_FIXTURE_PARAMS,
        "required_keys": ["chamber", "session", "bill_url"],
    },
    {
        "selector_class": BillSelector,
        "fixture_params": BILL_FIXTURE_PARAMS,
        "required_keys": [
            "title",
            "bill_no",
            "bill_no_dwnld",
            "orig_chamber",
            "lead_sponsor",
            "lead_sponsor_link",
            "intro_date",
        ],
    },
    {
        "selector_class": BillVoteSelector,
        "fixture_params": VOTE_PAGE_FIXTURE_PARAMS,
        "required_keys": ["title"],
    },
    # Add other selectors here...
]
