"""
Configuration for automatic selector testing
Used in integration_test/*/test_selector.py
Inputs:
- selector_class
- fixture_params (located in ./html_fixture_params.py))
- required_keys (list of required successful selector attributes)
"""
from src.data_pipeline.extract.selector_templates.arkleg.LegislatorListSelector import LegislatorListSelector
from src.data_pipeline.extract.selector_templates.arkleg.LegislatorSelector import LegislatorSelector
from test.fixture_params import LEGISLATOR_FIXTURE_PARAMS, LEGISLATOR_LIST_PARAMS

SELECTOR_TESTS = [
    {
        "selector_class": LegislatorSelector,
        "fixture_params": LEGISLATOR_FIXTURE_PARAMS,
        "required_keys": ["title", "district", "seniority"],
    },
    {
        "selector_class": LegislatorListSelector,
        "fixture_params": LEGISLATOR_LIST_PARAMS,
        "required_keys": ["legislator_link"],
    },


    # Add other selectors here...
]

