"""
Configuration for automatic selector testing

Inputs:
- selector_class
- fixture_params (located in ./html_fixture_params.py))
- required_keys (list of required successful selector attributes)
"""

from src.data_pipeline.extract.selector_templates.ArStateLegislatorSelector import ArStateLegislatorSelector
from test.fixture_params import LEGISLATOR_FIXTURE_PARAMS

SELECTOR_TESTS = [
    {
        "selector_class": ArStateLegislatorSelector,
        "fixture_params": LEGISLATOR_FIXTURE_PARAMS,
        "required_keys": ["title", "district", "seniority"],
    },

    # Add other selectors here...
]

