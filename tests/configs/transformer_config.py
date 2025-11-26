"""Configuration for automatic selector testing
Used in integration_test/*/test_selector.py
Inputs:
- transformer_class
- fixture_params (located in ./html_fixture_params.py)
- required_keys (list of required successful selector attributes)
"""

from src.data_pipeline.transform.templates.arkleg.legislator_transformer import (
    LegislatorTransformer,
)
from tests.configs.transform_fixture_params import LEG_TRANSFORM_PATHS

TRANSFORMER_TESTS = [
    {
        "transformer_dict": LegislatorTransformer,
        "fixture_params": LEG_TRANSFORM_PATHS,
    },
]
