"""Configuration for automatic selector testing
Used in integration_test/*/test_selector.py
Inputs:
- transformer_class
- fixture_params (located in ./html_fixture_params.py)
- required_keys (list of required successful selector attributes)
"""

from src.data_pipeline.validate.models.arkleg.bill_category import BillCategoryValidator
from src.data_pipeline.validate.models.arkleg.bill_list import BillListValidator
from tests.configs.validator_fixture_params import (
    BILL_CAT_VALIDATE_PATHS,
    BILL_LIST_VALIDATE_PATHS,
)

VALIDATOR_TESTS = [
    {
        "fixture_params": BILL_CAT_VALIDATE_PATHS,
        "validator_cls": BillCategoryValidator,
    },
    {
        "fixture_params": BILL_LIST_VALIDATE_PATHS,
        "validator_cls": BillListValidator,
    },
]
