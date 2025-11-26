"""Configuration for automatic selector testing
Used in integration_test/*/test_selector.py
Inputs:
- transformer_class
- fixture_params (located in ./html_fixture_params.py)
- required_keys (list of required successful selector attributes)
"""

from src.data_pipeline.transform.templates.arkleg.bill_category_transformer import (
    BillCategoryTransformer,
)
from src.data_pipeline.transform.templates.arkleg.bill_list_transformer import BillListTransformer
from src.data_pipeline.transform.templates.arkleg.bill_transformer import BillTransformer
from src.data_pipeline.transform.templates.arkleg.bill_vote_transformer import BillVoteTransformer
from src.data_pipeline.transform.templates.arkleg.legislator_list_transformer import (
    LegislatorListTransformer,
)
from src.data_pipeline.transform.templates.arkleg.legislator_transformer import (
    LegislatorTransformer,
)
from src.data_pipeline.validate.models.arkleg.bill_category import BillCategoryValidator
from tests.configs.transform_fixture_params import (
    BILL_CAT_TRANSFORM_PATHS,
    BILL_LIST_TRANSFORM_PATHS,
    BILL_TRANSFORM_PATHS,
    BILL_VOTE_TRANSFORM_PATHS,
    LEG_LIST_TRANSFORM_PATHS,
    LEG_TRANSFORM_PATHS,
)

TRANSFORMER_TESTS = [
    {
        "transformer_dict": LegislatorTransformer,
        "fixture_params": LEG_TRANSFORM_PATHS,
    },
    {
        "transformer_dict": LegislatorListTransformer,
        "fixture_params": LEG_LIST_TRANSFORM_PATHS,
    },
    {
        "transformer_dict": BillCategoryTransformer,
        "fixture_params": BILL_CAT_TRANSFORM_PATHS,
        "validator_cls": BillCategoryValidator,
    },
    {
        "transformer_dict": BillListTransformer,
        "fixture_params": BILL_LIST_TRANSFORM_PATHS,
    },
    {
        "transformer_dict": BillTransformer,
        "fixture_params": BILL_TRANSFORM_PATHS,
    },
    {
        "transformer_dict": BillVoteTransformer,
        "fixture_params": BILL_VOTE_TRANSFORM_PATHS,
    },
]
