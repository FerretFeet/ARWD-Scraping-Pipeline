import pytest
from pydantic_core._pydantic_core import ValidationError

from src.data_pipeline.validate.pipeline_validator import PipelineValidator
from tests.configs.validator_config import VALIDATOR_TESTS

__POSSIBLE_ERRORS = [ValidationError]

param_list = [
    (_validator_info, param)
    for _validator_info in VALIDATOR_TESTS
    for param in _validator_info["fixture_params"]
]


@pytest.mark.parametrize(
    ("validator_info", "validator_input_fixture"),
    param_list,
    indirect=["validator_input_fixture"],
)
def test_pipeline_transformer_expected_output(validator_info, validator_input_fixture):
    """Generalized tests for all transformers"""
    input_val = validator_input_fixture["fixture_val"]
    validator_cls = validator_info["validator_cls"]

    pipeline_validator = PipelineValidator()
    expected_error = validator_input_fixture["expected_error"]
    if expected_error:
        with pytest.raises(expected_error):
            pipeline_validator.validate(validator_cls, input_val)
        return
    result = pipeline_validator.validate(validator_cls, input_val)

    assert result is not None
    assert isinstance(
        result,
        dict,
    ), f"Expected pipeline validator to return a dict, got {type(result)}"
