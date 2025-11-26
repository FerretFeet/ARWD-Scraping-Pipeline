"""Integration test start with html, into extract_parse_content, into transformer"""

import pytest

from src.data_pipeline.transform.pipeline_transformer import PipelineTransformer
from src.data_pipeline.validate.pipeline_validator import PipelineValidator
from tests.configs.transformer_config import TRANSFORMER_TESTS

param_list = [
    (_transformer_info, param)
    for _transformer_info in TRANSFORMER_TESTS
    for param in _transformer_info["fixture_params"]
]


@pytest.mark.xfail(raises=KeyError, reason="not all validators yet implemented")
@pytest.mark.parametrize(
    ("transformer_info", "transformer_input_fixture"),
    param_list,
    indirect=["transformer_input_fixture"],
)
def test_extract_to_transform_expected_output(transformer_info, transformer_input_fixture):
    """Generalized tests for all selectors."""
    input_val = transformer_input_fixture["fixture_val"]
    transformer = transformer_info["transformer_dict"]
    validator_cls = transformer_info["validator_cls"]

    pipeline_transformer = PipelineTransformer()
    pipeline_validator = PipelineValidator()

    transformed = pipeline_transformer.transform_content(transformer, input_val)
    validator_result = pipeline_validator.validate(validator_cls, transformed)

    assert validator_result is not None
    assert isinstance(validator_result, dict)
    assert len(validator_result) >= len(transformed) >= len(input_val)
