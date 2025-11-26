import pytest

from src.data_pipeline.transform.pipeline_transformer import PipelineTransformer
from tests.configs.transformer_config import TRANSFORMER_TESTS

param_list = [
    (_transformer_info, param)
    for _transformer_info in TRANSFORMER_TESTS
    for param in _transformer_info["fixture_params"]
]


@pytest.mark.parametrize(
    ("transformer_info", "transformer_input_fixture"),
    param_list,
    indirect=["transformer_input_fixture"],
)
def test_pipeline_transformer_expected_output(transformer_info, transformer_input_fixture):
    """Generalized tests for all transformers"""
    input_val = transformer_input_fixture["fixture_val"]
    transformer = transformer_info["transformer_dict"]

    pipeline_transformer = PipelineTransformer()
    expected_error = transformer_input_fixture["expected_error"]
    if expected_error is not None:
        with pytest.raises(expected_error):
            pipeline_transformer.transform_content(transformer, input_val)
        return
    result = pipeline_transformer.transform_content(transformer, input_val)

    assert (
        result is not None
    ), f"Transformer {type(transformer)} returned None from input {input_val}"
    assert len(result) >= len(input_val)
    for val in result.values():
        assert isinstance(val, (str, list, int, float)) or val is None, (
            f"Transformer"
            f" {type(transformer)} returned unexpected val type, "
            f"{type(val)}, from input {input_val}"
        )
