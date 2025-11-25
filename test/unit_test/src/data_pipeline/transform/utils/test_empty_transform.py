import pytest

from src.data_pipeline.transform.utils.empty_transform import empty_transform


@pytest.mark.parametrize("input_str", [("TEST INPUT"), ["TEST INPUT", "TEST INPUT"], 12359932])
def test_empty_transform(input_str):
    result = empty_transform(input_str)
    assert result == input_str
