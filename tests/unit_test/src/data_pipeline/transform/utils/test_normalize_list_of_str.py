import pytest

from src.data_pipeline.transform.utils.normalize_list_of_str import (
    normalize_list_of_str,
)


class TestNormalizeListOfStr:
    def test_normalize_list_of_str_success(self):
        """Test expected usage"""
        list_input = ["Str 1", "STRING TWO", "StRinG    \n  Three", 123]
        expected_result = ["str 1", "string two", "string three"]
        result = normalize_list_of_str(list_input)
        assert result == expected_result

    def test_normalize_list_of_str_failure(self):
        list_input = [lambda: "bad input"]
        with pytest.raises(TypeError):
            normalize_list_of_str(list_input, strict=True)
