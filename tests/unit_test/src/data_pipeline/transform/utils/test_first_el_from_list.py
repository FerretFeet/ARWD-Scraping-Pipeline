# Capture logger warnings
import logging

import pytest

from src.data_pipeline.transform.utils.first_el_from_list import first_el_from_list
from src.utils.logger import logger

logger.setLevel(logging.WARNING)


class TestFirstElFromList:

    def test_none_input(self):
        assert first_el_from_list(None) is None

    def test_single_item_list(self):
        assert first_el_from_list(["hello"]) == "hello"

    def test_multi_item_list_strict_false(self, caplog):
        caplog.set_level(logging.WARNING)
        result = first_el_from_list(["a", "b", "c"], strict=False)
        assert result == "a"
        # Warning should be logged
        assert any("Expected one item in list" in rec.message for rec in caplog.records)

    def test_multi_item_list_strict_true(self):
        with pytest.raises(ValueError, match="Expected one item in list"):
            first_el_from_list(["a", "b"], strict=True)

    def test_string_input(self):
        assert first_el_from_list("single string") == "single string"

    def test_non_string_list_items(self):
        result = first_el_from_list([42, "text"], strict=False)
        assert result == 42  # noqa: PLR2004
