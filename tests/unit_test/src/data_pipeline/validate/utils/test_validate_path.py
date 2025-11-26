import pytest

from src.data_pipeline.validate.utils.validate_path_str import validate_path_str


def test_validate_path_success():
    """Confirms a path starting with '/' is considered valid."""
    valid_path = "/category/electronics"
    assert validate_path_str(valid_path) == valid_path


def test_validate_path_success_root():
    """Confirms the root path '/' is considered valid."""
    root_path = "/"
    assert validate_path_str(root_path) == root_path


def test_validate_path_missing_leading_slash_fails():
    """Confirms a path without a leading '/' raises a ValueError."""
    invalid_path = "category/electronics"

    with pytest.raises(ValueError, match=f"Value '{invalid_path}' must start with /"):
        validate_path_str(invalid_path)


def test_validate_path_empty_string_fails():
    """Confirms an empty string is treated as invalid (it doesn't start with '/')."""
    empty_path = ""

    with pytest.raises(ValueError, match=f"Value '{empty_path}' must start with /"):
        validate_path_str(empty_path)
