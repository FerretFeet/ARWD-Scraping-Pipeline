import pytest

from src.data_pipeline.validate.utils.validate_url_str import validate_base_url


def test_validate_base_url_success():
    """Confirms valid input returns the input string."""
    input_str = "https://api.test.com"
    assert validate_base_url(input_str) == input_str


def test_validate_base_url_no_http_fails():
    """Confirms no http/https raises ValueError directly."""
    with pytest.raises(ValueError, match="base_url must start with http"):
        validate_base_url("ftp://api.test.com")


def test_validate_base_url_trailing_slash_fails():
    """Confirms trailing slash raises ValueError directly."""
    with pytest.raises(ValueError, match="base_url must not end with /"):
        validate_base_url("https://api.test.com/")
