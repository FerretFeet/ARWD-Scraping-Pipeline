"""Utility for validating URL strings with pydantic."""

from typing import Annotated

from pydantic import AfterValidator


def validate_base_url(v: str) -> str:
    """
    Validate the base URL string.

    Ensure string starts with http and does not end with a '/'
    """
    if not v.startswith("http"):
        msg = "base_url must start with http"
        raise ValueError(msg)
    if v.endswith("/"):
        msg = "base_url must not end with /"
        raise ValueError(msg)
    return v


BaseHttpUrlString = Annotated[str, AfterValidator(validate_base_url)]
