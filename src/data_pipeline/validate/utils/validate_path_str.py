"""Utility for validating path strings with pydantic."""

from typing import Annotated

from pydantic import AfterValidator


def validate_path_str(v: str) -> str:
    """
    Validate a path string.

    Ensure starts with '/'
    """
    if not v.startswith("/"):
        msg = f"Value '{v}' must start with /"
        raise ValueError(msg)
    return v


PathString = Annotated[str, AfterValidator(validate_path_str)]
