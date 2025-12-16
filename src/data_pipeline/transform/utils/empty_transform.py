"""Transformer helper function to return unchanged value."""

from typing import TypeVar

T = TypeVar("T")


def empty_transform(arg: T, *, strict: bool = False) -> T:  # noqa: UP047
    """Do nothing, return value unchanged."""
    return arg
