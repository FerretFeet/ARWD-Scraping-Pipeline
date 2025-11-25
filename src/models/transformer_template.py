"""Transformer template."""

from collections.abc import Callable
from typing import Any

TransformerTemplate = dict[str, Callable[..., Any]]
