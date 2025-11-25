"""Transformer template."""

from typing import Callable, Dict, List

TransformerTemplate = Dict[str, Callable[[str | List[str], bool], Dict[str, str] | str | int]]
