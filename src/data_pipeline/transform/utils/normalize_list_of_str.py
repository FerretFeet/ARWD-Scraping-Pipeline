"""Transformer helper function to normalize a list of strings."""

from typing import List

from src.data_pipeline.transform.utils.normalize_str import normalize_str
from src.utils.logger import logger


def normalize_list_of_str(list_of_str: List[str], strict: bool = False) -> List[str] | None:
    """Normalize a list of strings by stripping excessive whitespace and converting to lowercase."""
    if not list_of_str:
        return None
    if not isinstance(list_of_str, list):
        msg = f"Expected a list, received {type(list_of_str)}"
        logger.warning(msg)
        if strict:
            raise TypeError(msg)
    try:
        result = []
        for s in list_of_str:
            temp = normalize_str(s, strict)
            if temp is not None:
                result.append(temp)
        return result
    except TypeError as e:
        msg = f"could not transform list of str {e}"
        logger.warning(msg)
        if strict:
            raise TypeError(msg)
        return None
