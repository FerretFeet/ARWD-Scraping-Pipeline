"""Transformer helper function to normalize strings."""

import re

from src.utils.logger import logger


def normalize_str(
    string: str,
    *,
    strict: bool = False,
    remove_substr: str | None = None,
) -> str | None:
    """Normalize white space to single-space, no leading or trailing; All chars become lowercase."""
    if string is None:
        return None
    if isinstance(string, list):
        if len(string) > 1:
            msg = f"Expected 1 string, got {len(string)} instead"
            logger.warning(msg)
            if strict:
                raise TypeError(msg)
        string = string[0]
    if remove_substr is not None:
        string = string.replace(remove_substr, "")
    try:
        string = normalize_space(string)
        # Convert to lowercase

        return string.lower()
    except (TypeError, AttributeError) as e:
        msg = f"Unable to normalize string {string} \n {e}"
        logger.warning(msg)
        if strict:
            raise TypeError(msg) from e
        return None


def normalize_space(s: str) -> str:
    """Normalize white space to single-spaces, no leading or trailing."""
    return re.sub(r"\s+", " ", s.strip())
