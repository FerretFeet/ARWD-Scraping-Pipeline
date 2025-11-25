"""Transformer helper function to cast str to int."""

from src.utils.logger import logger


def cast_to_int(string: str, *, strict: bool = False) -> int | None:
    """Cast a string to an int or None if it can't be cast to int."""
    if isinstance(string, list):
        if len(string) > 1:
            msg = f"Expected 1 string, got {len(string)} instead"
            logger.warning(msg)
            if strict:
                raise ValueError(msg)
        string = string[0]

    try:
        return int(string)
    except ValueError as e:
        msg = f"Failed to cast {string} to int"
        if strict:
            raise ValueError(msg) from e
        return None
