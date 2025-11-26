"""Transform function to return the first item of a list of strings."""

from src.utils.logger import logger


def first_el_from_list(input_list: list[str] | str | None, *, strict: bool = False) -> str | None:
    """Take in a list and return first str from list. If str input return str."""
    if not input_list:
        return None
    val = input_list
    if isinstance(input_list, list):
        if len(input_list) > 1:
            msg = f"Expected one item in list to extract str, got {len(input_list)}"
            logger.warning(msg)
            if strict:
                raise ValueError(msg)
        val = input_list[0]
    return val
