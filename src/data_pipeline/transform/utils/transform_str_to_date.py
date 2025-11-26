"""Convert an eligble string to a date object."""

import html
from datetime import datetime
from zoneinfo import ZoneInfo

from src.data_pipeline.transform.utils.normalize_str import normalize_str
from src.utils.logger import logger


def transform_str_to_date(date_str: str, *, strict: bool = False) -> datetime | None:
    """
    Parse various date string formats into a `date` object.

    Handles time parts, non-breaking spaces, and missing values gracefully.
    Raises ValueError if the date string is invalid.
    """
    if not date_str:
        return None

    if isinstance(date_str, list):
        if len(date_str) > 1:  # Expected length
            msg = f"Date_str passed with too many strings. Expected 1, got {len(date_str)}"
            logger.warning(msg)
            if strict:
                raise ValueError(msg)
        date_str = date_str[0]

    date_str = html.unescape(normalize_str(str(date_str)))
    CST = ZoneInfo("America/Chicago")  # noqa: N806

    # Accepted formats
    formats = ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y")
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).replace(tzinfo=CST)
        except (ValueError, TypeError):
            continue  # try again
    # Match failed
    msg = f"Invalid input to parse_date. \n Received: {date_str} \n expected: {formats}"
    logger.warning(msg)
    if strict:
        raise ValueError(msg)
    return None  # Returns in above format for loop if valid
