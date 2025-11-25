"""Transformer helper function to strip non-numerals from phone numbers."""

import re

from src.utils.logger import logger


def transform_phone(phone: str, *, strict: bool = False) -> str | None:
    """
    Return a phone number with only numerals.

    Prepends country code to phone number if not present.
    country code defaults to 1.
    """
    country_code = 1
    if isinstance(phone, list):
        if len(phone) > 1:
            msg = f"Expected one phone in param list, got {len(phone)}"
            logger.warning(msg)
            if strict:
                raise ValueError(msg)
        phone = phone[0]
    if not phone:
        return None
    cleaned_phone = re.sub(r"\D", "", phone)
    if len(cleaned_phone) == 10:  # noqa: PLR2004
        cleaned_phone = str(country_code) + cleaned_phone
    elif len(cleaned_phone) < 10 or len(cleaned_phone) > 11:  # noqa: PLR2004
        msg = f"Invalid length of phone number: {cleaned_phone}"
        logger.warning(msg)
        if strict:
            raise ValueError(msg)

    return cleaned_phone
