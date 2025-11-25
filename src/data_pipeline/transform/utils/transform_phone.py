"""Transformer helper function to strip non-numerals from phone numbers."""

import re

from src.utils.logger import logger


def transform_phone(phone: str, strict: bool = False, country_code: int = 1) -> str | None:
    """
    Return a phone number with only numerals.

    Prepends country code to phone number if not present.
    country code defaults to 1.
    """
    country_code = country_code
    if isinstance(phone, list):
        if len(phone) > 1:
            logger.warning(f"Expected one phone in param list, got {len(phone)}")
            if strict:
                raise ValueError(f"Expected one phone in param list, got {len(phone)}")
        phone = phone[0]
    if not phone:
        return None
    cleaned_phone = re.sub(r"\D", "", phone)
    if len(cleaned_phone) == 10:
        cleaned_phone = str(country_code) + cleaned_phone
    elif len(cleaned_phone) < 10 or len(cleaned_phone) > 11:
        logger.warning(f"Invalid length of phone number: {cleaned_phone}")
        if strict:
            raise ValueError(f"Invalid length of phone number: {cleaned_phone}")

    return cleaned_phone
