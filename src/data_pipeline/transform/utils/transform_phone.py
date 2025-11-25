import re

from src.utils.logger import logger


def transform_phone(phone: str, strict: bool = False) -> str | None:
    """
    Return a phone number with only numerals
    Includes hard-coded country code of 1.
    """
    country_code = "1"
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
