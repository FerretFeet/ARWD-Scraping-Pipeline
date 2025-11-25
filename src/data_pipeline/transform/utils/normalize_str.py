import re

from src.utils.logger import logger


def normalize_str(string: str, strict:bool = False) -> str | None:
    """
    Normalize white space to single-spaces, no leading or trailing
    All chars become lowercase.
    """
    if string is None: return None
    if isinstance(string, list):
        if len(string) > 1:
            msg = f"Expected 1 string, got {len(string)} instead"
            logger.warning(msg)
            if strict:
                raise ValueError(msg)
        string = string[0]
    try:
        string = normalize_space(string)
        # Convert to lowercase
        return string.lower()
    except Exception as e:
        msg = f'Unable to normalize string: {string}'
        logger.warning(msg)
        if strict:
            raise ValueError(msg)
        return None

def normalize_space(s: str) -> str:
    return re.sub(r'\s+', ' ', s.strip())