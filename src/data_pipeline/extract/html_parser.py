"""Class to parse html.text using beautiful soup selectors."""
from bs4 import BeautifulSoup

from src.utils.logger import logger


class HTMLParser:
    """Parse html.text using beautiful soup selectors."""

    def __init__(self, *, strict: bool = False) -> None:
        """Initialize HTML parser."""
        self.strict = strict

    def safe_get(self, soup: BeautifulSoup, selector: str, attr: str = "text") -> list[str] | None:
        """Safely run CSS selectors and return either text or attribute values."""
        if (
            not isinstance(soup, BeautifulSoup)
            or not isinstance(selector, str)
            or not isinstance(attr, str)
        ):
            message = (
                f"Parameter passed of incorrect type: \n"
                f"soup (bs4): {type(soup)}, selector (str): {type(selector)}, "
                f"attr (str): {type(attr)}"
            )
            logger.error(message)
            raise TypeError(message)

        selected_elems = soup.select(selector)

        if not selected_elems:
            msg = f"[safe_get] No matches found for selector '{selector}'"
            logger.warning(msg)
            return None

        values: list[str] = []

        for elem in selected_elems:
            if attr == "text":
                values.append(elem.text)
            elif elem.has_attr(attr):
                values.append(elem.get(attr))
            else:
                message = f'Element "{elem}" does not have attribute "{attr}"'
                logger.warning(message)
                if self.strict:
                    raise AttributeError(message)
                continue
        return values if values else None

    def get_content(
        self,
        template: dict,
        html_text: str,
    ) -> dict[str, str | list[str] | None | dict[str, str | list[str] | None]]:
        """
        Parse HTML using beautiful soup selectors.

        The 'template.selectors' dict can contain:
        - key: (selector, attr, label) -> For simple, declarative scraping
        - key: callable_function(soup)      -> For complex, imperative scraping
        """
        logger.info("Html parser start")

        if not isinstance(template, dict) or not isinstance(html_text, str):
            message = (
                f"Parameter passed of incorrect type:\n"
                f"website: {type(template)}, path: {type(html_text)}"
            )
            logger.error(message)
            raise TypeError(message)
        content_holder: dict = {}
        soup = BeautifulSoup(html_text, "html.parser")

        if soup:
            for key, val in template.items():
                if callable(val):
                    # If selector is a helper function
                    try:
                        data = val(soup)
                        if not isinstance(data, (list, dict, set)) and data is not None:
                            data = [data]
                        content_holder[key] = data
                    except (AttributeError, Exception) as e:
                        logger.warning(f"Error running custom parser for '{key}': {e}")
                        content_holder[key] = None
                        if self.strict:
                            raise

                else:
                    selector = attr = None
                    # --- STRATEGY 2: Rule is a simple tuple or str---
                    if isinstance(val, tuple):
                        if len(val) == 1:
                            selector = str(val[0])
                        elif len(val) == 2:  # noqa: PLR2004
                            selector, attr = val
                        else:
                            msg = f"Too many values input to selector tuple: \n {key}:  {val}"
                            logger.error(msg)
                            raise ValueError(msg)
                    else:
                        selector = val
                    args = [selector] if attr is None else [selector, attr]
                    content_holder[key] = self.safe_get(soup, *args)

        logger.info("\nCrawler executed and retrieved content")
        return content_holder
