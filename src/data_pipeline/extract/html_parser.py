"""Class to parse html.text using beautiful soup selectors."""
from collections.abc import Callable

from bs4 import BeautifulSoup, ResultSet, Tag

from src.utils.logger import logger


class HTMLParser:
    """Parse html.text using beautiful soup selectors."""

    def __init__(self, *, strict: bool = False) -> None:
        """Initialize HTML parser."""
        self.strict = strict

    def safe_get(self, soup: BeautifulSoup, selector: str, attr: str = "text") -> list[str] | None:
        """Safely run CSS selectors and return either text or attribute values."""
        def validate_input(soup: BeautifulSoup, selector: str, attr: str) -> ResultSet[Tag] | None:
            """Validate safe_get function input."""
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

            _selected_elems = soup.select(selector)

            if not _selected_elems:
                msg = f"[safe_get] No matches found for selector '{selector}'"
                logger.warning(msg)
                return None
            return _selected_elems

        selected_elems = validate_input(soup, selector, attr)
        if not selected_elems: return None

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
        self._validate_input(template, html_text)

        content_holder: dict = {}
        soup = BeautifulSoup(html_text, "html.parser")

        if soup:
            for key, val in template.items():
                if callable(val):
                    # If selector is a helper function
                    content_holder[key] = self._handle_callable_parser(soup,
                                                               key, val)
                else:
                    content_holder[key] = self._handle_raw_selector(soup, val, key)


        return content_holder

    def _validate_input(self, template:dict, html_text:str) -> None:
        """Validate input for HTML parser."""
        if not isinstance(template, dict) or not isinstance(html_text, str):
            message = (
                f"Parameter passed of incorrect type:\n"
                f"website: {type(template)}, path: {type(html_text)}"
            )
            logger.error(message)
            raise TypeError(message)

    def _handle_callable_parser(self, soup: BeautifulSoup,
                                key: str, val: Callable) -> None | list | dict | set:
        """Parse html for attribute with a callable function."""
        parsed_val = None
        try:
            data = val(soup)
            if not isinstance(data, (list, dict, set)) and data is not None:
                data = [data]
            parsed_val = data
        except (AttributeError, Exception) as e:
            logger.warning(f"Error running custom parser for '{key}': {e}")
            parsed_val = None
            if self.strict:
                raise
        return parsed_val

    def _handle_raw_selector(self, soup: BeautifulSoup, val: tuple, key: str) -> list[str] | None:
        """Parse soup with a selector and possibly a target attr (default text)."""
        selector = attr = None
        if isinstance(val, tuple):
            if len(val) == 1:
                selector = val[0]
            elif len(val) == 2:  # noqa: PLR2004
                selector, attr = val
            else:
                msg = f"Too many values input to selector tuple: \n {key}:  {val}"
                logger.error(msg)
                raise ValueError(msg)
        else:
            selector = val
        args = [selector] if attr is None else [selector, attr]
        return self.safe_get(soup, *args)
