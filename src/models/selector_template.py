"""SelectorTemplate class."""

from collections.abc import Callable
from dataclasses import dataclass

from bs4 import BeautifulSoup

Selector = str | tuple[str, str] | Callable[[BeautifulSoup], list[str] | None]


@dataclass
class SelectorTemplate:
    """SelectorTemplate class for parsing beautiful soup objects."""

    url: str
    selectors: dict[str, Selector]
