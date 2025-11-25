from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple, Union

from bs4 import BeautifulSoup

Selector = Union[str, Tuple[str, str], Callable[[BeautifulSoup], Optional[List[str]]]]


@dataclass
class SelectorTemplate:
    url: str
    selectors: Dict[str, Selector]
