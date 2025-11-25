from dataclasses import dataclass
from typing import Dict, Callable, Tuple, List, Optional, Union

from bs4 import BeautifulSoup

TransformerTemplate = Dict[str, Callable[[str | List[str], bool], Dict[str, str] | str | int]]