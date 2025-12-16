"""Pipeline function for extracting and parsing web page content using selector templates."""

import time

from requests import Session

from src.data_pipeline.extract import webcrawler
from src.models.selector_template import SelectorTemplate


def extract_parse_content(
    link: str,
    base_url: str,
    session: Session,
    selector_cls: type[SelectorTemplate],
    crawler: type[webcrawler.Crawler],
    delay: float = 1.25,
) -> dict[str, str | list[str]]:
    """Pipeline function for extracting and parsing web page content using selector templates."""
    selector = selector_cls(base_url)
    result = crawler.get_content(selector, link, session)
    time.sleep(delay)
    return result
