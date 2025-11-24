"""Function for extracting and parsing web page content using selector templates."""
import time
from typing import List, Dict

from requests import Session

from src.data_pipeline.extract import WebCrawler
from src.models.SelectorTemplate import SelectorTemplate


def extract_parse_content(
    link: str,
    base_url: str,
    session: Session,
    selector_cls: type[SelectorTemplate],
    Crawler: type[WebCrawler.Crawler],
    delay: float = 1.25,
) -> Dict[str, str | List[str]]:
    """
    General scraper for Arkansas state site pages.
    """
    selector = selector_cls(base_url) # type: ignore
    result = Crawler.get_content(selector, link, session)
    time.sleep(delay)
    return result