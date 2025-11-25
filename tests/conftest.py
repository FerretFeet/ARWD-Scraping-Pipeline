from pathlib import Path
from unittest.mock import patch

import pytest
import requests
from bs4 import BeautifulSoup

from src.data_pipeline.extract.WebCrawler import Crawler


def pytest_addoption(parser):
    parser.addoption(
        "--refresh-html-fixtures",
        action="store_true",
        default=False,
        help="Download missing HTML fixtures if they are not present locally.",
    )


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "html"
# FIXTURE_DIR.mkdir(parents=True, exist_ok=True)


def download_fixture(url: str, path: Path):
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    path.write_text(resp.text, encoding="utf-8")
    return path


@pytest.fixture
def html_selector_fixture(request):
    """Takes params in this shape:

        (name, url, variant)

    and returns:

        {
            "soup": BeautifulSoup,
            "path": Path,
            "filename": str,
            "url": url,
            "variant": variant
        }

    Also patches Crawler.get_page to return soup.
    """
    name, url, variant = request.param
    force_refresh = request.config.getoption("--refresh-html-fixtures")

    # Example: name="bill_page/bill", variant="v1"
    filename = f"{name}.{variant}.html"
    fp = FIXTURE_DIR / filename
    fp.parent.mkdir(parents=True, exist_ok=True)

    # Download if needed
    if force_refresh or not fp.exists():
        download_fixture(url, fp)

    # Load into BeautifulSoup
    html_content = fp.read_text(encoding="utf-8")
    soup = BeautifulSoup(html_content, "html.parser")

    # Patch crawler.get_page for this tests only
    with patch.object(Crawler, "get_page", return_value=soup):
        yield {
            "soup": soup,
            "path": fp,
            "filename": filename,
            "url": url,
            "variant": variant,
        }
