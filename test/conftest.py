from unittest import mock
from unittest.mock import patch

import pytest
import requests
from pathlib import Path

from bs4 import BeautifulSoup

from src.data_pipeline.extract.WebCrawler import Crawler

BILL_FIXTURE_PARAMS = [
    ("bill_page/bill", "https://www.arkleg.state.ar.us/Bills/Detail?id=HB1001&ddBienniumSession=2025%2F2025R", 'v1'),
    ("bill_page/bill", "https://www.arkleg.state.ar.us/Bills/Detail?id=HB1001&ddBienniumSession=2017%2F2017R", "v2"),
    ("bill_page/bill", "https://www.arkleg.state.ar.us/Bills/Detail?id=HB1001&ddBienniumSession=2003%2FR","v3")
]

BILL_LIST_FIXTURE_PARAMS = [
    ("bill_list_page/bill_list", "https://www.arkleg.state.ar.us/Bills/ViewBills?&type=HB&ddBienniumSession=2025%2F2025R", 'v1'),
    ("bill_list_page/bill_list", "https://www.arkleg.state.ar.us/Bills/ViewBills?&type=HB&ddBienniumSession=2017%2F2017R", "v2"),
    ("bill_list_page/bill_list", "https://www.arkleg.state.ar.us/Bills/ViewBills?&type=HB&ddBienniumSession=2003%2FR","v3")
]

VOTE_PAGE_PARAMS = [
    ("vote_page/vote", "https://www.arkleg.state.ar.us/Bills/Detail?id=HB1001&ddBienniumSession=2025%2F2025R", 'v1'),
    ("vote_page/vote", "https://www.arkleg.state.ar.us/Bills/Detail?id=HB1001&ddBienniumSession=2017%2F2017R", "v2"),
    ("vote_page/vote", "https://www.arkleg.state.ar.us/Bills/Detail?id=HB1001&ddBienniumSession=2003%2FR", "v3"),
    ("vote_page/vote", "https://www.arkleg.state.ar.us/Bills/Detail?id=HB1001&ddBienniumSession=2003%2FR", "v4")

]

LEGISLATOR_FIXTURE_PARAMS = [
    ("legislator/legislator", "https://arkleg.state.ar.us/Legislators/Detail?member=J.+Boyd&ddBienniumSession=2025%2F2025R", 'v1'),
    ("legislator/legislator", "https://arkleg.state.ar.us/Legislators/Detail?member=Mcelroy&ddBienniumSession=2017%2F2017R", "v2"),
    ("legislator/legislator", "https://arkleg.state.ar.us/Legislators/Detail?member=Bledsoe&ddBienniumSession=2017%2F2017R","v3")

]
LEGISLATOR_LIST_PARAMS = [
    ("bill_page/bill", "https://www.arkleg.state.ar.us/Bills/Detail?id=HB1001&ddBienniumSession=2025%2F2025R", 'v1'),
    ("bill_page/bill", "https://www.arkleg.state.ar.us/Bills/Detail?id=HB1001&ddBienniumSession=2017%2F2017R", "v2"),
    ("bill_page/bill", "https://www.arkleg.state.ar.us/Bills/Detail?id=HB1001&ddBienniumSession=2003%2FR","v3")

]



def pytest_addoption(parser):
    parser.addoption(
        "--refresh-html-fixtures",
        action="store_true",
        default=False,
        help="Download missing HTML fixtures if they are not present locally."
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
    """
    Takes params in this shape:

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

    # Patch crawler.get_page for this test only
    with patch.object(Crawler, "get_page", return_value=soup):
        yield {
            "soup": soup,
            "path": fp,
            "filename": filename,
            "url": url,
            "variant": variant,
        }