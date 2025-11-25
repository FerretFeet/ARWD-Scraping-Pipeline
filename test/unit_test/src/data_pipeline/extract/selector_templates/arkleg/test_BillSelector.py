from unittest.mock import patch

import pytest
from bs4 import BeautifulSoup

from src.data_pipeline.extract import WebCrawler
from src.data_pipeline.extract.selector_templates.arkleg.BillSelector import (
    BillSelector,
)
from src.utils.paths import project_root


@pytest.fixture(scope="session")
def known_bill_soup_fixture() -> BeautifulSoup:
    """Load saved HTML fixture for legislators page."""
    fixture_path = project_root / "test" / "fixtures" / "html" / "bill_page" / "bill.known.html"
    with fixture_path.open(encoding="utf-8") as f:
        html = f.read()
        return BeautifulSoup(html, 'html.parser')


class TestArStateLegislatorSelector:

    @patch.object(WebCrawler.Crawler, 'get_page')
    def test_known_bill_known_return(self, mock_get_page,
                                              known_bill_soup_fixture):
        selector = BillSelector("/stem/")
        crawler = WebCrawler.Crawler("")
        rel_url = '/return/path'
        mock_get_page.return_value = known_bill_soup_fixture
        result = crawler.get_content(selector, rel_url)

        assert result is not None

        assert result['title'] == ['HB1001 - AN ACT FOR THE ARKANSAS HOUSE OF REPRESENTATIVES OF THE NINETY-FIFTH GENERAL ASSEMBLY APPROPRIATION FOR THE 2024-2025 FISCAL YEAR. ']
        assert result['bill_no'] == ['PDFHB1001']
        assert result['bill_no_dwnld'] == ['/Home/FTPDocument?path=%2FBills%2F2025R%2FPublic%2FHB1001.pdf']
        assert result['act_no'] == ['PDF3']
        assert result['act_no_dwnld'] == ['/Acts/FTPDocument?path=%2FACTS%2F2025R%2FPublic%2F&file=3.pdf&ddBienniumSession=2025%2F2025R']
        assert result['orig_chamber'] == ['House']
        assert result['lead_sponsor'] == ['House Management']
        assert result['lead_sponsor_link'] == ['/Committees/Detail?code=963&ddBienniumSession=2025%2F2025R']
        assert result['intro_date'] == ['1/13/2025\xa02:39:05 PM']
        assert result['act_date'] == ['1/27/2025']
        assert result['vote_links'] == ['/Bills/Votes?id=HB1001&rcs=38&chamber=Senate&ddBienniumSession=2025%2F2025R',
                                        '/Bills/Votes?id=HB1001&rcs=29&chamber=House&ddBienniumSession=2025%2F2025R']