from unittest.mock import patch

import pytest
from bs4 import BeautifulSoup

from src.data_pipeline.extract import WebCrawler
from src.data_pipeline.extract.selector_templates.arkleg.BillVoteSelector import (
    BillVoteSelector,
)
from src.utils.paths import project_root


@pytest.fixture(scope="session")
def known_bill_vote_soup_fixture() -> BeautifulSoup:
    """Load saved HTML fixture for legislators page."""
    fixture_path = project_root / "test" / "fixtures" / "html" / "vote_page" / "vote.known.html"
    with fixture_path.open(encoding="utf-8") as f:
        html = f.read()
        return BeautifulSoup(html, 'html.parser')


class TestArStateLegislatorSelector:

    @patch.object(WebCrawler.Crawler, 'get_page')
    def test_known_bill_vote_known_return(self, mock_get_page,
                                              known_bill_vote_soup_fixture):
        selector = BillVoteSelector("/stem/")
        crawler = WebCrawler.Crawler("")
        rel_url = '/return/path'
        mock_get_page.return_value = known_bill_vote_soup_fixture
        result = crawler.get_content(selector, rel_url)

        assert result is not None

        assert result['title'] == ["House Vote - Tuesday, February 5, 2013 1:43:39 PM"]
        assert len(result['yea_names']) == len(result['yea_links']) == 96
        assert len(result['nay_names']) == len(result['nay_links']) == 0
        assert len(result['non_voting_names']) == len(result['non_voting_links']) == 4
        assert len(result['present_names']) == len(result['present_links']) == 0
        assert len(result['excused_names']) == len(result['excused_links']) == 0

