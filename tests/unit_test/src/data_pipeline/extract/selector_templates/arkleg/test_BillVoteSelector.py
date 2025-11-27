from unittest.mock import patch

import pytest
from bs4 import BeautifulSoup

from src.data_pipeline.extract import webcrawler
from src.data_pipeline.extract.selector_templates.arkleg.bill_vote_selector import (
    BillVoteSelector,
)
from src.utils.paths import project_root

KNOWN_YEA = 96
KNOWN_NAY = 0
KNOWN_NON = 4
KNOWN_EXCUSED = 0
KNOWN_PRESENT = 0


@pytest.fixture(scope="session")
def known_bill_vote_soup_fixture() -> BeautifulSoup:
    """Load saved HTML fixture for legislators page."""
    fixture_path = project_root / "tests" / "fixtures" / "html" / "vote_page" / "vote.known.html"
    with fixture_path.open(encoding="utf-8") as f:
        html = f.read()
        return BeautifulSoup(html, "html.parser")


class TestArStateLegislatorSelector:
    @patch.object(webcrawler.Crawler, "get_page")
    def test_known_bill_vote_known_return(self, mock_get_page, known_bill_vote_soup_fixture):
        selector = BillVoteSelector("/stem/")
        crawler = webcrawler.Crawler("")
        rel_url = "/return/path"
        mock_get_page.return_value = known_bill_vote_soup_fixture
        result = crawler.get_content(selector, rel_url)

        assert result is not None

        assert result["title"] == ["House Vote - Tuesday, February 5, 2013 1:43:39 PM"]
        combined = [
            v
            for v in (
                (result.get("yea_voters") or [])
                + (result.get("non_voting_voters") or [])
                + (result.get("excused_voters") or [])
                + (result.get("present_voters") or [])
            )
            if v is not None
        ]
        for name, link in combined:
            if name is not None:
                assert link is not None, f"Got None link to match voter name {name}"
            if link is not None:
                assert name is not None, f"Got None name to match voter link {link}"
