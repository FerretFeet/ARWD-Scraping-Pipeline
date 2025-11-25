from unittest.mock import patch

import pytest
from bs4 import BeautifulSoup

from src.data_pipeline.extract import webcrawler
from src.data_pipeline.extract.selector_templates.arkleg.bill_list_selector import (
    BillListSelector,
)
from src.utils.paths import project_root


@pytest.fixture(scope="session")
def known_bill_list_soup_fixture() -> BeautifulSoup:
    """Load saved HTML fixture for legislators page."""
    fixture_path = (
        project_root / "tests" / "fixtures" / "html" / "bill_list_page" / "bill_list.known.html"
    )
    with fixture_path.open(encoding="utf-8") as f:
        html = f.read()
        return BeautifulSoup(html, "html.parser")


class TestArStateLegislatorSelector:
    @patch.object(webcrawler.Crawler, "get_page")
    def test_known_bill_list_known_return(self, mock_get_page, known_bill_list_soup_fixture):
        selector = BillListSelector("/stem/")
        crawler = webcrawler.Crawler("")
        rel_url = "/return/path"
        mock_get_page.return_value = known_bill_list_soup_fixture
        result = crawler.get_content(selector, rel_url)

        assert result is not None

        assert result["chamber"] == ["House Bills"]
        assert result["session"] == ["2025 - Regular Session, 2025"]
        assert len(result["bill_url"]) == 20  # noqa: PLR2004
        assert result["bill_url"][0] == "/Bills/Detail?id=HB1001&ddBienniumSession=2025%2F2025R"
        assert result["next_page"] == [
            "?start=20&type=HB&ddBienniumSession=2025%2F2025R#SearchResults",
        ]
