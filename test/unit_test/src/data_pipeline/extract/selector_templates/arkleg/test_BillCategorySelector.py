from unittest.mock import patch

import pytest
from bs4 import BeautifulSoup

from src.data_pipeline.extract import WebCrawler
from src.data_pipeline.extract.selector_templates.arkleg.BillCategorySelector import (
    BillCategorySelector,
)
from src.utils.paths import project_root


@pytest.fixture(scope="session")
def known_bill_categories_soup_fixture() -> BeautifulSoup:
    """Load saved HTML fixture for legislators page."""
    fixture_path = project_root / "test" / "fixtures" / "html" / "bill_cat" / "page.known.html"
    with fixture_path.open(encoding="utf-8") as f:
        html = f.read()
        return BeautifulSoup(html, "html.parser")


class TestArStateLegislatorSelector:
    @patch.object(WebCrawler.Crawler, "get_page")
    def test_known_bill_category_known_return(
        self, mock_get_page, known_bill_categories_soup_fixture
    ):
        selector = BillCategorySelector("/stem/")
        crawler = WebCrawler.Crawler("")
        rel_url = "/return/path"
        mock_get_page.return_value = known_bill_categories_soup_fixture
        result = crawler.get_content(selector, rel_url)

        assert result is not None

        assert len(result["bill_cat_link"]) == 9
