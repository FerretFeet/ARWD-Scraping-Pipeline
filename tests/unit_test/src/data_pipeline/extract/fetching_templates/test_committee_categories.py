from unittest.mock import patch

import pytest

from src.data_pipeline.extract import webcrawler
from src.data_pipeline.extract.fetching_templates.arkleg_fetchers import (
    CommitteeCategories,
)
from src.data_pipeline.extract.html_parser import HTMLParser
from src.utils.paths import project_root


@pytest.fixture(scope="session")
def known_bill_list_html_fixture() -> str:
    """Load saved HTML fixture for legislators page."""
    fixture_path = (
        project_root / "tests" / "fixtures" / "html" / "committee_cats" / "cats.known.html"
    )
    with fixture_path.open(encoding="utf-8") as f:
        html = f.read()
        return html


class TestCommitteeCategoryFetchSelector:
    @patch.object(webcrawler.Crawler, "get_page")
    def test_known_committee_cat_list_known_return(self, mock_get_page, known_bill_list_html_fixture):
        selector = CommitteeCategories().selectors
        parser = HTMLParser()
        result = parser.get_content(selector, known_bill_list_html_fixture)

        assert result is not None
        expected_result = [
            "/Committees/List?type=Joint&ddBienniumSession=2013%2F2013R",
            "/Committees/List?type=Senate&ddBienniumSession=2013%2F2013R",
            "/Committees/List?type=House&ddBienniumSession=2013%2F2013R",
            "/Committees/List?type=Task+Force&ddBienniumSession=2013%2F2013R",
        ]
        assert result["committee_categories"] == expected_result
