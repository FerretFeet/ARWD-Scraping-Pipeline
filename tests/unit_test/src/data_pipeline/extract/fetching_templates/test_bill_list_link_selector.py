from unittest.mock import patch

import pytest

from src.data_pipeline.extract import webcrawler
from src.data_pipeline.extract.fetching_templates.arkleg_fetchers import (
    BillListLinkSelector,
)
from src.data_pipeline.extract.html_parser import HTMLParser
from src.utils.paths import project_root


@pytest.fixture(scope="session")
def known_committee_list_html_fixture() -> str:
    """Load saved HTML fixture for legislators page."""
    fixture_path = (
        project_root / "tests" / "fixtures" / "html" / "bill_list_page" / "bill_list.known.html"
    )
    with fixture_path.open(encoding="utf-8") as f:
        html = f.read()
        return html


class TestBillListLinkSelector:
    @patch.object(webcrawler.Crawler, "get_page")
    def test_known_bill_list_known_return(self, mock_get_page, known_committee_list_html_fixture):
        selector = BillListLinkSelector().selectors
        parser = HTMLParser()
        result = parser.get_content(selector, known_committee_list_html_fixture)

        assert result is not None
        for key in result.keys():
            assert result[key] is not None
