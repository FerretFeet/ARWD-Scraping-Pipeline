from unittest.mock import patch

import pytest

from src.data_pipeline.extract import webcrawler
from src.data_pipeline.extract.fetching_templates.arkleg_fetchers import (
    CommitteeListLinkSelector,
)
from src.data_pipeline.extract.html_parser import HTMLParser
from src.utils.paths import project_root


@pytest.fixture(scope="session")
def known_committee_list_html_fixture() -> str:
    """Load saved HTML fixture for legislators page."""
    fixture_path = (
        project_root / "tests" / "fixtures" / "html" / "committee_list" / "clist.known.html"
    )
    with fixture_path.open(encoding="utf-8") as f:
        return f.read()


class TestCommitteeListFetchSelector:
    @patch.object(webcrawler.Crawler, "get_page")
    def test_known_bill_list_known_return(self, mock_get_page, known_committee_list_html_fixture):
        selector = CommitteeListLinkSelector().selectors
        parser = HTMLParser()
        result = parser.get_content(selector, known_committee_list_html_fixture)

        assert result is not None
        assert result["next_page"] is not None
