from unittest.mock import Mock

from requests import Session

from src.data_pipeline.extract.extract_parse_content import extract_parse_content


class DummySelector:
    """Minimal SelectorTemplate stub"""
    def __init__(self, base_url):
        self.base_url = base_url


class DummyCrawler:
    """Minimal Crawler stub"""
    @staticmethod
    def get_content(site, link, session):
        return {
            "title": "Test Title",
            "data": ["item1", "item2"]
        }


def test_extract_parse_content(monkeypatch):
    # Arrange
    test_link = "/test-page"
    test_base = "https://example.com"
    test_session = Mock(spec=Session)
    delay = 0  # remove sleep for test speed

    # Act
    result = extract_parse_content(
        link=test_link,
        base_url=test_base,
        session=test_session,
        selector_cls=DummySelector,
        Crawler=DummyCrawler,
        delay=delay
    )

    # Assert
    assert isinstance(result, dict)
    assert "title" in result
    assert result["title"] == "Test Title"
    assert result["data"] == ["item1", "item2"]
