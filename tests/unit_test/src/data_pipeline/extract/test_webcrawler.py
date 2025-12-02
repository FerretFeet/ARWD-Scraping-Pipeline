from unittest.mock import MagicMock, patch

import pytest

from src.data_pipeline.extract.webcrawler import Crawler


@pytest.fixture
def crawler():
    return Crawler("https://example.com", strict=True, max_retries=3, retry_backoff=0.1)


def test_get_page_success_on_first_attempt(crawler):
    """Crawler should return HTML immediately when the first request succeeds."""
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.text = "<html>ok</html>"

    with patch.object(crawler.session, "get", return_value=mock_response):
        assert crawler.get_page("page") == "<html>ok</html>"


def test_get_page_retries_then_succeeds(crawler):
    """Crawler should retry on failure, then succeed on a later attempt."""
    mock_fail = MagicMock()
    mock_fail.raise_for_status.side_effect = Exception("boom")

    mock_success = MagicMock()
    mock_success.raise_for_status.return_value = None
    mock_success.text = "<html>final</html>"

    with patch.object(
        crawler.session,
        "get",
        side_effect=[mock_fail, mock_fail, mock_success],
    ) as get_mock, patch("time.sleep") as sleep_mock:
        result = crawler.get_page("page")

    assert result == "<html>final</html>"
    assert get_mock.call_count == 3
    assert sleep_mock.call_count == 2  # sleeps only between failures


def test_get_page_all_retries_fail(crawler):
    """All retries exhausted → final ConnectionError."""
    mock_fail = MagicMock()
    mock_fail.raise_for_status.side_effect = Exception("boom")

    with patch.object(
        crawler.session,
        "get",
        return_value=mock_fail,
    ), patch("time.sleep") as sleep_mock, pytest.raises(ConnectionError):
        crawler.get_page("page")

    # Should sleep (max_retries - 1) times
    assert sleep_mock.call_count == crawler.max_retries - 1


def test_get_page_raises_for_status_triggers_retry(crawler):
    """raise_for_status failure should be treated like a failed fetch."""
    mock_fail = MagicMock()
    mock_fail.raise_for_status.side_effect = Exception("bad-status")

    with patch.object(
        crawler.session,
        "get",
        return_value=mock_fail,
    ), patch("time.sleep"), pytest.raises(ConnectionError):
        crawler.get_page("page")


def test_increment_session_called(crawler):
    """Ensure increment_session is called once per request attempt."""
    crawler.increment_session = MagicMock()

    mock_success = MagicMock()
    mock_success.raise_for_status.return_value = None
    mock_success.text = "<html></html>"

    with patch.object(crawler.session, "get", return_value=mock_success):
        crawler.get_page("page")

    crawler.increment_session.assert_called_once()
