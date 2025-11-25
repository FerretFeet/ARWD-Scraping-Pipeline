from typing import List
from unittest.mock import Mock, patch

import pytest
import requests
from bs4 import BeautifulSoup

from src.data_pipeline.extract.WebCrawler import Crawler
from src.models.SelectorTemplate import SelectorTemplate


# --------------------
# Fixtures
# --------------------
@pytest.fixture
def mock_session():
    return Mock(spec=requests.Session)


# --------------------
# Helper functions
# --------------------

def create_mock_selector_template() -> Mock:
    """Return a mock object resembling a Website instance with static selectors"""
    mock_website = Mock(SelectorTemplate)
    mock_website.url = "http://example.com"
    mock_website.selectors = {
        "titleTag": "h1#title",
        "linkTag": (".link", "href")
    }
    return mock_website


def parse_with_callable(soup: BeautifulSoup) -> List | None:
    """Example callable selector function to use in mocks"""
    temp = Crawler.safe_get(soup, 'a.link')
    if temp:
        return temp


def create_mock_selector_template_callable() -> Mock:
    """Return a mock Website with a callable selector"""
    mock_website = Mock(SelectorTemplate)
    mock_website.name = "Mock Site"
    mock_website.url = "http://example.com"

    mock_website.selectors = {
        "linkTag": parse_with_callable
    }
    return mock_website


def create_mock_soup() -> BeautifulSoup:
    """Return a BeautifulSoup object from static HTML"""
    html_content = """
    <html>
        <body>
            <h1 id="title">Page Title Text</h1>
            <div class="row" aria-rowindex="1">
                <div aria-colindex="1">
                    <a class="link" aria-colindex="1" href="/page1">First Link Text</a>
                </div>
                <div aria-colindex="2">
                    <a class="link" aria-colindex="2" href="/page2">Second Link Text</a>
                </div>
                <div aria-colindex="3"></div>
            </div>
            <div data-value="123">Data Element</div>
        </body>
    </html>
    """
    return BeautifulSoup(html_content, 'html.parser')


# --------------------
# Tests
# --------------------
def test_crawler_class_exists():
    assert isinstance(Crawler, type), "Crawler should be a class"


def test_crawler_has_required_attrs():
    site = "http://www.example.com"
    crawler = Crawler(site)
    assert crawler.site == site


class TestGetPage:
    def test_get_page_success(self, mock_session):
        site = "http://www.example.com"
        url = "http://www.example.com"
        session = mock_session

        response = Mock()
        response.text = "<html><body><h1>Hello World</h1></body></html>"
        response.status_code = 200
        session.get.return_value = response

        crawler = Crawler(site)
        soup = crawler.get_page(session, url)
        assert isinstance(soup, BeautifulSoup), "get_page method should return a BeautifulSoup object"

    def test_get_page_raises_connection_error(self, mock_session):
        site = "http://www.example.com"
        url = "http://www.example.com"
        session = mock_session

        session.get.side_effect = requests.exceptions.ConnectionError

        crawler = Crawler(site)
        with pytest.raises(ConnectionError):
            crawler.get_page(session, url)

    def test_get_page_raises_type_error(self, mock_session):
        crawler = Crawler("http://example.com")

        # Wrong session type
        with pytest.raises(TypeError):
            crawler.get_page("incorrect param", "http://example.com")

        # Wrong URL type
        with pytest.raises(TypeError):
            crawler.get_page(mock_session, 5)

    def test_get_page_raises_other_exceptions(self, mock_session):
        site = "http://www.example.com"
        url = "http://www.example.com"
        session = mock_session

        session.get.side_effect = Exception('something bad')

        crawler = Crawler(site)
        with pytest.raises(Exception):
            crawler.get_page(session, url)

    def test_get_page_raises_http_error(self, mock_session):
        site = "http://www.example.com"
        session = mock_session

        response = Mock()
        response.status_code = 404
        # Important: The mock must raise HTTPError when raise_for_status is called
        response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Error")
        session.get.return_value = response

        crawler = Crawler(site)

        with pytest.raises((ConnectionError, requests.exceptions.HTTPError)):
            crawler.get_page(session, site)


class TestSafeGet:
    soup = create_mock_soup()
    def test_safe_get_defaults_text(self):
        soup = self.soup
        crawler = Crawler("http://www.example.com")
        selector = "a.link"
        result = crawler.safe_get(soup, selector)
        expected_result = ["First Link Text", "Second Link Text"]
        assert result == expected_result

    def test_safe_get_custom_data_attribute(self):
        """Tests retrieving a non-standard attribute like a data attribute."""
        soup = self.soup
        crawler = Crawler("http://www.example.com")
        selector = 'div'
        attribute_name = 'data-value'
        expected_result = ["123"]
        result = crawler.safe_get(soup, selector, attribute_name)
        assert result == expected_result

    def test_safe_get_no_match_returns_none(self):
        soup = self.soup
        crawler = Crawler("http://www.example.com")
        selector = 'div.nonexistent-div'
        result = crawler.safe_get(soup, selector)
        assert result is None

    def test_safe_get_empty_match_returns_empty_str(self):
        soup = self.soup
        selector = "div[aria-colindex='3']"
        result = Crawler.safe_get(soup, selector)
        assert result ==['']

    def test_safe_get_raises_type_error_with_incorrect_params(self):
        soup = self.soup
        selector = "div[aria-colindex='3']"
        with pytest.raises(TypeError):
            Crawler.safe_get(123, selector)
        with pytest.raises(TypeError):
            Crawler.safe_get(soup, 123)

    def test_safe_get_strict_mode_raises_error(self):
        soup = self.soup
        # Use patch to temporarily set strict to True without affecting other tests
        with patch.object(Crawler, 'strict', True):
            # Test missing selector
            with pytest.raises(ValueError, match="returned nothing"):
                Crawler.safe_get(soup, 'div.nonexistent')

            # Test missing attribute
            with pytest.raises(AttributeError, match="does not have attribute"):
                Crawler.safe_get(soup, 'h1', 'nonexistent-attr')



class TestGetContent:
    @patch.object(Crawler, "get_page")
    def test_get_content_success(self, mock_get_page):
        website = create_mock_selector_template()
        path = "test"
        mock_get_page.return_value = create_mock_soup()  # Assume this returns a BS4 object

        result = Crawler.get_content(website, path)

        # This now works because we return a ScrapeResult object
        assert result['rel_url'] == path
        # Depending on how create_real_soup is set up, ensure these assertions match
        assert result['titleTag'] == ["Page Title Text"]

    @patch.object(Crawler, "get_page")
    def test_get_content_callable_selector(self, mock_get_page):
        # content_holder removed from arguments (it was causing the crash)
        website = create_mock_selector_template_callable()
        mock_get_page.return_value = create_mock_soup()
        path = ''

        # Corrected arguments
        result = Crawler.get_content(website, path)
        assert result['linkTag'] == ['First Link Text', 'Second Link Text']

    @patch.object(Crawler, "get_page")
    def test_get_content_short_selector(self, mock_get_page):
        website = create_mock_selector_template()
        # CORRECTED: Added comma to make it a tuple
        website.selectors['titleTag'] = ('h1',)
        mock_get_page.return_value = create_mock_soup()
        path = ''

        result = Crawler.get_content(website, path)
        assert result['titleTag'] == ['Page Title Text']


    @patch.object(Crawler, "get_page")
    def test_get_content_bad_params(self, mock_get_page):
        website = create_mock_selector_template()
        mock_get_page.return_value = create_mock_soup()

        with pytest.raises(TypeError):
            Crawler.get_content(123, 'valid param')
        with pytest.raises(TypeError):
            Crawler.get_content(website, 123)

    @patch.object(Crawler, "get_page")
    def test_get_content_returns_none(self, mock_get_page):
        website = create_mock_selector_template()

        website.selectors['badSelector'] = 'p#NotAMatch'
        website.selectors['badSelectorTuple'] = ('p#NotAMatch',)

        mock_get_page.return_value = create_mock_soup()
        result = Crawler.get_content(website, 'false_path')
        assert result['rel_url'] == 'false_path'
        assert result['badSelector'] is None
        assert result['badSelectorTuple'] is None

    def test_session_management_internal(self):
        """Test that Crawler closes the session if it created it internally."""
        website = create_mock_selector_template()

        # We need to spy on the Session class
        with patch("src.data_pipeline.extract.WebCrawler.Session") as MockSessionClass:
            mock_session_instance = MockSessionClass.return_value
            # Mock get_page to avoid actual network calls
            with patch.object(Crawler, "get_page", return_value=create_mock_soup()):
                Crawler.get_content(website, '/test')

            # Assert session was closed
            mock_session_instance.close.assert_called_once()

    def test_session_management_external(self):
        """Test that Crawler DOES NOT close an external session."""
        website = create_mock_selector_template()
        external_session = Mock(spec=requests.Session)

        with patch.object(Crawler, "get_page", return_value=create_mock_soup()):
            Crawler.get_content(website, '/test', session=external_session)

        # Assert external session was NOT closed
        external_session.close.assert_not_called()

    @patch.object(Crawler, "get_page")
    def test_get_content_callable_handles_exception(self, mock_get_page):
        website = create_mock_selector_template()

        def crashing_parser(soup):
            raise ValueError("Parser exploded")

        website.selectors['crashyTag'] = crashing_parser
        mock_get_page.return_value = create_mock_soup()

        # Should not raise exception, but return None for that key
        result = Crawler.get_content(website, '')
        assert result['crashyTag'] is None



