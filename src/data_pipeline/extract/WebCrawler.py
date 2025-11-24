from typing import List
from bs4 import BeautifulSoup
from requests import Session, RequestException

from src.models.SelectorTemplate import SelectorTemplate
from src.utils.logger import logger


class Crawler:
    strict: bool = False  # toggle for strict validation

    def __init__(self, site: str):
        self.site = site
        self.session = Session()

    @staticmethod
    def get_page(session: Session, url: str) -> BeautifulSoup:
        """
        Make an http request and return a beautiful soup object of the page
        """
        if not isinstance(session, Session) or not isinstance(url, str):
            message = f'Parameter inserted of incorrect type. \n' \
                      f'session (Session) {type(session)}, url (str) {type(url)}'
            logger.error(message)
            raise TypeError(message)
        try:
            html = session.get(url)
            html.raise_for_status()  # Raise an exception for bad responses (4xx, 5xx)

        except RequestException as e:
            # Propagate network/HTTP errors as a specific application exception
            message = f'Failed to fetch URL {url}: {e}'
            logger.error(message)
            raise ConnectionError(message)

        except Exception as e:
            message = f'Failed to fetch URL {url}: {e}'
            logger.error(message)
            raise Exception(e)

        return BeautifulSoup(html.text, 'html.parser')

    @staticmethod
    def safe_get(soup: BeautifulSoup, selector: str, attr: str = 'text') -> List[str] | None:
        """
        Utility Function to get a specified attribute from a beautiful soup object.
        Selects text attribute of element by default
        Returns None on failure or a List of Strings on success
        """
        if not isinstance(soup, BeautifulSoup) \
        or not isinstance(selector, str) \
        or not isinstance(attr, str):
            message = (f'Parameter passed of incorrect type: \n'
                       f'soup (bs4): {type(soup)}, selector (str): {type(selector)}, attr (str): {type(attr)}')
            logger.error(message)
            raise TypeError(message)

        selected_elems = soup.select(selector)
        if not selected_elems:
            logger.warning(f"[safe_get] No matches found for selector '{selector}'")
            if Crawler.strict:
                raise ValueError(f'Selector "{selector}" returned nothing')
            return None
        values: List[str] = []
        for elem in selected_elems:
            if attr == 'text':
                values.append(elem.text)
            elif elem.has_attr(attr):
                values.append(elem.get(attr))
            else:
                message = f'Element "{elem}" does not have attribute "{attr}"'
                logger.warning(message)
                if Crawler.strict:
                    raise AttributeError(message)
                continue
        return values if values else None



    @staticmethod
    def get_content(website: SelectorTemplate, path: str, session=None):
        """
        The 'website.selectors' dict can contain:
        - key: (selector, attr, label) -> For simple, declarative scraping
        - key: callable_function(soup)      -> For complex, imperative scraping
        """
        if not isinstance(website, SelectorTemplate) or not isinstance(path, str):
            message = (f'Parameter passed of incorrect type:\n'
                       f'website: {type(website)}, path: {type(path)}')
            logger.error(message)
            raise TypeError(message)

        page_url = website.url + path
        content_holder = {}
        session_flag = session is None
        session = session or Session()
        soup = Crawler.get_page(session, page_url)
        if soup:
            for key, val in website.selectors.items():
                if callable(val):
                    # If selector is a helper function
                    try:
                        data = val(soup)
                        if not isinstance(data, list) and data is not None:
                            data = [data]
                        content_holder[key] = data
                    except Exception as e:
                        logger.warning(f"Error running custom parser for '{key}': {e}")
                        content_holder[key] = None
                else:
                    selector = attr = None
                    # --- STRATEGY 2: Rule is a simple tuple or str---
                    if isinstance(val, tuple):
                        if len(val) == 1:
                            selector = str(val[0])
                        elif len(val) == 2:
                            selector, attr = val
                        else:
                            logger.error(f"Too many values input to selector tuple: \n {key}:  {val}")
                            raise Exception(f'Too many args in selector tuple: {val}')
                    else:
                        selector = val
                    args = [selector] if attr is None else [selector, attr]
                    content_holder[key] = Crawler.safe_get(soup, *args)

        content_holder['rel_url'] = path
        session.close() if session_flag else None
        logger.info(f'\nCrawler executed and retrieved content')
        return content_holder

