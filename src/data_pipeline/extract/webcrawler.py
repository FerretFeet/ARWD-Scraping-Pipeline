"""Web Crawler for requesting and parsing HTML content."""
import time

from requests import RequestException, Session

from src.config.settings import config
from src.utils.logger import logger


class Crawler:
    """Web Crawler for requesting and parsing HTML content."""

    def __init__(self, site: str, *, strict: bool | None = None, max_retries: int = 3,
        retry_backoff: float = 0.5) -> None:
        """Initialize the Crawler with domain base-url and optional strict parameter."""
        self.site = site
        self.strict = config["strict"] if strict is None else strict
        self.session = Session()
        self.session_counter = 0

        self.max_retries = max_retries
        self.retry_backoff = retry_backoff

    def increment_session(self) -> None:
        """Reset the session after 100 requests."""
        self.session_counter += 1
        if self.session_counter % 100 == 0:
            self.session.close()
            self.session = Session()

    def get_page(self, url: str) -> str:
        """Fetch a URL with automatic retries and return HTML text."""
        self.increment_session()
        last_exc = None

        for attempt in range(1, self.max_retries + 1):
            try:
                html = self.session.get(url)
                html.raise_for_status()

            except (RequestException, Exception) as e:
                last_exc = e

                if attempt < self.max_retries:
                    sleep_for = self.retry_backoff * attempt
                    logger.warning(
                        f"[CRAWLER] Failed to fetch {url} "
                        f"(attempt {attempt}/{self.max_retries}): {e} "
                        f"— retrying in {sleep_for:.2f}s",
                    )
                    time.sleep(sleep_for)
                else:
                    # Final attempt failed → now treated as fatal
                    message = (
                        f"Failed to fetch URL {url} after {self.max_retries} attempts: {e}"
                    )
                    logger.error(message)
                    raise ConnectionError(message) from e
            else:
                return html.text  # SUCCESS


        # Should never reach here
        msg = f"Unknown error while fetching {url}"
        raise ConnectionError(msg) from last_exc
