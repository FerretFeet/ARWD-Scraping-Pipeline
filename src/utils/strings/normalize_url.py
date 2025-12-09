import html
from urllib.parse import unquote, urlparse


def normalize_url(url: str) -> str:
    """Return unescaped path+query, decoding HTML and URL encoding."""
    if not url:
        return ""
    # Decode HTML entities
    url = html.unescape(url)
    parsed = urlparse(url)
    path_query = parsed.path
    if parsed.query:
        path_query += "?" + parsed.query
    return unquote(path_query)
