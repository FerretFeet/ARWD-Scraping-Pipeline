"""Utility function for transforming a url."""

from urllib.parse import urlparse


def get_url_base_path(url: str, *, include_path: bool = True) -> str:
    """Strip the url of queries. Optionally strip path."""
    temp = urlparse(url)
    result = temp.scheme + "://" + temp.netloc
    if include_path:
        result += temp.path
    return result
