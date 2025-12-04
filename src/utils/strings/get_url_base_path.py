from urllib.parse import urlparse


def get_url_base_path(url: str) -> str:
    temp = urlparse(url)
    return temp.netloc + temp.path
