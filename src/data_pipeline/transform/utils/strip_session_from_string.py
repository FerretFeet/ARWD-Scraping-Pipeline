"""Utility function to transform a url from ark.leg.us."""

from urllib.parse import unquote


def strip_session_from_link(
    link: str,
    *,
    strict: bool = False,
    getSession: bool = True,
) -> str:
    """Strip session code from the query in the link. optionally return stripped link instead."""
    search_str1 = "&ddBienniumSession="
    search_str2 = "?ddBienniumSession="
    link = unquote(link)
    if search_str1 not in link:
        if search_str2 not in link:
            msg = f"Link '{link}'  does not contain '{search_str1}' or '{search_str2}'"
            raise ValueError(msg)
        links = link.rsplit(search_str2, 1)
        return links[1]
    links = link.rsplit(search_str1, 1)  # rear split
    if getSession:
        return links[1]
    return links[0]
