def strip_session_from_link(link: str, *, strict: bool = False) -> str:
    search_str1 = "&ddBienniumSession="
    search_str2 = "?ddBienniumSession="
    search_strs = [search_str1, search_str2]
    if search_str1 not in link:
        if search_str2 not in link:
            raise ValueError(f"Link '{link}'  does not contain '{search_str1}' or '{search_str2}'")
        links = link.rsplit(search_str2, 1)
        return links
    links = link.rsplit(search_str1, 1) #rear split

    return links[1]
