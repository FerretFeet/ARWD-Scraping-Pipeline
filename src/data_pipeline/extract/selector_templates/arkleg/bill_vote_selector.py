"""Selector template for https://arkleg.state.ar.us/Bills/Votes?."""

import re

from bs4 import BeautifulSoup

from src.models.selector_template import SelectorTemplate


class BillVoteSelector(SelectorTemplate):
    """Selector for Arkleg Bill Vote Page."""

    next_page: str

    def __init__(self, url: str) -> None:
        """Initialize the selector template."""
        super().__init__(
            url=url,
            selectors={
                "title": ("div h1"),
                "yea_names": _VoteListParsers.parse_yea_names,
                "yea_links": _VoteListParsers.parse_yea_links,
                "nay_names": _VoteListParsers.parse_nay_names,
                "nay_links": _VoteListParsers.parse_nay_links,
                "non_voting_names": _VoteListParsers.parse_non_voting_names,
                "non_voting_links": _VoteListParsers.parse_non_voting_links,
                "present_names": _VoteListParsers.parse_present_names,
                "present_links": _VoteListParsers.parse_present_links,
                "excused_names": _VoteListParsers.parse_excused_names,
                "excused_links": _VoteListParsers.parse_excused_links,
            },
        )


class _VoteListParsers:
    @staticmethod
    def _parse_votes(
        soup: BeautifulSoup,
        vote_cat: str,
        attr: str | None = None,
    ) -> list[str] | None:
        regex = re.compile(rf"{vote_cat}\s*:\s*(\d+)", re.IGNORECASE)
        label = None
        match = None
        # Find the correct <b> tag
        for b in soup.find_all("b"):
            match = regex.search(b.get_text())
            if match:
                label = b
                break

        if not label or not match:
            return None  # category missing

        count = int(match.group(1))
        if count == 0:
            return []  # no votes, return empty list

        row_div = label.find_parent("div", class_="row")
        if not row_div:
            return None

        vote_container = row_div.find_next("div", class_="voteList")
        if not vote_container:
            return None

        result = []
        for a in vote_container.find_all("a"):
            if attr:
                result.append(a.get(attr))
            else:
                result.append(a.get_text(strip=True))
        return result

    @staticmethod
    def parse_yea_names(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Yeas")

    @staticmethod
    def parse_yea_links(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Yeas", "href")

    @staticmethod
    def parse_nay_names(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Nays")

    @staticmethod
    def parse_nay_links(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Nays", "href")

    @staticmethod
    def parse_non_voting_names(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Non Voting")

    @staticmethod
    def parse_non_voting_links(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Non Voting", "href")

    @staticmethod
    def parse_present_names(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Present")

    @staticmethod
    def parse_present_links(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Present", "href")

    @staticmethod
    def parse_excused_names(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Excused")

    @staticmethod
    def parse_excused_links(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Excused", "href")
