"""Selector template for https://arkleg.state.ar.us/Bills/Votes?."""

import re

from bs4 import BeautifulSoup

from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.data_pipeline.transform.utils.normalize_list_of_str_link import normalize_list_of_str_link
from src.data_pipeline.transform.utils.normalize_str import normalize_str
from src.models.selector_template import SelectorTemplate
from src.structures.registries import PipelineRegistries, PipelineRegistryKeys, register_processor


@register_processor(PipelineRegistryKeys.BILL_VOTE, PipelineRegistries.PROCESS)
class BillVoteSelector(SelectorTemplate):
    """Selector for Arkleg Bill Vote Page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "title": (("div h1"), normalize_str),
                "yea_voters": (_VoteListParsers.parse_yea_names, normalize_list_of_str_link),
                "nay_voters": (_VoteListParsers.parse_nay_names, normalize_list_of_str_link),
                "non_voting_voters": (
                    _VoteListParsers.parse_non_voting_names,
                    normalize_list_of_str_link,
                ),
                "present_voters": (
                    _VoteListParsers.parse_present_names,
                    normalize_list_of_str_link,
                ),
                "excused_voters": (
                    _VoteListParsers.parse_excused_names,
                    normalize_list_of_str_link,
                ),
                "dynamic_state_example": (
                    lambda node, state_tree: self.get_dynamic_state(node, state_tree,
                                                        {"bill_id": None}, None),
                    empty_transform,
                ),
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
            text = a.get(attr)
            link = result.append(a.get_text(strip=True))
            result.append(text, link)
        return result

    @staticmethod
    def parse_yea_names(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Yeas")

    @staticmethod
    def __parse_yea_links(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Yeas", "href")

    @staticmethod
    def parse_nay_names(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Nays")

    @staticmethod
    def __parse_nay_links(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Nays", "href")

    @staticmethod
    def parse_non_voting_names(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Non Voting")

    @staticmethod
    def __parse_non_voting_links(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Non Voting", "href")

    @staticmethod
    def parse_present_names(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Present")

    @staticmethod
    def __parse_present_links(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Present", "href")

    @staticmethod
    def parse_excused_names(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Excused")

    @staticmethod
    def __parse_excused_links(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Excused", "href")
