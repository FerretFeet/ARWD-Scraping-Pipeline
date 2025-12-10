"""Selector template for https://arkleg.state.ar.us/Bills/Votes?."""
import html
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup

from src.data_pipeline.transform.utils.empty_transform import empty_transform
from src.data_pipeline.transform.utils.normalize_str import normalize_str
from src.models.selector_template import SelectorTemplate
from src.structures.directed_graph import Node


class BillVoteSelector(SelectorTemplate):
    """Selector for Arkleg Bill Vote Page."""

    def __init__(self) -> None:
        """Initialize the selector template."""
        super().__init__(
            selectors={
                "title": ("div h1", _VoteListTransformers.transform_vote_title),
                "yea_voters": (_VoteListParsers.parse_yea_names, empty_transform),
                "nay_voters": (_VoteListParsers.parse_nay_names, empty_transform),
                "non_voting_voters": (
                    _VoteListParsers.parse_non_voting_names,
                    empty_transform,
                ),
                "present_voters": (
                    _VoteListParsers.parse_present_names,
                    empty_transform,
                ),
                "excused_voters": (
                    _VoteListParsers.parse_excused_names,
                    empty_transform,
                ),
                "state_bill_id": (
                    lambda node, state_tree, parsed_data: self.get_dynamic_state_from_parents(
                        node, state_tree, {"bill_id": None}, None,
                    ).data,
                    empty_transform,
                ),
                "state_yea_voter_lookup": (self.yea_lookup, empty_transform),
                "state_nay_voter_lookup": (self.nay_lookup, empty_transform),
                "state_present_voter_lookup": (self.present_lookup, empty_transform),
                "state_nonvoting_voter_lookup": (self.nonvoting_lookup, empty_transform),
                "state_excused_voter_lookup": (self.excused_lookup, empty_transform),
            },
        )

    def state_vote_lookup(self, node: Node, state_tree, parsed_data, pdkey):
        urls = parsed_data.get(pdkey)
        print(f"DEBUG STATE VOTE LOOKUP {urls}")
        if not urls:
            return {pdkey: {}}

        returnlist = []
        for url in urls:
            rkey = "legislator_id"

            found_node = self.get_dynamic_state(
                node,
                state_tree,
                {rkey: None},
                {"url": html.unescape(url)},
            )
            print(f"STATE VOTE FOUND NODE {found_node}")
            if found_node:
                returnlist.append(found_node.data[rkey])
            else:
                return None
        print(f"STATE VOTE FOUND NODES {len(returnlist)}")
        return {pdkey: returnlist}

    def yea_lookup(self, node, state_tree, parsed_data):
        return self.state_vote_lookup(node, state_tree, parsed_data, "yea_voters")

    def nay_lookup(self, node, state_tree, parsed_data):
        return self.state_vote_lookup(node, state_tree, parsed_data, "nay_voters")

    def nonvoting_lookup(self, node, state_tree, parsed_data):
        return self.state_vote_lookup(node, state_tree, parsed_data, "non_voting_voters")

    def present_lookup(self, node, state_tree, parsed_data):
        return self.state_vote_lookup(node, state_tree, parsed_data, "present_voters")

    def excused_lookup(self, node, state_tree, parsed_data):
        return self.state_vote_lookup(node, state_tree, parsed_data, "excused_voters")


class _VoteListTransformers:
    @staticmethod
    def transform_vote_title(vote_title_list: list[str], *, strict:bool = False,
                             tz_name: str="America/Chicago"):
        """
        Take a list like ['House Vote - Tuesday, February 5, 2013 1:43:39 PM'].
        
        returns a dict with keys: vote_timestamp (datetime) and chamber (str)
        """
        if not vote_title_list or not vote_title_list[0]:
            return {"vote_timestamp": None, "chamber": None}
        text = vote_title_list[0] if isinstance(vote_title_list, list) else vote_title_list

        # Extract chamber (House or Senate)
        chamber_match = re.match(r"(House|Senate) Vote", text)
        chamber = chamber_match.group(1) if chamber_match else None

        # Extract timestamp part (everything after the first ' - ')
        try:
            timestamp_str = text.split(" - ", 1)[1]
            naive_dt = datetime.strptime(timestamp_str, "%A, %B %d, %Y %I:%M:%S %p")  # noqa: DTZ007
            vote_timestamp = naive_dt.replace(tzinfo=ZoneInfo(tz_name))
        except (IndexError, ValueError):
            vote_timestamp = None

        return {"vote_timestamp": vote_timestamp, "chamber": normalize_str(chamber)}


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
            rattr = a.get(attr)
            # text = a.get_text(strip=True)
            result.append(html.unescape(rattr))
        return result

    @staticmethod
    def parse_yea_names(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Yeas", "href")

    @staticmethod
    def __parse_yea_links(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Yeas", "href")

    @staticmethod
    def parse_nay_names(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Nays", "href")

    @staticmethod
    def __parse_nay_links(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Nays", "href")

    @staticmethod
    def parse_non_voting_names(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Non Voting", "href")

    @staticmethod
    def __parse_non_voting_links(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Non Voting", "href")

    @staticmethod
    def parse_present_names(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Present", "href")

    @staticmethod
    def __parse_present_links(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Present", "href")

    @staticmethod
    def parse_excused_names(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Excused", "href")

    @staticmethod
    def __parse_excused_links(soup: BeautifulSoup) -> list[str] | None:
        return _VoteListParsers._parse_votes(soup, "Excused", "href")
