import datetime
import zoneinfo
from html import unescape
from queue import Queue
from unittest.mock import patch

import pytest
from bs4 import BeautifulSoup

from src.config.pipeline_enums import PipelineRegistryKeys
from src.config.settings import PIPELINE_REGISTRY
from src.data_pipeline.extract import webcrawler
from src.data_pipeline.extract.html_parser import HTMLParser
from src.data_pipeline.transform.pipeline_transformer import PipelineTransformer
from src.structures.directed_graph import DirectionalGraph, Node
from src.utils.paths import project_root
from src.workers.pipeline_workers import ProcessorWorker

KNOWN_YEA = 96
KNOWN_NAY = 0
KNOWN_NON = 4
KNOWN_EXCUSED = 0
KNOWN_PRESENT = 0



@pytest.fixture
def fake_state():
    state = DirectionalGraph()

    # Root nodes
    rootnode = state.add_new_node(
        unescape("https://arkleg.state.ar.us/"),
        PipelineRegistryKeys.ARK_LEG_SEEDER,
        None,
    )
    commcat = state.add_new_node(
        unescape("https://arkleg.state.ar.us/Committees"),
        PipelineRegistryKeys.COMMITTEES_CAT,
        [rootnode],
    )
    billlistnode = state.add_new_node(
        unescape("https://arkleg.state.ar.us/Bills/ViewBills?type=HB"),
        PipelineRegistryKeys.BILL_LIST,
        [rootnode],
    )
    leglistnode = state.add_new_node(
        unescape("https://arkleg.state.ar.us/Legislators/List"),
        PipelineRegistryKeys.LEGISLATOR_LIST,
        [rootnode],
    )

    # Map of names -> URLs extracted from the HTML
    name_url_map = {
    "Alexander": "/Legislators/Detail?member=Alexander&ddBienniumSession=2013%2F2013R",
    "D. Altes": "/Legislators/Detail?member=D.+Altes&ddBienniumSession=2013%2F2013R",
    "C. Armstrong": "/Legislators/Detail?member=C.+Armstrong&ddBienniumSession=2013%2F2013R",
    "E. Armstrong": "/Legislators/Detail?member=E.+Armstrong&ddBienniumSession=2013%2F2013R",
    "Baine": "/Legislators/Detail?member=Baine&ddBienniumSession=2013%2F2013R",
    "Baird": "/Legislators/Detail?member=Baird&ddBienniumSession=2013%2F2013R",
    "Ballinger": "/Legislators/Detail?member=Ballinger&ddBienniumSession=2013%2F2013R",
    "Baltz": "/Legislators/Detail?member=Baltz&ddBienniumSession=2013%2F2013R",
    "Bell": "/Legislators/Detail?member=Bell&ddBienniumSession=2013%2F2013R",
    "Biviano": "/Legislators/Detail?member=Biviano&ddBienniumSession=2013%2F2013R",
    "Bragg": "/Legislators/Detail?member=Bragg&ddBienniumSession=2013%2F2013R",
    "Branscum": "/Legislators/Detail?member=Branscum&ddBienniumSession=2013%2F2013R",
    "Broadaway": "/Legislators/Detail?member=Broadaway&ddBienniumSession=2013%2F2013R",
    "J. Burris": "/Legislators/Detail?member=J.+Burris&ddBienniumSession=2013%2F2013R",
    "Carnine": "/Legislators/Detail?member=Carnine&ddBienniumSession=2013%2F2013R",
    "Catlett": "/Legislators/Detail?member=Catlett&ddBienniumSession=2013%2F2013R",
    "Clemmer": "/Legislators/Detail?member=Clemmer&ddBienniumSession=2013%2F2013R",
    "Collins": "/Legislators/Detail?member=Collins&ddBienniumSession=2013%2F2013R",
    "Copenhaver": "/Legislators/Detail?member=Copenhaver&ddBienniumSession=2013%2F2013R",
    "Cozart": "/Legislators/Detail?member=Cozart&ddBienniumSession=2013%2F2013R",
    "Dale": "/Legislators/Detail?member=Dale&ddBienniumSession=2013%2F2013R",
    "Davis": "/Legislators/Detail?member=Davis&ddBienniumSession=2013%2F2013R",
    "Deffenbaugh": "/Legislators/Detail?member=Deffenbaugh&ddBienniumSession=2013%2F2013R",
    "J. Dickinson": "/Legislators/Detail?member=J.+Dickinson&ddBienniumSession=2013%2F2013R",
    "Dotson": "/Legislators/Detail?member=Dotson&ddBienniumSession=2013%2F2013R",
    "C. Douglas": "/Legislators/Detail?member=C.+Douglas&ddBienniumSession=2013%2F2013R",
    "D. Douglas": "/Legislators/Detail?member=D.+Douglas&ddBienniumSession=2013%2F2013R",
    "J. Edwards": "/Legislators/Detail?member=J.+Edwards&ddBienniumSession=2013%2F2013R",
    "Eubanks": "/Legislators/Detail?member=Eubanks&ddBienniumSession=2013%2F2013R",
    "Farrer": "/Legislators/Detail?member=Farrer&ddBienniumSession=2013%2F2013R",
    "Ferguson": "/Legislators/Detail?member=Ferguson&ddBienniumSession=2013%2F2013R",
    "Fielding": "/Legislators/Detail?member=Fielding&ddBienniumSession=2013%2F2013R",
    "Fite": "/Legislators/Detail?member=Fite&ddBienniumSession=2013%2F2013R",
    "Gillam": "/Legislators/Detail?member=Gillam&ddBienniumSession=2013%2F2013R",
    "Gossage": "/Legislators/Detail?member=Gossage&ddBienniumSession=2013%2F2013R",
    "Hammer": "/Legislators/Detail?member=Hammer&ddBienniumSession=2013%2F2013R",
    "Harris": "/Legislators/Detail?member=Harris&ddBienniumSession=2013%2F2013R",
    "Hawthorne": "/Legislators/Detail?member=Hawthorne&ddBienniumSession=2013%2F2013R",
    "Hickerson": "/Legislators/Detail?member=Hickerson&ddBienniumSession=2013%2F2013R",
    "Hillman": "/Legislators/Detail?member=Hillman&ddBienniumSession=2013%2F2013R",
    "Hobbs": "/Legislators/Detail?member=Hobbs&ddBienniumSession=2013%2F2013R",
    "Hodges": "/Legislators/Detail?member=Hodges&ddBienniumSession=2013%2F2013R",
    "Holcomb": "/Legislators/Detail?member=Holcomb&ddBienniumSession=2013%2F2013R",
    "Hopper": "/Legislators/Detail?member=Hopper&ddBienniumSession=2013%2F2013R",
    "House": "/Legislators/Detail?member=House&ddBienniumSession=2013%2F2013R",
    "Hutchison": "/Legislators/Detail?member=Hutchison&ddBienniumSession=2013%2F2013R",
    "Jean": "/Legislators/Detail?member=Jean&ddBienniumSession=2013%2F2013R",
    "Jett": "/Legislators/Detail?member=Jett&ddBienniumSession=2013%2F2013R",
    "Julian": "/Legislators/Detail?member=Julian&ddBienniumSession=2013%2F2013R",
    "Kerr": "/Legislators/Detail?member=Kerr&ddBienniumSession=2013%2F2013R",
    "Kizzia": "/Legislators/Detail?member=Kizzia&ddBienniumSession=2013%2F2013R",
    "Lampkin": "/Legislators/Detail?member=Lampkin&ddBienniumSession=2013%2F2013R",
    "Lea": "/Legislators/Detail?member=Lea&ddBienniumSession=2013%2F2013R",
    "Leding": "/Legislators/Detail?member=Leding&ddBienniumSession=2013%2F2013R",
    "Lenderman": "/Legislators/Detail?member=Lenderman&ddBienniumSession=2013%2F2013R",
    "Linck": "/Legislators/Detail?member=Linck&ddBienniumSession=2013%2F2013R",
    "Love": "/Legislators/Detail?member=Love&ddBienniumSession=2013%2F2013R",
    "Lowery": "/Legislators/Detail?member=Lowery&ddBienniumSession=2013%2F2013R",
    "Magie": "/Legislators/Detail?member=Magie&ddBienniumSession=2013%2F2013R",
    "S. Malone": "/Legislators/Detail?member=S.+Malone&ddBienniumSession=2013%2F2013R",
    "Mayberry": "/Legislators/Detail?member=Mayberry&ddBienniumSession=2013%2F2013R",
    "McCrary": "/Legislators/Detail?member=McCrary&ddBienniumSession=2013%2F2013R",
    "McElroy": "/Legislators/Detail?member=McElroy&ddBienniumSession=2013%2F2013R",
    "McGill": "/Legislators/Detail?member=McGill&ddBienniumSession=2013%2F2013R",
    "McLean": "/Legislators/Detail?member=McLean&ddBienniumSession=2013%2F2013R",
    "D. Meeks": "/Legislators/Detail?member=D.+Meeks&ddBienniumSession=2013%2F2013R",
    "S. Meeks": "/Legislators/Detail?member=S.+Meeks&ddBienniumSession=2013%2F2013R",
    "Miller": "/Legislators/Detail?member=Miller&ddBienniumSession=2013%2F2013R",
    "Murdock": "/Legislators/Detail?member=Murdock&ddBienniumSession=2013%2F2013R",
    "Neal": "/Legislators/Detail?member=Neal&ddBienniumSession=2013%2F2013R",
    "Nickels": "/Legislators/Detail?member=Nickels&ddBienniumSession=2013%2F2013R",
    "B. Overbey": "/Legislators/Detail?member=B.+Overbey&ddBienniumSession=2013%2F2013R",
    "Payton": "/Legislators/Detail?member=Payton&ddBienniumSession=2013%2F2013R",
    "Perry": "/Legislators/Detail?member=Perry&ddBienniumSession=2013%2F2013R",
    "Ratliff": "/Legislators/Detail?member=Ratliff&ddBienniumSession=2013%2F2013R",
    "Rice": "/Legislators/Detail?member=Rice&ddBienniumSession=2013%2F2013R",
    "Sabin": "/Legislators/Detail?member=Sabin&ddBienniumSession=2013%2F2013R",
    "Scott": "/Legislators/Detail?member=Scott&ddBienniumSession=2013%2F2013R",
    "Shepherd": "/Legislators/Detail?member=Shepherd&ddBienniumSession=2013%2F2013R",
    "Slinkard": "/Legislators/Detail?member=Slinkard&ddBienniumSession=2013%2F2013R",
    "F. Smith": "/Legislators/Detail?member=F.+Smith&ddBienniumSession=2013%2F2013R",
    "Steel": "/Legislators/Detail?member=Steel&ddBienniumSession=2013%2F2013R",
    "Talley": "/Legislators/Detail?member=Talley&ddBienniumSession=2013%2F2013R",
    "T. Thompson": "/Legislators/Detail?member=T.+Thompson&ddBienniumSession=2013%2F2013R",
    "Vines": "/Legislators/Detail?member=Vines&ddBienniumSession=2013%2F2013R",
    "W. Wagner": "/Legislators/Detail?member=W.+Wagner&ddBienniumSession=2013%2F2013R",
    "Wardlaw": "/Legislators/Detail?member=Wardlaw&ddBienniumSession=2013%2F2013R",
    "Westerman": "/Legislators/Detail?member=Westerman&ddBienniumSession=2013%2F2013R",
    "D. Whitaker": "/Legislators/Detail?member=D.+Whitaker&ddBienniumSession=2013%2F2013R",
    "B. Wilkins": "/Legislators/Detail?member=B.+Wilkins&ddBienniumSession=2013%2F2013R",
    "H. Wilkins": "/Legislators/Detail?member=H.+Wilkins&ddBienniumSession=2013%2F2013R",
    "Williams": "/Legislators/Detail?member=Williams&ddBienniumSession=2013%2F2013R",
    "Womack": "/Legislators/Detail?member=Womack&ddBienniumSession=2013%2F2013R",
    "Word": "/Legislators/Detail?member=Word&ddBienniumSession=2013%2F2013R",
    "Wren": "/Legislators/Detail?member=Wren&ddBienniumSession=2013%2F2013R",
    "Wright": "/Legislators/Detail?member=Wright&ddBienniumSession=2013%2F2013R",
    # Non-voting
    "Barnett": "/Legislators/Detail?member=Barnett&ddBienniumSession=2013%2F2013R",
    "Richey": "/Legislators/Detail?member=Richey&ddBienniumSession=2013%2F2013R",
    "Walker": "/Legislators/Detail?member=Walker&ddBienniumSession=2013%2F2013R",
    "Mr. Speaker": "/Legislators/Detail?member=Carter&ddBienniumSession=2013%2F2013R",
}

    # Add legislator nodes with IDs
    for idx, (name, url) in enumerate(name_url_map.items(), start=1):
        node = state.add_new_node(
            unescape(f"https://arkleg.state.ar.us{url}"),
            PipelineRegistryKeys.LEGISLATOR,
            [leglistnode],
        )
        node.data = {"legislator_id": idx}

    # Add a sample bill node
    billnode = state.add_new_node(
        unescape("https://arkleg.state.ar.us/Bills/Detail?id=HB1001"),
        PipelineRegistryKeys.BILL,
        [billlistnode],
    )
    billnode.data = {"bill_id": 1}

    return state

@pytest.fixture
def mock_processor_worker(fake_state):
    worker = ProcessorWorker(Queue(), Queue(), fake_state, HTMLParser(), PipelineTransformer(),
                           PIPELINE_REGISTRY, strict=False)
    return worker



@pytest.fixture(scope="session")
def known_bill_vote_html_fixture() -> BeautifulSoup:
    """Load saved HTML fixture for legislators page."""
    fixture_path = project_root / "tests" / "fixtures" / "html" / "vote_page" / "vote.known.html"
    with fixture_path.open(encoding="utf-8") as f:
        html = f.read()
        return html


class TestBillVoteSelector:
    @patch.object(webcrawler.Crawler, "get_page")
    def test_known_bill_vote_known_return(self, mock_get_page, known_bill_vote_html_fixture,
                                          mock_processor_worker, fake_state):

        parentnode = list(fake_state.nodes.values())[-1]
        node = Node(PipelineRegistryKeys.BILL, unescape("https://arkleg.state.ar.us/Bills/Votes?id=HB1001&rcs=38&chamber=Senate&ddBienniumSession=2013%2F2013R"),
                    incoming={parentnode}, data={"html": known_bill_vote_html_fixture})
        t_parser, t_transformer, state_pairs = mock_processor_worker._get_processing_templates(node.url, node)  # noqa: SLF001
        print(f"NODE IS {node}")
        parsed_data = mock_processor_worker._parse_html(node.url,  # noqa: SLF001
                                          node.data["html"], t_parser)
        parsed_data, t_transformer = mock_processor_worker.inject_session_code(parsed_data, t_transformer, node)
        result = mock_processor_worker._transform_data(parsed_data, t_transformer)  # noqa: SLF001
        result = mock_processor_worker._attach_state_values(node, result, t_transformer, state_pairs)  # noqa: SLF001


        assert result["vote_timestamp"] ==  datetime.datetime(2013, 2, 5, 13, 43, 39, tzinfo=zoneinfo.ZoneInfo(key="America/Chicago"))
        assert result["chamber"] == "house"

        combined = [
            v
            for v in (
                (result.get("yea_voters") or [])
                + (result.get("non_voting_voters") or [])
                + (result.get("excused_voters") or [])
                + (result.get("present_voters") or [])
            )
            if v is not None
        ]
        for id in combined:
            assert isinstance(id, int)
