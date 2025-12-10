# ruff: noqa: E501
import datetime
import zoneinfo
from html import unescape
from queue import Queue
from unittest.mock import patch

import pytest

from src.config.pipeline_enums import PipelineRegistryKeys
from src.config.settings import PIPELINE_REGISTRY
from src.data_pipeline.extract import webcrawler
from src.data_pipeline.extract.html_parser import HTMLParser
from src.data_pipeline.transform.pipeline_transformer import PipelineTransformer
from src.structures.directed_graph import DirectionalGraph, Node
from src.utils.paths import project_root
from src.workers.pipeline_workers import ProcessorWorker


@pytest.fixture
def fake_state():
    state = DirectionalGraph()



    rootnode = state.add_new_node(unescape("https://arkleg.state.ar.us/"),
                                  PipelineRegistryKeys.ARK_LEG_SEEDER, None)
    commcat = state.add_new_node(unescape("https://arkleg.state.ar.us/Committees"),
                                 PipelineRegistryKeys.COMMITTEES_CAT, [rootnode])
    comm = state.add_new_node(unescape("https://arkleg.state.ar.us/Committees/Detail?code=963&ddBienniumSession=2025%2F2025R"),
                              PipelineRegistryKeys.COMMITTEE, [commcat])
    leglistnode = state.add_new_node(unescape("https://arkleg.state.ar.us/Legislators/List"),
                                     PipelineRegistryKeys.LEGISLATOR_LIST, [rootnode])
    leg1node = state.add_new_node(unescape("https://arkleg.state.ar.us/Legislators/Detail?member=Lundstrum&ddBienniumSession=2025%2F2025R"),
                                  PipelineRegistryKeys.LEGISLATOR, [leglistnode])
    leg2node = state.add_new_node(unescape("https://arkleg.state.ar.us/Legislators/Detail?member=C.+Cooper&ddBienniumSession=2025%2F2025R"),
                                  PipelineRegistryKeys.LEGISLATOR, [leglistnode])
    billlistnode = state.add_new_node(unescape("https://arkleg.state.ar.us/Bills/ViewBills?type=HB"),
                                      PipelineRegistryKeys.BILL_LIST, [rootnode])
    comm.data = {"committee_id": 1}
    leg1node.data = {"legislator_id": 1}
    leg2node.data = {"legislator_id": 2}

    return state

@pytest.fixture
def mock_processor_worker(fake_state):
    worker = ProcessorWorker(Queue(), Queue(), fake_state, HTMLParser(), PipelineTransformer(),
                           PIPELINE_REGISTRY, strict=False)
    return worker



@pytest.fixture(scope="session")
def known_bill_html_fixture() -> str:
    """Load saved HTML fixture for legislators page."""
    fixture_path = project_root / "tests" / "fixtures" / "html" / "bill_page" / "bill.known.html"
    with fixture_path.open(encoding="utf-8") as f:
        html = f.read()
        return html


class TestArStateLegislatorSelector:
    @patch.object(webcrawler.Crawler, "get_page")
    def test_known_bill_known_return(self, mock_get_page, known_bill_html_fixture,
                                     mock_processor_worker, fake_state):

        parentnode = list(fake_state.nodes.values())[-1]
        node = Node(PipelineRegistryKeys.BILL, unescape("https://arkleg.state.ar.us/Bills/Detail?id=HB1001&ddBienniumSession=2019%2F2019R"),
                    incoming={parentnode}, data={"html": known_bill_html_fixture})
        t_parser, t_transformer, state_pairs = mock_processor_worker._get_processing_templates(node.url, node)  # noqa: SLF001
        print(f"NODE IS {node}")
        parsed_data = mock_processor_worker._parse_html(node.url,  # noqa: SLF001
                                          node.data["html"], t_parser)
        parsed_data, t_transformer = mock_processor_worker.inject_session_code(parsed_data, t_transformer, node)
        result = mock_processor_worker._transform_data(parsed_data, t_transformer)  # noqa: SLF001
        result = mock_processor_worker._attach_state_values(node, result, t_transformer, state_pairs)  # noqa: SLF001


        assert result is not None

        assert result["title"] == "hb1001 - an act for the arkansas house of representatives of the ninety-fifth general assembly appropriation for the 2024-2025 fiscal year."
        assert result["bill_no"] == "hb1001"
        assert result["act_no"] == "3"
        assert result["orig_chamber"] == "house"
        assert result["lead_sponsor"] == {"committee_id": [1]}

        assert result["cosponsors"] == {"legislator_id": [1, 2]}
        assert result["other_primary_sponsor"] == {"committee_id": [1], "legislator_id": [1, 2]}

        assert result["intro_date"] == datetime.datetime(2025, 1, 13, 14, 39, 5, tzinfo=zoneinfo.ZoneInfo(key="America/Chicago"))
        assert result["act_date"] ==  datetime.datetime(2025, 1, 27, 0, 0, tzinfo=zoneinfo.ZoneInfo(key="America/Chicago"))
        assert result["bill_documents"] == {"amendments": ["/home/ftpdocument?path=%2famend%2f2025r%2fpublic%2fhb1001-h1.pdf"],
                                            "bill_text": ["/home/ftpdocument?path=%2fbills%2f2025r%2fpublic%2fhb1001.pdf"],
                                            "act_text": ["/acts/ftpdocument?path=%2facts%2f2025r%2fpublic%2f&file=3.pdf&ddbienniumsession=2025%2f2025r"]}
        assert result["session_code"] ==  "2019/2019R"
