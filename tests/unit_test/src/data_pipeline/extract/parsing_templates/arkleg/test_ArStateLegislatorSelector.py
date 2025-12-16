# ruff: noqa: E501
from html import unescape
from pathlib import Path
from queue import Queue
from unittest.mock import patch

import pytest

from src.config.pipeline_enums import PipelineRegistryKeys
from src.config.settings import PIPELINE_REGISTRY
from src.data_pipeline.extract import webcrawler
from src.data_pipeline.extract.html_parser import HTMLParser
from src.data_pipeline.load.pipeline_loader import PipelineLoader
from src.data_pipeline.transform.pipeline_transformer import PipelineTransformer
from src.structures.directed_graph import DirectionalGraph, Node
from src.utils.paths import project_root
from src.workers.pipeline_workers import LoaderWorker, ProcessorWorker


@pytest.fixture
def fake_state():
    state = DirectionalGraph()



    rootnode = state.add_new_node(unescape("https://arkleg.state.ar.us/"),
                                  PipelineRegistryKeys.ARK_LEG_SEEDER, None)
    commcat = state.add_new_node(unescape("https://arkleg.state.ar.us/Committees"),
                                 PipelineRegistryKeys.COMMITTEES_CAT, [rootnode])
    commlist = state.add_new_node(unescape("https://arkleg.state.ar.us/Committees/List"),
                                 PipelineRegistryKeys.COMMITTEES_LIST, [commcat])
    # All committee IDs you want to include
    committee_ids = [
        "985",
        "045",
        "000",
        "490",
        "920",
        "530",
        "450",
        "948",
        "952",
        "009",
        "024",
        "027",
        "028",
        "005",
        "905",
        "040",
        "030",
        "004",
        "008",
        "038",
        "081",
        "082",
        "083",
        "041",
        "051",
        "049",
        "020",
        "060",
        "010",
        "015",
    ]

    # Generate committee nodes dynamically
    for cid in committee_ids:
        url = unescape(
            f"https://arkleg.state.ar.us/Committees/Detail?"
            f"code={cid}&ddBienniumSession=2025%2F2025R",
        )

        node = state.add_new_node(
            url,
            PipelineRegistryKeys.COMMITTEE,
            [commlist],
        )

        node.data = {"committee_id": cid}

    state.add_new_node(unescape("https://arkleg.state.ar.us/Legislators/List"),
                                     PipelineRegistryKeys.LEGISLATOR_LIST, [rootnode])



    return state


@pytest.fixture
def loader_queue():
    return Queue()

@pytest.fixture
def mock_processor_worker(fake_state, loader_queue):
    return ProcessorWorker(Queue(), loader_queue, fake_state, HTMLParser(), PipelineTransformer(),
                           PIPELINE_REGISTRY, strict=False)

@pytest.fixture
def test_db(db_engine):
    db_engine.autocommit = False
    with db_engine.cursor() as cur:
        # Load the SQL file for the function
        fp = project_root / "sql" / "dml" / "functions" / "upsert_legislator.sql"
        with open(fp) as f:
            cur.execute(f.read())
        cur.execute("INSERT INTO sessions VALUES (%s, %s, %s)", ("2025/2025R", "reg_sess", "20250101"))

        yield db_engine
    db_engine.rollback()

@pytest.fixture
def mock_loader_worker(fake_state, test_db, loader_queue):
    return LoaderWorker(loader_queue, fake_state, test_db, PIPELINE_REGISTRY)



@pytest.fixture(scope="session")
def known_legislator_html_fixture() -> str:
    """Load saved HTML fixture for legislators page."""
    fixture_path = project_root / "tests" / "fixtures" / "html" / "legislator" / "legislator.known.html"
    with fixture_path.open(encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def sql_file_path():
    return Path("sql/dml/functions/upsert_legislator.sql")

@pytest.fixture
def loader(sql_file_path):
    return PipelineLoader(
        sql_file_path=sql_file_path,
        upsert_function_name="Upsert Legislator",
        required_params={
            "first_name": str,
            "last_name": str,
            "url": str,
            "district": str,
            "seniority": int,
            "chamber": str,
            "session_code": str,
        },
        insert="""SELECT upsert_legislator(
               p_first_name := %(p_first_name)s,
               p_last_name := %(p_last_name)s,
               p_url := %(p_url)s,
               p_phone := %(p_phone)s,
               p_email := %(p_email)s,
               p_address := %(p_address)s,
               p_district := %(p_district)s::TEXT,
               p_seniority := %(p_seniority)s,
               p_chamber := %(p_chamber)s,
               p_party := %(p_party)s,
               p_session_code := %(p_session_code)s,
               p_committee_ids := %(p_committee_ids)s
               ) AS legislator_id;""",
    )


class TestArStateLegislatorSelector:
    @patch.object(webcrawler.Crawler, "get_page")
    def test_known_legislator_known_return(self, mock_get_page, known_legislator_html_fixture,
                                     mock_processor_worker, fake_state, mock_loader_worker, loader,
                                           test_db):

        parentnode = list(fake_state.nodes.values())[-1]
        node = Node(PipelineRegistryKeys.LEGISLATOR, unescape("https://arkleg.state.ar.us/Legislators/Detail?member=Gilmore&ddBienniumSession=2025%2F2025R"),
                    incoming={parentnode}, data={"html": known_legislator_html_fixture})
        t_parser, t_transformer, state_pairs = mock_processor_worker._get_processing_templates(node.url, node)
        parsed_data = mock_processor_worker._parse_html(node.url,
                                          node.data["html"], t_parser)
        parsed_data, t_transformer = mock_processor_worker.inject_session_code(parsed_data, t_transformer, node)
        result = mock_processor_worker._transform_data(parsed_data, t_transformer)
        if result:
            result.update({"url": node.url})

        result = mock_processor_worker._attach_state_values(node, result, t_transformer, state_pairs)
        node.data = result

        assert result is not None
        assert result["first_name"] == "justin"
        assert result["last_name"] == "boyd"
        assert result["url"] == node.url
        assert result["phone"] == "14792622156"
        assert result["email"] == "justin.boyd@senate.ar.gov"
        assert result["seniority"] == 25
        assert result["party"] == "r"
        assert result["session_code"] == "2025/2025R"
        assert result["address"] == "p.o. box 2625, fort smith, 72902"
        assert result["chamber"] == "senate"
        assert len(result["committee_ids"]) == 30
        assert result["district"] == "27"

        # lresult = loader.execute(node.data, test_db)
        # lresult = mock_loader_worker._load_item(node)
        # assert lresult is not None
