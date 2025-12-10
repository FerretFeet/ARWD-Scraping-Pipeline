# # tests/test_orchestrator_e2e.py
# import json
# from pathlib import Path
#
# import pytest
#
# from src.data_pipeline.orchestrate import Orchestrator
# from src.structures.directed_graph import DirectionalGraph
# from src.structures.indexed_tree import PipelineStateEnum
# from src.structures.registries import ProcessorRegistry
# from tests.configs.selector_config import SELECTOR_TESTS
#
#
# @pytest.fixture
# def tmp_cache_files(tmp_path, monkeypatch):
#     """Temporary cache for known_links and state."""
#     known_links = tmp_path / "known_links.json"
#     known_links.write_text(json.dumps([]), encoding="utf-8")
#
#     state_cache = tmp_path / "state_cache.json"
#     state_cache.write_text(json.dumps({}), encoding="utf-8")
#
#     monkeypatch.setattr("src.data_pipeline.orchestrate.known_links_cache_file", str(known_links))
#     monkeypatch.setattr("src.data_pipeline.orchestrate.state_cache_file", str(state_cache))
#     return {"known_links": str(known_links), "state_cache": str(state_cache)}
#
#
# def build_test_registry():
#     """Return a ProcessorRegistry for testing."""
#     from src.config.registry_config import LOADER_CONFIG, PROCESSOR_CONFIG
#
#     PIPELINE_REGISTRY = ProcessorRegistry()
#     PIPELINE_REGISTRY.load_p_config(PROCESSOR_CONFIG)
#     PIPELINE_REGISTRY.load_l_config(LOADER_CONFIG)
#     return PIPELINE_REGISTRY
#
# # Build all parameter tuples for html_selector_fixture
# param_list = [
#     (selector_info, param)
#     for selector_info in SELECTOR_TESTS
#     for param in selector_info["fixture_params"]
# ]
#
# @pytest.mark.parametrize(
#     ("selector_info", "html_selector_fixture"),
#     param_list,
#     indirect=["html_selector_fixture"],  # sends param to fixture
# )
# def test_orchestrator_end_to_end(
#     selector_info,
#     html_selector_fixture,
#     tmp_cache_files,
#     test_db,
# ):
#     """
#     Full E2E orchestrator test:
#     - Runs real workers in threads
#     - Fetch → Process → Load
#     - Inserts into test DB
#     - Tests state save/load
#     """
#
#     # --- Setup ---
#     registry = build_test_registry()
#     state = DirectionalGraph()
#     seed_url = "arkleg.state.ar.us/"  # ARK_LEG_SEEDER
#
#     # Collect all HTML pages for the seeded URLs and children URLs
#     fixture_dir = Path("fixtures/html")
#
#     url_soups = {}
#
#     # Map filenames to URLs
#     file_to_url_map = {
#         "arkleg.state.ar.us.v1.html": "arkleg.state.ar.us/",
#         "Committees.v1.html": "arkleg.state.ar.us/Committees",
#         "Legislators_List.v1.html": "arkleg.state.ar.us/Legislators/List",
#         "Bills.v1.html": "arkleg.state.ar.us/Bills",
#         "Bills_SearchByRange.v1.html": "arkleg.state.ar.us/Bills/SearchByRange",
#         "Bills_ViewBills.v1.html": "arkleg.state.ar.us/Bills/ViewBills",
#         "Bills_Detail.v1.html": "arkleg.state.ar.us/Bills/Detail",
#         "Bills_Votes.v1.html": "arkleg.state.ar.us/Bills/Votes",
#         "Committees_List.v1.html": "arkleg.state.ar.us/Committees/List",
#         "Committees_Detail.v1.html": "arkleg.state.ar.us/Committees/Detail",
#     }
#
#     for file_name, url in file_to_url_map.items():
#         fp = fixture_dir / file_name
#         if fp.exists():
#             url_soups[url] = fp.read_text(encoding="utf-8")
#     patcher.start()
#
#     orch = Orchestrator(
#         registry=registry,
#         seed_urls=[seed_url],
#         db_conn=test_db,
#         state=state,
#         strict=False,
#     )
#
#     # --- Run orchestrator ---
#     orch.orchestrate()
#     patcher.stop()  # remove patch
#
#     # --- Assertions ---
#
#     # All queues empty
#     for stage, q in orch.queues.items():
#         assert q.qsize() == 0, f"Queue {stage} not empty after run"
#
#     # state.roots empty
#     assert len(orch.state.roots) == 0, "state.roots should be empty after run"
#
#     # All nodes completed
#     for node in orch.state.nodes.values():
#         assert node.state == PipelineStateEnum.COMPLETED, f"Node {node.url} not completed"
#
#     # Loader inserted rows into DB (example: bills table)
#     with test_db.cursor() as cur:
#         cur.execute("SELECT COUNT(*) FROM bills;")
#         num_bills = cur.fetchone()[0]
#     assert num_bills > 0, "Expected bills in DB"
#
#     # Optional: visited URLs contains at least the seed
#     assert seed_url in orch.visited
#
#     # --- Test state save/load ---
#     orch.state.save_file(Path(tmp_cache_files["state_cache"]))
#     new_state = DirectionalGraph()
#     new_state.load_from_file(Path(tmp_cache_files["state_cache"]))
#     # New state should have all nodes with same URLs
#     assert set(new_state.nodes.keys()) == set(orch.state.nodes.keys())
