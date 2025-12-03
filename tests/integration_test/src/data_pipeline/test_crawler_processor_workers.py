# import threading
# import time
# from collections.abc import Callable
# from pathlib import Path
# from queue import Empty, LifoQueue, Queue
# from typing import Any
# from unittest.mock import MagicMock, patch
# from urllib.parse import urlparse
#
# import pytest
#
# from src.structures.indexed_tree import PipelineStateEnum
# from src.structures.registries import PipelineRegistries, PipelineRegistryKeys, get_enum_by_url
# from src.workers.pipeline_workers import CrawlerWorker, LoaderObj, ProcessorWorker
#
# logger = MagicMock()
#
# class MockNode:
#     def __init__(self, id, url, state, node_type, parent=None):
#         self.id = id
#         self.url = url
#         self.state = state
#         self.type = node_type
#         self.parent = parent
#         self.data = {}
#         self.lock = threading.Lock()
#
#
# class MockIndexedTree:
#     def __init__(self):
#         self.nodes = {}
#         self.next_id = 1
#
#     def add_node(self, parent, url, state, node_type):
#         new_node = MockNode(self.next_id, url, state, node_type, parent)
#         self.nodes[self.next_id] = new_node
#         self.next_id += 1
#         return new_node
#
#     def get_node(self, node_id):
#         return self.nodes.get(node_id)  # Added for completeness
#
#
# BILL_LIST_FIXTURE_PARAMS = [
#     ("bill_list_page/bill_list", "https://www.arkleg.state.ar.us/Bills/ViewBills?&type=HB&ddBienniumSession=2025%2F2025R", "v1"),
# ]
# BILL_FIXTURE_PARAMS = [
#     ("bill_page/bill", "https://www.arkleg.state.ar.us/Bills/Detail?id=HB1001&ddBienniumSession=2025%2F2025R", "v1"),
#     ("bill_page/bill", "https://www.arkleg.state.ar.us/Bills/Detail?id=HB1002&ddBienniumSession=2025%2F2025R", "v2"),
# ]
#
# ALL_FIXTURE_PARAMS = BILL_LIST_FIXTURE_PARAMS + BILL_FIXTURE_PARAMS
#
# def create_fixture_map(fixture_params):
#     url_map = {}
#     for name, full_url, variant in fixture_params:
#         url_stem = urlparse(full_url.split("?", 1)[0]).path
#         if url_stem not in url_map:
#             url_map[url_stem] = []
#         url_map[url_stem].append({
#             "fixture_name": name,
#             "full_url": full_url,
#             "variant": variant,
#             "relative_file_path": Path(f"html/{name}.{variant}.html"),
#         })
#     return url_map
#
# URL_STEM_FIXTURE_MAP = create_fixture_map(ALL_FIXTURE_PARAMS)
#
# # --- 4. URL and Processor Resolution Utilities (Needed for patching) ---
#
# # def get_enum_by_url(url: str) -> PipelineRegistryKeys:
# #     """Resolves the URL to a specific PipelineRegistryKeys enum."""
# #     if "Bills/Detail" in url:
# #         return PipelineRegistryKeys.BILL
# #     if "ViewBills" in url:
# #         return PipelineRegistryKeys.BILL_LIST
# #     # Fallback for worker logic that might pass only the stem
# #     if url == "/Bills/Detail":
# #         return PipelineRegistryKeys.BILL
# #     if url == "/Bills/ViewBills":
# #         return PipelineRegistryKeys.BILL_LIST
# #     return PipelineRegistryKeys.BILL_LIST # Defaulting for test stability
#
# # --- 5. Concrete/Mocked Pipeline Components ---
#
# class ConcreteCrawler:
#     def __init__(self, base_path: Path):
#         self.base_path = base_path
#
#     def get_page(self, url_stem: str) -> str:
#         fixture_data_list = URL_STEM_FIXTURE_MAP.get(url_stem)
#         if not fixture_data_list:
#             raise ValueError(f"URL stem '{url_stem}' not found in local fixture map.")
#
#         # For simplicity, we use the first variant found
#         fixture_variant = fixture_data_list[0]
#         file_path: Path = self.base_path / fixture_variant["relative_file_path"]
#
#         if not file_path.exists():
#             raise FileNotFoundError(f"Fixture file not found at: {file_path}")
#
#         # 🟢 FIX: Read the actual content of the generated HTML file
#         content = file_path.read_text(encoding="utf-8")
#
#         # NOTE: Remove all the old manual content injection logic!
#         # Your HTML files must now contain the markers (e.g., "HB1001 Content")
#         # that the ConcreteHTMLParser relies on.
#
#         return content
#
#
# class ConcreteHTMLParser:
#     def __init__(self, strict: bool):
#         pass
#
#     def get_content(self, parsing_template: dict[str, Any], html_text: str) -> dict[str, Any]:
#         # ---------------------------------------------
#         # FETCH stage: BILL_LIST
#         # ---------------------------------------------
#         if "bill_list_selector_key" in parsing_template:
#             # Return ONLY URLs for crawler
#             bill_details = URL_STEM_FIXTURE_MAP.get("/Bills/Detail", [])
#             return {"bill_url": [entry["full_url"] for entry in bill_details]}
#
#         # ---------------------------------------------
#         # PROCESS stage: BILL
#         # ---------------------------------------------
#         if "HB1001" in html_text:
#             return {
#                 "title": "An Act To Test HB1001",
#                 "bill_no": "HB1001",
#                 "orig_chamber": "House",
#             }
#
#         if "HB1002" in html_text:
#             return {
#                 "title": "An Act To Test HB1002",
#                 "bill_no": "HB1002",
#                 "orig_chamber": "House",
#             }
#
#         # default
#         return {}
#
#
# class MockProcessorRegistry:
#     def get_processor(self, key: PipelineRegistryKeys, registry: PipelineRegistries) -> dict[str, Any]:
#
#         if registry == PipelineRegistries.FETCH and key == PipelineRegistryKeys.BILL_LIST:
#             return {"bill_list_selector_key": "links"}
#
#         if registry == PipelineRegistries.PROCESS and key == PipelineRegistryKeys.BILL:
#             # Define transformations
#             return {
#                 "title": ("selector_title", lambda data: data.upper()), # Transformation
#                 "bill_no": ("selector_bill_no", lambda data: data),
#                 "orig_chamber": ("selector_chamber", lambda data: data),
#                 "state_key": (lambda node, state: {}, lambda x: x),
#             }
#         return {}
#
# class MockPipelineTransformer:
#     def __init__(self, strict: bool):
#         pass
#
#     def transform_content(self, template: dict[str, Callable], parsed_data: dict[str, Any]) -> dict[str, Any]:
#         transformed_data = {}
#
#         for final_key, transform_func in template.items():
#             if callable(transform_func) and final_key in parsed_data:
#                 transformed_data[final_key] = transform_func(parsed_data[final_key])
#             elif callable(transform_func) and not parsed_data:
#                 pass
#
#         return transformed_data
#
#
# # --- 6. Pytest Fixtures ---
#
# @pytest.fixture
# def fixture_dir_and_data(tmp_path):
#     """Creates the temporary fixture files needed by the ConcreteCrawler."""
#     base_dir = tmp_path
#
#     for url_stem, variants in URL_STEM_FIXTURE_MAP.items():
#         for data in variants:
#             file_path: Path = base_dir / data["relative_file_path"]
#             file_path.parent.mkdir(parents=True, exist_ok=True)
#
#             # Create a simple placeholder file (content handled by ConcreteCrawler mock)
#             file_path.write_text(f"Placeholder for {data['full_url']}", encoding="utf-8")
#
#     return base_dir
#
# # --- 7. The Integration Test ---
#
# def test_full_worker_pipeline_with_fixtures(fixture_dir_and_data):
#
#     # 1. Setup Queues and State
#     fetch_q = LifoQueue()
#     process_q = LifoQueue()
#     output_q = Queue()
#     state_tree = MockIndexedTree()
#
#     # 2. Setup Concrete/Integrated Components
#     concrete_crawler = ConcreteCrawler(base_path=fixture_dir_and_data)
#     concrete_registry = MockProcessorRegistry()
#     WORKER_MODULE = "src.workers.pipeline_workers"
#     # 3. Instantiate Workers and CORRECT PATCHING
#     # We patch the components in the module where the workers look for them (WORKER_MODULE)
#     with patch(f"{WORKER_MODULE}.get_enum_by_url", new=get_enum_by_url), \
#             patch(f"{WORKER_MODULE}.HTMLParser", new=ConcreteHTMLParser), \
#             patch(f"{WORKER_MODULE}.PipelineTransformer", new=MockPipelineTransformer), \
#             patch(f"{WORKER_MODULE}.IndexedTree", new=MockIndexedTree), \
#             patch(f"{WORKER_MODULE}.Crawler", new=MagicMock(return_value=concrete_crawler)), \
#             patch(f"{WORKER_MODULE}.ProcessorRegistry", new=MagicMock(return_value=concrete_registry)):
#
#         # The workers are instantiated and will use the patched components
#         crawler_worker = CrawlerWorker(fetch_q, process_q, state_tree, concrete_crawler, concrete_registry, strict=True)
#         processor_worker = ProcessorWorker(process_q, output_q, state_tree, concrete_registry, strict=True)
#
#         # 4. Start Workers
#         crawler_worker.start()
#         processor_worker.start()
#
#         # 5. Seed the Pipeline
#         initial_url = URL_STEM_FIXTURE_MAP["/Bills/ViewBills"][0]["full_url"]
#         initial_node = state_tree.add_node(
#             parent=None, url=initial_url, state=PipelineStateEnum.AWAITING_FETCH,
#             node_type=PipelineRegistryKeys.BILL_LIST,
#         )
#         fetch_q.put(initial_node)
#
#         # 6. Wait for all tasks
#         fetch_q.join()
#         process_q.join()
#         time.sleep(0.5)
#
#         # 7. Stop Workers
#         crawler_worker.running = False
#         processor_worker.running = False
#         fetch_q.put(None)
#         process_q.put(None)
#         crawler_worker.join()
#         processor_worker.join()
#
#         # 8. Assertions
#         # Initial Bill List + 2 Bill Details = 3
#         assert output_q.qsize() == 3, f"Expected 3 items in the output queue, got {output_q.qsize()}"
#
#         output_items: list[LoaderObj] = []
#         try:
#             while True:
#                 item = output_q.get_nowait()
#                 if item is None: continue
#                 output_items.append(item)
#         except Empty:
#             pass
#
#         # Assert transformation was applied to the detail pages
#         bill_1_obj = next(obj for obj in output_items if "HB1001" in obj.node.url)
#         bill_2_obj = next(obj for obj in output_items if "HB1002" in obj.node.url)
#
#         assert bill_1_obj.params["title"] == "AN ACT TO TEST HB1001", "Transformation to uppercase failed."
#         assert bill_2_obj.params["title"] == "AN ACT TO TEST HB1002", "Transformation to uppercase failed."
#
#         print("✅ Integration test passed successfully!")
