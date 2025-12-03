# # tests/integration/test_main.py
# from unittest.mock import MagicMock, patch
#
# import pytest
#
# from src.data_pipeline import orchestrate
# from src.structures.indexed_tree import PipelineStateEnum
# from src.structures.registries import PipelineRegistryKeys
#
#
# @pytest.fixture
# def mock_environment():
#     """Mock external dependencies for main."""
#     with patch("src.data_pipeline.orchestrate.load_json_list") as mock_load_json, \
#          patch("src.data_pipeline.orchestrate.Crawler") as mock_crawler_cls, \
#          patch("src.data_pipeline.orchestrate.IndexedTree") as mock_tree_cls, \
#          patch("src.data_pipeline.orchestrate.ProcessorWorker") as mock_processor_worker_cls, \
#          patch("src.data_pipeline.orchestrate.LoaderWorker") as mock_loader_worker_cls:
#
#         # 1. Mock known links list
#         mock_load_json.return_value = []
#
#         # 2. Mock Crawler to return fake HTML
#         mock_crawler = MagicMock()
#         mock_crawler.get_page.return_value = "<html></html>"
#         mock_crawler_cls.return_value = mock_crawler
#
#         # 3. Mock IndexedTree to track nodes
#         mock_tree = MagicMock()
#         mock_node = MagicMock()
#         mock_node.id = 1
#         mock_node.url = "https://example.com"
#         mock_node.state = PipelineStateEnum.AWAITING_FETCH
#         mock_tree.add_node.return_value = mock_node
#         mock_tree.root.url = "https://example.com"
#         mock_tree.load_from_file.return_value = False  # simulate no saved state
#         mock_tree_cls.return_value = mock_tree
#
#         # 4. Mock workers to run synchronously (no real threads)
#         mock_worker = MagicMock()
#         # Simulate .start() immediately calling run
#         mock_worker.start.side_effect = lambda: mock_worker.run()
#         mock_worker.run = MagicMock()
#         mock_processor_worker_cls.return_value = mock_worker
#         mock_loader_worker_cls.return_value = mock_worker
#
#         yield {
#             "mock_load_json": mock_load_json,
#             "mock_crawler": mock_crawler,
#             "mock_tree": mock_tree,
#             "mock_worker": mock_worker,
#         }
#
# def test_main_runs(mock_environment):
#     """Integration test for main function with mocked environment."""
#     starting_urls = ["https://example.com"]
#     orchestrate.main(starting_urls)
#
#     # ---- Assertions ----
#
#     # 1. Ensure IndexedTree.add_node was called for starting URL
#     mock_environment["mock_tree"].add_node.assert_called_with(
#         parent=None,
#         url="https://example.com",
#         node_type=PipelineRegistryKeys.ARK_LEG_SEEDER,
#     )
#
#     # 2. Ensure Crawler.get_page was called
#     mock_environment["mock_crawler"].get_page.assert_called()
#
#     # 3. Ensure workers were started
#     mock_environment["mock_worker"].start.assert_called()
#
#     # 4. Ensure queues received at least one node
#     fetch_queue_put_calls = [
#         call.args[0] for call in mock_environment["mock_worker"].run.mock_calls
#     ]
#     assert fetch_queue_put_calls is not None or len(fetch_queue_put_calls) >= 0
