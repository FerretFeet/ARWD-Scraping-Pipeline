# test_orchestrator_fixed.py
import unittest
from pathlib import Path
from queue import LifoQueue, Queue
from unittest.mock import MagicMock, PropertyMock, patch
from urllib.parse import urlparse

from src.config.pipeline_enums import PipelineRegistries

# Assuming src.data_pipeline.orchestrate is the module containing Orchestrator
# and src.workers.base_worker is the module containing BaseWorker.
# Note: Renaming import paths to reflect the structure from the prompt's traceback.
from src.data_pipeline.orchestrate import Orchestrator
from src.structures.indexed_tree import IndexedTree, PipelineStateEnum
from src.structures.indexed_tree import Node as IndexedTreeNode
from src.structures.registries import ProcessorRegistry
from src.workers.base_worker import BaseWorker


# --- Mock Classes for Dependencies ---
class MockIndexedTree:
    """A minimal mock for indexed_tree.IndexedTree."""

    def __init__(self, name):
        self.name = name
        self.root = None  # Simulate initial state
        self.nodes = {} # Added to track nodes for integration test

    def load_from_file(self, path):
        # Simulate successful load if path contains a specific string
        if "loaded" in str(path):
            self.root = "MOCK_ROOT"  # Indicate a loaded state
            return True
        return False

    def reconstruct_order(self):
        # Return a list of mock nodes for loaded states
        if self.root:
            return [
                MagicMock(
                    spec=IndexedTreeNode,
                    url="http://loaded.com/1",
                    state=PipelineStateEnum.AWAITING_FETCH,
                ),
            ]
        return []

    def add_node(self, **kwargs):
        # Mock node creation
        mock_node = MagicMock(spec=IndexedTreeNode, **kwargs)
        mock_node.url = kwargs.get("url")
        mock_node.netloc = "testdomain.com"
        # Track the node
        self.nodes[mock_node.url] = mock_node
        if kwargs.get("parent") is None:
            self.root = mock_node
        return mock_node

# ----------------------------------------------------------------------
## 🧪 Unit Tests (TestOrchestratorUnit)
# ----------------------------------------------------------------------

class TestOrchestratorUnit(unittest.TestCase):
    def setUp(self):
        """Set up a fresh Orchestrator instance for each test."""
        # Mock external classes/objects
        self.mock_registry = MagicMock(spec=ProcessorRegistry)
        self.mock_db_conn = MagicMock()

        # Mock state classes (we use the custom mock above)
        self.MockStateTree = MockIndexedTree

        # Mock queues (using real queues but can be inspected)
        self.crawler_queue = LifoQueue()
        self.processor_queue = Queue()
        self.loader_queue = Queue()

        self.seed_urls = [
            "http://example.com/page1",
            "http://anothersite.org/start",
            "http://example.com/page2",
        ]

        # Initialize Orchestrator with minimal mocks
        self.orchestrator = Orchestrator(
            registry=self.mock_registry,
            seed_urls=self.seed_urls,
            db_conn=self.mock_db_conn,
            state_tree=self.MockStateTree,
            # Mock complex dependencies to be passed to workers
            crawler=MagicMock(),
            parser=MagicMock(),
            transformer=MagicMock(),
            fetch_scheduler=MagicMock(),
        )

    # Note: Patch paths updated to use 'src.data_pipeline.orchestrate'
    @patch("src.data_pipeline.orchestrate.load_json_list", return_value=["http://example.com/page1"])
    def test_filter_and_organize_seed_urls(self, mock_load_json_list):
        """Test initial filtering and domain organization."""
        # The Orchestrator's __init__ runs the filtering and organizing

        # Re-initialize to test __init__ logic cleanly
        orchestrator_init_test = Orchestrator(
            registry=self.mock_registry,
            seed_urls=self.seed_urls,
            db_conn=self.mock_db_conn,
            state_tree=self.MockStateTree,
        )

        # Check Organization: urls should be grouped by domain
        expected_organized_urls = {
            "anothersite.org": ["http://anothersite.org/start"],
            "example.com": ["http://example.com/page2"],
        }

        self.assertEqual(orchestrator_init_test.seed_urls, expected_organized_urls)

    def test_next_seed(self):
        """Test popping the next seed URL from the correct domain."""
        # Initial state setup
        self.orchestrator.seed_urls = {
            "example.com": ["http://example.com/page1", "http://example.com/page2"],
            "anothersite.org": ["http://anothersite.org/start"],
        }

        # Pop 1 from example.com
        url1 = self.orchestrator._next_seed("example.com")
        self.assertEqual(url1, "http://example.com/page1")

        # Pop 1 from anothersite.org
        url2 = self.orchestrator._next_seed("anothersite.org")
        self.assertEqual(url2, "http://anothersite.org/start")

        # Pop the last one from example.com
        url3 = self.orchestrator._next_seed("example.com")
        self.assertEqual(url3, "http://example.com/page2")

        # Attempt to pop from an empty list
        url_none = self.orchestrator._next_seed("anothersite.org")
        self.assertIsNone(url_none)

    @patch("src.data_pipeline.orchestrate.state_cache_file", new=Path("/mock/cache"))
    def test_setup_states_and_load_from_cache(self):
        """Test state setup and loading from mock cache file."""
        seed_urls = {
            "newdomain.com": ["http://newdomain.com/start"],
            "loadeddomain.com": ["http://loadeddomain.com/start"],
        }

        nodes_to_stack = self.orchestrator.setup_states(
            seed_urls, cache_base_path=Path("/mock/cache"),
        )

        # Check states were created
        self.assertIn("newdomain.com", self.orchestrator.state)
        self.assertIn("loadeddomain.com", self.orchestrator.state)

        # Check loaded domain has a root (simulating successful load)
        self.assertIsNotNone(self.orchestrator.state["loadeddomain.com"].root)

        # Check new domain does not have a root (simulating first run)
        self.assertIsNone(self.orchestrator.state["newdomain.com"].root)

        # Check nodes_to_stack returns interleaved results from loaded domains
        self.assertEqual(len(nodes_to_stack), 1)  # Only one mock node from loadeddomain
        self.assertEqual(nodes_to_stack[0].url, "http://loaded.com/1")

    @patch("src.data_pipeline.orchestrate.get_enum_by_url", return_value="MOCK_ENUM")
    @patch("src.data_pipeline.orchestrate.get_url_base_path", return_value="http://testdomain.com")
    def test_enqueue_links_string_url(self, mock_get_base_path, mock_get_enum):
        """Test enqueuing a single string URL as a new root node."""

        # 1. Setup a state for the domain
        domain = "testdomain.com"
        self.orchestrator.state[domain] = self.MockStateTree(name=domain)

        test_url = "http://testdomain.com/new"

        self.orchestrator._enqueue_links(test_url, self.crawler_queue)

        # 1. Check the queue size
        self.assertEqual(self.crawler_queue.qsize(), 1)

        # 2. Check the correct node was added to the queue
        mock_node = self.crawler_queue.get()
        self.assertEqual(mock_node.url, test_url)

        # 3. Check the correct node was added to the visited list
        self.assertIn(test_url, self.orchestrator.visited)

        # 4. Check the state was correctly set
        self.assertEqual(mock_node.state, PipelineStateEnum.AWAITING_FETCH)

    def test_enqueue_links_list_of_nodes(self):
        """Test enqueuing a list of existing nodes."""

        mock_node1 = MagicMock(spec=IndexedTreeNode, url="http://node1.com")
        mock_node2 = MagicMock(spec=IndexedTreeNode, url="http://node2.com")
        enqueue_list = [mock_node1, mock_node2]

        self.orchestrator._enqueue_links(enqueue_list, self.crawler_queue)

        # 1. Check the queue size
        self.assertEqual(self.crawler_queue.qsize(), 2)

        # 2. Check the nodes were added and state updated (LIFO order: 2 then 1)
        q_item1 = self.crawler_queue.get()
        q_item2 = self.crawler_queue.get()

        self.assertEqual(q_item1, mock_node2)
        self.assertEqual(q_item2, mock_node1)

        # 3. Check visited list
        self.assertIn("http://node1.com", self.orchestrator.visited)
        self.assertIn("http://node2.com", self.orchestrator.visited)

    @patch("src.config.pipeline_enums.PipelineRegistries.FETCH.get_worker_class")
    @patch("src.config.pipeline_enums.PipelineRegistries.PROCESS.get_worker_class")
    @patch("src.config.pipeline_enums.PipelineRegistries.LOAD.get_worker_class")
    def test_setup_workers(self, MockLoadWorkerCls, MockProcessWorkerCls, MockFetchWorkerCls):
        """Test initialization of worker objects with correct dependencies."""

        mock_crawler_instance = MockCrawlerWorker = MagicMock(name="MockCrawlerWorker")
        mock_processor_instance = MockProcessorWorker = MagicMock(name="MockProcessorWorker")
        mock_loader_instance = MockLoaderWorker = MagicMock(name="MockLoaderWorker")

        # Patch the get_worker_class() to return a callable that returns the mock instance
        MockFetchWorkerCls.return_value = lambda *a, **kw: mock_crawler_instance
        MockProcessWorkerCls.return_value = lambda *a, **kw: mock_processor_instance
        MockLoadWorkerCls.return_value = lambda *a, **kw: mock_loader_instance

        w_crawler, w_processor, w_loader = self.orchestrator._setup_workers()

        # Now you can assert the returned objects
        self.assertIs(w_crawler, MockCrawlerWorker)
        self.assertIs(w_processor, MockProcessorWorker)
        self.assertIs(w_loader, MockLoaderWorker)

    @patch("src.data_pipeline.orchestrate.BaseWorker")
    def test_start_workers(self, MockBaseWorker):
        """Test calling start() on all workers."""
        worker1 = MagicMock(spec=BaseWorker)
        worker2 = MagicMock(spec=BaseWorker)

        self.orchestrator.start_workers([worker1, worker2])

        worker1.start.assert_called_once()
        worker2.start.assert_called_once()

    @patch("src.data_pipeline.orchestrate.BaseWorker")
    def test_shutdown_workers(self, MockBaseWorker):
        """Test sending None to queues and calling join() on workers."""
        worker1 = MagicMock(spec=BaseWorker)
        worker2 = MagicMock(spec=BaseWorker)

        q1 = Queue()
        q2 = Queue()

        self.orchestrator.shutdown_workers([q1, q2], [worker1, worker2])

        # Check None was added to queues
        self.assertIsNone(q1.get_nowait())
        self.assertIsNone(q2.get_nowait())

        # Check workers were joined
        worker1.join.assert_called_once()
        worker2.join.assert_called_once()


# ----------------------------------------------------------------------
## 🧪 Integration Test (TestOrchestratorIntegration)
# ----------------------------------------------------------------------
class SyncMockWorker:
    def __init__(self, *args, **kwargs):
        self.input_queue = kwargs.get("input_queue")
        self.output_queue = kwargs.get("output_queue")
        self.state = kwargs.get("state_tree")  # dict of site_key -> IndexedTree
        self.name = kwargs.get("name", "MOCK")
        self.stage_label = kwargs.get("stage_label", "")
        self.processed_count = 0

    def start(self): pass
    def join(self): pass

    def process_one_item(self):
        try:
            item = self.input_queue.get_nowait()
        except Exception:
            return False

        if item is None:
            if self.output_queue:
                self.output_queue.put(None)
            return "SENTINEL"

        self.processed_count += 1

        # Only add node in the crawler stage
        if self.stage_label == "FETCH":
            url = getattr(item, "url", item)
            site_key = urlparse(url).netloc.lower()
            tree = self.state.setdefault(site_key, IndexedTree(name=site_key))
            tree.add_node(url=url, node_type="MOCK_NODE", parent=None)

        if self.output_queue:
            self.output_queue.put(item)

        self.input_queue.task_done()
        return True


class SyncMockLoaderWorker(SyncMockWorker):
    """Synchronous mock for the final Loader stage."""
    def process_one_item(self):
        try:
            item = self.input_queue.get_nowait()
        except Exception:
            return False

        if item is None:
            return "SENTINEL"

        self.processed_count += 1
        self.input_queue.task_done()
        return True

# ------------------------------
# Integration test
# ------------------------------
@patch("src.data_pipeline.orchestrate.load_json_list", return_value=[])
@patch("src.data_pipeline.orchestrate.get_enum_by_url", return_value="MOCK_ENUM")
@patch("src.data_pipeline.orchestrate.get_url_base_path", side_effect=lambda url: url)
@patch("src.data_pipeline.orchestrate.Orchestrator.orchestrate")
class TestOrchestratorSynchronousIntegration(unittest.TestCase):

    @patch.object(PipelineRegistries.FETCH, "get_worker_class", new_callable=PropertyMock)
    @patch.object(PipelineRegistries.PROCESS, "get_worker_class", new_callable=PropertyMock)
    @patch.object(PipelineRegistries.LOAD, "get_worker_class", new_callable=PropertyMock)
    def test_full_orchestration_flow(
        self, mock_load_class, mock_process_class, mock_fetch_class,
        mock_orchestrate_method, mock_get_base_path, mock_get_enum, mock_load_json_list,
    ):
        # Assign synchronous mock workers
        mock_fetch_class.return_value = SyncMockWorker
        mock_process_class.return_value = SyncMockWorker
        mock_load_class.return_value = SyncMockLoaderWorker

        # Seed URLs as a list
        seed_urls = ["http://siteA.com/start", "http://siteB.com/start"]

        # Instantiate orchestrator with real IndexedTree
        orchestrator = Orchestrator(
            registry=MagicMock(),
            seed_urls=seed_urls,
            db_conn=MagicMock(),
            state_tree=IndexedTree,
            crawler=MagicMock(),
            parser=MagicMock(),
            transformer=MagicMock(),
            fetch_scheduler=MagicMock(),
        )

        # Setup state and queues
        unvisited_nodes = orchestrator.setup_states(orchestrator.seed_urls, cache_base_path=Path("/mock/cache"))
        orchestrator._load_queues(unvisited_nodes)

        crawler_queue = orchestrator.queues[PipelineRegistries.FETCH]
        processor_queue = orchestrator.queues[PipelineRegistries.PROCESS]
        loader_queue = orchestrator.queues[PipelineRegistries.LOAD]

        # Put all seed URLs in the crawler queue
        for urls in orchestrator.seed_urls.values():
            for url in urls:
                crawler_queue.put(url)

        # Setup workers
        w_crawler, w_processor, w_loader = orchestrator._setup_workers()

        # Process items manually
        expected_items = len(seed_urls)
        processed_total = 0
        MAX_CYCLES = 10

        for _ in range(MAX_CYCLES):
            for w in [w_crawler, w_processor, w_loader]:
                result = w.process_one_item()
                if result is True:
                    processed_total += 1
            if processed_total >= expected_items * 3:
                break

        self.assertEqual(processed_total, expected_items * 3, f"Pipeline did not process all items (got {processed_total})")

        # Shutdown workers
        orchestrator.shutdown_workers([crawler_queue, processor_queue, loader_queue],
                                      [w_crawler, w_processor, w_loader])

        # Process sentinels
        for _ in range(6):
            for w in [w_crawler, w_processor, w_loader]:
                w.process_one_item()

        # Assert queues are empty
        self.assertTrue(crawler_queue.empty())
        self.assertTrue(processor_queue.empty())
        self.assertTrue(loader_queue.empty())

        # Assert all workers processed all items
        self.assertEqual(w_crawler.processed_count, expected_items)
        self.assertEqual(w_processor.processed_count, expected_items)
        self.assertEqual(w_loader.processed_count, expected_items)

        # Assert trees have nodes
        for url in seed_urls:
            site_key = urlparse(url).netloc.lower()
            tree = orchestrator.state[site_key]
            self.assertIsNotNone(tree.root)
            self.assertEqual(len(tree.nodes), 1)
