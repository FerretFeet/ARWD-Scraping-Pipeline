# test_orchestrator_fixed.py
import unittest
from pathlib import Path
from queue import LifoQueue, Queue
from unittest.mock import MagicMock, patch

# Assuming src.data_pipeline.orchestrate is the module containing Orchestrator
# and src.workers.base_worker is the module containing BaseWorker.
# Note: Renaming import paths to reflect the structure from the prompt's traceback.
from src.data_pipeline.orchestrate import Orchestrator
from src.structures.indexed_tree import Node as IndexedTreeNode
from src.structures.indexed_tree import PipelineStateEnum
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
            crawler_queue=self.crawler_queue,
            processor_queue=self.processor_queue,
            loader_queue=self.loader_queue,
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

    @patch("src.data_pipeline.orchestrate.CrawlerWorker")
    @patch("src.data_pipeline.orchestrate.ProcessorWorker")
    @patch("src.data_pipeline.orchestrate.LoaderWorker")
    def test_setup_workers(self, MockLoaderWorker, MockProcessorWorker, MockCrawlerWorker):
        """Test initialization of worker objects with correct dependencies."""

        w_crawler, w_processor, w_loader = self.orchestrator._setup_workers()

        # 1. Check if the worker constructors were called
        self.assertTrue(MockCrawlerWorker.called)
        self.assertTrue(MockProcessorWorker.called)
        self.assertTrue(MockLoaderWorker.called)

        # 2. Check the arguments passed to CrawlerWorker
        (args, kwargs) = MockCrawlerWorker.call_args
        self.assertEqual(args[0], self.crawler_queue)
        self.assertEqual(args[1], self.processor_queue)
        self.assertEqual(args[2], self.orchestrator.state)
        # ... other checks remain valid ...
        self.assertEqual(kwargs["name"], "CRAWLER")

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

class MockWorker(BaseWorker):
    """
    Mock Worker for middle stages (Crawler/Processor).
    Handles corrected BaseWorker init, propagates sentinel, and calls task_done()
    only for actual work items.
    """
    def __init__(self, input_queue, output_queue, state, *args, **kwargs):
        # FIX 1: Corrected super() call to match BaseWorker/threading.Thread signature
        super().__init__(input_queue, name=kwargs.get("name", "MOCK"))
        self.output_queue = output_queue
        self.state = state
        self.processed_count = 0
        self.running = True

    def run(self):
        while self.running:
            item = self.input_queue.get()

            if item is None:
                # FIX 2A: Correctly handle sentinel (None)
                self.running = False

                # Propagate sentinel to the next stage
                if self.output_queue:
                    self.output_queue.put(None)

                # IMPORTANT: DO NOT call task_done() for the sentinel.
                continue

            # --- Actual Work Item Processing ---
            self.processed_count += 1

            # Put item in the next queue
            if self.output_queue:
                self.output_queue.put(item)

            # Signal done on the input queue for this work item
            self.input_queue.task_done()

    def join(self):
        self.input_queue.join()
        super().join()


class MockLoaderWorker(BaseWorker):
    """
    Mock Worker for the final stage (Loader).
    Handles corrected BaseWorker init, consumes sentinel, and calls task_done()
    to signal completion of work on the final queue.
    """
    def __init__(self, input_queue, output_queue, state, *args, **kwargs):
        # FIX 1: Corrected super() call
        super().__init__(input_queue, name=kwargs.get("name", "MOCK"))
        self.output_queue = output_queue # Will be None
        self.state = state
        self.processed_count = 0
        self.running = True

    def run(self):
        while self.running:
            item = self.input_queue.get()

            if item is None:
                # FIX 2B: Correctly handle sentinel (None) for the final worker
                self.running = False
                # Do NOT call task_done() or propagate
                continue

            # --- Actual Work Item Processing ---
            self.processed_count += 1

            # Signal done on the input queue for this work item (The loader_queue)
            self.input_queue.task_done()

    def join(self):
        self.input_queue.join()
        super().join()


class SyncMockWorker:
    """Mock worker for middle stages (Crawler/Processor) that executes synchronously."""
    def __init__(self, input_queue, output_queue, state, *args, **kwargs):
        # Must accept all args passed by Orchestrator._setup_workers
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.state = state
        self.name = kwargs.get("name", "MOCK")
        self.processed_count = 0

    def start(self):
        # We don't start a thread
        pass

    def join(self):
        # We don't join a thread
        pass

    def process_one_item(self):
        """Pulls one item, processes it, and returns a flag indicating status."""
        try:
            # Use get_nowait() to avoid blocking
            item = self.input_queue.get_nowait()
        except self.input_queue.Empty:
            return False # No work done

        if item is None:
            # Sentinel received, propagate it and return flag
            if self.output_queue:
                self.output_queue.put(None)
            return "SENTINEL"

        # --- Actual Work Item Processing ---
        self.processed_count += 1

        # Propagate the item
        if self.output_queue:
            self.output_queue.put(item)

        # Signal done on the input queue for this work item
        self.input_queue.task_done()

        return True # Work item processed and propagated

class SyncMockLoaderWorker(SyncMockWorker):
    """Synchronous Mock for the final Loader stage."""
    def process_one_item(self):
        try:
            item = self.input_queue.get_nowait()
        except self.input_queue.Empty:
            return False # No work done

        if item is None:
            return "SENTINEL" # Stop processing, do not propagate

        self.processed_count += 1

        # Final stage, call task_done() and do not propagate
        self.input_queue.task_done()
        return True # Work item processed and finished

# --- Mock Indexed Tree (as defined in unit test section) ---

# ----------------------------------------------------------------------

@patch("src.data_pipeline.orchestrate.CrawlerWorker", new=SyncMockWorker)
@patch("src.data_pipeline.orchestrate.ProcessorWorker", new=SyncMockWorker)
@patch("src.data_pipeline.orchestrate.LoaderWorker", new=SyncMockLoaderWorker)
@patch("src.data_pipeline.orchestrate.load_json_list", return_value=[])
@patch("src.data_pipeline.orchestrate.get_enum_by_url", return_value="MOCK_ENUM")
@patch("src.data_pipeline.orchestrate.get_url_base_path", side_effect=lambda url: url)
# We mock 'orchestrate' to prevent the real threading/joining logic from running
@patch("src.data_pipeline.orchestrate.Orchestrator.orchestrate")
class TestOrchestratorSynchronousIntegration(unittest.TestCase):

    MockStateTree = MockIndexedTree

    def setUp(self):
        # Initialize Queues
        self.crawler_queue = LifoQueue()
        self.processor_queue = Queue()
        self.loader_queue = Queue()

        self.seed_urls = ["http://siteA.com/start", "http://siteB.com/start"]

        # Instantiate the Orchestrator
        self.orchestrator = Orchestrator(
            registry=MagicMock(),
            seed_urls=self.seed_urls,
            db_conn=MagicMock(),
            state_tree=self.MockStateTree,
            crawler_queue=self.crawler_queue,
            processor_queue=self.processor_queue,
            loader_queue=self.loader_queue,
            crawler=MagicMock(), parser=MagicMock(), transformer=MagicMock(), fetch_scheduler=MagicMock(),
        )

    def test_full_orchestration_flow(self, mock_orchestrate_method, mock_get_base_path, mock_get_enum, mock_load_json_list):
        """Tests the entire orchestration flow synchronously by manually cycling workers."""

        # 1. Simulate Setup Phase: This populates self.orchestrator.state with domain keys.
        unvisited_nodes = self.orchestrator.setup_states(
            self.orchestrator.seed_urls, cache_base_path=Path("/mock/cache"),
        )
        self.orchestrator._load_queues(unvisited_nodes)

        # 2. Get the synchronous workers (they are NOT threads)
        w_crawler, w_processor, w_loader = self.orchestrator._setup_workers()

        # 3. Manually run the pipeline for the 2 seed items (2 items * 3 stages = 6 total process steps)
        expected_items = 2
        processed_total = 0

        # Use a safe maximum loop count to avoid true infinite hang
        MAX_CYCLES = 10

        for _ in range(MAX_CYCLES):

            # Process one step for the Crawler
            if w_crawler.process_one_item() is True:
                processed_total += 1

            # Process one step for the Processor
            if w_processor.process_one_item() is True:
                processed_total += 1

            # Process one step for the Loader
            if w_loader.process_one_item() is True:
                processed_total += 1

            # Exit condition: 2 items * 3 stages = 6 successful process steps
            if processed_total >= expected_items * 3:
                break

        self.assertEqual(processed_total, 6, f"Expected 6 processing steps, got {processed_total}. Pipeline stalled.")

        # 4. Simulate Shutdown (sending None)
        # This puts the first batch of 3 sentinels into the queues.
        workers = [w_crawler, w_processor, w_loader]
        self.orchestrator.shutdown_workers([self.crawler_queue, self.processor_queue, self.loader_queue], workers)

        # 5. CRITICAL: Process all 6 sentinels (3 originals + 3 propagated) to clear queues

        # A. Process the 3 sentinels sent by shutdown_workers:

        # Crawler consumes its sentinel (propagates None to Processor)
        self.assertEqual(w_crawler.process_one_item(), "SENTINEL")

        # Processor consumes its sentinel (propagates None to Loader)
        self.assertEqual(w_processor.process_one_item(), "SENTINEL")

        # Loader consumes its sentinel (stops)
        self.assertEqual(w_loader.process_one_item(), "SENTINEL")

        # B. Process the 3 sentinels propagated during Step A:

        # Processor consumes propagated sentinel from Crawler (propagates None to Loader)
        self.assertEqual(w_processor.process_one_item(), "SENTINEL")

        # Loader consumes propagated sentinel from Processor (stops)
        self.assertEqual(w_loader.process_one_item(), "SENTINEL")

        # Loader consumes final propagated sentinel (stops)
        self.assertEqual(w_loader.process_one_item(), "SENTINEL")


        # 6. Assertions

        self.assertTrue(self.crawler_queue.empty(), "Crawler queue should be empty.")
        self.assertTrue(self.processor_queue.empty(), "Processor queue should be empty.")
        self.assertTrue(self.loader_queue.empty(), "Loader queue should be empty.")

        self.assertEqual(w_crawler.processed_count, 2, "Crawler should have processed two seed URLs.")
        self.assertEqual(w_processor.processed_count, 2, "Processor should have processed two seed URLs.")
        self.assertEqual(w_loader.processed_count, 2, "Loader should have processed two seed URLs.")

        self.assertIsNotNone(self.orchestrator.state["sitea.com"].root, "Root for siteA should be set.")
        self.assertIsNotNone(self.orchestrator.state["siteb.com"].root, "Root for siteB should be set.")
        self.assertEqual(len(self.orchestrator.state["sitea.com"].nodes), 1)
        self.assertEqual(len(self.orchestrator.state["siteb.com"].nodes), 1)
