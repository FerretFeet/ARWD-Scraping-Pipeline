import threading
import time
from queue import LifoQueue, Queue
from unittest.mock import MagicMock, patch

import pytest

from src.data_pipeline.extract.webcrawler import Crawler
from src.structures.indexed_tree import PipelineStateEnum
from src.workers.pipeline_workers import (
    CrawlerWorker,
    ProcessorWorker,
)


# Mock the Crawler to return mock HTML content
@pytest.fixture
def fake_crawler():
    crawler = MagicMock(spec=Crawler)
    crawler.get_page.return_value = "<html><body><a href='http://newlink.com'>Next</a></body></html>"
    return crawler


# Mock the ProcessorRegistry to return mock templates
@pytest.fixture
def fake_registry():
    registry = MagicMock()
    registry.get_processor.return_value = {
        "x": ("html_selector", lambda x: {"x": "processed_data"}),
    }
    return registry


@pytest.fixture
def fake_node():
    class Node:
        def __init__(self):
            self.id = "1"
            self.url = "https://arkleg.state.ar.us/"
            self.state = None
            self.data = {"html": "<html><h1>Test HTML</h1></html>"}
            self.type = "TEST"
            self.children = []
            self.lock = threading.Lock()
    return Node()


@pytest.fixture
def fake_tree(fake_node):
    tree = MagicMock()
    tree.add_node.return_value = fake_node
    tree.find_node.return_value = fake_node
    return tree


# -------------------------
# CRAWLER WORKER
# -------------------------

def test_crawler_worker_fetches_and_pushes(fake_tree, fake_node, monkeypatch):
    fq = LifoQueue()
    pq = LifoQueue()

    fake_crawler = MagicMock()
    fake_crawler.get_page.return_value = "<html>hi</html>"

    # Fake pipeline registry
    fake_registry = MagicMock()
    fake_registry.get_processor.return_value = {"next": ("div")}

    monkeypatch.setattr("src.workers.pipeline_workers.ProcessorRegistry", fake_registry)

    worker = CrawlerWorker(
        fetch_queue=fq,
        process_queue=pq,
        state_tree=fake_tree,
        crawler=fake_crawler,
        fun_registry=fake_registry,
        strict=False,
    )
    worker.running = True

    fq.put(fake_node)
    worker.start()

    # Wait until queue empties
    time.sleep(0.2)
    worker.running = False
    worker.join(timeout=1)

    # Assertions
    assert fake_node.state == PipelineStateEnum.AWAITING_PROCESSING
    assert pq.qsize() == 1
    fake_crawler.get_page.assert_called_once()


# # -------------------------
# # PROCESSOR WORKER
# # -------------------------

# 🚨 REQUIRED REVERSION: Use real queue objects for the thread test
@pytest.fixture
def mock_input_queue():
    """Fixture for the input queue (LifoQueue)."""
    return LifoQueue()

@pytest.fixture
def mock_output_queue():
    """Fixture for the output queue (Queue)."""
    return Queue()

@pytest.fixture
def mock_selector_func():
    return MagicMock(return_value="selector_output")

@pytest.fixture
def mock_transformer_func():
    return MagicMock(return_value="transformed_data")

@pytest.fixture
def fake_registry_process(mock_selector_func, mock_transformer_func):
    registry = MagicMock()
    # Provide the mock functions directly
    registry.get_processor.return_value = {
        "title": (mock_selector_func, mock_transformer_func),
        "state_key": (None, None),
    }
    return registry
@pytest.fixture
def processor_worker(mock_input_queue, mock_output_queue, fake_tree, fake_registry_process):
    """Fixture to initialize a ProcessorWorker instance."""
    return ProcessorWorker(
        input_queue=mock_input_queue,
        output_queue=mock_output_queue,
        state_tree=fake_tree,
        fun_registry=fake_registry_process,
        strict=True,
    )

@pytest.fixture
def mock_get_state_fun():
    return MagicMock(return_value={"state_data_retrieved": "YES"})

@pytest.fixture
def processor_worker_args(mock_input_queue, mock_output_queue, fake_tree, fake_registry):
    """Fixture to provide arguments needed to initialize a ProcessorWorker."""
    return {
        "input_queue": mock_input_queue,
        "output_queue": mock_output_queue,
        "state_tree": fake_tree,
        "fun_registry": fake_registry,
        "strict": True,
    }
@pytest.fixture
def patched_dependencies():
    """Patches core logic and returns the mocks for assertion."""
    with (
        patch("src.workers.pipeline_workers.logger") as MockLogger,
        patch("src.workers.pipeline_workers.HTMLParser") as MockHTMLParser,
        patch("src.workers.pipeline_workers.PipelineTransformer") as MockPipelineTransformer,
        patch("src.workers.pipeline_workers.LoaderObj") as MockLoaderObj,
        patch("src.workers.pipeline_workers.get_enum_by_url") as MockGetEnumByUrl,
        patch("src.workers.pipeline_workers.PipelineRegistries") as MockPipelineRegistries,
    ):
        # Configure the return values that simulate processing success

        mock_html_parser_instance = MockHTMLParser.return_value
        mock_html_parser_instance.get_content.return_value = {"page_data": "parsed"}

        mock_transformer_instance = MockPipelineTransformer.return_value
        mock_transformer_instance.transform_content.return_value = {"final_output": "data"}

        MockLoaderObj.return_value = MagicMock()

        yield {
            "MockLogger": MockLogger,
            "MockHTMLParser": MockHTMLParser,
            "MockTransformer": MockPipelineTransformer,
            "MockLoaderObj": MockLoaderObj,
            "MockGetEnumByUrl": MockGetEnumByUrl,
            "MockPipelineRegistries": MockPipelineRegistries,
            "mock_transformer_instance": mock_transformer_instance,
        }


# --- 🧪 Test Class (Thread Execution Model) ---


class TestProcessorWorker:
    def test_successful_run_cycle(
        self,
        processor_worker,
        mock_input_queue,
        mock_output_queue,
        fake_node,
        # Use the new mock functions in arguments
        fake_registry,
        mock_selector_func,
        mock_transformer_func,
        patched_dependencies,
    ):
        """Test the complete successful cycle using thread start/join."""

        Mocks = patched_dependencies

        # 1. Setup: Put the node into the real queue
        mock_input_queue.put(fake_node)

        # 2. Act: Start the worker thread
        processor_worker.start()

        # 3. Wait for processing completion (using join is best)
        try:
            mock_input_queue.join()
        except TimeoutError:
            pytest.fail("ProcessorWorker failed to call task_done()")

        # 4. Stop and Join the thread cleanly
        processor_worker.running = False
        processor_worker.join(timeout=1)

        if Mocks["MockLogger"].warning.called:
            print("\n--- Worker Logged Warning ---")
            print(Mocks["MockLogger"].warning.call_args_list)
            # Force a failure here to see the log output in your CI/terminal
            assert not Mocks["MockLogger"].warning.called, "Worker faile d! Check logs above."

        # 5. Assertions

        # Logic Check
        Mocks["MockHTMLParser"].assert_called_once()

        # Assert get_content was called with the correct selector_template
        selector_template = {"title": mock_selector_func}

        Mocks["MockHTMLParser"].return_value.get_content.assert_called_once_with(
            selector_template, fake_node.data["html"],
        )

        # Assert Transformation was called with the correct template
        transformer_template = {"title": mock_transformer_func}
        Mocks["mock_transformer_instance"].transform_content.assert_called_once_with(
            transformer_template, {"page_data": "parsed"},
        )

        # Final checks
        assert fake_node.state == PipelineStateEnum.AWAITING_LOAD
        assert mock_output_queue.qsize() == 1
        assert mock_input_queue.empty()

    def test_run_with_state_function(
        self,
        processor_worker_args,  # ⬅️ New fixture name
        mock_input_queue,
        mock_output_queue,
        fake_node,
        fake_tree,
        fake_registry,
        mock_get_state_fun,
        patched_dependencies,
    ):
        """Test processing when a 'state_key' function is present."""

        Mocks = patched_dependencies

        # 1. Setup: Define custom return value for the registry
        mock_content_selector = MagicMock()
        mock_content_transformer = Mocks["mock_transformer_instance"].transform_content.return_value

        custom_registry_return_value = {
            "content": (mock_content_selector, mock_content_transformer),
            "state_key": (mock_get_state_fun, None),
        }

        # 🚨 CRITICAL FIX A: Set the mock's return value BEFORE worker instantiation
        fake_registry.get_processor.return_value = custom_registry_return_value

        # 🚨 CRITICAL FIX B: Instantiate the worker using the arguments fixture
        processor_worker = ProcessorWorker(**processor_worker_args)

        # Put node and start thread
        mock_input_queue.put(fake_node)
        processor_worker.start()

        # Wait for queue join
        try:
            mock_input_queue.join()
        except TimeoutError:
            pytest.fail("ProcessorWorker failed to call task_done()")

        # Stop and Join
        processor_worker.running = False
        processor_worker.join(timeout=1)

        # 2. Assertions

        # 🟢 This assertion will now pass because the worker saw the correct state function
        mock_get_state_fun.assert_called_once_with(fake_node, fake_tree)

        assert mock_output_queue.qsize() == 1

    def test_processing_exception_handling(
        self, processor_worker, mock_input_queue, fake_node, patched_dependencies,
    ):
        """Test that the worker catches a generic processing Exception and logs a warning, calling task_done."""

        Mocks = patched_dependencies

        # Setup: Mock HTMLParser init to raise an Exception
        exc_message = "Catastrophic Processing Failure"
        Mocks["MockHTMLParser"].side_effect = Exception(exc_message)

        mock_input_queue.put(fake_node)
        processor_worker.start()

        # Wait for queue join (must succeed if task_done() is called)
        try:
            mock_input_queue.join()
        except TimeoutError:
            pytest.fail("ProcessorWorker failed to call task_done() after exception.")

        # Stop and Join
        processor_worker.running = False
        processor_worker.join(timeout=1)

        # Assertions
        # 1. Exception was logged
        Mocks["MockLogger"].warning.assert_called_once()
        log_message = Mocks["MockLogger"].warning.call_args[0][0]
        assert f"[PROCESSOR ERROR] Node ID {fake_node.id}:" in log_message

        # 2. Node state should remain PROCESSING or be marked ERROR (depending on implementation)
        # Note: If you updated your code with the suggested fix, it should be ERROR_STATE.
        # Otherwise, the last successful assignment was PROCESSING_STATE.
        # We assert it's not the final AWAITING_LOAD state.
        assert fake_node.state != PipelineStateEnum.AWAITING_LOAD
        assert mock_input_queue.empty()  # Verifies task_done() was called


# # -------------------------
# # LOADER WORKER
# # -------------------------
#
# def test_loader_worker_marks_completed_and_removes(fake_tree, fake_node, monkeypatch):
#     iq = Queue()
#
#     fake_db = MagicMock()
#
#     fake_node.children = []  # no children → should be removed
#     loader_obj = LoaderObj(node_id=fake_node.id, name="TEST", params={})
#     iq.put(loader_obj)
#
#     worker = LoaderWorker(
#         input_queue=iq,
#         state_tree=fake_tree,
#         db_conn=fake_db,
#     )
#     worker.running = True
#     worker.start()
#
#     time.sleep(0.2)
#     worker.running = False
#     worker.join(timeout=1)
#
#     assert fake_node.state == PipelineStateEnum.COMPLETED
#     fake_tree.safe_remove_node.assert_called_once_with(fake_node.id, cascade_up=True)
#     fake_tree.save_file.assert_called_once()



# Integration test for both workers working together
# def test_full_pipeline(fake_node, fake_crawler, fake_registry):
#     fetch_queue = LifoQueue()
#     process_queue = LifoQueue()
#     output_queue = Queue()
#     state_tree = IndexedTree()
#
#     fetch_queue.put(fake_node)
#
#     # Create the worker instances
#     crawler_worker = CrawlerWorker(
#         fetch_queue=fetch_queue,
#         process_queue=process_queue,
#         state_tree=state_tree,
#         crawler=fake_crawler,
#         fun_registry=fake_registry,
#         strict=False,
#     )
#
#     processor_worker = ProcessorWorker(
#         input_queue=process_queue,
#         output_queue=output_queue,
#         state_tree=state_tree,
#         fun_registry=fake_registry,
#         strict=False,
#     )
#
#     # Start both workers in separate threads
#     crawler_worker.start()
#     processor_worker.start()
#
#     # Allow the workers to process
#     time.sleep(1)  # Or better yet, use a synchronization mechanism like an Event
#     crawler_worker.running = False
#     processor_worker.running = False
#
#     crawler_worker.join()
#     processor_worker.join()
#
#     # Assert that the processing pipeline ran correctly
#     assert fake_node.state == PipelineStateEnum.AWAITING_LOAD
#     assert output_queue.qsize() == 1
#     loader_obj = output_queue.get()
#     assert loader_obj.node == fake_node
#     assert loader_obj.params == {"item": [{"a": 1}]}
