import threading
from queue import LifoQueue, Queue
from unittest.mock import MagicMock, patch
from urllib.parse import urlparse

import pytest

from src.structures.indexed_tree import PipelineStateEnum
from src.workers.pipeline_workers import (
    CrawlerWorker,
    LoaderWorker,
    ProcessorWorker,
)


@pytest.fixture
def fake_db_conn():
    """Mocks the psycopg database connection."""
    db_conn = MagicMock()
    return db_conn

@pytest.fixture
def fake_loader_obj(fake_node):
    """Mocks the LoaderObj that the worker consumes."""
    loader_obj = MagicMock()
    loader_obj.node = fake_node
    # The worker looks up the processor using loader_obj.name
    loader_obj.name = "PAGE_ENUM_FOR_LOADER"
    # The worker passes loader_obj.params to the loader function
    loader_obj.params = {"transformed_data": "final"}
    return loader_obj
# Mock the Crawler to return mock HTML content
@pytest.fixture
def fake_crawler():
    crawler = MagicMock()  # spec=Crawler not needed if not asserting spec details
    # Return HTML with a link to simulate new links found by the parser
    crawler.get_page.return_value = (
        "<html><body><a href='http://newlink.com/page.html?q=1'>Next</a></body></html>"
    )
    return crawler


# Mock the ProcessorRegistry to return mock templates
@pytest.fixture
def fake_registry():
    registry = MagicMock()
    # Template needed for link parsing (FETCH registry)
    # The worker passes the enum value (a string) as the first arg.
    registry.get_processor.return_value = {
        "links": ("a[href]",),  # A simple selector tuple for the parser
    }
    return registry


@pytest.fixture
def fake_node():
    class Node:
        def __init__(self):
            self.id = "1"
            # 🚨 Must be a valid structure for splitting: http(s)://domain/path
            self.url = "https://arkleg.state.ar.us/page.html"
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


@pytest.fixture
def patched_dependencies():
    """
    Patches core logic for a pipeline worker (Fetch, Process, Load) and
    returns all mock objects for assertion.
    """

    # --- Define and Configure Mock Instances OUTSIDE Patch Context ---

    # 1. HTMLParser Instance (for both fetching links and basic content)
    mock_html_parser_instance = MagicMock()
    # Mock return for a successful fetch/parse (links and page data)
    mock_html_parser_instance.get_content.return_value = {
        "page_data": "parsed",
        "links": ["http://newlink.com/page.html?q=1"],  # Include links for crawler worker
    }

    # 2. PipelineTransformer Instance
    mock_transformer_instance = MagicMock()
    # Mock return for a successful transformation
    mock_transformer_instance.transform_content.return_value = {"final_output": "data"}

    # 3. LoaderObj Class Mock
    MockLoaderObj = MagicMock()

    mock_loader_fun = MagicMock()
    mock_loader_fun.return_value = {"updated_data": "success"}

    with (
        patch("src.workers.pipeline_workers.logger") as MockLogger,
        # 🟢 Use return_value to ensure the worker gets our pre-configured instance
        patch(
            "src.workers.pipeline_workers.HTMLParser",
            return_value=mock_html_parser_instance,
        ) as MockHTMLParser,
        # Transformer is mocked by its name, returning our pre-configured instance
        patch(
            "src.workers.pipeline_workers.PipelineTransformer",
            return_value=mock_transformer_instance,
        ) as MockPipelineTransformer,
        # LoaderObj is mocked by the class definition above
        patch("src.workers.pipeline_workers.LoaderObj", new=MockLoaderObj),
        patch("src.workers.pipeline_workers.get_enum_by_url") as MockGetEnumByUrl,
        patch("src.workers.pipeline_workers.PipelineRegistries") as MockPipelineRegistries,
        # Patch PipelineStateEnum only if you need to decouple the mock from the real enum.
        # Otherwise, if you use the real enum, you can omit this patch.
    ):
        # Configure simple class/function mocks
        # Example: Mocking the enum setup from the second fixture
        MockGetEnumByUrl.side_effect = [
            "CURRENT_PAGE_ENUM",  # Used for registry lookup
            "PARSED_ENUM",
            "NEW_LINK_ENUM",
            # Used for new node type
        ]

        MockFunRegistry = MagicMock()
        MockFunRegistry.get_processor.return_value = mock_loader_fun

        # --- Yield All Mock Objects ---

        yield {
            "MockLogger": MockLogger,
            "MockHTMLParser": MockHTMLParser,  # Mocked Class
            "MockTransformer": MockPipelineTransformer,  # Mocked Class
            "MockLoaderObj": MockLoaderObj,  # Mocked Class
            "MockGetEnumByUrl": MockGetEnumByUrl,
            "MockPipelineRegistries": MockPipelineRegistries,
            "MockLoaderFun": mock_loader_fun,
            "MockFunRegistry": MockFunRegistry,
            # 🟢 Mocked Instances (for asserting method calls)
            "html_parser_instance": mock_html_parser_instance,  # Instance for .get_content calls
            "mock_transformer_instance": mock_transformer_instance,  # Instance for .transform_content calls
        }


# --- ------------------------- ---
# --- CRAWLER WORKER TEST CLASS ---
# --- ------------------------- ---


class TestCrawlerWorker:
    def test_crawler_worker_fetches_and_pushes_success(
        self,
        fake_tree,
        fake_node,
        fake_crawler,
        fake_registry,
        patched_dependencies,
    ):
        """Test the complete successful cycle: fetch page, update state, find links, put node on process queue."""

        fq = LifoQueue()
        pq = LifoQueue()
        Mocks = patched_dependencies

        # 🚨 Use the real Enum values for initial comparison
        # (This is implicitly handled by the fixture setup, but good practice to know)

        worker = CrawlerWorker(
            fetch_queue=fq,
            process_queue=pq,
            state_tree=fake_tree,
            crawler=fake_crawler,
            fun_registry=fake_registry,
            strict=False,
        )
        worker.running = True
        html_parser_instance = Mocks["html_parser_instance"]
        # Act
        fq.put(fake_node)
        worker.start()

        # Wait until queue empties (task_done is called)
        try:
            # Join queue for deterministic wait
            fq.join(timeout=5)
        except Exception:
            pass

        # Stop and join the thread
        worker.running = False
        worker.join(timeout=1)

        # Assertions

        # 1. Assert Node States and Queues
        # 🚨 FIX 2: Assert against the REAL enum value (or the mocked one from Mocks)
        assert fake_node.state == PipelineStateEnum.AWAITING_PROCESSING
        assert pq.qsize() == 1  # Node moved to process queue
        assert fq.qsize() == 2  # A new link was found and put back on the fetch queue

        # 2. Assert Crawler and HTML Parser Calls
        fake_crawler.get_page.assert_called_once()

        # Worker splits URL and passes the stem to get_page (e.g., 'arkleg.state.ar.us/page.html')
        parsed_url = urlparse(fake_node.url)
        expected_url_stem = parsed_url.path
        fake_crawler.get_page.assert_called_once_with(expected_url_stem)

        Mocks["MockHTMLParser"].assert_called_once_with(strict=False)
        Mocks["html_parser_instance"].get_content.assert_called_once()

        # 3. Assert Link Logic (New Node Creation)
        assert fake_tree.add_node.call_count == 2
        fake_tree.add_node.assert_called_with(
            parent=fake_node.id,
            url="http://newlink.com/page.html?q=1",
            state=PipelineStateEnum.AWAITING_FETCH,
            node_type="NEW_LINK_ENUM",  # Use the second side_effect value
        )
        # Assert get_enum_by_url was called twice
        assert Mocks["MockGetEnumByUrl"].call_count == 3, ("expect get enum by url to be called once"
                                                           "+ once per added link")

    def test_crawler_worker_handles_exception(
        self,
        fake_tree,
        fake_node,
        fake_crawler,
        fake_registry,
        patched_dependencies,
    ):
        """Test that the worker catches an exception during fetching and logs an error."""

        fq = LifoQueue()
        pq = LifoQueue()
        Mocks = patched_dependencies

        # 1. Setup: Make the crawler fail immediately
        error_message = "Network Connection Lost"
        fake_crawler.get_page.side_effect = Exception(error_message)

        worker = CrawlerWorker(
            fetch_queue=fq,
            process_queue=pq,
            state_tree=fake_tree,
            crawler=fake_crawler,
            fun_registry=fake_registry,
            strict=False,
        )
        worker.running = True

        # Act
        fq.put(fake_node)
        worker.start()

        # Wait for the worker to process the node and fail
        try:
            fq.join(timeout=5)
        except Exception:
            pass

        worker.running = False
        worker.join(timeout=1)

        # Assertions

        # 1. Assert Error Logging
        Mocks["MockLogger"].warning.assert_called_once()
        log_message = Mocks["MockLogger"].warning.call_args[0][0]
        assert "Network Connection Lost" in log_message

        # 2. Assert State and Queues
        # The node should be marked as ERROR
        assert fake_node.state == PipelineStateEnum.ERROR
        assert pq.qsize() == 0  # Node should not move to process queue
        assert fq.qsize() == 0  # Node was consumed, and no new links were added

        # 3. Assert HTMLParser was NOT called
        Mocks["MockHTMLParser"].assert_not_called()

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
# @pytest.fixture
# def patched_dependencies():
#     """Patches core logic and returns the mocks for assertion."""
#     with (
#         patch("src.workers.pipeline_workers.logger") as MockLogger,
#         patch("src.workers.pipeline_workers.HTMLParser") as MockHTMLParser,
#         patch("src.workers.pipeline_workers.PipelineTransformer") as MockPipelineTransformer,
#         patch("src.workers.pipeline_workers.LoaderObj") as MockLoaderObj,
#         patch("src.workers.pipeline_workers.get_enum_by_url") as MockGetEnumByUrl,
#         patch("src.workers.pipeline_workers.PipelineRegistries") as MockPipelineRegistries,
#     ):
#         # Configure the return values that simulate processing success
#
#         mock_html_parser_instance = MockHTMLParser.return_value
#         mock_html_parser_instance.get_content.return_value = {"page_data": "parsed"}
#
#         mock_transformer_instance = MockPipelineTransformer.return_value
#         mock_transformer_instance.transform_content.return_value = {"final_output": "data"}
#
#         MockLoaderObj.return_value = MagicMock()
#
#         yield {
#             "MockLogger": MockLogger,
#             "MockHTMLParser": MockHTMLParser,
#             "MockTransformer": MockPipelineTransformer,
#             "MockLoaderObj": MockLoaderObj,
#             "MockGetEnumByUrl": MockGetEnumByUrl,
#             "MockPipelineRegistries": MockPipelineRegistries,
#             "mock_transformer_instance": mock_transformer_instance,
#         }


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
            transformer_template, {"page_data": "parsed", "links": ["http://newlink.com/page.html?q=1"]},
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
class TestLoaderWorker:
    def test_loader_worker_completes_successfully(
            self,
            fake_tree,
            fake_node,
            fake_loader_obj,
            fake_db_conn,
            fake_registry,
            # NOTE: Pass your fixture that mocks fun_registry here
            patched_dependencies,
    ):
        """Tests the worker executes load function, commits DB, and sets state to COMPLETED."""

        Mocks = patched_dependencies

        # 1. Setup Queues and Worker
        input_queue = LifoQueue()

        # Add a mock child to prevent the node from being removed in this success case
        mock_child = MagicMock()
        mock_child.state = PipelineStateEnum.AWAITING_FETCH # Node will be kept because child isn't COMPLETED
        fake_node.children = ["child_id_1"]
        fake_tree.find_node.return_value = mock_child

        # NOTE: Use the mocked fun_registry object (MockFunRegistry) from your combined fixture
        # If your fun_registry fixture is separate, use that name instead of Mocks["MockFunRegistry"]
        worker = LoaderWorker(
            input_queue=input_queue,
            state_tree=fake_tree,
            db_conn=fake_db_conn,
            fun_registry=Mocks["MockFunRegistry"],
            strict=False,
        )
        worker.running = True

        # Act
        input_queue.put(fake_loader_obj)
        worker.start()

        # Wait until queue empties
        try:
            input_queue.join(timeout=5)
        except Exception:
            pass

        worker.running = False
        worker.join(timeout=1)

        # Assertions

        # 1. Assert Database and Load Function Calls
        Mocks["MockLoaderFun"].assert_called_once_with(
            fake_loader_obj.params,
            fake_db_conn,
        )
        fake_db_conn.commit.assert_called_once()
        fake_db_conn.rollback.assert_not_called()

        # 2. Assert State and Data Update
        assert fake_node.state == PipelineStateEnum.COMPLETED
        assert fake_node.data == {"updated_data": "success"} # Matches mock_loader_fun return

        # 3. Assert Tree/File Operations
        fake_tree.safe_remove_node.assert_not_called()
        fake_tree.save_file.assert_called_once()
        Mocks["MockLogger"].info.assert_called() # Check logging happened

        # 4. Assert Queue Cleanup
        assert input_queue.qsize() == 0
