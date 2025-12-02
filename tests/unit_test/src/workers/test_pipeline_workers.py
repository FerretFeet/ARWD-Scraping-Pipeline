import threading
import time
from queue import LifoQueue, Queue
from unittest.mock import MagicMock

import pytest

from src.data_pipeline.extract.webcrawler import Crawler
from src.structures.indexed_tree import PipelineStateEnum
from src.workers.pipeline_workers import (
    CrawlerWorker,
    LoaderObj,
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
            self.data = {}
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

def test_processor_worker_processes_and_emits_loader(fake_tree, fake_node, monkeypatch):
    iq = LifoQueue()
    oq = Queue()

    # Mock the HTMLParser to return fake parsed content
    fake_html_parser = MagicMock()
    fake_html_parser.get_content.return_value = {"x": "parsed"}

    # Mock the PipelineTransformer to return transformed data
    fake_transformer = MagicMock()
    fake_transformer.transform_content.return_value = [{"a": 1}]

    # Mock the templates (selectors and transformers)
    fake_templates = MagicMock()
    fake_templates.selectors = {
        "x": ("sel", lambda x: x),
    }

    # Mock the ProcessorRegistry to return the mock templates
    fake_registry = MagicMock()
    fake_registry.get_processor.return_value = fake_templates

    # Patch HTMLParser and PipelineTransformer in the worker
    monkeypatch.setattr("src.workers.pipeline_workers.HTMLParser", lambda strict: fake_html_parser)
    monkeypatch.setattr("src.workers.pipeline_workers.PipelineTransformer", lambda strict: fake_transformer)
    monkeypatch.setattr("src.workers.pipeline_workers.ProcessorRegistry", fake_registry)

    # Set up the fake node with HTML data
    fake_node.data["html"] = "<html></html>"
    iq.put(fake_node)

    # Create and start the worker thread
    worker = ProcessorWorker(
        input_queue=iq,
        output_queue=oq,
        state_tree=fake_tree,
        strict=False,
        fun_registry=fake_registry,
    )

    worker.running = True
    worker.start()

    # Sleep to allow the thread to process the node
    time.sleep(0.2)  # Can be replaced by waiting for a signal or using an event

    # Stop the worker thread
    worker.running = False
    worker.join(timeout=1)  # Ensure thread finishes

    # Assert the final state of the node
    assert fake_node.state == PipelineStateEnum.AWAITING_LOAD

    # Check the output queue has a loader object
    assert oq.qsize() == 1
    out = oq.get()
    assert isinstance(out, LoaderObj)
    assert out.node == fake_node
    assert out.params == {"a": 1}  # This comes from the transformed data

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
