from queue import LifoQueue, Queue
from unittest.mock import MagicMock
from urllib.parse import urlparse

import pytest

from src.structures.indexed_tree import PipelineStateEnum
from src.workers.pipeline_workers import (
    CrawlerWorker,
    LoaderObj,
    LoaderWorker,
    ProcessorWorker,
)


@pytest.fixture
def fake_db_conn():
    db_conn = MagicMock()
    db_conn.commit.return_value = None
    db_conn.rollback.return_value = None
    return db_conn


@pytest.fixture
def fake_loader_obj(fake_node):
    loader_obj = MagicMock(spec=LoaderObj)
    loader_obj.node = fake_node
    loader_obj.name = "PAGE_ENUM"
    loader_obj.params = {"data": "value"}
    return loader_obj


@pytest.fixture
def fake_fun_registry():
    registry = MagicMock()
    loader_fun = MagicMock()
    loader_fun.return_value = {"db_result": "ok"}
    registry.get_processor.return_value = loader_fun
    return registry




@pytest.fixture
def fake_node():
    class Node:
        def __init__(self):
            self.id = "1"
            self.url = "https://arkleg.state.ar.us/Legislators/List"
            self.state = None
            self.data = {"html": "<html></html>"}
            self.type = "TEST"
            self.children = []
            self.lock = MagicMock()

        def __repr__(self):
            return "<Node 1>"
    return Node()

@pytest.fixture
def fake_tree(fake_node):
    tree = MagicMock()
    tree.add_node.return_value = fake_node
    return tree

@pytest.fixture
def lifoqueues():
    return LifoQueue(), LifoQueue()

@pytest.fixture
def worker(fake_tree, lifoqueues):
    fetch_q, process_q = lifoqueues
    crawler = MagicMock()
    parser = MagicMock()
    registry = MagicMock()

    return CrawlerWorker(
        fetch_queue=fetch_q,
        process_queue=process_q,
        state_tree=fake_tree,
        crawler=crawler,
        parser=parser,
        fun_registry=registry,
        strict=False,
    )


class TestCrawlerWorker:
    def test_fetch_html(self, worker, fake_node):
        worker.crawler.get_page.return_value = "<html>"
        html = worker._fetch_html(fake_node.url)

        expected_path = urlparse(fake_node.url).geturl()
        worker.crawler.get_page.assert_called_once_with(expected_path)

        assert html == "<html>"

    def test_parse_html(self, worker):
        worker.parser.get_content.return_value = {"links": ["a", "b"]}

        worker.fun_registry.get_processor.return_value = {"template": "x"}

        result = worker._parse_html(
            "https://arkleg.state.ar.us/page",
            "<html>aaa</html>",
        )

        worker.fun_registry.get_processor.assert_called_once()
        worker.parser.get_content.assert_called_once()

        assert result == {"links": ["a", "b"]}

    def test_enqueue_links(self, worker, fake_tree, fake_node, lifoqueues):
        fetch_q, _ = lifoqueues

        fake_tree.add_node.return_value = MagicMock(id="child")

        worker._enqueue_links(fake_node, {
            "links": ["url1", "url2"],
        })

        # Two children added
        assert fake_tree.add_node.call_count == 2

        # Two children queued
        assert fetch_q.qsize() == 2

    def test_process_success(self, worker, fake_node, fake_tree, lifoqueues):
        fetch_q, process_q = lifoqueues

        # Simulated worker behavior
        worker.crawler.get_page.return_value = "<html>"
        worker.parser.get_content.return_value = {"links": ["A", "B"]}

        # FETCH stage template
        worker.fun_registry.get_processor.side_effect = [
            {"fetch_template": True},  # FETCH
            {"process_template": True},  # PROCESS (next step exists)
        ]

        worker.process(fake_node)

        # Must set state correctly
        assert fake_node.state == PipelineStateEnum.AWAITING_PROCESSING

        # Must insert HTML into node
        assert fake_node.data["html"] == "<html>"

        # Must move to process queue
        assert process_q.qsize() == 1

    def test_process_no_next_step(self, worker, fake_node, fake_tree):
        worker.crawler.get_page.return_value = "<html>"
        worker.parser.get_content.return_value = {"links": []}

        worker.fun_registry.get_processor.side_effect = [
            {"fetch_template": True},  # FETCH
            None,  # PROCESS → no next stage
        ]

        worker.process(fake_node)

        # Should NOT be queued for processing
        assert fake_node.state == PipelineStateEnum.COMPLETED

    def test_error_in_fetch_html(self, worker, fake_node, lifoqueues):
        fetch_q, process_q = lifoqueues

        # Cause _fetch_html to raise
        worker.crawler.get_page.side_effect = Exception("fetch failed")

        # Queue the node and start the thread
        # LIFO Queue
        fetch_q.put(None)  # signal end
        fetch_q.put(fake_node)

        worker.start()
        worker.join()

        # BaseWorker should mark node as ERROR
        assert fake_node.state == PipelineStateEnum.ERROR

        # Should not go to processor queue
        assert process_q.qsize() == 0

    def test_error_in_parse_html(self, worker, fake_node, lifoqueues):
        fetch_q, process_q = lifoqueues

        worker.crawler.get_page.return_value = "<html>"
        worker.parser.get_content.side_effect = Exception("parse failed")

        fetch_q.put(None)
        fetch_q.put(fake_node)

        worker.start()
        worker.join()

        assert fake_node.state == PipelineStateEnum.ERROR
        assert process_q.qsize() == 0

    def test_error_in_enqueue_links(self, worker, fake_node, lifoqueues):
        fetch_q, process_q = lifoqueues

        worker.crawler.get_page.return_value = "<html>"
        worker.parser.get_content.return_value = {"links": ["A"]}

        # Force tree.add_node to explode
        worker.state.add_node.side_effect = Exception("tree failed")

        fetch_q.put(None)
        fetch_q.put(fake_node)

        worker.start()
        worker.join()

        assert fake_node.state == PipelineStateEnum.ERROR
        assert process_q.qsize() == 0

    def test_task_done_after_error(self, worker, fake_node, lifoqueues):
        fetch_q, _ = lifoqueues

        worker.crawler.get_page.side_effect = Exception("boom")
        fetch_q.put(None)
        fetch_q.put(fake_node)

        worker.start()
        worker.join()

        # If task_done wasn't called, this will raise ValueError
        fetch_q.join()

@pytest.fixture
def processor_worker(fake_tree):
    input_q = LifoQueue()
    output_q = Queue()
    parser = MagicMock()
    transformer = MagicMock()
    registry = MagicMock()

    return ProcessorWorker(
        input_queue=input_q,
        output_queue=output_q,
        state_tree=fake_tree,
        parser=parser,
        transformer=transformer,
        fun_registry=registry,
        strict=False,
    )


class TestProcessorWorker:

    def test_parse_and_transform(self, processor_worker, fake_node):
        processor_worker.parser.get_content.return_value = {"parsed_key": "parsed_value"}
        processor_worker.transformer.transform_content.return_value = {"transformed_key": "transformed_value"}
        processor_worker.fun_registry.get_processor.return_value = {
            "state_key": (lambda n, s: {"state_val": 123}, lambda x: "fn"),
            "title": ("sel", "tr"),
        }

        processor_worker.process(fake_node)

        assert fake_node.state == PipelineStateEnum.AWAITING_LOAD

        assert isinstance(fake_node.data, LoaderObj)
        loader_obj = fake_node.data
        assert loader_obj.params["transformed_key"] == "transformed_value"

        assert processor_worker.output_queue.get_nowait() == loader_obj

    def test_process_raises_on_invalid_loader(self, processor_worker, fake_node):
        processor_worker.parser.get_content.return_value = {"parsed_key": "parsed_value"}
        processor_worker.transformer.transform_content.return_value = None  # invalid transformed data
        processor_worker.fun_registry.get_processor.return_value = {
            "test_key": (lambda n, s: {"state_val": 123}, lambda x: "fn"),
            "state_key": (lambda n, s: {"state_val": 123}, lambda x: "fn"),
        }

        with pytest.raises(Exception) as excinfo:
            processor_worker.process(fake_node)

        assert "loader obj not able to be created" in str(excinfo.value)

    def test_error_in_parse_html(self, processor_worker, fake_node):
        processor_worker.parser.get_content.side_effect = Exception("parse failed")
        processor_worker.fun_registry.get_processor.return_value = {
            "state_key": (lambda n, s: {"state_val": 123}, lambda x: "fn"),
        }

        try:
            processor_worker.process(fake_node)
        except Exception:
            processor_worker.handle_error(fake_node, Exception("parse failed"))

        assert fake_node.state == PipelineStateEnum.ERROR

    def test_error_in_transform_data(self, processor_worker, fake_node):
        processor_worker.parser.get_content.return_value = {"parsed_key": "parsed_value"}
        processor_worker.transformer.transform_content.side_effect = Exception("transform failed")
        processor_worker.fun_registry.get_processor.return_value = {
            "state_key": (lambda n, s: {"state_val": 123}, lambda x: "fn"),
        }

        try:
            processor_worker.process(fake_node)
        except Exception:
            processor_worker.handle_error(fake_node, Exception("transform failed"))

        assert fake_node.state == PipelineStateEnum.ERROR

    def test_get_processing_templates(self, processor_worker):
        processor_worker.fun_registry.get_processor.return_value = {
            "state_key": (lambda n, s: {}, lambda x: None),
            "a": ("sel", "tr"),
            "b": ("sel2", "tr2"),
        }
        selector, transformer, state_pair = processor_worker._get_processing_templates("https://arkleg.state.ar.us/page")
        assert selector == {"a": "sel", "b": "sel2"}
        assert transformer == {"a": "tr", "b": "tr2"}
        assert state_pair[0] is not None


@pytest.fixture
def loader_worker(fake_tree, fake_db_conn, fake_fun_registry):
    q = Queue()
    worker = LoaderWorker(
        input_queue=q,
        state_tree=fake_tree,
        db_conn=fake_db_conn,
        fun_registry=fake_fun_registry,
        strict=False,
    )
    return worker


class TestLoaderWorker:

    def test_process_success(self, loader_worker, fake_loader_obj, fake_db_conn, fake_tree):
        loader_worker.process(fake_loader_obj)

        # Node state should be updated
        assert fake_loader_obj.node.state == PipelineStateEnum.COMPLETED

        # Node data should reflect loader function output
        assert fake_loader_obj.node.data == {"db_result": "ok"}

        # Database commit called
        fake_db_conn.commit.assert_called_once()
        fake_db_conn.rollback.assert_not_called()

        # Node removal logic
        fake_tree.safe_remove_node.assert_called_with(fake_loader_obj.node.id, cascade_up=True)

    def test_process_load_returns_none(self, loader_worker, fake_loader_obj, fake_db_conn):
        # Simulate loader returning None
        loader_worker.fun_registry.get_processor.return_value = lambda params, db: None

        loader_worker.process(fake_loader_obj)

        assert fake_loader_obj.node.data is None
        assert fake_loader_obj.node.state == PipelineStateEnum.COMPLETED
        fake_db_conn.commit.assert_called_once()

    def test_process_raises_exception(self, loader_worker, fake_loader_obj, fake_db_conn):
        # Simulate loader function raising exception
        def raise_error(params, db):
            raise Exception("DB error")
        loader_worker.fun_registry.get_processor.return_value = raise_error

        with pytest.raises(Exception) as excinfo:
            loader_worker.process(fake_loader_obj)

        assert "DB error" in str(excinfo.value)
        fake_db_conn.rollback.assert_called_once()
        fake_db_conn.commit.assert_called_once()  # finally always commits

    def test_remove_if_children_not_completed(self, loader_worker, fake_loader_obj, fake_tree):
        # Add a child node that is not completed
        child_node = MagicMock()
        child_node.state = PipelineStateEnum.PROCESSING
        fake_loader_obj.node.children.append("child_id")
        fake_tree.find_node.return_value = child_node

        loader_worker._remove_if_children_completed(fake_loader_obj.node)

        # safe_remove_node should NOT be called
        fake_tree.safe_remove_node.assert_not_called()

    def test_remove_if_children_completed(self, loader_worker, fake_loader_obj, fake_tree):
        # Add a child node that is completed
        child_node = MagicMock()
        child_node.state = PipelineStateEnum.COMPLETED
        fake_loader_obj.node.children.append("child_id")
        fake_tree.find_node.return_value = child_node

        loader_worker._remove_if_children_completed(fake_loader_obj.node)

        # Node should be removed since all children are completed
        fake_tree.safe_remove_node.assert_called_with(fake_loader_obj.node.id, cascade_up=True)
