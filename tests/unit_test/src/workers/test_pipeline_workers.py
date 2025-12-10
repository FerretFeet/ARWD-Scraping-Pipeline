from queue import LifoQueue, Queue
from unittest.mock import MagicMock
from urllib.parse import urlparse

import pytest

from src.data_pipeline.extract.html_parser import HTMLParser
from src.data_pipeline.transform.pipeline_transformer import PipelineTransformer
from src.data_pipeline.utils.fetch_scheduler import FetchScheduler
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
    loader_fun.execute.return_value = {"db_result": "ok"}
    registry.get_processor.return_value = loader_fun
    return registry




@pytest.fixture
def fake_node():
    class Node:
        def __init__(self):
            self.id = "1"
            self.url = "arkleg.state.ar.us/Legislators/List?ddBienniumSession=2019%2F2019R"
            self.state = None
            self.data = {"html": "<html></html>"}
            self.type = "TEST"
            self.children = []
            self.lock = MagicMock()

        def __repr__(self):
            return "<Node 1>"

        def set_state(self, state):
            self.state = state
    return Node()




@pytest.fixture
def fake_graph(fake_node):
    graph = MagicMock()
    graph.nodes = {}  # simulate real graph storage keyed by URL

    def _add_new_node(url, node_type, incoming, **kwargs):
        # Only create if not present
        if url not in graph.nodes:
            node = MagicMock(id=f"child-{url}")
            graph.nodes[url] = node
            return node
        return graph.nodes[url]

    graph.add_new_node.side_effect = _add_new_node
    return graph

@pytest.fixture
def lifoqueues():
    return LifoQueue(), LifoQueue()

@pytest.fixture
def worker(fake_graph, lifoqueues):
    fetch_q, process_q = lifoqueues
    parser = MagicMock(spec=HTMLParser)
    registry = MagicMock()  # <- just remove spec
    mock_crawler_cls = MagicMock()
    mock_crawler_instance = MagicMock()
    mock_crawler_cls.return_value = mock_crawler_instance

    worker = CrawlerWorker(
        input_queue=fetch_q,
        output_queue=process_q,
        state=fake_graph,
        crawler_cls=mock_crawler_cls,
        parser=parser,
        fun_registry=registry,
        fetch_scheduler=FetchScheduler(),
        strict=False,
    )
    worker.create_crawlers(fake_graph.roots)
    return worker

class TestCrawlerWorker:

    def test_fetch_html(self, worker, fake_node):
        domain = urlparse(fake_node.url).netloc
        worker.create_crawlers({fake_node})
        crawler = worker.crawlers[domain]
        crawler.get_page.return_value = "<html>"

        html = worker._fetch_html(fake_node.url)
        crawler.get_page.assert_called_once_with(fake_node.url)
        assert html == "<html>"

    def test_parse_html(self, worker):
        worker.parser.get_content.return_value = {"links": ["a", "b"]}
        worker.fun_registry.get_processor.return_value = {"template": "x"}

        result = worker._parse_html("https://arkleg.state.ar.us/page", "<html></html>")

        worker.fun_registry.get_processor.assert_called_once()
        worker.parser.get_content.assert_called_once()
        assert result == {"links": ["a", "b"]}

    def test_enqueue_links(self, worker, fake_graph, fake_node, lifoqueues):
        fetch_q, _ = lifoqueues
        fake_graph.add_new_node.return_value = MagicMock(id="child")
        worker._enqueue_links(fake_node, {"links": ["url1", "url2"]})
        worker._enqueue_links(fake_node, {"links": ["url1", "url2"]})



        # Should call add_node twice
        assert fake_graph.add_new_node.call_count == 2
        # Should enqueue both in fetch queue
        assert fetch_q.qsize() == 2

    def test_process_success(self, worker, fake_node, lifoqueues):
        fetch_q, process_q = lifoqueues
        domain = urlparse(fake_node.url).netloc
        worker.create_crawlers({fake_node})
        worker.crawlers[domain].get_page.return_value = "<html>"
        worker.parser.get_content.return_value = {"links": ["A", "B"]}
        worker.fun_registry.get_processor.side_effect = [
            {"fetch_template": True},  # FETCH
            {"process_template": True},  # PROCESS next step
        ]

        worker.process(fake_node)

        assert fake_node.state == PipelineStateEnum.AWAITING_PROCESSING
        assert fake_node.data["html"] == "<html>"
        assert process_q.qsize() == 1

    def test_process_no_next_step(self, worker, fake_node):
        domain = urlparse(fake_node.url).netloc
        worker.create_crawlers({fake_node})
        worker.crawlers[domain].get_page.return_value = "<html>"
        worker.parser.get_content.return_value = {"links": []}
        worker.fun_registry.get_processor.side_effect = [
            {"fetch_template": True},
            None,  # No PROCESS stage
        ]

        worker.process(fake_node)
        assert fake_node.state == PipelineStateEnum.AWAITING_CHILDREN

    def test_error_in_fetch_html(self, worker, fake_node, lifoqueues):
        fetch_q, process_q = lifoqueues
        domain = urlparse(fake_node.url).netloc
        worker.create_crawlers({fake_node})
        worker.crawlers[domain].get_page.side_effect = Exception("fetch failed")

        fetch_q.put(None)
        fetch_q.put(fake_node)

        worker.start()
        worker.join()

        assert fake_node.state == PipelineStateEnum.ERROR
        assert process_q.get() == None
        assert process_q.qsize() == 0

    def test_error_in_parse_html(self, worker, fake_node, lifoqueues):
        fetch_q, process_q = lifoqueues
        domain = urlparse(fake_node.url).netloc
        worker.create_crawlers({fake_node})
        worker.crawlers[domain].get_page.return_value = "<html>"
        worker.parser.get_content.side_effect = Exception("parse failed")

        fetch_q.put(None)
        fetch_q.put(fake_node)

        worker.start()
        worker.join()

        assert fake_node.state == PipelineStateEnum.ERROR
        assert process_q.get() == None
        assert process_q.qsize() == 0

    def test_error_in_enqueue_links(self, worker, fake_node, lifoqueues):
        fetch_q, process_q = lifoqueues
        domain = urlparse(fake_node.url).netloc
        worker.create_crawlers({fake_node})
        print(f"Worker crawler keys = {worker.crawlers.keys()}")
        worker.crawlers[domain].get_page.return_value = "<html>"
        worker.parser.get_content.return_value = {"links": ["A"]}
        worker.state.add_new_node.side_effect = Exception("tree failed")

        fetch_q.put(None)
        fetch_q.put(fake_node)

        worker.start()
        worker.join()

        assert fake_node.state == PipelineStateEnum.ERROR
        assert process_q.get() == None
        assert process_q.qsize() == 0


@pytest.fixture
def processor_worker(fake_graph):
    parser = MagicMock(spec=HTMLParser)
    transformer = MagicMock(spec=PipelineTransformer)
    registry = MagicMock()
    input_q = LifoQueue()
    output_q = Queue()
    return ProcessorWorker(
        input_queue=input_q,
        output_queue=output_q,
        state=fake_graph,
        parser=parser,
        transformer=transformer,
        fun_registry=registry,
        strict=False,
    )


@pytest.fixture
def loader_worker(fake_graph, fake_db_conn, fake_fun_registry):
    q = Queue()
    return LoaderWorker(
        input_queue=q,
        state=fake_graph,
        db_conn=fake_db_conn,
        fun_registry=fake_fun_registry,
        strict=False,
    )


class TestProcessorWorker:

    def test_process_success(self, processor_worker, fake_node):
        processor_worker.parser.get_content.return_value = {"parsed_key": "parsed_value"}
        processor_worker.transformer.transform_content.return_value = {"transformed_key": "transformed_value"}
        processor_worker.fun_registry.get_processor.return_value = {
            "state_key": (lambda n, s, y: {"state_val": 123}, lambda x: "fn"),
            "title": ("sel", "tr"),
        }

        processor_worker.process(fake_node)

        assert fake_node.state == PipelineStateEnum.AWAITING_LOAD
        assert isinstance(fake_node.data, LoaderObj)
        loader_obj = fake_node.data
        assert loader_obj.params["transformed_key"] == "transformed_value"
        assert processor_worker.output_queue.get_nowait() == loader_obj

    def test_process_raises_on_invalid_loader(self, processor_worker, fake_node):
        """
        Ensure ProcessorWorker.process raises an exception when the loader object
        cannot be created (i.e., transformed_data is None).
        """

        # Mock parser to return some parsed data
        processor_worker.parser.get_content.return_value = {"parsed_key": "parsed_value"}

        # Patch _get_processing_templates to return valid templates
        processor_worker._get_processing_templates = MagicMock(
            return_value=(
                {"title": lambda html: "parsed"},  # selector_template
                {"title": lambda x: x},  # transformer_template
                {},  # state_key_pairs
            ),
        )

        # Patch _transform_data to return None, forcing loader creation failure
        processor_worker._transform_data = MagicMock(return_value=None)

        # Ensure _attach_state_values returns a dict so we reach loader creation
        processor_worker._attach_state_values = MagicMock(
            return_value={"parsed_key": "parsed_value"},
        )

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

    def test_get_processing_templates(self, processor_worker, fake_node):
        processor_worker.fun_registry.get_processor.return_value = {
            "state_key": (lambda n, s: {}, lambda x: None),
            "a": ("sel", "tr"),
            "b": ("sel2", "tr2"),
        }
        selector, transformer, state_pair = processor_worker._get_processing_templates(
            "https://arkleg.state.ar.us/page", fake_node)
        assert selector == {"a": "sel", "b": "sel2"}
        assert transformer == {"a": "tr", "b": "tr2"}
        assert state_pair is not None
        assert list(state_pair.keys()) == ["state_key"]
        value = state_pair["state_key"]
        assert isinstance(value, tuple)
        assert len(value) == 2
        assert all(callable(v) for v in value)


class TestLoaderWorker:

    def test_process_success(self, loader_worker, fake_loader_obj, fake_db_conn, fake_graph):
        loader_worker.process(fake_loader_obj)

        assert fake_loader_obj.node.state == PipelineStateEnum.COMPLETED
        assert fake_loader_obj.node.data == {"db_result": "ok"}
        fake_db_conn.commit.assert_called_once()
        fake_db_conn.rollback.assert_not_called()
        fake_graph.safe_remove_root.assert_called_with(fake_loader_obj.node.url)

    def test_process_load_returns_none(self, loader_worker, fake_loader_obj, fake_db_conn):
        fake_loader = MagicMock()
        fake_loader.execute.return_value = None  # simulate SQL execution returning None

        loader_worker.fun_registry.get_processor.return_value = fake_loader

        loader_worker.process(fake_loader_obj)

        assert fake_loader_obj.node.data is None
        assert fake_loader_obj.node.state == PipelineStateEnum.COMPLETED
        fake_db_conn.commit.assert_called_once()

    def test_process_raises_exception(self, loader_worker, fake_loader_obj, fake_db_conn):
        def raise_error(params, db): raise Exception("DB error")

        fake_loader = MagicMock()
        fake_loader.execute.side_effect = Exception("DB error")

        loader_worker.fun_registry.get_processor.return_value = fake_loader

        with pytest.raises(Exception) as excinfo:
            loader_worker.process(fake_loader_obj)

        assert "DB error" in str(excinfo.value)
        fake_db_conn.rollback.assert_called_once()
        fake_db_conn.commit.assert_called_once()

    def test_remove_if_children_not_completed(self, loader_worker, fake_loader_obj, fake_graph):
        child_node = MagicMock()
        child_node.state = PipelineStateEnum.PROCESSING
        fake_loader_obj.node.children.append("child_id")
        fake_graph.find_node.return_value = child_node

        loader_worker._remove_nodes(fake_loader_obj.node)
        fake_graph.safe_remove_node.assert_not_called()

    def test_remove_if_children_completed(self, loader_worker, fake_loader_obj, fake_graph):
        child_node = MagicMock()
        child_node.state = PipelineStateEnum.COMPLETED
        fake_loader_obj.node.children.append("child_id")
        fake_graph.find_node.return_value = child_node

        loader_worker._remove_nodes(fake_loader_obj.node)
        fake_graph.safe_remove_root.assert_called_with(fake_loader_obj.node.url)
