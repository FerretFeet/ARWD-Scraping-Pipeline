# tests/integration_test/src/data_pipeline/test_orchestrate.py
from queue import LifoQueue, Queue
from unittest.mock import MagicMock, patch

import pytest

from src.data_pipeline.orchestrate import Orchestrator
from src.structures.indexed_tree import IndexedTree, PipelineStateEnum
from src.structures.registries import ProcessorRegistry


@pytest.fixture
def empty_tree():
    """Return an empty IndexedTree. Root will be created in Orchestrator."""
    return IndexedTree()  # No root node yet

@pytest.fixture
def processor_registry():
    """Provide a dummy processor registry"""
    return ProcessorRegistry()

@patch("src.data_pipeline.extract.webcrawler.Crawler.get_page", return_value="<html></html>")
@patch("src.data_pipeline.extract.html_parser.HTMLParser.get_content")
@patch("src.data_pipeline.transform.pipeline_transformer.PipelineTransformer.transform_content",
       return_value={"dummy_field": "dummy_value"})
@patch("src.structures.registries.ProcessorRegistry.get_processor")
@patch("src.workers.pipeline_workers.LoaderWorker._remove_if_children_completed", lambda self, node: None)
def test_orchestrator_full_flow(mock_get_processor, mock_transform, mock_parser, mock_crawler,
                                empty_tree, processor_registry):
    """
    Full integration test of Orchestrator with real workers.
    - Tree starts empty
    - Root node created dynamically from seed URLs
    - External I/O and DB are mocked
    """

    # Make parser return at least one child URL for each processed node
    mock_parser.side_effect = lambda template, html: {"child_page": "https://arkleg.state.ar.us/Legislators/List"}

    # Make registry always return a dummy processor template for all pages/stages
    mock_get_processor.side_effect = lambda page_enum, stage: {"state_key": (lambda node, tree: {}, {})}

    # Create orchestrator with multiple seed URLs
    orchestrator = Orchestrator(
        state=empty_tree,
        registry=processor_registry,
        seed_urls=["https://arkleg.state.ar.us/", "https://arkleg.state.ar.us/Bills/SearchByRange"],
        db_conn=MagicMock(),  # mock DB connection
        crawler_queue=LifoQueue(),
        processor_queue=Queue(),
        loader_queue=Queue(),
        strict=False,
    )

    # Run the full pipeline
    orchestrator.orchestrate()

    # --- Assertions ---

    # All queues should be empty
    assert orchestrator.crawler_queue.empty()
    assert orchestrator.processor_queue.empty()
    assert orchestrator.loader_queue.empty()

    # Tree should have at least the root node and its children
    # All seeds should have been visited
    assert len(orchestrator.visited) > 0
    for url in ["https://arkleg.state.ar.us/", "https://arkleg.state.ar.us/Bills/SearchByRange"]:
        assert url in orchestrator.visited

    all_nodes = orchestrator._visited_nodes  # noqa: SLF001
    assert len(all_nodes) > 0

    # All nodes should be processed (some stage of FETCHED → COMPLETED)
    for node in all_nodes:
        assert node.state in (
            PipelineStateEnum.COMPLETED,
        )
