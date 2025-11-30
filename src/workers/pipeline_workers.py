"""Thread workers for pipeline tasks."""

import threading
import time
from dataclasses import dataclass
from queue import Empty, LifoQueue, Queue

import psycopg

from src.config.settings import state_cache_file
from src.data_pipeline.extract.html_parser import HTMLParser
from src.data_pipeline.extract.webcrawler import Crawler
from src.data_pipeline.transform.pipeline_transformer import PipelineTransformer
from src.models.selector_template import SelectorTemplate
from src.structures.indexed_tree import IndexedTree, PipelineStateEnum
from src.structures.registries import PIPELINE_REGISTRY, PipelineRegistries, PipelineRegistryKeys
from src.utils.logger import logger


class CrawlerWorker(threading.Thread):
    """Thread to consume the crawler queue and fetch external data."""

    def __init__(
        self,
        fetch_queue: LifoQueue,
        process_queue: LifoQueue,
        state_tree: IndexedTree,
        crawler: Crawler,
        *,
        strict: bool = False,
    ) -> None:
        """Initialize the crawler worker."""
        super().__init__()
        self.fetch_queue = fetch_queue
        self.process_queue = process_queue
        self.state = state_tree
        self.running = True
        self.strict = strict
        self.crawler = crawler

    def run(self) -> None:
        """Run the crawler worker."""
        while self.running:
            try:
                # Get a node from the queue (blocks for 1 second)
                node = self.fetch_queue.get(timeout=1)
                with node.lock:
                    node.state = PipelineStateEnum.FETCHING

                    url = node.url
                    if url:
                        logger.info(f"[CRAWLER] Fetching URL for Node ID {node.id}: {url}")
                        base_url, url_stem = url.split("/", 1)
                        html_text = self.crawler.get_page(url_stem)
                        node.data["html"] = html_text

                        # PARSE HTML FOR NEXT LINKS
                        parser_url = url.split("?", 1)[0]
                        html_parser = HTMLParser(strict=self.strict)
                        parsing_template = PIPELINE_REGISTRY.get(parser_url).get(
                            PipelineRegistries.FETCH,
                        )
                        new_links = html_parser.get_content(parsing_template, html_text)  # type: ignore  # noqa: PGH003

                        # Set up new links to fetch
                        for key, item in new_links.items():
                            if not item:
                                continue
                            if not isinstance(item, list):
                                item = [item]  # noqa: PLW2901
                            for it in item:
                                new_node = self.state.add_node(
                                    parent=node.id,
                                    url=it,
                                    state=PipelineStateEnum.AWAITING_FETCH,
                                    node_type=PipelineRegistryKeys(key.upper()),
                                )
                                self.fetch_queue.put(new_node)

                        # Finish
                        node.state = PipelineStateEnum.AWAITING_PROCESSING
                        self.process_queue.put(node)
                        self.fetch_queue.task_done()
                        time.sleep(2)  # Sleep so don't flood web server

            except Empty:
                # If queue is empty, break if the system should shut down,
                # or continue if waiting for new jobs (as we do here).
                continue
            except Exception as e:
                msg = f"[CRAWLER ERROR] Node ID {node.id}: {e}"
                logger.warning(msg)


class ProcessorWorker(threading.Thread):
    """Thread to consume the processor queue and handle internal data processing."""

    def __init__(
        self,
        input_queue: LifoQueue,
        output_queue: Queue,
        state_tree: IndexedTree,
        *,
        strict: bool,
    ) -> None:
        """Initialize the processor worker."""
        super().__init__()
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.state = state_tree
        self.running = True
        self.strict = strict

    def run(self) -> None:
        """Run the processor worker."""
        while self.running:
            try:
                node = self.input_queue.get(timeout=1)
                with node.lock:
                    node.state = PipelineStateEnum.PROCESSING
                    url = node.url
                    logger.info(f"[PROCESSOR] Processing data for Node ID {node.id}")
                    parser_url = url.split("?", 1)[0]
                    html_parser = HTMLParser(strict=self.strict)
                    parsing_templates = PIPELINE_REGISTRY.get(parser_url).get(
                        PipelineRegistries.PROCESS,
                    )
                    selector_template = {
                        key: val[0] for key, val in parsing_templates.selectors.items()
                    }
                    selector_template = SelectorTemplate("", selector_template)
                    transformer_template = {
                        key: val[1] for key, val in parsing_templates.selectors.items()
                    }
                    parsed_data = html_parser.get_content(selector_template, node.data["html"])
                    transformer = PipelineTransformer(strict=self.strict)
                    transformed_data = transformer.transform_content(
                        transformer_template,
                        parsed_data,
                    )
                    if not isinstance(transformed_data, list):
                        transformed_data = [transformed_data]

                    for item in transformed_data:
                        loader_obj = LoaderObj(node_id=node.id, name=node.type, params={item})
                    node.state = PipelineStateEnum.AWAITING_LOAD
                    self.output_queue.put(loader_obj)

                    self.input_queue.task_done()

            except Empty:
                continue
            except Exception as e:
                logger.warning(f"[PROCESSOR ERROR] Node ID {node.id}: {e}")


@dataclass
class LoaderObj:
    node_id: str
    name: str
    params: dict


class LoaderWorker(threading.Thread):
    """Thread to consume the loader queue and handle database insertion."""

    def __init__(
        self,
        input_queue: Queue,
        state_tree: IndexedTree,
        db_conn: psycopg.Connection,
    ) -> None:
        """Initialize the loader worker."""
        super().__init__()
        self.input_queue = input_queue
        self.state = state_tree
        self.running = True
        self.db_conn = db_conn

    def run(self) -> None:
        """Run the processor worker."""
        while self.running:
            try:
                loader_obj = self.input_queue.get(timeout=1)
                node = self.state.find_node(loader_obj.node_id)
                with node.lock:
                    node.state = PipelineStateEnum.LOADING
                    logger.info(f"[LOADER] Loading data for Node ID {node.id}")
                    # loader_obj.name registry lookup
                    # loader_obj.params insert into load function
                    # node.data = loaded_result
                    node.state = PipelineStateEnum.COMPLETED
                    keep_node = False
                    for child_id in node.children:
                        child = self.state.find_node(child_id)
                        if child.state != PipelineStateEnum.COMPLETED:
                            keep_node = True

                    if not keep_node:
                        self.state.safe_remove_node(node.id, cascade_up=True)
                    self.state.save_file(state_cache_file)

                    self.input_queue.task_done()
            except Empty:
                continue
            except Exception as e:
                logger.warning(f"[LOADER ERROR] Node ID {node.id}: {e}")
