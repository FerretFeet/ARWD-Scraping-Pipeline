"""Thread workers for pipeline tasks."""

import threading
import time
from dataclasses import dataclass
from queue import Empty, LifoQueue, Queue
from urllib.parse import urlparse

import psycopg

from src.config.settings import state_cache_file
from src.data_pipeline.extract.html_parser import HTMLParser
from src.data_pipeline.extract.webcrawler import Crawler
from src.data_pipeline.transform.pipeline_transformer import PipelineTransformer
from src.structures import indexed_tree
from src.structures.indexed_tree import IndexedTree, PipelineStateEnum
from src.structures.registries import (
    PipelineRegistries,
    PipelineRegistryKeys,
    ProcessorRegistry,
    get_enum_by_url,
)
from src.utils.logger import logger


@dataclass
class LoaderObj:
    node: indexed_tree.Node
    name: PipelineRegistryKeys
    params: dict

class CrawlerWorker(threading.Thread):
    """Thread to consume the crawler queue and fetch external data."""

    def __init__(
        self,
        fetch_queue: LifoQueue,
        process_queue: LifoQueue,
        state_tree: IndexedTree,
        crawler: Crawler,
        fun_registry: ProcessorRegistry,
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
        self.fun_registry = fun_registry

    def run(self) -> None:
        """Run the crawler worker."""
        while self.running:
            node = None
            try:
                # 1. Get Node (Blocks for 1 second)
                node = self.fetch_queue.get(timeout=1)
                print(f"[CRAWLER] Fetched node {node.id} with URL: {node.url}")

                # 2. Process Node
                try:
                    with node.lock:
                        node.state = PipelineStateEnum.FETCHING
                        print(f"[CRAWLER] Node {node.id} state set to FETCHING")

                        # --- Processing Logic ---

                        url = node.url
                        if url:
                            print(f"[CRAWLER] Fetching URL for Node ID {node.id}: {url}")
                            # Assuming URL format allows splitting by first "/"
                            parsed_url = urlparse(url)
                            url_stem = parsed_url.path

                            html_text = self.crawler.get_page(url_stem)
                            node.data["html"] = html_text
                            print(f"[CRAWLER] Fetched HTML content for Node ID {node.id}")
                            # 2.1 Parse HTML for next links
                            parser_url = url.split("?", 1)[0]
                            print(f"PARSER URL = {parser_url}")
                            html_parser = HTMLParser(strict=self.strict)
                            parser_enum = get_enum_by_url(parser_url)
                            print(f" parser enum is {parser_enum}")
                            parsing_template = self.fun_registry.get_processor(
                                parser_enum, PipelineRegistries.FETCH,
                            )
                            new_links = html_parser.get_content(parsing_template, html_text)
                            print(f"[CRAWLER] Found new links: {new_links}")

                            # 2.2 Queue New Links
                            for key, item in new_links.items():
                                if not item:
                                    continue
                                print(f"item is: {item}")
                                if not isinstance(item, list):
                                    item = [item]  # noqa: PLW2901
                                for it in item:
                                    print(f"it is {it}")
                                    url_type = it.split("?", 1)[0]
                                    print("error here?")
                                    url_enum = get_enum_by_url(url_type)
                                    print(f"making new node with {url_enum}")
                                    new_node = self.state.add_node(
                                        parent=node.id,
                                        url=it,
                                        state=PipelineStateEnum.AWAITING_FETCH,
                                        node_type=url_enum,
                                    )
                                    self.fetch_queue.put(new_node)
                                    print(f"[CRAWLER] Added new node {new_node.id} to fetch queue")

                            # 2.3 Finish processing the current node
                            print("UPDATING STATE")
                            node.state = PipelineStateEnum.AWAITING_PROCESSING
                            print("STATE UPDATED")
                            self.process_queue.put(node)
                            print(f"[CRAWLER] Node {node.id} state set to AWAITING_PROCESSING")

                # 3. Handle processing exceptions
                except Exception as processing_error:
                    # Set the state to ERROR if the node was successfully retrieved
                    if node:
                        node.state = PipelineStateEnum.ERROR
                        logger.warning(f"[CRAWLER ERROR] Node ID {node.id}: {processing_error}")
                    # Re-raise the error to be caught by the outer block for logging/flow control
                    raise processing_error

                # 4. Final Cleanup (Always called if node was retrieved)
                finally:
                    # 🟢 CRITICAL: Ensure task_done is called exactly once here.
                    self.fetch_queue.task_done()
                    print(f"Node {node.id} task done.")

                # Optional: Sleep outside the lock for throttling
                time.sleep(0.2)

            except Empty:
                # Queue timed out (empty)
                time.sleep(0.2)
                continue  # Go back to the top of the while loop

            except Exception as e:
                # Handles exceptions from queue.get() or re-raised exceptions from processing
                if not node:
                    logger.error(f"[CRAWLER FATAL] Error during queue get: {e}")

class ProcessorWorker(threading.Thread):
    """Thread to consume the processor queue and handle internal data processing."""

    def __init__(
        self,
        input_queue: LifoQueue,
        output_queue: Queue,
        state_tree: IndexedTree,
        fun_registry: ProcessorRegistry,
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
        self.fun_registry = fun_registry

    def run(self) -> None:
        """Run the processor worker."""
        while self.running:
            node = None  # Initialize node here for outer scope access
            try:
                node = self.input_queue.get(timeout=1)
                logger.info(
                    f"[PROCESSOR] Fetched node {node.id} for processing with URL: {node.url}",
                )

                # 🚨 FIX: Ensure task_done is called, even on exception
                try:
                    with node.lock:
                        node.state = PipelineStateEnum.PROCESSING

                        url = node.url
                        parser_url = url.split("?", 1)[0]
                        # ... (rest of parsing/transformer logic) ...

                        html_parser = HTMLParser(strict=self.strict)

                        parser_enum = get_enum_by_url(parser_url)
                        parsing_templates = self.fun_registry.get_processor(
                            parser_enum, PipelineRegistries.PROCESS,
                        )

                        # Separate selectors and transformers from parsing templates
                        selector_template = {
                            key: val[0]
                            for key, val in parsing_templates.items()
                            if key != "state_key"
                        }
                        transformer_template = {
                            key: val[1] for key, val in parsing_templates.items()
                                if key != "state_key"
                        }
                        get_state_fun = parsing_templates.get("state_key", [None])[0]

                        state_dict = {} if not get_state_fun else get_state_fun(node, self.state)

                        parsed_data = html_parser.get_content(selector_template, node.data["html"])
                        parsed_data.update(state_dict)
                        keys = state_dict.keys()
                        state_transform = {}
                        for key in keys:
                            state_transform.update({key: parsing_templates.get("state_key", None)[1]})
                        transformer_template.update(state_transform)
                        transformer = PipelineTransformer(strict=self.strict)
                        transformed_data = transformer.transform_content(
                            transformer_template, parsed_data,
                        )
                        if isinstance(transformed_data, list):
                            transformed_data = transformed_data[0]
                        if isinstance(transformed_data, dict):
                            loader_obj = LoaderObj(
                                node=node, name=node.type, params=dict(transformed_data.items()),
                            )
                        else:
                            loader_obj = LoaderObj(
                                node=node, name=node.type, params={"item": transformed_data},
                            )

                        node.state = PipelineStateEnum.AWAITING_LOAD
                        self.output_queue.put(loader_obj)
                        logger.info(
                            f"[PROCESSOR] Node {node.id} state set to AWAITING_LOAD,"
                            f"LoaderObj put in output queue",
                        )

                except Exception as inner_e:
                    # 🚨 FIX: Revert state to FAILED/ERROR if processing fails
                    if node and node.state == PipelineStateEnum.PROCESSING:
                        # Assuming you have a FAILED or ERROR state enum
                        node.state = PipelineStateEnum.ERROR

                        # Re-raise the exception to be logged by the outer block
                    raise inner_e

                finally:
                    # 🚨 FIX: Ensure task_done() is always called after processing a fetched node
                    self.input_queue.task_done()

            except Empty:
                # Only sleep and continue if the queue was empty (timeout hit)
                time.sleep(0.2)
                continue

            except Exception as e:
                # This handles exceptions raised by the inner block (re-raised)
                if node:
                    logger.warning(f"[PROCESSOR ERROR] Node ID {node.id}: {e}")
                else:
                    logger.warning(f"[PROCESSOR ERROR] Could not fetch node: {e}")

class LoaderWorker(threading.Thread):
    """Thread to consume the loader queue and handle database insertion."""

    def __init__(
        self,
        input_queue: Queue,
        state_tree: IndexedTree,
        db_conn: psycopg.Connection,
        fun_registry: ProcessorRegistry,
        *,
        strict: bool = False,
    ) -> None:
        """Initialize the loader worker."""
        super().__init__()
        self.input_queue = input_queue
        self.state = state_tree
        self.running = True
        self.db_conn = db_conn
        self.fun_registry = fun_registry
        self.strict= strict

    def run(self) -> None:
        """Run the processor worker."""
        while self.running:
            try:
                loader_obj: LoaderObj = self.input_queue.get(timeout=1)
                node = loader_obj.node
                with node.lock:
                    node.state = PipelineStateEnum.LOADING
                    logger.info(f"[LOADER] Loading data for Node ID {node.id}")
                    page_enum = loader_obj.name
                    loader_fun = self.fun_registry.get_processor(page_enum, PipelineRegistries.LOAD)
                    loader = PipelineLoader(strict=self.strict)
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
