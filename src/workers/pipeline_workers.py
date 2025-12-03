"""Thread workers for pipeline tasks."""

import threading
import time
from dataclasses import dataclass
from queue import Empty, LifoQueue, Queue
from urllib.parse import urlparse

import psycopg

from src.config.settings import known_links_cache_file, state_cache_file
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
from src.utils.json_list import append_to_json_list
from src.utils.logger import logger
from src.utils.strings.get_url_base_path import get_url_base_path
from src.workers.base_worker import BaseWorker


@dataclass
class LoaderObj:
    node: indexed_tree.Node
    name: PipelineRegistryKeys
    params: dict

class CrawlerWorker(BaseWorker):
    """Thread to consume the crawler queue and fetch external data."""

    def __init__(
        self,
        fetch_queue: LifoQueue,
        process_queue: LifoQueue,
        state_tree: IndexedTree,
        crawler: Crawler,
        parser: HTMLParser,
        fun_registry: ProcessorRegistry,
        *,
        strict: bool = False,
    ) -> None:
        """Initialize the crawler worker."""
        super().__init__(fetch_queue)
        self.process_queue = process_queue
        self.state = state_tree
        self.running = True
        self.strict = strict
        self.crawler = crawler
        self.parser = parser
        self.fun_registry = fun_registry

    def process(self, node: indexed_tree.Node) -> None:
        self._set_state(node, PipelineStateEnum.FETCHING)
        html = self._fetch_html(node.url)
        links: dict = self._parse_html(node.url, html)
        self._enqueue_links(node, links)
        if self._check_processing_step(node.url):
            print("Preparing for next step")
            node.data["html"] = html
            self._set_state(node, PipelineStateEnum.AWAITING_PROCESSING)
            self.process_queue.put(node)
            print("Finished preparing for next step")
        else: # No next step, dont send to queue
            self._set_state(node, PipelineStateEnum.COMPLETED)
            print("No next step, finished.")







    def _check_processing_step(self, url: str) -> bool:
        print(f"_check_processing_step: url {url}")
        parsed_url = get_url_base_path(url)
        p_enum = get_enum_by_url(parsed_url)
        return bool(self.fun_registry.get_processor(p_enum, PipelineRegistries.PROCESS))



    def _enqueue_links(self, node: indexed_tree.Node, links: dict) -> None:
        print(f"_enqueue_links: node {node.id}, links: {links}")

        for item in links.values():
            if not item:
                continue
            if not isinstance(item, list):
                item = [item]  # noqa: PLW2901
            for it in item:
                url_type = get_url_base_path(node.url)
                url_enum = get_enum_by_url(url_type)
                new_node = self.state.add_node(
                    parent=node,
                    url=it,
                    state=PipelineStateEnum.AWAITING_FETCH,
                    node_type=url_enum,
                )
                print(f"_enqueue_links: new node {new_node.id}")
                self.input_queue.put(new_node)

    def _parse_html(self, url: str, html: str) -> dict:
        print(f"_parse_html: {url}, {html[:20]}")
        parsed_url = get_url_base_path(url)
        parser_enum = get_enum_by_url(parsed_url)
        template = self.fun_registry.get_processor(parser_enum, PipelineRegistries.FETCH)
        print(f"template: {template}")
        return self.parser.get_content(template, html)

    def _fetch_html(self, url: str) -> str:
        print(f"_fetch_html: {url}")
        parsed_url = urlparse(url)
        return self.crawler.get_page(parsed_url.geturl())



    def _set_state(self, node: indexed_tree.Node, state: PipelineStateEnum) -> None:
        print(f"_set_state: node id: {node.id}, state: {state}")
        with node.lock:
            node.state = state


    # def run(self) -> None:
    #     """Run the crawler worker."""
    #     while self.running:
    #         node = None
    #         try:
    #             # 1. Get Node (Blocks for 1 second)
    #             node = self.fetch_queue.get(timeout=1)
    #             logger.info(f"[CRAWLER] Fetched node {node.id} with URL: {node.url}")
    #             if node is None:
    #                 self.fetch_queue.task_done()
    #                 return
    #             # 2. Process Node
    #             try:
    #                 with node.lock:
    #                     node.state = PipelineStateEnum.FETCHING
    #
    #                     # --- Processing Logic ---
    #
    #                     url = node.url
    #                     if url:
    #                         # Assuming URL format allows splitting by first "/"
    #                         parsed_url = urlparse(url)
    #                         url_stem = parsed_url.path
    #
    #                         html_text = self.crawler.get_page(url_stem)
    #                         node.data["html"] = html_text
    #                         # 2.1 Parse HTML for next links
    #                         parser_url = url.split("?", 1)[0]
    #                         html_parser = HTMLParser(strict=self.strict)
    #                         parser_enum = get_enum_by_url(parser_url)
    #                         parsing_template = self.fun_registry.get_processor(
    #                             parser_enum, PipelineRegistries.FETCH,
    #                         )
    #                         new_links = html_parser.get_content(parsing_template, html_text)
    #
    #                         # 2.2 Queue New Links
    #                         for key, item in new_links.items():
    #                             if not item:
    #                                 continue
    #                             if not isinstance(item, list):
    #                                 item = [item]
    #                             for it in item:
    #                                 url_type = it.split("?", 1)[0]
    #                                 url_enum = get_enum_by_url(url_type)
    #                                 new_node = self.state.add_node(
    #                                     parent=node.id,
    #                                     url=it,
    #                                     state=PipelineStateEnum.AWAITING_FETCH,
    #                                     node_type=url_enum,
    #                                 )
    #                                 self.fetch_queue.put(new_node)
    #                                 logger.info(f"[CRAWLER] Added new node "
    #                                             f"{new_node.id} to fetch queue")
    #
    #                         # 2.3 Finish processing the current node
    #                         node.state = PipelineStateEnum.AWAITING_PROCESSING
    #                         self.process_queue.put(node)
    #
    #             # 3. Handle processing exceptions
    #             except Exception as processing_error:
    #                 # Set the state to ERROR if the node was successfully retrieved
    #                 if node:
    #                     node.state = PipelineStateEnum.ERROR
    #                     logger.warning(f"[CRAWLER ERROR] Node ID {node.id}: {processing_error}")
    #                 # Re-raise the error to be caught by the outer block for logging/flow control
    #                 raise processing_error
    #
    #             # 4. Final Cleanup (Always called if node was retrieved)
    #             finally:
    #                 # 🟢 CRITICAL: Ensure task_done is called exactly once here.
    #                 self.fetch_queue.task_done()
    #                 logger.info(f"[CRAWLER] Node {node.id} task done.")
    #
    #             # Optional: Sleep outside the lock for throttling
    #             time.sleep(0.2)
    #
    #         except Empty:
    #             # Queue timed out (empty)
    #             time.sleep(0.2)
    #             continue  # Go back to the top of the while loop
    #
    #         except Exception as e:
    #             # Handles exceptions from queue.get() or re-raised exceptions from processing
    #             if not node:
    #                 logger.error(f"[CRAWLER FATAL] Error during queue get: {e}")

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
                if node is None:
                    self.input_queue.task_done()
                    return
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
                        original_templates: dict = self.fun_registry.get_processor(
                            parser_enum, PipelineRegistries.PROCESS,
                        )
                        parsing_templates = original_templates.copy()

                        state_key_pair = parsing_templates.pop("state_key", (None,None))
                        get_state_fun = state_key_pair[0]
                        state_transform_fun = state_key_pair[1]
                        # Separate selectors and transformers from parsing templates
                        selector_template = {
                            key: val[0]
                            for key, val in parsing_templates.items()
                        }
                        transformer_template = {
                            key: val[1] for key, val in parsing_templates.items()
                        }

                        state_dict = {} if not get_state_fun else get_state_fun(node, self.state)
                        state_transform_dict = {}
                        for key in state_dict.keys():
                            state_transform_dict.update({key: state_transform_fun})
                        parsed_data = html_parser.get_content(selector_template, node.data["html"])
                        parsed_data.update(state_dict)
                        transformer_template.update(state_transform_dict)
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
                        print("done")

                except Exception as inner_e:
                    # 🚨 FIX: Revert state to FAILED/ERROR if processing fails
                    if node and node.state == PipelineStateEnum.PROCESSING:
                        # Assuming you have a FAILED or ERROR state enum
                        node.state = PipelineStateEnum.ERROR

                        # Re-raise the exception to be logged by the outer block
                    raise inner_e

                finally:
                    logger.info(f"[PROCESSOR] Node {node.id} task done")
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
        """Run the loader worker."""
        # Use 'self.running and self.input_queue.empty()' condition if you need to end on empty queue
        while self.running:
            loader_obj: LoaderObj = None # Initialize to None for error handling

            try:
                logger.info("[LOADER] Beginning process")
                # 1. Get the item (Safely handles Empty exception)
                loader_obj = self.input_queue.get(timeout=1)
                if loader_obj is None:
                    self.input_queue.task_done()
                    return
                node = loader_obj.node

                # --- Begin Transaction and Lock State ---

                with node.lock:
                    node.state = PipelineStateEnum.LOADING
                    logger.info(f"[LOADER] Starting transaction for Node ID {node.id}")

                # 2. Lookup Function
                page_enum = loader_obj.name
                loader_fun = self.fun_registry.get_processor(page_enum, PipelineRegistries.LOAD)

                # 3. Execute Load Function (where DB insert happens)
                result = loader_fun(loader_obj.params, self.db_conn)



                # Lock for final state update
                with node.lock:
                    node.data = result
                    node.state = PipelineStateEnum.COMPLETED

                    # 5. Check Children and Remove Node (KEEP THIS IN THE LOCK)
                    keep_node = any(
                        self.state.find_node(child_id).state != PipelineStateEnum.COMPLETED
                        for child_id in node.children
                    )

                    if not keep_node:
                        # self.state.safe_remove_node should handle its own state tree locks
                        self.state.safe_remove_node(node.id, cascade_up=True)

                # 6. Save State File (Needs external lock if state_cache_file is shared)
                # If self.state.save_file is not thread-safe, this must be protected
                # by a dedicated global state-saving lock. For now, we assume it's thread-safe
                # or we are relying on IndexedTree to handle file locking.
                # 4. Commit DB & Update State
                self.db_conn.commit() # 🚨 Commit the DB changes

                self.state.save_file(state_cache_file)
                append_to_json_list(known_links_cache_file, node.url)


            except Empty:
                continue # Safely skip and continue loop

            except Exception as e:
                # 🚨 Handle Failure: Log error, rollback DB, update state
                logger.warning(f"[LOADER ERROR] Node ID {node.id if loader_obj else 'Unknown'}: {e}")

                if loader_obj:
                    # Rollback the transaction if it failed
                    self.db_conn.rollback()

                    with node.lock:
                        node.state = PipelineStateEnum.ERROR # Mark node as failed

            finally:
                # 🚨 GUARANTEE: Ensure the task is always marked done if we got an item
                if loader_obj:
                    logger.info(f"[LOADER] Node ID {node.id} task done")
                    self.input_queue.task_done()
