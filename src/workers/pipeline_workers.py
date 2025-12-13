"""Thread workers for pipeline tasks."""
import time
from dataclasses import dataclass
from queue import LifoQueue, Queue
from urllib.parse import urljoin, urlparse

import psycopg

from src.config.pipeline_enums import PipelineRegistries, PipelineRegistryKeys
from src.config.settings import known_links_cache_file, state_cache_file
from src.data_pipeline.extract.html_parser import HTMLParser
from src.data_pipeline.extract.webcrawler import Crawler
from src.data_pipeline.transform.pipeline_transformer import PipelineTransformer
from src.data_pipeline.transform.utils.strip_session_from_string import strip_session_from_link
from src.data_pipeline.utils.fetch_scheduler import FetchScheduler
from src.structures import directed_graph
from src.structures.directed_graph import DirectionalGraph
from src.structures.indexed_tree import PipelineStateEnum
from src.structures.registries import (
    ProcessorRegistry,
    get_enum_by_url,
)
from src.utils.logger import logger
from src.utils.strings.get_url_base_path import get_url_base_path
from src.workers.base_worker import BaseWorker


@dataclass
class LoaderObj:
    node: directed_graph.Node
    name: PipelineRegistryKeys
    params: dict


def get_registry_template(registry: ProcessorRegistry, enum1: PipelineRegistryKeys,
                          enum2: PipelineRegistries):
    try:
        return registry.get_processor(enum1, enum2)
    except Exception as e:
        if "No processor found for" in str(e):
            return None
        raise
class CrawlerWorker(BaseWorker):
    """Thread to consume the crawler queue and fetch external data."""

    def __init__(
        self,
        input_queue: LifoQueue,
        output_queue: Queue,
        state: DirectionalGraph,
        crawler_cls: type[Crawler],
        parser: HTMLParser,
        fun_registry: ProcessorRegistry,
        *,
        fetch_scheduler: FetchScheduler,
        strict: bool = False,
        name: str = "Crawler Worker",
    ) -> None:
        """Initialize the crawler worker."""
        super().__init__(input_queue, output_queue, name=name)
        self.state = state
        self.running = True
        self.strict = strict
        self.crawler_cls = crawler_cls
        self.crawlers = {}
        self.parser = parser
        self.fun_registry = fun_registry
        self.fetch_scheduler = fetch_scheduler

    def process(self, node: directed_graph.Node) -> None:
        # print(f"Fetchers state: {self.state.get_roots()}")
        unqueued_nodes = self.state.find_in_graph(
            None, {"state": PipelineStateEnum.CREATED}, find_single=False,
        )
        if unqueued_nodes:
            for idx, tnode in enumerate(unqueued_nodes.copy()):
                tnode.set_state(PipelineStateEnum.AWAITING_FETCH)
                if idx == 0:
                    self.input_queue.put(node) # replace to ensure it doesn't bury the needed nodes
                    node = tnode
                    continue
                self.input_queue.put(tnode)

        final_flag = False
        self.create_crawlers(self.state.roots)
        while True: # loop to try to keep order while using scheduler
            working_node = self._check_scheduler(node)
            if working_node is node:
                final_flag = True
            if not working_node:
                if self.fetch_scheduler.time_until_next():
                    logger.info(f"[{self.name.upper()}]: Going to sleep until next node ready.")
                    time.sleep(self.fetch_scheduler.time_until_next())
                continue
            try:
                self._set_state(working_node, PipelineStateEnum.FETCHING)
                html = self._fetch_html(working_node.url)
                links: dict = self._parse_html(working_node.url, html)
                if links:
                    self._enqueue_links(node, links)
                if self._check_processing_step(working_node.url):
                    working_node.data["html"] = html
                    self._set_state(working_node, PipelineStateEnum.AWAITING_PROCESSING)
                    self.output_queue.put(working_node)
                else: # No next step, dont send to queue
                    self._set_state(working_node, PipelineStateEnum.AWAITING_CHILDREN)
                    node.data = links
            except Exception as e:
                logger.error(f"[{self.name.upper()}]: {e}")
                self._set_state(working_node, PipelineStateEnum.ERROR)
                raise
            if final_flag:
                break

    def create_crawlers(self, unique_domains: set[directed_graph.Node]) -> None:
        for key in unique_domains:
            self.crawlers[urlparse(key.url).netloc] = self.crawler_cls(key.url, strict=self.strict)


    def _check_processing_step(self, url: str) -> bool:
        parsed_url = get_url_base_path(url)
        p_enum = get_enum_by_url(parsed_url)
        return bool(get_registry_template(self.fun_registry, p_enum, PipelineRegistries.PROCESS))



    def _enqueue_links(self, node: directed_graph.Node, links: dict) -> None:


        for key, item in links.items():
            base_url = (
                get_url_base_path(node.url, include_path=False)
                if not key.startswith("ext_")
                else get_url_base_path(node.url)
            )  # Bill List next pages just attach query to path
            if not item:
                continue
            if not isinstance(item, list):
                item = [item]  # noqa: PLW2901

            for it in item:
                it = urljoin(base_url, it)
                if it in self.state.nodes:
                    continue
                url_type = get_url_base_path(it)
                url_enum = get_enum_by_url(url_type)
                new_node = self.state.add_new_node(it, url_enum, [node],
                    state=PipelineStateEnum.AWAITING_FETCH,
                )
                if new_node:
                    self.input_queue.put(new_node)
        self.state.save_file(state_cache_file)


    def _parse_html(self, url: str, html: str) -> dict | None:
        parsed_url = get_url_base_path(url)
        parser_enum = get_enum_by_url(parsed_url)
        template = get_registry_template(self.fun_registry, parser_enum, PipelineRegistries.FETCH)
        return self.parser.get_content(template.copy(), html) if template else None

    def _check_scheduler(self, node: directed_graph.Node) -> directed_graph.Node | None:

        domain = urlparse(node.url).netloc

        priority_item: directed_graph.Node = self.fetch_scheduler.pop_due()
        if not priority_item and self.fetch_scheduler.can_fetch_now(domain): return node
        if (not priority_item and self.fetch_scheduler.time_until_next() and
                self.fetch_scheduler.time_until_next() < 0.005):
            while not self.fetch_scheduler.can_fetch_now(domain):
                continue
            return node
        if priority_item:
            domain = urlparse(node.url).netloc
            self.fetch_scheduler.schedule_retry(node,
                                                self.fetch_scheduler.next_allowed_time(domain))
            pi_domain = urlparse(priority_item.url).netloc
            self.fetch_scheduler.mark_fetched(pi_domain)
            return priority_item
        return None


    def _fetch_html(self, url: str) -> str:
        parsed_url = urlparse(url)
        html = self.crawlers[parsed_url.netloc].get_page(parsed_url.geturl())
        self.fetch_scheduler.mark_fetched(parsed_url.netloc)
        return html



class ProcessorWorker(BaseWorker):
    """Thread to consume the processor queue and handle internal data processing."""

    def __init__(
        self,
        input_queue: Queue,
        output_queue: Queue,
        state: directed_graph.DirectionalGraph,
        parser: HTMLParser,
        transformer: PipelineTransformer,
        fun_registry: ProcessorRegistry,
        *,
        strict: bool,
        name: str = "Processor Worker",
    ) -> None:
        """Initialize the processor worker."""
        super().__init__(input_queue, name=name)
        self.output_queue = output_queue
        self.state = state
        self.parser = parser
        self.transformer = transformer
        self.strict = strict
        self.fun_registry = fun_registry


    def process(self, node: directed_graph.Node) -> None:
        self._set_state(node, PipelineStateEnum.PROCESSING)
        t_parser, t_transformer, state_pairs = self._get_processing_templates(node.url, node)
        if not t_parser or not t_transformer:
            msg = f"Expected parser and transformer templates for node {node} in processor worker"
            raise Exception(msg)  # noqa: TRY002
        parsed_data = self._parse_html(node.url, node.data["html"], t_parser)
        # --- Attach state values after parse, parser not able to handle
        parsed_data, t_transformer = self.inject_session_code(parsed_data, t_transformer, node)
        transformed_data = self._transform_data(parsed_data, t_transformer)
        if transformed_data:
            transformed_data.update({"url": node.url})
        temptemplates = self._attach_state_values(node, transformed_data, t_transformer, state_pairs)
        if temptemplates is None:
            return
        node.data = transformed_data
        # loader_obj = self._create_loader_object(transformed_data, node)
        if node.data:
            self._set_state(node, PipelineStateEnum.AWAITING_LOAD)
            self.output_queue.put(node)
        else:
            msg = f"loader obj not able to be created for {node}"
            logger.error(msg)
            raise Exception(msg)  # noqa: TRY002

    def _create_loader_object(self, transformed_data: dict, node: directed_graph.Node):
        if not transformed_data: return None
        if isinstance(transformed_data, list):
            transformed_data = transformed_data[0]
        if isinstance(transformed_data, dict):
            loader_obj = LoaderObj(
                node=node,
                name=node.type,
                params=dict(transformed_data.items()),
            )
        else:
            loader_obj = LoaderObj(
                node=node,
                name=node.type,
                params={"item": transformed_data},
            )
        return loader_obj

    def _transform_data(self, parsed_data: dict, transformer_template: dict) -> dict:
        return self.transformer.transform_content(
            transformer_template,
            parsed_data,
        )

    def _attach_state_values(
        self, node: directed_graph.Node, parsed_data: dict, t_transformer: dict,
            state_pairs: dict[str, tuple],
    ) -> dict | None:
        for key in state_pairs:
            state_dict = state_pairs[key][0](node, self.state, parsed_data)
            if state_dict == 0:
                continue
            if not state_dict:
                #Wait for node data to be loaded
                node.set_state(PipelineStateEnum.AWAITING_PROCESSING)
                logger.info(f"[{self.name.upper()}]: Waiting for state transformer dependancies"
                            f" to process for key {key}:\n"
                            f"parsed_data: {parsed_data}\n"
                            f"state_dict: {state_dict}\n")
                # TODO: implement a scheduler or another mechanism to stop the thread from repeatedly trying to process a node waiting on dependent nodes
                time.sleep(3)
                self.input_queue.put(node)

                return None
            parsed_data.update(state_dict)
            t_transformer.update(state_dict)
        return parsed_data

    def inject_session_code(self, parse_data: dict, trans_template: dict,
                            node: directed_graph.Node) -> tuple:
        parse_data.update({"session_code": node.url})
        trans_template.update({"session_code": strip_session_from_link})
        return parse_data, trans_template


    def _get_processing_templates(self, url_or_templates: str | dict, node: directed_graph.Node):
        if isinstance(url_or_templates, str):
            parsed_url = get_url_base_path(url_or_templates)
            parser_enum = get_enum_by_url(parsed_url)
            __original_templates = get_registry_template(self.fun_registry, parser_enum,
                                                         PipelineRegistries.PROCESS)
        else:
            __original_templates = url_or_templates
        if not __original_templates: return None, None, None
        parsing_templates = __original_templates.copy()
        state_key_pairs = {}
        for key in parsing_templates.copy():
            if "state" in key:
                state_key_pairs.update({key: parsing_templates.pop(key, (None, None))})

        # Separate selectors and transformers from parsing templates
        selector_template = {key: val[0] for key, val in parsing_templates.items()}
        transformer_template = {key: val[1] for key, val in parsing_templates.items()}

        return selector_template, transformer_template, state_key_pairs


    def _parse_html(self, url: str, html: str, t_parse: dict) -> dict:
        return self.parser.get_content(t_parse, html)




class LoaderWorker(BaseWorker):
    """Thread to consume the loader queue and handle database insertion."""

    def __init__(
        self,
        input_queue: Queue,
        state: directed_graph.DirectionalGraph,
        db_conn: psycopg.Connection,
        fun_registry: ProcessorRegistry,
        *,
        strict: bool = False,
        name:str = "Loader Worker",
    ) -> None:
        """Initialize the loader worker."""
        super().__init__(input_queue, name=name)
        self.state = state
        self.db_conn = db_conn
        self.fun_registry = fun_registry
        self.strict= strict

    def process(self, item: directed_graph.Node):
        node = item
        self._set_state(node, PipelineStateEnum.LOADING)
        try:
            result: dict = self._load_item(item)
            if result:
                node.data = result
            else:
                node.data = None
            self._set_state(node, PipelineStateEnum.COMPLETED)
            with self.state.lock:
                self._remove_nodes(node)
                # TODO: Fix load from save
                self.state.save_file(state_cache_file)
            logger.info(f"[{self.name.upper()}]: Finished processing item: {item} "
                        f"with result: {result}")
        except Exception as e:
            self.db_conn.rollback()
            node.state = PipelineStateEnum.ERROR
            logger.warning(f"Uncaught exception in loader worker with node {item}: {e}")
            raise
        finally:
            self.db_conn.commit()

    def _remove_nodes(self, node: directed_graph.Node):
        self.state.safe_remove_root(node.url, known_links_cache_file)

    def _load_item(self, item: directed_graph.Node) -> dict:
        page_enum = item.type
        loader_fun = self.fun_registry.get_processor(page_enum, PipelineRegistries.LOAD)
        return loader_fun.execute(item.data, self.db_conn)
