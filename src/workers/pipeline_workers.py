"""Thread workers for pipeline tasks."""
import time
from dataclasses import dataclass
from queue import LifoQueue, Queue
from urllib.parse import urlparse

import psycopg

from src.config.pipeline_enums import PipelineRegistries, PipelineRegistryKeys
from src.data_pipeline.extract.html_parser import HTMLParser
from src.data_pipeline.extract.webcrawler import Crawler
from src.data_pipeline.transform.pipeline_transformer import PipelineTransformer
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
        final_flag = False
        self.create_crawlers(self.state.roots)
        while True:
            working_node = self._check_scheduler(node)
            if working_node is node:
                final_flag = True
            if not working_node:
                time.sleep(self.fetch_scheduler.time_until_next())
                continue
            self._set_state(working_node, PipelineStateEnum.FETCHING)
            html = self._fetch_html(working_node.url)
            links: dict = self._parse_html(working_node.url, html)
            if links:
                self._enqueue_links(node, links)
            if self._check_processing_step(working_node.url):
                print("Preparing for next step")
                working_node.data["html"] = html
                self._set_state(working_node, PipelineStateEnum.AWAITING_PROCESSING)
                self.output_queue.put(working_node)
                print("Finished preparing for next step")
            else: # No next step, dont send to queue
                self._set_state(working_node, PipelineStateEnum.COMPLETED)
                print("No next step, finished.")
            if final_flag:
                break

    def create_crawlers(self, unique_domains: set[directed_graph.Node]) -> None:
        for key in unique_domains:
            self.crawlers[urlparse(key.url).netloc] = self.crawler_cls(key.url, strict=self.strict)


    def _check_processing_step(self, url: str) -> bool:
        print(f"_check_processing_step: url {url}")
        parsed_url = get_url_base_path(url)
        p_enum = get_enum_by_url(parsed_url)
        return bool(get_registry_template(self.fun_registry, p_enum, PipelineRegistries.PROCESS))



    def _enqueue_links(self, node: directed_graph.Node, links: dict) -> None:
        print(f"_enqueue_links: node {node.id}, links: {links}")
        for item in links.values():
            if not item:
                continue
            if not isinstance(item, list):
                item = [item]  # noqa: PLW2901
            for it in item:
                url_type = get_url_base_path(node.url)
                url_enum = get_enum_by_url(url_type)
                new_node = self.state.add_new_node(it, url_enum, [node],
                    state=PipelineStateEnum.AWAITING_FETCH,
                )
                print(f"_enqueue_links: new node {new_node.id}")
                self.input_queue.put(new_node)

    def _parse_html(self, url: str, html: str) -> dict | None:
        print(f"_parse_html: {url}, {html[:20]}")
        parsed_url = get_url_base_path(url)
        parser_enum = get_enum_by_url(parsed_url)
        template = get_registry_template(self.fun_registry, parser_enum, PipelineRegistries.FETCH)
        template = template.copy()
        return self.parser.get_content(template, html) if template else None

    def _check_scheduler(self, node: directed_graph.Node) -> directed_graph.Node | None:

        domain = urlparse(node.url).netloc

        priority_item: directed_graph.Node = self.fetch_scheduler.pop_due()
        if not priority_item and self.fetch_scheduler.can_fetch_now(domain): return node
        if priority_item:
            domain = urlparse(node.url).netloc
            self.fetch_scheduler.schedule_retry(node,
                                                self.fetch_scheduler.next_allowed_time(domain))
            pi_domain = urlparse(priority_item.url).netloc
            self.fetch_scheduler.mark_fetched(pi_domain)
            return priority_item
        return None


    def _fetch_html(self, url: str) -> str:
        print(f"_fetch_html: {url}")
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
        t_parser, t_transformer, state_pairs = self._get_processing_templates(node.url)
        if not t_parser or not t_transformer:
            msg = f"Expected parser and transformer templates for node {node} in processor worker"
            raise Exception(msg)  # noqa: TRY002
        parsed_data = self._parse_html(node.url, node.data["html"])
        # --- Attach state values after parse, parser not able to handle
        parsed_data, t_transformer = self._attach_state_values(node, parsed_data, t_transformer, state_pairs)
        transformed_data = self._transform_data(parsed_data, t_transformer)
        loader_obj = self._create_loader_object(transformed_data, node)
        if loader_obj:
            print(f"LOADER OBJ {loader_obj}")
            node.data = loader_obj
            self._set_state(node, PipelineStateEnum.AWAITING_LOAD)
            self.output_queue.put(loader_obj)
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
    ) -> tuple:
        for key in state_pairs:
            state_dict = state_pairs[key][0](node, self.state)
            parsed_data.update(state_dict)  # Call function
            t_transformer.update(dict.fromkeys(state_dict.keys(), state_pairs[key][1]))
        return parsed_data, t_transformer

    def _get_processing_templates(self, url_or_templates: str | dict):
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


    def _parse_html(self, url: str, html: str) -> dict:
        print(f"_parse_html: {url}, {html[:20]}")
        parsed_url = get_url_base_path(url)
        parser_enum = get_enum_by_url(parsed_url)
        template = self.fun_registry.get_processor(parser_enum, PipelineRegistries.FETCH)
        print(f"template: {template}")
        return self.parser.get_content(template, html)



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

    def process(self, item: LoaderObj):
        node = item.node
        self._set_state(node, PipelineStateEnum.LOADING)
        try:
            result: dict = self._load_item(item)
            if result:
                node.data = result
            else:
                node.data = None
            self._set_state(node, PipelineStateEnum.COMPLETED)
            self._remove_nodes(node)
        except Exception as e:
            self.db_conn.rollback()
            node.state = PipelineStateEnum.ERROR
            logger.warning(f"Uncaught exception in loader worker: {e}")
            raise
        finally:
            self.db_conn.commit()

    def _remove_nodes(self, node: directed_graph.Node):
        print("remove nodes called")
        self.state.safe_remove_root(node)

    def _load_item(self, item: LoaderObj) -> dict:
        page_enum = item.name
        loader_fun = self.fun_registry.get_processor(page_enum, PipelineRegistries.LOAD)
        temp = loader_fun.execute(item.params, self.db_conn)
        return temp
