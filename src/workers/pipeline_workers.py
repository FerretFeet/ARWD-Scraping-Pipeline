"""Thread workers for pipeline tasks."""

from dataclasses import dataclass
from queue import LifoQueue, Queue
from urllib.parse import urlparse

import psycopg

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
from src.utils.strings.get_url_base_path import get_url_base_path
from src.workers.base_worker import BaseWorker


@dataclass
class LoaderObj:
    node: indexed_tree.Node
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
        fetch_queue: LifoQueue,
        process_queue: Queue,
        state_tree: IndexedTree,
        crawler: Crawler,
        parser: HTMLParser,
        fun_registry: ProcessorRegistry,
        *,
        strict: bool = False,
        name: str = "Crawler Worker",
    ) -> None:
        """Initialize the crawler worker."""
        super().__init__(fetch_queue, name=name)
        self.output_queue = process_queue
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
        if links:
            self._enqueue_links(node, links)
        if self._check_processing_step(node.url):
            print("Preparing for next step")
            node.data["html"] = html
            self._set_state(node, PipelineStateEnum.AWAITING_PROCESSING)
            self.output_queue.put(node)
            print("Finished preparing for next step")
        else: # No next step, dont send to queue
            self._set_state(node, PipelineStateEnum.COMPLETED)
            print("No next step, finished.")


    def _check_processing_step(self, url: str) -> bool:
        print(f"_check_processing_step: url {url}")
        parsed_url = get_url_base_path(url)
        p_enum = get_enum_by_url(parsed_url)
        return bool(get_registry_template(self.fun_registry, p_enum, PipelineRegistries.PROCESS))



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

    def _parse_html(self, url: str, html: str) -> dict | None:
        print(f"_parse_html: {url}, {html[:20]}")
        parsed_url = get_url_base_path(url)
        parser_enum = get_enum_by_url(parsed_url)
        template = get_registry_template(self.fun_registry, parser_enum, PipelineRegistries.FETCH)
        return self.parser.get_content(template, html) if template else None

    def _fetch_html(self, url: str) -> str:
        print(f"_fetch_html: {url}")
        parsed_url = urlparse(url)
        return self.crawler.get_page(parsed_url.geturl())



class ProcessorWorker(BaseWorker):
    """Thread to consume the processor queue and handle internal data processing."""

    def __init__(
        self,
        input_queue: Queue,
        output_queue: Queue,
        state_tree: IndexedTree,
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
        self.state = state_tree
        self.parser = parser
        self.transformer = transformer
        self.strict = strict
        self.fun_registry = fun_registry


    def process(self, node: indexed_tree.Node) -> None:
        self._set_state(node, PipelineStateEnum.PROCESSING)
        t_parser, t_transformer, state_pair = self._get_processing_templates(node.url)
        if not t_parser or not t_transformer:
            msg = f"Expected parser and transformer templates for node {node} in processor worker"
            raise Exception(msg)  # noqa: TRY002
        parsed_data = self._parse_html(node.url, node.data["html"])
        # --- Attach state values after parse, parser not able to handle
        parsed_data, t_transformer = self._attach_state_values(node, parsed_data, t_transformer, state_pair)
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

    def _create_loader_object(self, transformed_data: dict, node: indexed_tree.Node):
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
        self, node: indexed_tree.Node, parsed_data: dict, t_transformer: dict, state_pair: tuple,
    ) -> tuple:
        state_dict = state_pair[0](node, self.state)
        parsed_data.update(state_dict)
        t_transformer.update(dict.fromkeys(state_dict, state_pair[1]))
        return parsed_data, t_transformer

    def _get_processing_templates(self, url: str):
        parsed_url = get_url_base_path(url)
        parser_enum = get_enum_by_url(parsed_url)
        __original_templates = get_registry_template(self.fun_registry, parser_enum,
                                                     PipelineRegistries.PROCESS)
        if not __original_templates: return None, None, None
        parsing_templates = __original_templates.copy()

        state_key_pair = parsing_templates.pop("state_key", (None, None))

        # Separate selectors and transformers from parsing templates
        selector_template = {key: val[0] for key, val in parsing_templates.items()}
        transformer_template = {key: val[1] for key, val in parsing_templates.items()}


        return selector_template, transformer_template, state_key_pair


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
        state_tree: IndexedTree,
        db_conn: psycopg.Connection,
        fun_registry: ProcessorRegistry,
        *,
        strict: bool = False,
        name:str = "Loader Worker",
    ) -> None:
        """Initialize the loader worker."""
        super().__init__(input_queue, name=name)
        self.state = state_tree
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
            self._remove_if_children_completed(node)
        except Exception as e:
            self.db_conn.rollback()
            logger.warning(f"Uncaught exception in loader worker: {e}")
            raise
        finally:
            self.db_conn.commit()

    def _remove_if_children_completed(self, node: indexed_tree.Node):
        keep_node = any(
            self.state.find_node(child_id).state != PipelineStateEnum.COMPLETED
            for child_id in node.children
        )
        if not keep_node:
            self.state.safe_remove_node(node.id, cascade_up=True)

    def _load_item(self, item: LoaderObj) -> dict:
        page_enum = item.name
        loader_fun = self.fun_registry.get_processor(page_enum, PipelineRegistries.LOAD)

        return loader_fun(item.params, self.db_conn)
