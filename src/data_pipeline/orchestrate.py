"""main.py."""
from itertools import zip_longest
from pathlib import Path
from queue import Queue

import psycopg
from urllib3.util import parse_url

from src.config.pipeline_enums import PipelineRegistries
from src.config.settings import known_links_cache_file, state_cache_file
from src.data_pipeline.extract.html_parser import HTMLParser
from src.data_pipeline.extract.webcrawler import Crawler
from src.data_pipeline.transform.pipeline_transformer import PipelineTransformer
from src.data_pipeline.utils.fetch_scheduler import FetchScheduler
from src.structures import indexed_tree
from src.structures.indexed_tree import PipelineStateEnum
from src.structures.registries import ProcessorRegistry, get_enum_by_url
from src.utils.json_list import load_json_list
from src.utils.strings.get_url_base_path import get_url_base_path
from src.workers.base_worker import BaseWorker

STRICT = False

db_conn = ""
class Orchestrator:
    def __init__(
        self,
        registry: ProcessorRegistry,
        seed_urls: list,
        db_conn: psycopg.Connection,
        *,
        state_tree: type[indexed_tree.IndexedTree],
        strict: bool = False,
        crawler: type[Crawler] = Crawler,
        parser: type[HTMLParser] = HTMLParser,
        transformer: type[PipelineTransformer] = PipelineTransformer,
        fetch_scheduler: type[FetchScheduler] = FetchScheduler,
    ) -> None:
        """Initialize the Orchestrator."""
        self.registry = registry
        self.db_conn = db_conn
        self.strict = strict
        self.fetch_scheduler_cls = fetch_scheduler
        self.transformer_cls = transformer
        self.crawler_cls = crawler
        self.parser_cls = parser
        self.state_cls = state_tree
        self.state: dict[str, indexed_tree.IndexedTree] = {}
        self.visited: list[str] = []

        # Organize queues dynamically by stage
        self.queues: dict[PipelineRegistries, Queue] = {
            stage: stage.queue_type() for stage in PipelineRegistries
        }

        # Keep enum handy for iteration
        self.pipeline_stages = list(PipelineRegistries)

        # Seed URLs
        known = self.get_visited_urls(known_links_cache_file)
        __seed_urls = self.filter_seed_urls(seed_urls, known)
        self.seed_urls = self.organize_unique_domains(__seed_urls)



    def orchestrate(self):

        # Create one state for each unique url
        # check if state is empty, if empty, add next seed url as node, else continue

        unvisited_nodes = self.setup_states(self.seed_urls, cache_base_path=state_cache_file)
        self._load_queues(unvisited_nodes)
        workers = self._setup_workers()
        self.start_workers(workers)
        queue_ordered_list = [self.queues[stage] for stage in PipelineRegistries]
        self.manage_workers(workers, queue_ordered_list)
        self.shutdown_workers(queue_ordered_list, workers)



    def shutdown_workers(self, queues: list[Queue], workers: list[BaseWorker]) -> None:
        """Shutdown workers."""
        for q in queues:
            q.put(None)
        for w in workers:
            w.join()



    def _next_seed(self, match_key: str) -> str:
        """Pop next seed url from seedurls list."""
        if not self.seed_urls[match_key]:
            return None
        return self.seed_urls[match_key].pop(0)

    def manage_workers(self, workers: list[BaseWorker], ordered_queues: list[Queue]) -> None:
        """
        Manage workers.

        Once root url is finished scraping, wait for pipeline to finish, add new root link
        """
        start_queue = ordered_queues[0]
        while True:
            for key, tree in self.state.items():
                if tree.root is None:
                    next_url = self._next_seed(key)
                    if not next_url: continue
                    self._enqueue_links(next_url, start_queue)
                    continue
            if start_queue.empty():
                break
        for queue in ordered_queues:
            queue.join()


    def start_workers(self, worker_list: list[BaseWorker]) -> None:
        """Start workers."""
        for worker in worker_list:
            worker.start()

    def _setup_workers(self) -> list[BaseWorker]:
        """Initialize workers dynamically from PipelineRegistries enum."""
        workers: list[BaseWorker] = []

        # Store last output queue for linking stages
        last_queue = None

        for stage in self.pipeline_stages:
            input_queue = last_queue if last_queue else self.queues[stage]
            # Determine output queue by the next stage, or None for last stage
            idx = self.pipeline_stages.index(stage)
            next_stage = (
                self.pipeline_stages[idx + 1] if idx + 1 < len(self.pipeline_stages) else None
            )
            output_queue = self.queues[next_stage] if next_stage else None

            worker_cls = stage.get_worker_class()
            # Pass the minimal required arguments for now, can be expanded per worker
            if stage is PipelineRegistries.FETCH:
                worker = worker_cls(
                    input_queue=input_queue,
                    output_queue=output_queue,
                    state_tree=self.state,
                    crawler_cls=self.crawler_cls,
                    parser=self.parser_cls(),
                    fun_registry=self.registry,
                    fetch_scheduler=self.fetch_scheduler_cls(),
                    strict=self.strict,
                    name=f"{stage.label}_WORKER",
                )
            elif stage is PipelineRegistries.PROCESS:
                worker = worker_cls(
                    input_queue=input_queue,
                    output_queue=output_queue,
                    state_tree=self.state,
                    parser=self.parser_cls(),
                    transformer=self.transformer_cls(),
                    fun_registry=self.registry,
                    strict=self.strict,
                    name=f"{stage.label}_WORKER",
                )
            elif stage is PipelineRegistries.LOAD:
                worker = worker_cls(
                    input_queue=input_queue,
                    state_tree=self.state,
                    db_conn=self.db_conn,
                    fun_registry=self.registry,
                    strict=self.strict,
                    name=f"{stage.label}_WORKER",
                )
            else:
                raise ValueError(f"Unknown stage {stage}")

            workers.append(worker)
            last_queue = output_queue

        return workers




    def _load_queues(self, unvisited_nodes: list[indexed_tree.Node]) -> None:
        """Put starting values in queues."""
        root_items = {}
        root_queue = self.queues[PipelineRegistries.FETCH]
        for key, item in self.state.items():
            #add items that werent loaded
            if item.root is None:
                next_seed = self._next_seed(key)
                self._enqueue_links(next_seed, root_queue)
        if unvisited_nodes:
            self._enqueue_links(unvisited_nodes, root_queue)




    def _enqueue_links(self, enqueue_list: list[indexed_tree.Node] | str, queue: Queue) -> None:
        """
        Put enqueued values in queues.

        Enqueue list can be a list of Nodes to be added directly to the queue in the order provided,
            if enqueue list is None, enqueue next seed url.
        """
        if isinstance(enqueue_list, list) and isinstance(enqueue_list[0], indexed_tree.Node):
            if enqueue_list:
                for item in enqueue_list:
                    item.state = PipelineStateEnum.AWAITING_FETCH
                    queue.put(item)
                    self.visited.append(item.url)
                    continue
        elif isinstance(enqueue_list, str):
            print("CREATING NODE")
            node = self.state[parse_url(enqueue_list).netloc].add_node(
                node_type=get_enum_by_url(get_url_base_path(enqueue_list)),
                parent=None,
                url=enqueue_list,
                state=PipelineStateEnum.AWAITING_FETCH,
            )
            queue.put(node)
            self.visited.append(enqueue_list)

        else:
            msg = f"enqueue_list must be list of nodes or str, not {type(enqueue_list)}"
            raise TypeError(msg)



    def organize_unique_domains(self, seed_urls: list[str]) -> dict[str, list[str]]:
        url_dict = {}
        for url in seed_urls:
            domain = parse_url(url).netloc
            if domain in url_dict:
                url_dict[domain].append(url)
            else:
                url_dict[domain] = [url]
        return url_dict


    def get_visited_urls(self, cache_path: str) -> list[str]:
        """Return visited urls."""
        return load_json_list(cache_path)

    def filter_seed_urls(self, seed_urls: list[str], known_urls: list[str]) -> list[str]:
        """Remove known urls from seed_urls."""
        return [url for url in seed_urls if url not in known_urls]

    def setup_states(self, seed_urls: dict[str, list[str]], cache_base_path: Path) -> list[indexed_tree.Node]:
        nodes_to_stack = {}

        for key, val in seed_urls.items():
            v = val[0]
            tree = self.state_cls(name=key)
            self.state[key] = tree
            if tree.load_from_file(cache_base_path / f"-{key}"):
                nodes_to_stack[key] = self.state[key].reconstruct_order()
                continue
        interleaved_nodes = [item for items in zip_longest(*nodes_to_stack.values())
                             for item in items if item is not None]
        return interleaved_nodes
