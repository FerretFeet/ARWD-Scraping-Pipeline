"""main.py."""
import queue
import time
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
from src.structures import directed_graph
from src.structures.directed_graph import DirectionalGraph
from src.structures.indexed_tree import PipelineStateEnum
from src.structures.registries import ProcessorRegistry, get_enum_by_url
from src.utils.json_list import load_json_list
from src.utils.logger import logger
from src.utils.strings.get_url_base_path import get_url_base_path
from src.workers.base_worker import BaseWorker

STRICT = False

db_conn = ""

# TODO: Increase parameterization by generalizing workers and worker-processing classes so each worker can be configured from registry.  # noqa: E501, FIX002, TD002, TD003
class Orchestrator:
    def __init__(
        self,
        registry: ProcessorRegistry,
        seed_urls: list,
        db_conn: psycopg.Connection,
        *,
        state: DirectionalGraph,
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
        self.state = state
        self.visited: list[str] = []
        self.workers = []

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
        self.roots = []


    def orchestrate(self):

        # Create one state for each unique url
        # check if state is empty, if empty, add next seed url as node, else continue

        unvisited_nodes = self.setup_states(self.seed_urls, cache_base_path=state_cache_file)
        self._load_queues(unvisited_nodes)
        workers = self._setup_workers()
        self.start_workers(workers)
        queue_ordered_list = [self.queues[stage] for stage in PipelineRegistries]
        logger.info(f"FETCH QUEUE SIZE AFTER LOADING: {self.queues[PipelineRegistries.FETCH].qsize()}")

        self.manage_workers(workers, queue_ordered_list)
        self.shutdown_workers(queue_ordered_list, workers)



    def shutdown_workers(self, queues: list[Queue], workers: list[BaseWorker]) -> None:
        """Shutdown workers."""
        for q in queues:
            q.put(None)
        self._drain_sentinels(self.queues)

        for w in workers:
            w.join(timeout=5)
            if w.is_alive():
                logger.warning(f"THREAD {w.name} FAILED SHUTDOWN")
        time.sleep(0.005)

    def _next_seed(self, match_key: str) -> str:
        """Pop next seed url from seedurls list."""
        try:
            if not self.seed_urls[match_key]:
                return None
        except KeyError:
            return None
        next_seed =  self.seed_urls[match_key].pop(0)
        if len(self.seed_urls[match_key]) == 0:
            self.seed_urls.pop(match_key)
        return next_seed

    def manage_workers(self, workers: list[BaseWorker], ordered_queues: list[Queue]) -> None:
        """
        Manage workers.

        Once root url is finished scraping, wait for pipeline to finish, add new root link
        """
        start_queue = ordered_queues[0]

        # PHASE 1: DYNAMIC SCHEDULING (Runs until no seeds are left)
        # print(f"STATE ROOTS {self.state.get_roots()}")

        while any(urls for urls in self.seed_urls.values()):
            # 1. Inject next seed URL if a slot is free
            active_root_netlocs = {parse_url(node.url).netloc for node in self.state.get_roots()
                                   if isinstance(node, directed_graph.Node)}

            for key in list(self.seed_urls.keys()):
                domain_netloc = parse_url(key).netloc
                if domain_netloc not in active_root_netlocs:
                    next_url = self._next_seed(key)
                    if next_url:
                        logger.info(f"[ORCHESTRATOR]: ADD NEW SEED: {next_url}")
                        self._enqueue_links(next_url, start_queue)
            # Yield control to workers to process scheduled items
            time.sleep(0.001)
            # Ensure all workers are still active, else begin shutdown
            for worker in workers:
                if not worker.is_alive(): break

            # PHASE 2: PIPELINE CLEARANCE (Block until all work is done)
        # The loop above is finished. All seeds have been scheduled.


        # This loop blocks the main thread until all items put into the queue
        # during Phase 1 have been processed by the workers (task_done() called).
        for queue in ordered_queues:
            queue.join()

        logger.info("PIPELINE CLEARED. PROCEEDING TO SHUTDOWN.")

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
                    state=self.state,
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
                    state=self.state,
                    parser=self.parser_cls(),
                    transformer=self.transformer_cls(),
                    fun_registry=self.registry,
                    strict=self.strict,
                    name=f"{stage.label}_WORKER",
                )
            elif stage is PipelineRegistries.LOAD:
                worker = worker_cls(
                    input_queue=input_queue,
                    state=self.state,
                    db_conn=self.db_conn,
                    fun_registry=self.registry,
                    strict=self.strict,
                    name=f"{stage.label}_WORKER",
                )
            else:
                raise ValueError(f"Unknown stage {stage}")

            workers.append(worker)
            self.workers.append(worker)
            last_queue = output_queue

        return workers

    def _drain_sentinels(self, queues: dict):
        """Explicitly drains the final sentinel from all downstream queues."""
        # Assuming FETCH is the only queue that might still be logically empty.
        # We only need to drain PROCESS and LOAD, as they are the output queues
        # that received the sentinels from the previous worker.

        drain_queues = [queues[PipelineRegistries.PROCESS], queues[PipelineRegistries.LOAD]]

        for q in drain_queues:
            try:
                # We expect a sentinel, so we just consume it.
                q.get_nowait()
                logger.info(f"ORCHESTRATOR DRAINED SENTINEL from {q}")
                q.task_done()
            except queue.Empty:
                pass  # Already drained or empty, that's fine

    def _load_queues(self, unvisited_nodes: list[directed_graph.Node]) -> None:
        """Put starting values in queues."""
        root_items = {}
        root_queue = self.queues[PipelineRegistries.FETCH]
        process_queue = self.queues[PipelineRegistries.PROCESS]
        loader_queue = self.queues[PipelineRegistries.LOAD]
        if unvisited_nodes:
            for node in unvisited_nodes:
                if not node.incoming:
                    self.state.roots.add(node)
                if node.state in [PipelineStateEnum.CREATED, PipelineStateEnum.AWAITING_FETCH,
                                  PipelineStateEnum.FETCHING]:
                    root_queue.put(node)
                elif node.state in [PipelineStateEnum.AWAITING_PROCESSING, PipelineStateEnum.PROCESSING]:
                    process_queue.put(node)
                elif node.state in [PipelineStateEnum.AWAITING_LOAD, PipelineStateEnum.LOADING]:
                    loader_queue.put(node)





    def _enqueue_links(self, enqueue_list: list[directed_graph.Node] | str, queue: Queue) -> None:
        """
        Put enqueued values in queues.

        Enqueue list can be a list of Nodes to be added directly to the queue in the order provided,
            if enqueue list is None, enqueue next seed url.
        """
        if isinstance(enqueue_list, list):
            if enqueue_list:
                for item in enqueue_list:
                    item.set_state(PipelineStateEnum.AWAITING_FETCH)
                    queue.put(item)
                    self.visited.append(item.url)
        elif isinstance(enqueue_list, str):
            node = self.state.add_new_node(enqueue_list,
                get_enum_by_url(get_url_base_path(enqueue_list)),
                None,
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

    def setup_states(self, seed_urls: dict[str, list[str]], cache_base_path: Path)\
            -> list[directed_graph.Node]:
        nodes_to_stack = {}
        addtl_nodes = []
        addtl_keys = []

        if self.state.load_from_file(cache_base_path):
            nodes_to_stack = self.state.nodes

        for key, urllist in seed_urls.copy().items():
            if (key not in nodes_to_stack
                    and key not in addtl_keys):
                url = self._next_seed(key)
                turl = get_url_base_path(url)
                urlenum = get_enum_by_url(turl)
                new_node = self.state.add_new_node(url, urlenum, None)
                addtl_nodes.append(new_node)
                addtl_keys.append(key)
                if new_node:
                    self.state.roots.add(new_node)

        return [item for items in zip_longest(nodes_to_stack.values(), addtl_nodes)
                             for item in items if item is not None]
