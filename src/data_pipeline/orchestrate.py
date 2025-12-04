"""main.py."""
from itertools import zip_longest
from pathlib import Path
from queue import LifoQueue, Queue

import psycopg
from urllib3.util import parse_url

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
from src.workers.pipeline_workers import CrawlerWorker, LoaderWorker, ProcessorWorker

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
        crawler_queue: LifoQueue | None = None,
        processor_queue: Queue | None = None,
        loader_queue: Queue | None = None,
        crawler: type[Crawler] = Crawler,
        parser: type[HTMLParser] = HTMLParser,
        transformer: type[PipelineTransformer] = PipelineTransformer,
        fetch_scheduler: type[FetchScheduler] = FetchScheduler,
    ) -> None:
        """Initialize the Orchestrator."""
        self.fetch_scheduler_cls = fetch_scheduler
        self.transformer_cls = transformer
        self.crawler_cls = crawler
        self.parser_cls = parser
        self.registry = registry
        self.db_conn = db_conn
        self.strict = strict
        self.crawler_queue = crawler_queue or LifoQueue()
        self.processor_queue = processor_queue or Queue()
        self.loader_queue = loader_queue or Queue()
        self.visited: list = []
        self._visited_nodes: list = []
        __seed_urls = self.filter_seed_urls(
            seed_urls, self.get_visited_urls(known_links_cache_file))
        self.seed_urls: dict[str, list[str]] = self.organize_unique_domains(__seed_urls)

        # self.state=state
        self.state_cls = state_tree
        self.state: dict[str, indexed_tree.IndexedTree] = {}

        self.id_counter = 0

    def orchestrate(self):

        # Create one state for each unique url
        # check if state is empty, if empty, add next seed url as node, else continue

        unvisited_nodes = self.setup_states(self.seed_urls, cache_base_path=state_cache_file)
        self._load_queues(unvisited_nodes)
        w_crawler, w_processor, w_loader = self._setup_workers()
        self.start_workers([w_crawler, w_processor, w_loader])
        self.manage_workers(w_crawler, w_processor, w_loader)
        self.shutdown_workers([self.crawler_queue, self.processor_queue, self.loader_queue],
                              [w_crawler, w_processor, w_loader])


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

    def manage_workers(self, w_crawler: BaseWorker, w_processor: BaseWorker, w_loader: BaseWorker)\
            -> None:
        """
        Manage workers.

        Once root url is finished scraping, wait for pipeline to finish, add new root link
        """
        while True:
            for key, tree in self.state.items():
                if tree.root is None:
                    next_url = self._next_seed(key)
                    if not next_url: continue
                    self._enqueue_links(next_url, self.crawler_queue)
                    continue
            if self.crawler_queue.empty():
                break
        self.crawler_queue.join()
        self.processor_queue.join()
        self.loader_queue.join()

    def start_workers(self, worker_list: list[BaseWorker]) -> None:
        """Start workers."""
        for worker in worker_list:
            worker.start()


    def _setup_workers(self) -> tuple[BaseWorker, BaseWorker, BaseWorker]:
        """Initialize workers."""
        crawler = self.crawler_cls
        fetch_scheduler = self.fetch_scheduler_cls()
        parser = self.parser_cls()
        transformer = self.transformer_cls()
        crawler_worker = CrawlerWorker(self.crawler_queue, self.processor_queue, self.state,
                                       crawler, parser, self.registry, strict=self.strict,
                                       fetch_scheduler=fetch_scheduler, name="CRAWLER")
        processor_worker = ProcessorWorker(
            self.processor_queue, self.loader_queue, self.state, parser, transformer, self.registry,
            strict=self.strict, name="PROCESSOR")
        loader_worker = LoaderWorker(self.loader_queue, self.state, self.db_conn, self.registry,
                                     strict=self.strict, name="LOADER")
        return crawler_worker, processor_worker, loader_worker




    def _load_queues(self, unvisited_nodes: list[indexed_tree.Node]) -> None:
        """Put starting values in queues."""
        root_items = {}
        for key, item in self.state.items():
            #add items that werent loaded
            if item.root is None:
                next_seed = self._next_seed(key)
                self._enqueue_links(next_seed, self.crawler_queue)
        if unvisited_nodes:
            self._enqueue_links(unvisited_nodes, self.crawler_queue)




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
            self.crawler_queue.put(node)
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
