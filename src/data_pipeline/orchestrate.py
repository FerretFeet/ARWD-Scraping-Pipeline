"""main.py."""
from pathlib import Path
from queue import LifoQueue, Queue

import psycopg

from src.config.settings import known_links_cache_file, state_cache_file
from src.data_pipeline.extract.html_parser import HTMLParser
from src.data_pipeline.extract.webcrawler import Crawler
from src.data_pipeline.transform.pipeline_transformer import PipelineTransformer
from src.structures import indexed_tree
from src.structures.indexed_tree import PipelineStateEnum
from src.structures.registries import PipelineRegistryKeys, ProcessorRegistry
from src.utils.json_list import load_json_list
from src.utils.strings.get_url_base_path import get_url_base_path
from src.workers.base_worker import BaseWorker
from src.workers.pipeline_workers import CrawlerWorker, LoaderWorker, ProcessorWorker

STRICT = False

db_conn = ""
class Orchestrator:
    def __init__(
        self,
        state: indexed_tree.IndexedTree,
        registry: ProcessorRegistry,
        seed_urls: list,
        db_conn: psycopg.Connection,
        *,
        strict: bool = False,
        crawler_queue: LifoQueue | None = None,
        processor_queue: Queue | None = None,
        loader_queue: Queue | None = None,
    ) -> None:
        """Initialize the Orchestrator."""
        self.state = state
        self.registry = registry
        self.seed_urls = seed_urls
        self.db_conn = db_conn
        self.strict = strict
        self.crawler_queue = crawler_queue or LifoQueue()
        self.processor_queue = processor_queue or Queue()
        self.loader_queue = loader_queue or Queue()
        self.visited: list = []
        self._visited_nodes: list = []

    def orchestrate(self):
        """Orchestrate main function."""
        self.seed_urls = self.filter_seed_urls(self.seed_urls,
                                          self.get_visited_urls(known_links_cache_file))
        while self.seed_urls:
            self.run_single_iteration()
            self.reset()


    def reset(self):
        for node in self.state.nodes:
            self.visited.append(self.state.find_node(node).url)
            self._visited_nodes.append(self.state.find_node(node))
        self.state.__init__()

    def run_single_iteration(self):
        unvisited_nodes = self.setup_state_get_unvisited(state_cache_file)
        self.load_queues(unvisited_nodes)
        w_crawler, w_processor, w_loader = self.setup_workers()
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

    def next_seed(self) -> str:
        """Pop next seed url from seedurls list."""
        if not self.seed_urls:
            return None
        return self.seed_urls.pop(0)

    def manage_workers(self, w_crawler: BaseWorker, w_processor: BaseWorker, w_loader: BaseWorker)\
            -> None:
        """
        Manage workers.

        Once root url is finished scraping, wait for pipeline to finish, add new root link
        """
        while True:
            if self.crawler_queue.empty():
                self.crawler_queue.join()
                self.processor_queue.join()
                self.loader_queue.join()
                break

    def start_workers(self, worker_list: list[BaseWorker]) -> None:
        """Start workers."""
        for worker in worker_list:
            worker.start()



    def setup_workers(self) -> tuple[BaseWorker, BaseWorker, BaseWorker]:
        """Initialize workers."""
        crawler = Crawler((get_url_base_path(self.state.root.url)).rsplit("/", 1)[0],
                          strict=self.strict)
        parser = HTMLParser()
        transformer = PipelineTransformer()
        crawler_worker = CrawlerWorker(self.crawler_queue, self.processor_queue, self.state,
                                       crawler, parser, self.registry, strict=self.strict,
                                       name="CRAWLER")
        processor_worker = ProcessorWorker(
            self.processor_queue, self.loader_queue, self.state, parser, transformer, self.registry,
            strict=self.strict, name="PROCESSOR")
        loader_worker = LoaderWorker(self.loader_queue, self.state, self.db_conn, self.registry,
                                     strict=self.strict, name="LOADER")
        return crawler_worker, processor_worker, loader_worker

    def load_queues(self, unvisited_nodes: list[indexed_tree.Node]) -> None:
        """Put starting values in queues."""
        if not unvisited_nodes:
            unvisited_nodes = self.next_seed()
        return self.enqueue_links(unvisited_nodes, self.crawler_queue)


    def enqueue_links(self, enqueue_list: list[indexed_tree.Node] | str, queue: Queue) -> None:
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
                    continue
        elif isinstance(enqueue_list, str):
            print("CREATING NODE")
            node = self.state.add_node(
                node_type=PipelineRegistryKeys.ARK_LEG_SEEDER,
                parent=None,
                url=enqueue_list,
                state=PipelineStateEnum.AWAITING_FETCH,
            )
            self.crawler_queue.put(node)
        else:
            msg = f"enqueue_list must be list of nodes or str, not {type(enqueue_list)}"
            raise TypeError(msg)


    def get_visited_urls(self, cache_path: str) -> list[str]:
        """Return visited urls."""
        return load_json_list(cache_path)

    def filter_seed_urls(self, seed_urls: list[str], known_urls: list[str]) -> list[str]:
        """Remove known urls from seed_urls."""
        return [url for url in seed_urls if url not in known_urls]

    def setup_state_get_unvisited(self, cache_file: Path) -> None | list[indexed_tree.Node]:
        """Load state from cache and return order of unvisited nodes."""
        if self.state.load_from_file(cache_file):  # return 1 if needs to be restored else 0
            return self.state.reconstruct_order()
        return None


