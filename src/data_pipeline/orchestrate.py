"""main.py."""

import time
from queue import LifoQueue, Queue

from src.config.settings import known_links_cache_file, seed_links, state_cache_file
from src.data_pipeline.extract.webcrawler import Crawler
from src.structures.indexed_tree import IndexedTree, PipelineStateEnum
from src.structures.registries import PipelineRegistryKeys, ProcessorRegistry
from src.utils.json_list import load_json_list
from src.utils.logger import logger
from src.workers.pipeline_workers import CrawlerWorker, LoaderWorker, ProcessorWorker

STRICT = False

db_conn = ""
def main(starting_urls: list[str]) -> None:
    """Run the main function."""
    # ----- Init vars
    registry = ProcessorRegistry()
    state = IndexedTree()
    crawler_queue = LifoQueue()
    processor_queue = LifoQueue()
    loader_queue = Queue()

    # ---- Load values into vars

    ### Set up urls to visit, filter any already visited
    seed_urls: list = seed_links
    known_seed_links = load_json_list(known_links_cache_file)
    seed_urls = [link for link in seed_urls if link not in known_seed_links]
    seed_urls = seed_urls.extend(starting_urls) if seed_urls else starting_urls #Default list of urls  # noqa: E501

    # State Tree and Fetcher Queue
    ### Load and restore or initialize state tree
    unvisited_nodes = []
    if state.load_from_file(state_cache_file): #return 1 if needs to be restored else 0
        unvisited_nodes = state.reconstruct_order()

    if len(unvisited_nodes) == 0: # No state loaded
        if not seed_urls:
            logger.warning("no urls to crawl")
            return
        unvisited_nodes = [state.add_node(parent=None, url=seed_urls.pop(0),
                                          node_type=PipelineRegistryKeys.ARK_LEG_SEEDER)]
    for node in unvisited_nodes:
        crawler_queue.put(node)
        node.state = PipelineStateEnum.AWAITING_FETCH


    # ---- Set up Worker Threads
    #### Crawler
    crawler = Crawler(state.root.url.split("/", 1)[0], strict=STRICT)
    crawler_worker = CrawlerWorker(crawler_queue, processor_queue, state, crawler, registry)

    #### Processor
    processor_worker = ProcessorWorker(processor_queue, loader_queue, state,
                                       registry, strict=STRICT)

    #### Loader
    loader_worker = LoaderWorker(loader_queue, state, db_conn, registry, strict=STRICT)

    ##### ---- Begin Main Process ------

    crawler_worker.start()
    processor_worker.start()
    loader_worker.start()

    while True:
        # Continue running until there are no more urls, or timeout
        if state.root is None and len(seed_urls) > 0:
        # root should be deleted once all links are processed and loaded.
        # create the next root from seed_urls, if seed_urls is empty, break
            new_node = state.add_node(parent=None, url=seed_urls.pop(0),
                                      node_type=PipelineRegistryKeys.ARK_LEG_SEEDER)
            crawler_queue.put(new_node)
        if crawler_queue.empty() and processor_queue.empty() and loader_queue.empty():
            break
        # rest between checks
        time.sleep(2)

    # Shutdown
    crawler_queue.put(None)
    processor_queue.put(None)
    loader_queue.put(None)
    crawler_worker.join()
    processor_worker.join()
    loader_worker.join()

if __name__ == "__main__":
    main()
