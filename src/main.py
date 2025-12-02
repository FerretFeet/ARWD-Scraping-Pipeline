"""main.py."""

import time
from queue import LifoQueue, Queue

from src.config.settings import known_links_cache_file, state_cache_file
from src.data_pipeline.extract.webcrawler import Crawler
from src.structures.indexed_tree import IndexedTree, PipelineStateEnum
from src.structures.registries import PipelineRegistryKeys, ProcessorRegistry
from src.utils.json_list import append_to_json_list, load_json_list
from src.workers.pipeline_workers import CrawlerWorker, LoaderWorker, ProcessorWorker

STRICT = False


def main() -> None:
    """Run the main function."""
    # ----- Set up vars

    registry = ProcessorRegistry()  # Initialize the registry

    known_seed_links = load_json_list(known_links_cache_file)
    starting_seeds = []  # IMPORT
    seed_urls = [link for link in starting_seeds if link not in known_seed_links]

    # ---- Load or create starting state
    state = IndexedTree()
    new_url = None
    if not state.load_from_file(state_cache_file):
        new_url = seed_urls.pop(0)
        state.add_node(parent=None, url=new_url, node_type=PipelineRegistryKeys.ARK_LEG_SEEDER)

    unvisited_nodes = (
        state.reverse_in_order_traversal(
            state.root,
            node_attrs={"state": PipelineStateEnum.AWAITING_FETCH},
        )
        + state.reverse_in_order_traversal(
            state.root,
            node_attrs={"state": PipelineStateEnum.CREATED},
        )
        + state.reverse_in_order_traversal(
            state.root,
            node_attrs={"state": PipelineStateEnum.FETCHING},
        )
        + state.reverse_in_order_traversal(
            state.root,
            node_attrs={"state": PipelineStateEnum.AWAITING_PROCESSING},
        )
        + state.reverse_in_order_traversal(
            state.root,
            node_attrs={"state": PipelineStateEnum.PROCESSING},
        )
    )
    crawler_queue = LifoQueue()
    for node in unvisited_nodes:
        crawler_queue.put(node)
        node.state = PipelineStateEnum.AWAITING_FETCH

    processor_queue = LifoQueue()

    loader_queue = Queue()

    crawler = Crawler(state.root.url.split("/", 1)[0], strict=STRICT)
    crawler_worker = CrawlerWorker(crawler_queue, processor_queue, state, crawler)
    processor_worker = ProcessorWorker(processor_queue, loader_queue, state)
    loader_worker = LoaderWorker(loader_queue, state, db_conn)

    crawler_worker.start()
    processor_worker.start()
    loader_worker.start()

    while True:
        if state.root is None:
            if not seed_urls:
                break
            # End of session, restart with no seed link, save seed link as known
            append_to_json_list(known_links_cache_file, new_url)
            new_url = seed_urls.pop(0)
            new_node = state.add_node(
                parent=None,
                url=new_url,
                node_type=PipelineRegistryKeys.ARKLEGSEED,
            )
            new_node.state = PipelineStateEnum.AWAITING_FETCH
            crawler_queue.put(new_node.id)

        time.sleep(2)
    # Wait for current tasks to complete
    crawler_queue.join()
    processor_queue.join()
    loader_queue.join()

    # Shutdown
    crawler_worker.running = False
    processor_worker.running = False
    loader_worker.running = False

    crawler_queue.put(None)
    processor_queue.put(None)
    loader_queue.put(None)

    crawler_worker.join()
    processor_worker.join()
    loader_worker.join()


if __name__ == "__main__":
    main()
