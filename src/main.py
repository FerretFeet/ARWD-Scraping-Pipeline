"""main.py."""
import time
from queue import LifoQueue

from src.data_pipeline.extract.webcrawler import Crawler
from src.structures.indexed_tree import IndexedTree, PipelineStateEnum
from src.structures.registries import PipelineRegistryKeys
from src.utils.json_list import append_to_json_list, load_json_list
from src.utils.paths import project_root
from src.workers.pipeline_workers import CrawlerWorker, ProcessorWorker

STRICT = False
def main() -> None:
    """Run the main function."""
    cache_dir = project_root() / "cache"
    crawler_cache_dir = cache_dir / "crawler_cache.json"
    processor_cache_dir = cache_dir / "processor_cache.json"
    state_cache_dir = cache_dir / "state_cache.json"
    known_links_cache_dir = cache_dir / "known_links_cache.json"

    known_seed_links = load_json_list(known_links_cache_dir)
    starting_seeds = [] # IMPORT
    seed_urls =  [link for link in starting_seeds if link not in known_seed_links]

    # Load or create starting state
    state = IndexedTree()
    new_url = None
    if not state.load_from_file(state_cache_dir):
        new_url = seed_urls.pop(0)
        state.add_node(parent=None, url=new_url, node_type=PipelineRegistryKeys.ARKLEGSEED)

    unvisited_nodes = (state.reverse_in_order_traversal(state.root,
                                       node_attrs={"state": PipelineStateEnum.AWAITING_FETCH})
                    + state.reverse_in_order_traversal(state.root,
                                            node_attrs={"state": PipelineStateEnum.CREATED})
                    + state.reverse_in_order_traversal(state.root,
                                            node_attrs={"state": PipelineStateEnum.FETCHING}))
    crawler_queue = LifoQueue()
    for node_id in unvisited_nodes:
        node = state.find_node(node_id)
        crawler_queue.put(node_id)
        node.state = PipelineStateEnum.AWAITING_FETCH
    unprocessed_nodes = (state.preorder_traversal(state.root,
                                      node_attrs={"state": PipelineStateEnum.AWAITING_PROCESSING})
                         + state.preorder_traversal(state.root,
                                            node_attrs={"state": PipelineStateEnum.PROCESSING}))
    processor_queue = LifoQueue()
    for node_id in unprocessed_nodes:
        node = state.find_node(node_id)
        processor_queue.put(node_id)
        node.state = PipelineStateEnum.AWAITING_PROCESSING

    crawler = Crawler(state.root.url.split("/", 1)[0], strict=STRICT)
    crawler_worker = CrawlerWorker(crawler_queue, processor_queue, state, crawler)
    processor_worker = ProcessorWorker(processor_queue, state)

    crawler_worker.start()
    processor_worker.start()

    while True:
        if state.root is None:
            if not seed_urls:
                break
            append_to_json_list(known_links_cache_dir, new_url)
            new_url = seed_urls.pop(0)
            new_node = state.add_node(parent=None, url=new_url, node_type=PipelineRegistryKeys.ARKLEGSEED)
            new_node.state = PipelineStateEnum.AWAITING_FETCH
            crawler_queue.put(new_node.id)

        time.sleep(2)
    # Wait for current tasks to complete
    crawler_queue.join()
    processor_queue.join()

    # Shutdown
    crawler_worker.running = False
    processor_worker.running = False

    crawler_queue.put(None)
    processor_queue.put(None)

    crawler_worker.join()
    processor_worker.join()



if __name__ == "__main__":
    main()
