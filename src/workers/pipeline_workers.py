"""Thread workers for pipeline tasks."""
import threading
import time
from queue import Empty, LifoQueue

from src.data_pipeline.extract.html_parser import HTMLParser
from src.data_pipeline.extract.webcrawler import Crawler
from src.structures.indexed_tree import IndexedTree, PipelineStateEnum
from src.utils.logger import logger


class CrawlerWorker(threading.Thread):
    """Thread to consume the crawler queue and fetch external data."""

    def __init__(self, fetch_queue: LifoQueue, process_queue: LifoQueue,
                 state_tree: IndexedTree, crawler: Crawler, *, strict: bool = False) -> None:
        """Initialize the crawler worker."""
        super().__init__()
        self.fetch_queue = fetch_queue
        self.process_queue = process_queue
        self.state = state_tree
        self.running = True
        self.strict = strict
        self.crawler = crawler

    def run(self) -> None:
        """Run the crawler worker."""
        while self.running:
            try:
                # Get a node from the queue (blocks for 1 second)
                node_id = self.fetch_queue.get(timeout=1)
                node = self.state.find_node(node_id)
                node.state = PipelineStateEnum.FETCHING

                # Logic: Fetch the URL associated with the node
                url = node.url
                if url:
                    # SIMULATION: Replace this with actual HTTP request/crawling logic
                    logger.info(f"[CRAWLER] Fetching URL for Node ID {node.id}: {url}")
                    base_url, url_stem = url.split("/", 1)
                    html_text = self.crawler.get_page(url_stem)
                    node.data["html"] = html_text

                    # PARSE HTML FOR NEXT LINKS
                    parser_url = url.split("?", 1)[0]
                    html_parser = HTMLParser(strict=self.strict)
                    # parsing_template = FETCHING_REGISTRY.get(parser_url)
                    # new_links = html_parser.get_content(parsing_template, html_text)
                    # for key, item in new_links.items()
                    #   node = self.state.add_node(parent=node.id, url = item,
                    #                     state=PipelineStateEnum.AWAITING_FETCH, node_type=key)
                    #   node.state = AWAITING_FETCH
                    #   self.fetch_queue.put(node.id)


                    node.state = PipelineStateEnum.AWAITING_PROCESSING
                    self.process_queue.put(node_id)
                    self.fetch_queue.task_done()
                    time.sleep(2) # Sleep so don't flood web server


            except Empty:
                # If queue is empty, break if the system should shut down,
                # or continue if waiting for new jobs (as we do here).
                continue
            except Exception as e:
                msg = (f"[CRAWLER ERROR] Node ID {node.id}: {e}")
                logger.warning(msg)

class ProcessorWorker(threading.Thread):
    """Thread to consume the processor queue and handle internal data processing."""

    def __init__(self, queue: LifoQueue, state_tree: IndexedTree)-> None:
        """Initialize the processor worker."""
        super().__init__()
        self.queue = queue
        self.state = state_tree
        self.running = True

    def run(self) -> None:
        """Run the processor worker."""
        while self.running:
            try:
                node_id = self.queue.get(timeout=1)
                node = self.state.find_node(node_id)
                node.state = PipelineStateEnum.PROCESSING
                url = node.url
                logger.info(f"[PROCESSOR] Processing data for Node ID {node.id}")
                parser_url = url.split("?", 1)[0]
                html_parser = HTMLParser(strict=self.strict)
                # parsing_template = FETCHING_REGISTRY.get(parser_url)
                # GET template registry lookup
                # Run parse
                # run transform / validate
                # run load

                node.state = PipelineStateEnum.COMPLETED
                keep_node = False
                for child_id in node.children:
                    child = self.state.find_node(child_id)
                    if child.state != PipelineStateEnum.COMPLETED:
                        keep_node = True

                if not keep_node:
                    self.state.safe_remove_node(node_id, cascade_up=True)

                self.queue.task_done()

            except Empty:
                continue
            except Exception as e:
                logger.warning(f"[PROCESSOR ERROR] Node ID {node.id}: {e}")

