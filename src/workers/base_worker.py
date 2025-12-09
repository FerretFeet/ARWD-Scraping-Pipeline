import threading
from queue import Queue

from src.structures import directed_graph
from src.structures.indexed_tree import PipelineStateEnum
from src.utils.logger import logger


class BaseWorker(threading.Thread):
    def __init__(self, input_queue: Queue, output_queue: Queue | None = None, *, isDaemon:bool = True, name:str ="BaseWorker") -> None:  # noqa: N803
        super().__init__(name=name, daemon=isDaemon)
        self.input_queue = input_queue
        self.output_queue = output_queue

    def fetch_next(self):
        item = self.input_queue.get()
        if item is None:
            self.input_queue.task_done()
            return None
        return item

    def mark_done(self, item):
        self.input_queue.task_done()

    def run(self):
        while True:
            item = self.fetch_next()
            print(f"{self.name.upper()}: Processing item: {item}")
            if item is None:
                if self.output_queue:
                    self.output_queue.put(item)
                break
            if getattr(item, "state", None) == PipelineStateEnum.ERROR:
                self.mark_done(item)
                continue
            try:
                self.process(item)
            except Exception as e:
                logger.warning(f"[{self.name.upper()}]: Exception while processing item: {item}\t: {e}")
                self.handle_error(item, e)
            finally:
                self.mark_done(item)

    def process(self, item):
        raise NotImplementedError

    def handle_error(self, item, error):
        # optional logging or state update
        self._set_state(item, PipelineStateEnum.ERROR)
        print(f"[{self.name.upper()}]: Exception while processing item: {item}\t: {error}")
        print(f"Item state updated: {item.state}")
        logger.error(f"Uncaught processing error: {error}")


    def _set_state(self, node: directed_graph.Node, state: PipelineStateEnum) -> None:
        print(f"_set_state: node id: {node.id}, state: {state}")
        node.set_state(state)
