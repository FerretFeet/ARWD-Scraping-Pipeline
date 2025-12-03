import threading
from queue import Queue

from src.structures.indexed_tree import PipelineStateEnum
from src.utils.logger import logger


class BaseWorker(threading.Thread):
    def __init__(self, input_queue: Queue, *, name:str ="BaseWorker") -> None:
        super().__init__(name=name)
        self.input_queue = input_queue

    def fetch_next(self):
        item = self.input_queue.get(timeout=1)
        if item is None:
            self.input_queue.task_done()
            return None
        return item

    def mark_done(self, item):
        self.input_queue.task_done()

    def run(self):
        while True:
            item = self.fetch_next()
            if item is None:
                break
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
        item.state = PipelineStateEnum.ERROR
        print(f"[{self.name.upper()}]: Exception while processing item: {item}\t: {error}")
        print(f"Item state updated: {item.state}")
        logger.error(f"Uncaught processing error: {error}")
