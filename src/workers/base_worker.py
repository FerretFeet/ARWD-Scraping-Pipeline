"""Base thread worker class."""

import threading
from queue import Queue
from typing import Any, Never

from src.structures import directed_graph
from src.structures.indexed_tree import PipelineStateEnum
from src.utils.logger import logger


class BaseWorker(threading.Thread):
    """Base worker class."""

    def __init__(
        self,
        input_queue: Queue,
        output_queue: Queue | None = None,
        *,
        isDaemon: bool = True,
        name: str = "BaseWorker",
    ) -> None:
        """Initialize the worker."""
        super().__init__(name=name, daemon=isDaemon)
        self.input_queue = input_queue
        self.output_queue = output_queue

    def fetch_next(self) -> Any:
        """Fetch the next item from the input queue."""
        item = self.input_queue.get()
        if item is None:
            self.input_queue.task_done()
            return None
        return item

    def mark_done(self) -> None:
        """Mark the input queue as task done."""
        self.input_queue.task_done()

    def run(self) -> None:
        """Run the worker."""
        while True:
            item = self.fetch_next()
            logger.info(f"{self.name.upper()}: Processing item: {item}")
            if item is None:
                if self.output_queue:
                    self.output_queue.put(item)
                break
            if getattr(item, "state", None) == PipelineStateEnum.ERROR:
                self.mark_done()
                continue
            try:
                self.process(item)
            except Exception as e:  # noqa: BLE001
                msg = f"[{self.name.upper()}]: Exception while processing item: {item}\t: {e}"
                logger.warning(msg)
                self.handle_error(item)
            finally:
                logger.info(f"[{self.name.upper()}]: Finished processing item: {item}")
                self.mark_done()

    def process(self, item: Any) -> Never:
        """Process the item."""
        raise NotImplementedError

    def handle_error(self, item: Any) -> None:
        """Handle an error."""
        # optional logging or state update
        self._set_state(item, PipelineStateEnum.ERROR)
        logger.exception(f"[{self.name.upper()}]: Exception while processing item: {item}\t")
        self.input_queue.put(None)

    def _set_state(self, node: directed_graph.Node, state: PipelineStateEnum) -> None:
        node.set_state(state)
