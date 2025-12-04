import threading
import time
from dataclasses import dataclass, field
from queue import Empty, PriorityQueue
from typing import Any


@dataclass(order=True)
class DelayedItem:
    """Item in fetch scheduler."""

    next_time: float
    item: Any = field(compare=False)

class FetchScheduler:
    """Ensure min delay between page requests."""

    def __init__(self, min_delay: float = 2.5):
        self.min_delay = min_delay
        self.last_fetch: dict[str, float] = {}
        self.lock = threading.Lock()

        # Delayed tasks (min-heap by next_time). thread-safe wrapper via methods.
        self.delayed = PriorityQueue()

    def next_allowed_time(self, domain: str) -> float:
        """Return the earliest allowed fetch time for domain."""
        with self.lock:
            return self.last_fetch.get(domain, 0.0) + self.min_delay

    def can_fetch_now(self, domain: str) -> bool:
        """Fast check w/out updating state."""
        return time.time() >= self.next_allowed_time(domain)

    def mark_fetched(self, domain: str):
        """Mark domain as fetched right now."""
        with self.lock:
            self.last_fetch[domain] = time.time()

    def schedule_retry(self, item: Any, when: float):
        """Put item into the global delayed queue to be retried at 'when' (epoch seconds)."""
        self.delayed.put(DelayedItem(when, item))

    def pop_due(self, timeout: float = 0.0) -> Any | None:
        """
        Return a delayed item if its time has come, otherwise None.
        Non-blocking by default. If timeout > 0, will wait up to timeout seconds.
        """
        try:
            peek = self.delayed.get_nowait()
        except Empty:
            return None

        if peek.next_time <= time.time():
            return peek.item
        # Not due yet — push it back and return None.
        # We must reinsert the item; PriorityQueue has no peek, so we put back and sleep.
        self.delayed.put(peek)
        return None

    def time_until_next(self) -> float | None:
        """Return seconds until the next delayed item becomes due, or None if none."""
        try:
            # no safe peek for PriorityQueue; do a non-destructive strategy:
            item = self.delayed.get_nowait()
        except Empty:
            return None
        # reinsert and compute
        self.delayed.put(item)
        return max(0.0, item.next_time - time.time())
