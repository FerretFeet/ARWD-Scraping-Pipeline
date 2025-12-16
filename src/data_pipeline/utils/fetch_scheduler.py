"""Scheduler for fetch worker, ensure minimum time between repeated requests to a server."""

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

    def __init__(self, min_delay: float = 2.5) -> None:
        """Initialize FetchScheduler."""
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

    def mark_fetched(self, domain: str) -> None:
        """Mark domain as fetched right now."""
        with self.lock:
            self.last_fetch[domain] = time.time()

    def schedule_retry(self, item: Any, when: float) -> None:
        """Put item into the global delayed queue to be retried at 'when' (epoch seconds)."""
        self.delayed.put(DelayedItem(when, item))

    def pop_due(self) -> Any | None:
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
        self.delayed.put(peek)
        return None

    def time_until_next(self) -> float | None:
        """Return seconds until the next delayed item becomes due, or None if none."""
        try:
            item = self.delayed.get_nowait()
        except Empty:
            return None
        # reinsert and compute
        self.delayed.put(item)
        return max(0.0, item.next_time - time.time())
