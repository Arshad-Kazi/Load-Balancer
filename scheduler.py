from collections import deque
from typing import Optional
from .backend import Backend


class RoundRobin:
    """Cycles through healthy backends in round-robin order."""

    def __init__(self, backends: list[Backend]) -> None:
        self._queue: deque[Backend] = deque(backends)

    def next(self) -> Optional[Backend]:
        """Return the next healthy backend, or None if all are down."""
        for _ in range(len(self._queue)):
            backend = self._queue[0]
            self._queue.rotate(-1)
            if backend.healthy:
                return backend  
        return None