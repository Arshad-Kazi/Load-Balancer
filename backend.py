"""
backend.py — Backend server definition and per-backend metrics.
"""

import time
from dataclasses import dataclass, field


@dataclass
class Backend:
    host: str
    port: int
    healthy: bool = True
    total_requests: int = 0
    failed_requests: int = 0
    last_checked: float = field(default_factory=time.time)

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    def record_success(self) -> None:
        self.total_requests += 1

    def record_failure(self) -> None:
        self.total_requests += 1
        self.failed_requests += 1

    def stats(self) -> dict:
        return {
            "address": self.address,
            "healthy": self.healthy,
            "total_requests": self.total_requests,
            "failed_requests": self.failed_requests,
            "error_rate": (
                round(self.failed_requests / self.total_requests, 3)
                if self.total_requests
                else 0.0
            ),
        }