"""
health_checker.py — Periodic background health probing for all backends.
"""

import asyncio
import logging
import time

from .backend import Backend

log = logging.getLogger("load_balancer.health")


class HealthChecker:
    """
    Probes each backend on a fixed interval.
    - Attempts a TCP connect followed by an optional HTTP HEAD request.
    - Marks backends healthy/unhealthy and logs transitions.
    """

    def __init__(
        self,
        backends: list[Backend],
        interval: float = 10.0,
        timeout: float = 3.0,
        http_path: str = "/health",
    ) -> None:
        self._backends = backends
        self._interval = interval
        self._timeout = timeout
        self._http_path = http_path

    async def run(self) -> None:
        log.info("Health checker started (interval=%.1fs)", self._interval)
        while True:
            await asyncio.gather(*[self._check(b) for b in self._backends])
            await asyncio.sleep(self._interval)

    async def _check(self, backend: Backend) -> None:
        was_healthy = backend.healthy
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(backend.host, backend.port),
                timeout=self._timeout,
            )
            # Best-effort HTTP probe; raw TCP backends pass on connection alone.
            try:
                writer.write(
                    f"HEAD {self._http_path} HTTP/1.0\r\nHost: {backend.host}\r\n\r\n".encode()
                )
                await asyncio.wait_for(reader.read(64), timeout=self._timeout)
            except Exception:
                pass
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass

            backend.healthy = True
            backend.last_checked = time.time()
            if not was_healthy:
                log.info("Backend %s is back ONLINE", backend.address)

        except Exception:
            backend.healthy = False
            backend.last_checked = time.time()
            if was_healthy:
                log.warning("Backend %s went OFFLINE", backend.address)