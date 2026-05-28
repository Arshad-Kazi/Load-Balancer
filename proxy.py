"""
proxy.py — Bidirectional TCP proxy with retry logic.

ConnectionProxy is responsible for:
  1. Picking a healthy backend via the scheduler (with retries).
  2. Opening a connection to that backend.
  3. Streaming data in both directions until the connection closes.
"""

import asyncio
import logging

from .backend import Backend
from .scheduler import RoundRobin

log = logging.getLogger("load_balancer.proxy")


class ConnectionProxy:
    """Proxies a single client connection to a backend, retrying on failure."""

    def __init__(
        self,
        scheduler: RoundRobin,
        max_retries: int = 3,
        connect_timeout: float = 5.0,
        stream_timeout: float = 60.0,
    ) -> None:
        self._scheduler = scheduler
        self._max_retries = max_retries
        self._connect_timeout = connect_timeout
        self._stream_timeout = stream_timeout

    async def handle(
        self,
        client_r: asyncio.StreamReader,
        client_w: asyncio.StreamWriter,
    ) -> None:
        """Entry point: called once per accepted client connection."""
        peer = client_w.get_extra_info("peername")
        back_r, back_w, backend = await self._connect_with_retry(peer)

        if back_r is None:
            client_w.close()
            return

        await self._stream(client_r, client_w, back_r, back_w)

    # ── Internal helpers ──────────────────────────────────────────────────

    async def _connect_with_retry(
        self, peer
    ) -> tuple[asyncio.StreamReader | None, asyncio.StreamWriter | None, Backend | None]:
        """Try each backend in round-robin order up to max_retries times."""
        for attempt in range(1, self._max_retries + 1):
            backend = self._scheduler.next()
            if backend is None:
                log.error("No healthy backends available for %s", peer)
                return None, None, None

            try:
                back_r, back_w = await asyncio.wait_for(
                    asyncio.open_connection(backend.host, backend.port),
                    timeout=self._connect_timeout,
                )
                log.debug("Routed %s → %s (attempt %d)", peer, backend.address, attempt)
                backend.record_success()
                return back_r, back_w, backend

            except Exception as exc:
                log.warning(
                    "Attempt %d/%d — cannot reach %s: %s",
                    attempt, self._max_retries, backend.address, exc,
                )
                backend.record_failure()

        log.error("All retries exhausted for %s", peer)
        return None, None, None

    async def _stream(
        self,
        client_r: asyncio.StreamReader,
        client_w: asyncio.StreamWriter,
        back_r: asyncio.StreamReader,
        back_w: asyncio.StreamWriter,
    ) -> None:
        """Pipe bytes in both directions until one side closes or timeout."""
        try:
            await asyncio.wait_for(
                asyncio.gather(
                    self._pipe(client_r, back_w),
                    self._pipe(back_r, client_w),
                ),
                timeout=self._stream_timeout,
            )
        except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError):
            pass
        except Exception as exc:
            log.debug("Connection closed: %s", exc)
        finally:
            for writer in (client_w, back_w):
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass

    @staticmethod
    async def _pipe(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    break
                writer.write(chunk)
                await writer.drain()
        except Exception:
            pass