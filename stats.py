"""
stats.py — Lightweight HTTP endpoint that serves live JSON metrics.

Listens on (host, port+1) and responds to any request with a JSON
snapshot of connection counts and per-backend statistics.
"""

import asyncio
import json
import logging

from .backend import Backend

log = logging.getLogger("load_balancer.stats")


class StatsServer:
    """
    Serves a JSON stats page on a dedicated port.

    Expected usage:
        curl http://host:(lb_port+1)/
    """

    def __init__(
        self,
        host: str,
        port: int,
        backends: list[Backend],
        get_totals,          # callable() -> (total_connections, active_connections)
    ) -> None:
        self._host = host
        self._port = port
        self._backends = backends
        self._get_totals = get_totals

    async def run(self) -> None:
        server = await asyncio.start_server(self._handle, self._host, self._port)
        log.info("Stats endpoint → http://%s:%d/", self._host, self._port)
        async with server:
            await server.serve_forever()

    async def _handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        await reader.read(1024)  # drain the incoming request
        total, active = self._get_totals()
        payload = {
            "total_connections": total,
            "active_connections": active,
            "backends": [b.stats() for b in self._backends],
        }
        body = json.dumps(payload, indent=2).encode()
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            + f"Content-Length: {len(body)}\r\n\r\n".encode()
            + body
        )
        await writer.drain()
        writer.close()