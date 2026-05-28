"""
balancer.py — Top-level orchestrator.

LoadBalancer wires together all components and owns the server lifecycle.
"""

import asyncio
import logging

from .backend import Backend
from .scheduler import RoundRobin
from .health_checker import HealthChecker
from .tls import TLSContextBuilder
from .proxy import ConnectionProxy
from .stats import StatsServer

log = logging.getLogger("load_balancer")


class LoadBalancer:
    """
    Orchestrates all load-balancer components.

    Components
    ----------
    Backend          — per-server state and metrics
    RoundRobin       — selects the next healthy backend
    HealthChecker    — background prober; marks backends up/down
    TLSContextBuilder— builds the SSLContext when TLS is enabled
    ConnectionProxy  — handles a single client: retry logic + bidirectional pipe
    StatsServer      — JSON metrics endpoint on (port + 1)
    """

    def __init__(self, config: dict) -> None:
        # ── Network settings ──────────────────────────────────────────────
        self._host: str = config.get("host", "0.0.0.0")
        self._port: int = config.get("port", 8443)

        # ── TLS ───────────────────────────────────────────────────────────
        tls_cfg = config.get("tls", {})
        self._tls_enabled: bool = tls_cfg.get("enabled", False)
        self._tls_builder = TLSContextBuilder(
            cert_path=tls_cfg.get("cert", "certs/server.crt"),
            key_path=tls_cfg.get("key", "certs/server.key"),
        )

        # ── Backends ──────────────────────────────────────────────────────
        self._backends: list[Backend] = [Backend(**b) for b in config["backends"]]

        # ── Scheduler ─────────────────────────────────────────────────────
        self._scheduler = RoundRobin(self._backends)

        # ── Health checker ────────────────────────────────────────────────
        self._health_checker = HealthChecker(
            backends=self._backends,
            interval=config.get("health_check_interval", 10.0),
            http_path=config.get("health_check_path", "/health"),
        )

        # ── Proxy ─────────────────────────────────────────────────────────
        self._proxy = ConnectionProxy(
            scheduler=self._scheduler,
            max_retries=config.get("max_retries", 3),
            connect_timeout=config.get("connect_timeout", 5.0),
            stream_timeout=config.get("stream_timeout", 60.0),
        )

        # ── Connection counters ───────────────────────────────────────────
        self._total_connections: int = 0
        self._active_connections: int = 0

        # ── Stats server ──────────────────────────────────────────────────
        self._stats = StatsServer(
            host=self._host,
            port=self._port + 1,
            backends=self._backends,
            get_totals=lambda: (self._total_connections, self._active_connections),
        )

    # ── Client handler ────────────────────────────────────────────────────

    async def _handle(
        self,
        client_r: asyncio.StreamReader,
        client_w: asyncio.StreamWriter,
    ) -> None:
        self._total_connections += 1
        self._active_connections += 1
        try:
            await self._proxy.handle(client_r, client_w)
        finally:
            self._active_connections -= 1

    # ── Entry point ───────────────────────────────────────────────────────

    async def run(self) -> None:
        ssl_ctx = self._tls_builder.build() if self._tls_enabled else None
        server = await asyncio.start_server(
            self._handle,
            self._host,
            self._port,
            ssl=ssl_ctx,
            limit=2 ** 16,
        )
        proto = "TLS" if self._tls_enabled else "TCP"
        addrs = [s.getsockname() for s in server.sockets]
        log.info("Load balancer listening on %s [%s]", addrs, proto)
        log.info("Backends: %s", [b.address for b in self._backends])

        await asyncio.gather(
            server.serve_forever(),
            self._health_checker.run(),
            self._stats.run(),
        )