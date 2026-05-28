# Load Balancer — Documentation

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Module Reference](#module-reference)
   - [main.py — Entry Point](#mainpy--entry-point)
   - [lb.py — Orchestrator](#lbpy--orchestrator)
   - [backend.py — Backend Model](#backendpy--backend-model)
   - [scheduler.py — Round-Robin Scheduler](#schedulerpy--round-robin-scheduler)
   - [proxy.py — Connection Proxy](#proxypy--connection-proxy)
   - [health_checker.py — Health Checker](#health_checkerpy--health-checker)
   - [stats.py — Statistics Server](#statspy--statistics-server)
   - [tls.py — TLS Context Builder](#tlspy--tls-context-builder)
4. [Configuration](#configuration)
5. [How It Works — End-to-End Flow](#how-it-works--end-to-end-flow)
6. [Statistics API](#statistics-api)
7. [Running the Load Balancer](#running-the-load-balancer)

---

## Overview

This is an asynchronous TCP/TLS load balancer written in Python using `asyncio`. It distributes
incoming client connections across a pool of backend servers using a **round-robin** strategy,
with built-in health checking, connection retries, TLS termination, and a live statistics
endpoint.

**Key capabilities:**

| Feature | Detail |
|---|---|
| Protocol | TCP (optionally TLS-terminated) |
| Balancing algorithm | Round-robin across healthy backends |
| Health checking | Periodic TCP probe with optional HTTP HEAD |
| Fault tolerance | Per-connection retry across backends |
| Observability | Live JSON metrics on a dedicated HTTP port |
| Concurrency model | Fully async (`asyncio`) — single-process, no threads |

---

## Architecture

```
                         ┌──────────────────────────────────────────────┐
                         │              LoadBalancer (lb.py)             │
                         │                                               │
  Client                 │  ┌───────────┐   ┌────────────┐              │
  ──────► TCP/TLS ──────►│  │  Proxy    │──►│ RoundRobin │              │
  connections            │  │ (proxy.py)│   │(scheduler) │              │
                         │  └───────────┘   └────────────┘              │
                         │        │                │                     │
                         │        │          ┌─────▼──────┐             │
                         │        │          │  Backend[] │             │
                         │        │          │(backend.py)│             │
                         │        │          └─────┬──────┘             │
                         │        │                │                     │
                         │        └────────────────┘                    │
                         │              │  routes to                     │
                         │              ▼                                │
                         │     Backend server (host:port)                │
                         │                                               │
                         │  ┌──────────────┐    ┌───────────────┐       │
                         │  │HealthChecker │    │  StatsServer  │       │
                         │  │(background)  │    │(port+1 / HTTP)│       │
                         │  └──────────────┘    └───────────────┘       │
                         └──────────────────────────────────────────────┘
```

All components run concurrently inside a single `asyncio` event loop, started by `lb.run()`.

---

## Module Reference

---

### `main.py` — Entry Point

**Responsibility:** Parses CLI arguments, loads the configuration file, and starts the load balancer.

#### Functions

##### `parse_args() → Namespace`
Parses command-line arguments.

| Argument | Default | Description |
|---|---|---|
| `--config` | `config/config.json` | Path to the JSON configuration file |

##### `load_config(path: str) → dict`
Reads and returns the JSON configuration from the given file path.
Raises `FileNotFoundError` if the path does not exist.

##### `main()`
Top-level coroutine. Calls `parse_args`, `load_config`, constructs a `LoadBalancer` instance,
and calls `lb.run()`. Catches `KeyboardInterrupt` for clean shutdown.

---

### `lb.py` — Orchestrator

**Responsibility:** Wires all components together and drives the main event loop.

#### Class: `LoadBalancer`

The central class. Instantiated once per process. Composes a `RoundRobin` scheduler,
`HealthChecker`, `ConnectionProxy`, and `StatsServer`.

##### Constructor — `__init__(config: dict)`

Reads the following keys from `config`:

| Config key | Type | Default | Description |
|---|---|---|---|
| `host` | str | `"0.0.0.0"` | Address to listen on |
| `port` | int | `8443` | Port to listen on |
| `tls.enabled` | bool | `false` | Enable TLS termination |
| `tls.cert` | str | — | Path to TLS certificate |
| `tls.key` | str | — | Path to TLS private key |
| `backends` | list | — | List of `{host, port}` dicts |
| `health_check_interval` | float | `10.0` | Seconds between health checks |
| `health_check_path` | str | `"/health"` | HTTP path for health probes |
| `max_retries` | int | `3` | Max backend connection retries per client |
| `connect_timeout` | float | `5.0` | Backend connect timeout (seconds) |
| `stream_timeout` | float | `60.0` | Bidirectional pipe timeout (seconds) |

Instantiates (in order):
- `Backend` objects for each entry in `backends`
- `RoundRobin(backends)`
- `HealthChecker(backends, interval, path)`
- `TLSContextBuilder(cert, key)` if TLS enabled
- `ConnectionProxy(scheduler, max_retries, connect_timeout, stream_timeout)`
- `StatsServer(host, port+1, backends, get_totals_callback)`

##### Method: `_handle(reader, writer)`
Async callback passed to `asyncio.start_server`. Called once per incoming client connection.
Increments active connection counter, delegates to `proxy.handle()`, and decrements on exit.

##### Method: `run()`
Main coroutine. Builds the SSL context (if TLS enabled), starts the TCP server, then runs
the server, health checker, and stats server concurrently via `asyncio.gather()`.

---

### `backend.py` — Backend Model

**Responsibility:** Holds the address and live metrics for a single backend server.

#### Class: `Backend` (dataclass)

| Field | Type | Default | Description |
|---|---|---|---|
| `host` | str | — | Backend hostname or IP |
| `port` | int | — | Backend port |
| `healthy` | bool | `True` | Whether this backend is currently up |
| `total_requests` | int | `0` | Total requests routed here |
| `failed_requests` | int | `0` | Total failed connection attempts |
| `last_checked` | float | `0.0` | Unix timestamp of last health probe |

##### Property: `address → str`
Returns `"host:port"` as a formatted string.

##### Method: `record_success()`
Increments `total_requests` by 1.

##### Method: `record_failure()`
Increments both `total_requests` and `failed_requests` by 1.

##### Method: `stats() → dict`
Returns a snapshot dictionary:

```python
{
    "address":         "host:port",
    "healthy":         True | False,
    "total_requests":  int,
    "failed_requests": int,
    "error_rate":      float   # failed / total, rounded to 3 decimal places
}
```

---

### `scheduler.py` — Round-Robin Scheduler

**Responsibility:** Selects the next available healthy backend in fair rotation.

#### Class: `RoundRobin`

##### Constructor — `__init__(backends: list[Backend])`
Stores backends in a `collections.deque` for O(1) rotation.

##### Method: `next() → Backend | None`
Rotates through up to `len(backends)` positions. Returns the first backend where
`backend.healthy is True`. Returns `None` if all backends are unhealthy.

**Algorithm:**
```
for _ in range(len(backends)):
    rotate deque left by 1
    if deque[0].healthy:
        return deque[0]
return None
```

This ensures a deterministic, fair rotation with no backend visited twice per cycle.

---

### `proxy.py` — Connection Proxy

**Responsibility:** Manages the full lifecycle of one client connection — backend selection,
connection establishment with retries, and bidirectional data piping.

#### Class: `ConnectionProxy`

##### Constructor — `__init__(scheduler, max_retries, connect_timeout, stream_timeout)`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scheduler` | RoundRobin | — | Backend selector |
| `max_retries` | int | `3` | Max attempts before dropping the client |
| `connect_timeout` | float | `5.0` | Timeout for each backend TCP connect |
| `stream_timeout` | float | `60.0` | Max duration of the piped connection |

##### Method: `handle(client_r, client_w)`
Entry point for a new client. Calls `_connect_with_retry` to obtain a backend connection,
then calls `_stream` to pipe data. Closes the client writer if no backend is available.

##### Method: `_connect_with_retry(peer) → (reader, writer, backend) | (None, None, None)`
Attempts to connect to a healthy backend up to `max_retries` times.

For each attempt:
1. Calls `scheduler.next()` — returns `None` if no healthy backends remain
2. Opens a TCP connection with `asyncio.wait_for(..., connect_timeout)`
3. On success — calls `backend.record_success()`, returns the streams
4. On failure — calls `backend.record_failure()`, logs a warning, continues

Returns `(None, None, None)` when all retries are exhausted.

##### Method: `_stream(client_r, client_w, back_r, back_w)`
Pipes data in both directions concurrently with a total timeout of `stream_timeout`.
Uses `asyncio.gather()` over two `_pipe()` tasks:
- `client_r → back_w` (upload)
- `back_r → client_w` (download)

Handles `asyncio.TimeoutError`, `ConnectionResetError`, and `BrokenPipeError` gracefully.
Always closes both writers in a `finally` block.

##### Static method: `_pipe(reader, writer)`
Reads 64 KB chunks from `reader` and writes them to `writer` until EOF.
Silently discards all exceptions (one side closing is expected and normal).

---

### `health_checker.py` — Health Checker

**Responsibility:** Continuously probes all backends and updates their `healthy` flag.

#### Class: `HealthChecker`

##### Constructor — `__init__(backends, interval, timeout, http_path)`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `backends` | list[Backend] | — | Backends to monitor |
| `interval` | float | `10.0` | Seconds between full check cycles |
| `timeout` | float | `3.0` | Per-backend probe timeout |
| `http_path` | str | `"/health"` | Path for optional HTTP HEAD check |

##### Method: `run()`
Infinite loop coroutine. Each cycle probes all backends concurrently via
`asyncio.gather(*[self._check(b) for b in self.backends])`, then sleeps `interval` seconds.

##### Method: `_check(backend)`
Probes a single backend:

1. Opens a TCP connection within `timeout` seconds
2. Sends an HTTP HEAD request to `http_path` (best-effort; failure is silently ignored)
3. Marks `backend.healthy = True` and records `backend.last_checked`
4. Logs a state transition message if the backend just came back online

If any step raises an exception:
- Marks `backend.healthy = False`
- Records `backend.last_checked`
- Logs a state transition message if the backend just went offline

---

### `stats.py` — Statistics Server

**Responsibility:** Serves a JSON metrics snapshot over a plain HTTP endpoint on a dedicated port.

#### Class: `StatsServer`

##### Constructor — `__init__(host, port, backends, get_totals)`

| Parameter | Type | Description |
|---|---|---|
| `host` | str | Listen address |
| `port` | int | Listen port (typically `lb_port + 1`) |
| `backends` | list[Backend] | Backends to include in output |
| `get_totals` | `() → (int, int)` | Callback returning `(total, active)` connection counts |

##### Method: `run()`
Starts the HTTP server and logs the stats endpoint URL.

##### Method: `_handle(reader, writer)`
Handles one HTTP request. Drains up to 1024 bytes of the incoming request (content is
ignored), then writes an HTTP 200 response with a JSON body.

**Response shape:**

```json
{
  "total_connections": 142,
  "active_connections": 3,
  "backends": [
    {
      "address": "localhost:3000",
      "healthy": true,
      "total_requests": 71,
      "failed_requests": 0,
      "error_rate": 0.0
    },
    {
      "address": "localhost:3001",
      "healthy": true,
      "total_requests": 71,
      "failed_requests": 2,
      "error_rate": 0.028
    }
  ]
}
```

---

### `tls.py` — TLS Context Builder

**Responsibility:** Constructs a hardened `ssl.SSLContext` for the load balancer's listener.

#### Class: `TLSContextBuilder`

##### Constructor — `__init__(cert_path: str, key_path: str)`

| Parameter | Description |
|---|---|
| `cert_path` | Path to the PEM-encoded certificate file |
| `key_path` | Path to the PEM-encoded private key file |

##### Method: `build() → ssl.SSLContext`
1. Creates a server-side context: `ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)`
2. Loads the certificate chain: `ctx.load_cert_chain(cert_path, key_path)`
3. Enforces TLS 1.2 minimum: `ctx.minimum_version = ssl.TLSVersion.TLSv1_2`
4. Returns the configured context

---

## Configuration

The load balancer is driven entirely by a JSON configuration file.

**Default path:** `config/config.json`  
**Override:** `python main.py --config path/to/config.json`

### Full Example

```json
{
  "host": "0.0.0.0",
  "port": 8443,
  "health_check_interval": 10.0,
  "health_check_path": "/health",
  "max_retries": 3,
  "connect_timeout": 5.0,
  "stream_timeout": 60.0,
  "tls": {
    "enabled": false,
    "cert": "certs/server.crt",
    "key": "certs/server.key"
  },
  "backends": [
    { "host": "localhost", "port": 3000 },
    { "host": "localhost", "port": 3001 },
    { "host": "localhost", "port": 3002 }
  ]
}
```

### Field Reference

| Key | Type | Required | Description |
|---|---|---|---|
| `host` | string | No | Address to listen on. Default `"0.0.0.0"` |
| `port` | integer | No | Port to listen on. Default `8443` |
| `health_check_interval` | float | No | Seconds between health check cycles. Default `10.0` |
| `health_check_path` | string | No | HTTP path probed during health checks. Default `"/health"` |
| `max_retries` | integer | No | Backend connection attempts per client. Default `3` |
| `connect_timeout` | float | No | Seconds to wait for a backend TCP connect. Default `5.0` |
| `stream_timeout` | float | No | Max seconds a proxied connection may stay open. Default `60.0` |
| `tls.enabled` | boolean | No | Enable TLS termination. Default `false` |
| `tls.cert` | string | If TLS | Path to PEM certificate |
| `tls.key` | string | If TLS | Path to PEM private key |
| `backends` | array | **Yes** | List of backend objects (`host` + `port`) |

---

## How It Works — End-to-End Flow

```
1. Startup
   main.py        loads config → creates LoadBalancer
   lb.run()       builds TLS context (if enabled)
                  starts TCP server on host:port
                  starts HealthChecker (background)
                  starts StatsServer   (background)

2. Health checking (every interval seconds, concurrent)
   HealthChecker._check(backend)
     ├─ TCP connect within timeout
     ├─ HTTP HEAD to health_check_path (best-effort)
     └─ Sets backend.healthy = True / False
        Logs transitions: ONLINE / OFFLINE

3. New client connection
   lb._handle(reader, writer)
     ├─ total_connections += 1
     ├─ active_connections += 1
     ├─ proxy.handle(reader, writer)
     │    ├─ _connect_with_retry(peer)
     │    │    └─ for up to max_retries:
     │    │         scheduler.next() → healthy Backend
     │    │         asyncio.open_connection(host, port) ← connect_timeout
     │    │         on success → backend.record_success()
     │    │         on failure → backend.record_failure(), try next
     │    │
     │    └─ _stream(client, backend)
     │         ├─ asyncio.gather(pipe client→backend, pipe backend→client)
     │         ├─ enforces stream_timeout
     │         └─ closes both writers in finally
     │
     └─ active_connections -= 1

4. Stats query (any time)
   HTTP GET http://host:(port+1)/
     └─ returns JSON with totals and per-backend metrics
```

---

## Statistics API

The stats server listens on **`lb_port + 1`** (e.g. port `8444` when the LB runs on `8443`).

**Request:**
```
GET / HTTP/1.1
Host: localhost:8444
```

**Response:**
```json
{
  "total_connections": 142,
  "active_connections": 3,
  "backends": [
    {
      "address": "localhost:3000",
      "healthy": true,
      "total_requests": 71,
      "failed_requests": 0,
      "error_rate": 0.0
    }
  ]
}
```

**Quick check:**
```bash
curl http://localhost:8444/
```

---

## Running the Load Balancer

```bash
# With default config path (config/config.json)
python main.py

# With a custom config path
python main.py --config path/to/config.json

# Stop with Ctrl+C (graceful shutdown)
```
