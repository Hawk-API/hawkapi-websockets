# Changelog

## 0.2.1 — 2026-05-16

`HeartbeatMonitor.is_alive(cid)` now returns `False` for connections that
were never `touch()`ed. Previously the default `last_seen=0` plus a small
`time.monotonic()` value (on a freshly-started process) could yield a
delta inside the timeout window, falsely reporting unseen connections as
alive.

## 0.2.0 — 2026-05-16

Security hardening:

- `ConnectionManager.room_validator` hook — gate joins by ACL; rejection raises `PermissionError` (CWE-284).
- Per-connection send timeout (`send_timeout_seconds`, default 5s) on `send_to` and broadcast dispatch; slow peers are dropped instead of stalling the manager (CWE-400).
- `ConnectionManager.max_connections` cap — `connect()` raises `RuntimeError` once the limit is reached (CWE-400).
- `send_to` and `close_all` now take an internal-lock snapshot of the registry and never hold the lock across I/O (CWE-362).
- `HeartbeatMonitor` registers a manager `disconnect_hook` and prunes `_last_seen` for connections that vanished externally.
- `RedisBackplane._listen` survives Redis disconnects: exponential-backoff reconnect with re-subscribe and a stop event for clean shutdown.
- `init_websockets` / `resolve_manager` now key the cache by app identity via `WeakKeyDictionary`, eliminating `id()` ABA reuse.
- Removed the dead `backpressure_queue_size` field (replaced by `send_timeout_seconds`).

## 0.1.0 — 2026-05-16

Initial release.

- `ConnectionManager` — rooms, per-connection metadata, broadcasting with exclude lists.
- Auto-drop of broken connections during broadcast (one bad client won't block others).
- `RedisBackplane` — Redis pub/sub for multi-process broadcasting (extras `[redis]`).
- `HeartbeatMonitor` — interval pings + stale-connection eviction.
- `init_websockets(app, ...)` + `Depends(get_manager)`.
- `WebSocketLike` protocol — anything with `send_text/send_bytes/close` works.
