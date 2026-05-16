# Changelog

## 0.1.0 — 2026-05-16

Initial release.

- `ConnectionManager` — rooms, per-connection metadata, broadcasting with exclude lists.
- Auto-drop of broken connections during broadcast (one bad client won't block others).
- `RedisBackplane` — Redis pub/sub for multi-process broadcasting (extras `[redis]`).
- `HeartbeatMonitor` — interval pings + stale-connection eviction.
- `init_websockets(app, ...)` + `Depends(get_manager)`.
- `WebSocketLike` protocol — anything with `send_text/send_bytes/close` works.
