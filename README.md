# hawkapi-websockets

WebSocket utilities for [HawkAPI](https://github.com/Hawk-API/HawkAPI). Connection manager with rooms + broadcasting, optional Redis pub/sub backplane for multi-process fan-out, and a heartbeat monitor.

## Install

```bash
pip install hawkapi-websockets                # local-only (single-process)
pip install 'hawkapi-websockets[redis]'       # + Redis backplane
```

## Quickstart

```python
from hawkapi import Depends, HawkAPI, WebSocket
from hawkapi_websockets import ConnectionManager, get_manager, init_websockets

app = HawkAPI()
init_websockets(app)


@app.websocket("/ws/{room}")
async def ws_room(websocket: WebSocket, room: str, m: ConnectionManager = Depends(get_manager)):
    await websocket.accept()
    conn = await m.connect(websocket, rooms=[room])
    try:
        await m.broadcast_json({"event": "joined", "id": conn.id}, room=room)
        async for msg in websocket.iter_text():
            await m.broadcast_text(msg, room=room, exclude=[conn.id])
    finally:
        await m.disconnect(conn.id)
        await m.broadcast_json({"event": "left", "id": conn.id}, room=room)
```

## Authentication & Security

WebSockets bypass CORS, so the browser's same-origin protections do **not** apply.
A page on any site can open a WebSocket to your server and ride the user's cookies
(Cross-Site WebSocket Hijacking, CSWSH). This library is secure-by-default where it
can be and provides hooks where enforcement has to live in your app.

### Validate the Origin (CSWSH)

Pass `allowed_origins` and call `check_origin` **before** `accept()`:

```python
m = ConnectionManager(allowed_origins={"https://app.example.com"})
# or: init_websockets(app, allowed_origins={"https://app.example.com"})


@app.websocket("/ws/{room}")
async def ws_room(websocket, room, m=Depends(get_manager)):
    if not m.check_origin(websocket):
        await websocket.close(code=4403)  # forbidden
        return
    await websocket.accept()
    ...
```

When `allowed_origins` is `None` (the default) `check_origin` returns `True` and
performs **no** validation — you are responsible for enforcing it out of band.
Leaving it unset on a cookie-authenticated endpoint exposes you to CSWSH.

### Authenticate the connection

Provide an `on_connect` hook. It runs inside `connect()` before the connection is
tracked; return `False` (or raise) to reject — `connect()` raises `PermissionError`.
Authenticate with a token sent in a **header** (e.g. `Authorization` /
`Sec-WebSocket-Protocol`), never in the query string — query strings leak into logs,
referrers, and proxies.

```python
async def authenticate(websocket, metadata: dict) -> bool:
    token = (getattr(websocket, "headers", {}) or {}).get("authorization", "")
    user = await verify_token(token)
    if user is None:
        return False
    metadata["user_id"] = user.id   # mutate metadata to attach identity
    return True


m = ConnectionManager(on_connect=authenticate)
```

### Authorize rooms

`room_validator` is enforced for **both** `join()` and the initial `rooms=[...]`
passed to `connect()`. A denial raises `PermissionError`, so an unauthenticated
client can never enter a room it isn't allowed into.

### DoS limits

- `max_connections` defaults to `10_000`. Set it to `None` only if you have your own
  admission control — `None` means unbounded and is an A05 DoS risk.
- `max_message_bytes` defaults to `1_048_576` (1 MiB). It is **advisory**: your
  receive loop must enforce it. The `receive_text(conn)` / `receive_json(conn)`
  helpers wrap the underlying receive, reject oversized frames (closing with code
  `1009`), and drop the connection:

  ```python
  conn = await m.connect(websocket)
  while True:
      msg = await m.receive_text(conn)   # raises ValueError if over the limit
      ...
  ```

### Tenant isolation

- `require_room=True` makes room-less `broadcast_text`/`broadcast_json` raise
  `ValueError`, preventing accidental cross-tenant fan-out.
- The Redis backplane drops room-less messages by default (they would otherwise
  broadcast to every connection). Set `bind_manager(bp, m, allow_global=True)` —
  or `init_websockets(app, allow_global_broadcast=True)` — only if you really want
  server-wide broadcasts.

### Logging

Security-relevant events are logged on the `hawkapi_websockets` logger: `info` on
connect/disconnect, `warning` on Origin rejection, `on_connect`/`room_validator`
denial, oversized messages, dropped room-less backplane messages, and when
`max_connections` is hit.

## Broadcasting

```python
await m.broadcast_text("hi")                              # everyone
await m.broadcast_json({"event": "x"}, room="lobby")      # one room
await m.broadcast_text("hi", exclude=[conn.id])           # skip the sender
await m.send_to(connection_id, {"private": True})         # direct
```

Failed sends auto-drop the broken connection, so a misbehaving client never blocks the broadcast.

## Redis backplane

When you run multiple worker processes, broadcasts must travel across them. Plug in the Redis backplane and every `publish()` is fanned out to every replica's ConnectionManager.

```python
from hawkapi_websockets import RedisBackplane, init_websockets

backplane = RedisBackplane(url="redis://localhost:6379/0", channel="hawkapi:ws")
m = init_websockets(app, backplane=backplane)


# Anywhere in your code:
await backplane.publish({"kind": "json", "room": "lobby", "payload": {"event": "tick"}})
```

The backplane subscribes during `app.on_startup` and shuts down with the app.

## Heartbeat

```python
from hawkapi_websockets import HeartbeatConfig, HeartbeatMonitor

monitor = HeartbeatMonitor(manager=m, config=HeartbeatConfig(interval_seconds=30, timeout_seconds=90))
monitor.start()             # background task pings every interval, drops stale connections


@app.websocket("/ws")
async def ws(websocket):
    await websocket.accept()
    conn = await m.connect(websocket)
    monitor.touch(conn.id)
    try:
        async for msg in websocket.iter_text():
            monitor.touch(conn.id)        # client liveness signal
            ...
    finally:
        await m.disconnect(conn.id)
```

## Testing

The manager works with any object implementing `WebSocketLike` (`send_text`/`send_bytes`/`close`), so you can test broadcast logic without a real WebSocket connection.

```python
class FakeWS:
    def __init__(self): self.sent: list[str] = []
    async def send_text(self, data): self.sent.append(data)
    async def send_bytes(self, data): ...
    async def close(self, code=1000): ...


async def test_chat_broadcast():
    m = ConnectionManager()
    a, b = FakeWS(), FakeWS()
    await m.connect(a, connection_id="alice", rooms=["lobby"])
    await m.connect(b, connection_id="bob", rooms=["lobby"])
    await m.broadcast_text("hi", room="lobby", exclude=["alice"])
    assert b.sent == ["hi"]
```

## Development

```bash
git clone https://github.com/Hawk-API/hawkapi-websockets.git
cd hawkapi-websockets
uv sync --extra dev
uv run pytest -q
uv run ruff check . && uv run ruff format --check .
uv run pyright src/
```

## License

MIT.
