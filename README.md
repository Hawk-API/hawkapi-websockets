# hawkapi-websockets

WebSocket utilities for [HawkAPI](https://github.com/ashimov/HawkAPI). Connection manager with rooms + broadcasting, optional Redis pub/sub backplane for multi-process fan-out, and a heartbeat monitor.

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
git clone https://github.com/ashimov/hawkapi-websockets.git
cd hawkapi-websockets
uv sync --extra dev
uv run pytest -q
uv run ruff check . && uv run ruff format --check .
uv run pyright src/
```

## License

MIT.
