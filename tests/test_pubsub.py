"""Backplane wiring — pure logic with a fake Redis pubsub."""

from __future__ import annotations

from hawkapi_websockets import ConnectionManager, RedisBackplane, bind_manager

from .conftest import FakeWebSocket


async def test_bind_manager_dispatches_text_message() -> None:
    m = ConnectionManager()
    ws = FakeWebSocket()
    await m.connect(ws, connection_id="a", rooms=["lobby"])
    bp = RedisBackplane(url="redis://unused", channel="x")
    bind_manager(bp, m)
    handler = bp._handlers[0]
    await handler({"kind": "text", "room": "lobby", "payload": "hi"})
    assert ws.sent_text == ["hi"]


async def test_bind_manager_dispatches_json_message() -> None:
    m = ConnectionManager()
    ws = FakeWebSocket()
    await m.connect(ws, connection_id="a")
    bp = RedisBackplane()
    bind_manager(bp, m)
    handler = bp._handlers[0]
    await handler({"kind": "json", "payload": {"k": "v"}})
    assert ws.sent_text and '"k"' in ws.sent_text[0]


async def test_bind_manager_respects_exclude() -> None:
    m = ConnectionManager()
    a, b = FakeWebSocket(), FakeWebSocket()
    await m.connect(a, connection_id="a")
    await m.connect(b, connection_id="b")
    bp = RedisBackplane()
    bind_manager(bp, m)
    await bp._handlers[0]({"kind": "text", "payload": "hi", "exclude": ["a"]})
    assert a.sent_text == []
    assert b.sent_text == ["hi"]


class _FakePubSub:
    """Fake redis pubsub that raises on first listen(), succeeds on second."""

    def __init__(self) -> None:
        self.call_count = 0
        self.subscribed: list[str] = []
        self.delivered: list[dict[str, object]] = []
        self.message_queue: list[dict[str, object]] = [
            {"type": "message", "data": '{"kind": "text", "payload": "second"}'}
        ]

    async def subscribe(self, channel: str) -> None:
        self.subscribed.append(channel)

    async def unsubscribe(self, channel: str) -> None:
        pass

    async def close(self) -> None:
        pass

    async def listen(self):  # type: ignore[no-untyped-def]
        self.call_count += 1
        if self.call_count == 1:
            raise ConnectionError("simulated redis disconnect")
        for msg in self.message_queue:
            yield msg


async def test_listen_reconnects_on_redis_error() -> None:
    import asyncio

    bp = RedisBackplane(reconnect_initial_delay=0.01, reconnect_max_delay=0.01)
    fake = _FakePubSub()
    bp._pubsub = fake

    seen: list[dict[str, object]] = []

    async def handler(payload: dict[str, object]) -> None:
        seen.append(payload)

    bp.on(handler)

    task = asyncio.create_task(bp._listen())
    # Wait until the second listen() iteration delivers the message.
    for _ in range(200):
        await asyncio.sleep(0.01)
        if seen:
            break
    bp._stop.set()
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass

    assert fake.call_count >= 2, "listen() should have been retried after the error"
    assert fake.subscribed == ["hawkapi:ws"], "should resubscribe after disconnect"
    assert seen == [{"kind": "text", "payload": "second"}]
