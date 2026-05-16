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
