"""Heartbeat monitor."""

from __future__ import annotations

import time

from hawkapi_websockets import ConnectionManager, HeartbeatConfig, HeartbeatMonitor

from .conftest import FakeWebSocket


async def test_tick_sends_ping_to_alive_connections() -> None:
    m = ConnectionManager()
    ws = FakeWebSocket()
    await m.connect(ws, connection_id="a")
    mon = HeartbeatMonitor(manager=m, config=HeartbeatConfig(interval_seconds=10))
    mon.touch("a")
    sent = await mon.tick()
    assert sent == 1
    assert ws.sent_text == ["ping"]


async def test_tick_drops_stale_connections() -> None:
    m = ConnectionManager()
    ws = FakeWebSocket()
    await m.connect(ws, connection_id="a")
    mon = HeartbeatMonitor(manager=m, config=HeartbeatConfig(timeout_seconds=0.0))
    mon._last_seen["a"] = time.monotonic() - 100
    await mon.tick()
    assert "a" not in m.connections


def test_is_alive_returns_false_when_never_touched() -> None:
    mon = HeartbeatMonitor(manager=ConnectionManager())
    assert mon.is_alive("ghost") is False


def test_touch_updates_last_seen() -> None:
    mon = HeartbeatMonitor(manager=ConnectionManager())
    mon.touch("a")
    assert mon.is_alive("a") is True
