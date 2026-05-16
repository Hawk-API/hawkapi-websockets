"""ConnectionManager — connect, broadcast, rooms, backpressure."""

from __future__ import annotations

from hawkapi_websockets import ConnectionManager

from .conftest import FakeWebSocket


async def test_connect_assigns_id_and_tracks_connection() -> None:
    m = ConnectionManager()
    ws = FakeWebSocket()
    conn = await m.connect(ws)
    assert conn.id
    assert m.total_connections == 1
    assert conn.id in m.connections


async def test_connect_with_explicit_id_and_rooms() -> None:
    m = ConnectionManager()
    ws = FakeWebSocket()
    conn = await m.connect(ws, connection_id="alice", rooms=["lobby", "vip"])
    assert conn.id == "alice"
    assert m.room_size("lobby") == 1
    assert m.room_size("vip") == 1


async def test_disconnect_removes_from_rooms() -> None:
    m = ConnectionManager()
    await m.connect(FakeWebSocket(), connection_id="a", rooms=["r1"])
    await m.connect(FakeWebSocket(), connection_id="b", rooms=["r1"])
    await m.disconnect("a")
    assert m.room_size("r1") == 1
    await m.disconnect("b")
    # Empty rooms drop entirely.
    assert "r1" not in m.list_rooms()


async def test_join_and_leave_room() -> None:
    m = ConnectionManager()
    await m.connect(FakeWebSocket(), connection_id="a")
    await m.join("a", "lobby")
    assert m.room_size("lobby") == 1
    await m.leave("a", "lobby")
    assert m.room_size("lobby") == 0


async def test_broadcast_text_reaches_every_connection() -> None:
    m = ConnectionManager()
    ws1, ws2 = FakeWebSocket(), FakeWebSocket()
    await m.connect(ws1, connection_id="a")
    await m.connect(ws2, connection_id="b")
    sent = await m.broadcast_text("hi")
    assert sent == 2
    assert ws1.sent_text == ["hi"]
    assert ws2.sent_text == ["hi"]


async def test_broadcast_room_only_sends_to_members() -> None:
    m = ConnectionManager()
    ws_lobby, ws_other = FakeWebSocket(), FakeWebSocket()
    await m.connect(ws_lobby, connection_id="a", rooms=["lobby"])
    await m.connect(ws_other, connection_id="b")
    sent = await m.broadcast_json({"type": "hello"}, room="lobby")
    assert sent == 1
    assert ws_lobby.sent_text
    assert ws_other.sent_text == []


async def test_broadcast_exclude_skips_listed_ids() -> None:
    m = ConnectionManager()
    ws_a, ws_b = FakeWebSocket(), FakeWebSocket()
    await m.connect(ws_a, connection_id="a")
    await m.connect(ws_b, connection_id="b")
    sent = await m.broadcast_text("hi", exclude=["a"])
    assert sent == 1
    assert ws_a.sent_text == []
    assert ws_b.sent_text == ["hi"]


async def test_failing_connection_is_dropped() -> None:
    m = ConnectionManager()
    ws_bad = FakeWebSocket(fail_on_send=True)
    ws_ok = FakeWebSocket()
    await m.connect(ws_bad, connection_id="bad")
    await m.connect(ws_ok, connection_id="ok")
    sent = await m.broadcast_text("hi")
    assert sent == 1
    assert "bad" not in m.connections


async def test_send_to_returns_false_when_unknown() -> None:
    m = ConnectionManager()
    assert await m.send_to("ghost", "hi") is False


async def test_close_all_closes_every_connection() -> None:
    m = ConnectionManager()
    ws = FakeWebSocket()
    await m.connect(ws, connection_id="a")
    await m.close_all(code=4000)
    assert ws.closed_code == 4000
    assert m.total_connections == 0


async def test_send_to_dispatches_bytes_and_json() -> None:
    m = ConnectionManager()
    ws = FakeWebSocket()
    await m.connect(ws, connection_id="a")
    await m.send_to("a", b"raw")
    await m.send_to("a", {"k": "v"})
    await m.send_to("a", "plain")
    assert ws.sent_bytes == [b"raw"]
    assert any('"k"' in t for t in ws.sent_text)
    assert "plain" in ws.sent_text


async def test_room_validator_blocks_join() -> None:
    import pytest

    def deny(room: str, conn: object) -> bool:
        return False

    m = ConnectionManager(room_validator=deny)
    await m.connect(FakeWebSocket(), connection_id="a")
    with pytest.raises(PermissionError):
        await m.join("a", "secret")
    assert m.room_size("secret") == 0


async def test_room_validator_allows_join() -> None:
    async def allow(room: str, conn: object) -> bool:
        return room == "lobby"

    m = ConnectionManager(room_validator=allow)
    await m.connect(FakeWebSocket(), connection_id="a")
    await m.join("a", "lobby")
    assert m.room_size("lobby") == 1


async def test_send_timeout_drops_slow_connection() -> None:
    m = ConnectionManager(send_timeout_seconds=0.05)
    slow_ws = FakeWebSocket(send_delay=5.0)
    await m.connect(slow_ws, connection_id="slow")
    ok = await m.send_to("slow", "hi")
    assert ok is False
    assert "slow" not in m.connections


async def test_send_timeout_drops_slow_broadcast_target() -> None:
    m = ConnectionManager(send_timeout_seconds=0.05)
    slow = FakeWebSocket(send_delay=5.0)
    fast = FakeWebSocket()
    await m.connect(slow, connection_id="slow")
    await m.connect(fast, connection_id="fast")
    sent = await m.broadcast_text("hi")
    assert sent == 1
    assert "slow" not in m.connections
    assert fast.sent_text == ["hi"]


async def test_max_connections_caps_registry() -> None:
    import pytest

    m = ConnectionManager(max_connections=2)
    await m.connect(FakeWebSocket(), connection_id="a")
    await m.connect(FakeWebSocket(), connection_id="b")
    with pytest.raises(RuntimeError, match="max connections 2 reached"):
        await m.connect(FakeWebSocket(), connection_id="c")
    assert m.total_connections == 2


async def test_close_all_handles_concurrent_connect() -> None:
    """Sanity: close_all snapshots under the lock and tolerates new connects after."""
    m = ConnectionManager()
    for i in range(5):
        await m.connect(FakeWebSocket(), connection_id=f"c{i}")
    await m.close_all(code=1001)
    assert m.total_connections == 0
    # A subsequent connect after close_all should still work.
    await m.connect(FakeWebSocket(), connection_id="new")
    assert m.total_connections == 1
