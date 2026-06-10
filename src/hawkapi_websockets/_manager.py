"""Connection manager — track WebSockets, group by room, broadcast."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import uuid
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, Protocol

logger = logging.getLogger("hawkapi_websockets")


class WebSocketLike(Protocol):
    async def send_text(self, data: str) -> None: ...
    async def send_bytes(self, data: bytes) -> None: ...
    async def close(self, code: int = 1000) -> None: ...


@dataclass(slots=True)
class Connection:
    id: str
    websocket: WebSocketLike
    rooms: set[str] = field(default_factory=set)
    metadata: dict[str, Any] = field(default_factory=dict)

    async def send_text(self, data: str) -> None:
        await self.websocket.send_text(data)

    async def send_json(self, data: Any) -> None:
        await self.websocket.send_text(json.dumps(data))

    async def send_bytes(self, data: bytes) -> None:
        await self.websocket.send_bytes(data)


RoomValidator = Callable[[str, Connection], "Awaitable[bool] | bool"]
OnConnect = Callable[[WebSocketLike, dict[str, Any]], Awaitable[bool]]


@dataclass
class ConnectionManager:
    connections: dict[str, Connection] = field(default_factory=dict)
    rooms: dict[str, set[str]] = field(default_factory=dict)
    send_timeout_seconds: float = 5.0
    # Bounded by default (A05 DoS protection). Set to ``None`` to opt into
    # unlimited connections — only do this behind your own admission control.
    max_connections: int | None = 10_000
    # Advisory cap for the app's receive loop; see ``receive_text``/``receive_json``.
    max_message_bytes: int | None = 1_048_576
    # When set, the request Origin header is validated against this allow-list
    # in ``check_origin`` (CSWSH / A07 protection).
    allowed_origins: set[str] | None = None
    # When True, room-less broadcasts are rejected to avoid cross-tenant leaks.
    require_room: bool = False
    room_validator: RoomValidator | None = field(default=None)
    # Optional auth hook invoked during ``connect``; return False to reject.
    on_connect: OnConnect | None = field(default=None)
    disconnect_hooks: list[Callable[[str], None]] = field(default_factory=list)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def check_origin(self, websocket: WebSocketLike) -> bool:
        """Return whether the WebSocket's Origin header is allowed.

        When :attr:`allowed_origins` is ``None`` this always returns ``True``
        (no validation) — the caller is responsible for enforcing Origin checks
        out of band. When set, the request ``Origin`` header must exactly match
        one of the allowed values. Call this **before** ``websocket.accept()``.
        """
        if self.allowed_origins is None:
            return True
        headers = getattr(websocket, "headers", None) or {}
        origin = None
        try:
            origin = headers.get("origin")
        except AttributeError:  # pragma: no cover - defensive
            origin = None
        allowed = origin is not None and origin in self.allowed_origins
        if not allowed:
            logger.warning("origin %r rejected (not in allowed_origins)", origin)
        return allowed

    async def connect(
        self,
        websocket: WebSocketLike,
        *,
        connection_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        rooms: Iterable[str] = (),
    ) -> Connection:
        initial_rooms = set(rooms)
        cid = connection_id or uuid.uuid4().hex
        conn = Connection(
            id=cid,
            websocket=websocket,
            rooms=set(),
            metadata=dict(metadata or {}),
        )
        # Auth hook — mirror the room_validator pattern. Reject before the
        # connection is tracked so unauthorized clients never enter the registry.
        if self.on_connect is not None:
            decision = await self.on_connect(websocket, conn.metadata)
            if not decision:
                logger.warning("connect denied by on_connect hook for %s", cid)
                raise PermissionError("connection denied")
        # Enforce the room validator on every initial room, same as join().
        if self.room_validator is not None:
            for room in initial_rooms:
                decision = self.room_validator(room, conn)
                if inspect.isawaitable(decision):
                    decision = await decision
                if not decision:
                    logger.warning("connect denied for room %r (%s)", room, cid)
                    raise PermissionError(f"join denied for room {room!r}")
        async with self._lock:
            if self.max_connections is not None and len(self.connections) >= self.max_connections:
                logger.warning("max connections %s reached", self.max_connections)
                raise RuntimeError(f"max connections {self.max_connections} reached")
            conn.rooms = initial_rooms
            self.connections[cid] = conn
            for room in conn.rooms:
                self.rooms.setdefault(room, set()).add(cid)
            logger.info("connect %s rooms=%s", cid, sorted(conn.rooms))
            return conn

    async def disconnect(self, connection_id: str) -> None:
        async with self._lock:
            conn = self.connections.pop(connection_id, None)
            if conn is None:
                return
            for room in conn.rooms:
                members = self.rooms.get(room)
                if members is not None:
                    members.discard(connection_id)
                    if not members:
                        self.rooms.pop(room, None)
            logger.info("disconnect %s", connection_id)
        # Run hooks outside the lock so they can call back into the manager.
        for hook in list(self.disconnect_hooks):
            try:
                hook(connection_id)
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("disconnect hook raised for %s: %s", connection_id, exc)

    async def join(self, connection_id: str, room: str) -> None:
        """Add ``connection_id`` to ``room``.

        If a :attr:`room_validator` is configured, it is invoked with
        ``(room, connection)`` and must return ``True`` (or an awaitable that
        resolves to ``True``) for the join to be accepted. A falsy return value
        raises :class:`PermissionError`.
        """
        async with self._lock:
            conn = self.connections.get(connection_id)
            if conn is None:
                return
            validator = self.room_validator
        if validator is not None:
            decision = validator(room, conn)
            if inspect.isawaitable(decision):
                decision = await decision
            if not decision:
                logger.warning("join denied for room %r (%s)", room, connection_id)
                raise PermissionError(f"join denied for room {room!r}")
        async with self._lock:
            # Re-fetch in case the connection went away while we awaited.
            conn = self.connections.get(connection_id)
            if conn is None:
                return
            conn.rooms.add(room)
            self.rooms.setdefault(room, set()).add(connection_id)

    async def leave(self, connection_id: str, room: str) -> None:
        async with self._lock:
            conn = self.connections.get(connection_id)
            if conn is None:
                return
            conn.rooms.discard(room)
            members = self.rooms.get(room)
            if members is not None:
                members.discard(connection_id)
                if not members:
                    self.rooms.pop(room, None)

    async def broadcast_text(
        self,
        data: str,
        *,
        room: str | None = None,
        exclude: Iterable[str] = (),
    ) -> int:
        """Send ``data`` to every connection (or every member of ``room``). Returns send count."""
        if room is None and self.require_room:
            raise ValueError("room is required (require_room=True); refusing global broadcast")
        exclude_set = set(exclude)
        targets = await self._snapshot_targets(room, exclude_set)

        async def _send(c: Connection) -> None:
            await c.send_text(data)

        return await self._dispatch(targets, _send)

    async def broadcast_json(
        self,
        data: Any,
        *,
        room: str | None = None,
        exclude: Iterable[str] = (),
    ) -> int:
        payload = json.dumps(data)
        return await self.broadcast_text(payload, room=room, exclude=exclude)

    async def send_to(self, connection_id: str, data: Any) -> bool:
        async with self._lock:
            conn = self.connections.get(connection_id)
        if conn is None:
            return False
        try:
            if isinstance(data, str):
                await asyncio.wait_for(conn.send_text(data), timeout=self.send_timeout_seconds)
            elif isinstance(data, bytes):
                await asyncio.wait_for(conn.send_bytes(data), timeout=self.send_timeout_seconds)
            else:
                await asyncio.wait_for(conn.send_json(data), timeout=self.send_timeout_seconds)
        except Exception as exc:
            logger.warning("send_to %s failed: %s", connection_id, exc)
            await self.disconnect(connection_id)
            return False
        return True

    async def receive_text(
        self,
        conn: Connection,
        *,
        max_bytes: int | None = ...,  # type: ignore[assignment]
    ) -> str:
        """Receive one text frame, rejecting oversized payloads (A05 DoS).

        ``max_bytes`` defaults to :attr:`max_message_bytes`. When the decoded
        UTF-8 size exceeds the limit the connection is closed (policy
        violation, code ``1009``) and :class:`ValueError` is raised. Use this
        in your handler loop instead of calling ``websocket.receive_text``
        directly.
        """
        limit = self.max_message_bytes if max_bytes is ... else max_bytes
        data: str = await conn.websocket.receive_text()  # type: ignore[attr-defined]
        if limit is not None and len(data.encode("utf-8")) > limit:
            logger.warning("oversized message from %s; closing", conn.id)
            try:
                await conn.websocket.close(code=1009)
            finally:
                await self.disconnect(conn.id)
            raise ValueError(f"message exceeds max_message_bytes ({limit})")
        return data

    async def receive_json(
        self,
        conn: Connection,
        *,
        max_bytes: int | None = ...,  # type: ignore[assignment]
    ) -> Any:
        """Receive one text frame (size-checked) and parse it as JSON."""
        return json.loads(await self.receive_text(conn, max_bytes=max_bytes))

    def room_size(self, room: str) -> int:
        return len(self.rooms.get(room, set()))

    @property
    def total_connections(self) -> int:
        return len(self.connections)

    def list_rooms(self) -> list[str]:
        return sorted(self.rooms)

    async def close_all(self, code: int = 1000) -> None:
        """Close every open WebSocket. Useful at shutdown."""
        async with self._lock:
            snapshot = list(self.connections.items())
        for cid, conn in snapshot:
            try:
                await conn.websocket.close(code=code)
            except Exception:
                pass
            await self.disconnect(cid)

    async def _snapshot_targets(self, room: str | None, exclude: set[str]) -> list[Connection]:
        async with self._lock:
            return self._targets(room, exclude)

    def _targets(self, room: str | None, exclude: set[str]) -> list[Connection]:
        if room is None:
            ids = list(self.connections)
        else:
            ids = list(self.rooms.get(room, set()))
        return [
            self.connections[cid] for cid in ids if cid not in exclude and cid in self.connections
        ]

    async def _dispatch(self, targets: list[Connection], fn: Any) -> int:
        """Run ``fn(connection)`` concurrently; drop connections that raise."""
        if not targets:
            return 0
        sent = 0
        results = await asyncio.gather(
            *(self._safe(t, fn) for t in targets), return_exceptions=True
        )
        for ok in results:
            if ok is True:
                sent += 1
        return sent

    async def _safe(self, conn: Connection, fn: Any) -> bool:
        try:
            await asyncio.wait_for(fn(conn), timeout=self.send_timeout_seconds)
            return True
        except Exception as exc:
            logger.warning("dispatch to %s failed: %s", conn.id, exc)
            await self.disconnect(conn.id)
            return False


__all__ = ["Connection", "ConnectionManager", "OnConnect", "RoomValidator", "WebSocketLike"]
