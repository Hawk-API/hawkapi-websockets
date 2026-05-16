"""Heartbeat / keepalive helpers."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

from ._manager import Connection, ConnectionManager


@dataclass(slots=True)
class HeartbeatConfig:
    interval_seconds: float = 30.0
    timeout_seconds: float = 90.0
    message: str = "ping"


@dataclass
class HeartbeatMonitor:
    manager: ConnectionManager
    config: HeartbeatConfig = field(default_factory=HeartbeatConfig)
    _last_seen: dict[str, float] = field(default_factory=dict, init=False)
    _task: asyncio.Task[None] | None = field(default=None, init=False)
    _stop: asyncio.Event = field(default_factory=asyncio.Event, init=False)
    _hooked: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        self._ensure_hook()

    def _ensure_hook(self) -> None:
        if self._hooked:
            return
        hooks = getattr(self.manager, "disconnect_hooks", None)
        if hooks is None:  # pragma: no cover - defensive
            return
        if self.disconnect_hook not in hooks:
            hooks.append(self.disconnect_hook)
        self._hooked = True

    def disconnect_hook(self, connection_id: str) -> None:
        """Prune ``_last_seen`` when a connection is removed from the manager."""
        self._last_seen.pop(connection_id, None)

    def touch(self, connection_id: str) -> None:
        self._last_seen[connection_id] = time.monotonic()

    def is_alive(self, connection_id: str) -> bool:
        last = self._last_seen.get(connection_id)
        if last is None:
            return False
        return (time.monotonic() - last) <= self.config.timeout_seconds

    async def tick(self) -> int:
        """Send one heartbeat round; close dead connections. Returns number of pings sent."""
        self._ensure_hook()
        # Prune stale entries for connections that disappeared without going
        # through the manager's disconnect path (e.g. test helpers).
        active = set(self.manager.connections)
        for cid in list(self._last_seen):
            if cid not in active:
                self._last_seen.pop(cid, None)
        sent = 0
        now = time.monotonic()
        for cid in list(self.manager.connections):
            last = self._last_seen.get(cid, now)
            if (now - last) > self.config.timeout_seconds:
                await self.manager.disconnect(cid)
                continue
            ok = await self.manager.send_to(cid, self.config.message)
            if ok:
                sent += 1
        return sent

    async def run(self) -> None:
        self._stop.clear()
        while not self._stop.is_set():
            try:
                await self.tick()
            except Exception:
                pass
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.config.interval_seconds)
            except TimeoutError:
                pass

    def start(self) -> asyncio.Task[None]:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run())
        return self._task

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except (TimeoutError, asyncio.CancelledError):
                self._task.cancel()
        self._task = None


__all__ = ["Connection", "HeartbeatConfig", "HeartbeatMonitor"]
