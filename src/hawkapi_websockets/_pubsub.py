"""Cross-process broadcasting via Redis Pub/Sub.

The :class:`RedisBackplane` subscribes to a channel and re-publishes every
inbound message into a local :class:`ConnectionManager`. Combine with
``manager.broadcast_*`` calls that go through :meth:`publish` and you get
fan-out across every replica of your service.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from ._manager import ConnectionManager


logger = logging.getLogger("hawkapi_websockets.pubsub")

Handler = Callable[[dict[str, Any]], Awaitable[None]]


@dataclass
class RedisBackplane:
    url: str = "redis://localhost:6379/0"
    channel: str = "hawkapi:ws"
    reconnect_initial_delay: float = 0.5
    reconnect_max_delay: float = 30.0
    _client: Any = field(default=None, init=False)
    _pubsub: Any = field(default=None, init=False)
    _task: asyncio.Task[None] | None = field(default=None, init=False)
    _handlers: list[Handler] = field(default_factory=list, init=False)
    _stop: asyncio.Event = field(default_factory=asyncio.Event, init=False)

    async def _connect(self) -> None:
        if self._client is not None:
            return
        try:
            import redis.asyncio as redis  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "redis not installed; pip install 'hawkapi-websockets[redis]'"
            ) from exc
        self._client = redis.from_url(self.url, decode_responses=True)

    async def publish(self, message: dict[str, Any]) -> None:
        await self._connect()
        await self._client.publish(self.channel, json.dumps(message))

    def on(self, handler: Handler) -> None:
        self._handlers.append(handler)

    async def start(self) -> None:
        self._stop.clear()
        await self._connect()
        self._pubsub = self._client.pubsub()
        await self._pubsub.subscribe(self.channel)
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._listen())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        if self._pubsub is not None:
            try:
                await self._pubsub.unsubscribe(self.channel)
                await self._pubsub.close()
            except Exception:
                pass
            self._pubsub = None
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                pass
            self._client = None

    async def _listen(self) -> None:
        delay = self.reconnect_initial_delay
        while not self._stop.is_set():
            if self._pubsub is None:  # pragma: no cover - defensive
                break
            try:
                async for msg in self._pubsub.listen():
                    if msg is None or msg.get("type") != "message":
                        continue
                    try:
                        payload: dict[str, Any] = json.loads(msg["data"])
                    except (ValueError, TypeError):
                        continue
                    for handler in self._handlers:
                        try:
                            await handler(payload)
                        except Exception as exc:
                            logger.warning("pubsub handler raised: %s", exc)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._stop.is_set():
                    break
                logger.warning("redis pubsub disconnected: %s; reconnecting in %.1fs", exc, delay)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                    # _stop was set during sleep — exit cleanly.
                    break
                except TimeoutError:
                    pass
                delay = min(delay * 2, self.reconnect_max_delay)
                try:
                    await self._pubsub.subscribe(self.channel)
                except Exception as resub_exc:
                    logger.warning("redis pubsub resubscribe failed: %s", resub_exc)
                    continue
                # Reset delay after a successful resubscribe.
                delay = self.reconnect_initial_delay
            else:
                # Generator exited cleanly without exception — done.
                break


def bind_manager(
    backplane: RedisBackplane,
    manager: ConnectionManager,
    *,
    allow_global: bool = False,
) -> None:
    """Re-emit every backplane message through ``manager.broadcast_*``.

    Each incoming message must include a ``kind`` field (``"text"`` / ``"json"``),
    optional ``room``, and the ``payload`` to dispatch.

    A message with no ``room`` (or ``room: null`` from Redis) would otherwise
    fan out to **every** connection, leaking across tenants (A01). Such messages
    are dropped and logged unless ``allow_global=True`` is set explicitly.
    """

    async def _dispatch(message: dict[str, Any]) -> None:
        kind = message.get("kind", "json")
        room = message.get("room")
        payload = message.get("payload")
        exclude = message.get("exclude", [])
        if room is None and not allow_global:
            logger.warning("dropping room-less pubsub message (allow_global=False): kind=%s", kind)
            return
        if kind == "text" and isinstance(payload, str):
            await manager.broadcast_text(payload, room=room, exclude=exclude)
        else:
            await manager.broadcast_json(payload, room=room, exclude=exclude)

    backplane.on(_dispatch)


__all__ = ["Handler", "RedisBackplane", "bind_manager"]
