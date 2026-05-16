"""hawkapi-websockets — WebSocket utilities for HawkAPI.

Connection manager with rooms, per-connection metadata, broadcasting,
optional Redis pub/sub backplane for multi-process fan-out, and a
heartbeat monitor for liveness.
"""

from __future__ import annotations

from ._heartbeat import HeartbeatConfig, HeartbeatMonitor
from ._manager import Connection, ConnectionManager, WebSocketLike
from ._plugin import get_manager, init_websockets, resolve_manager
from ._pubsub import RedisBackplane, bind_manager

__version__ = "0.2.1"

__all__ = [
    "Connection",
    "ConnectionManager",
    "HeartbeatConfig",
    "HeartbeatMonitor",
    "RedisBackplane",
    "WebSocketLike",
    "__version__",
    "bind_manager",
    "get_manager",
    "init_websockets",
    "resolve_manager",
]
