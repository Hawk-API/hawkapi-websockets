"""Plugin entry point + DI helpers."""

from __future__ import annotations

from typing import Any

from hawkapi import HTTPException, Request

from ._manager import ConnectionManager
from ._pubsub import RedisBackplane, bind_manager


class _StateNamespace:
    websockets: Any


_ACTIVE: dict[int, ConnectionManager] = {}
_LAST: list[ConnectionManager | None] = [None]


def init_websockets(
    app: Any,
    *,
    manager: ConnectionManager | None = None,
    backplane: RedisBackplane | None = None,
    start_backplane: bool = True,
) -> ConnectionManager:
    """Attach a :class:`ConnectionManager` to ``app.state.websockets``.

    When ``backplane`` is provided, every published message is fanned out into
    ``manager`` automatically.
    """
    manager = manager or ConnectionManager()
    if backplane is not None:
        bind_manager(backplane, manager)
        if start_backplane and hasattr(app, "on_startup"):

            async def _start() -> None:
                await backplane.start()

            app.on_startup(_start)
        if hasattr(app, "on_shutdown"):

            async def _stop() -> None:
                await backplane.stop()
                await manager.close_all()

            app.on_shutdown(_stop)

    if getattr(app, "state", None) is None:
        app.state = _StateNamespace()
    app.state.websockets = manager
    _ACTIVE[id(app)] = manager
    _LAST[0] = manager
    return manager


def resolve_manager(app: Any) -> ConnectionManager | None:
    if app is None:
        return _LAST[0]
    found = _ACTIVE.get(id(app))
    if found is not None:
        return found
    state = getattr(app, "state", None)
    if state is not None and hasattr(state, "websockets"):
        return state.websockets  # type: ignore[no-any-return]
    return _LAST[0]


def get_manager(request: Request) -> ConnectionManager:
    found = resolve_manager(request.scope.get("app"))
    if found is None:
        raise HTTPException(
            500, detail="WebSockets not configured — call init_websockets(app, ...)"
        )
    return found


__all__ = ["get_manager", "init_websockets", "resolve_manager"]
