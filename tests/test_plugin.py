"""init_websockets + get_manager dependency."""

from __future__ import annotations

from typing import Any

from hawkapi import Depends, HawkAPI
from hawkapi.testing import TestClient

from hawkapi_websockets import (
    ConnectionManager,
    get_manager,
    init_websockets,
    resolve_manager,
)


def test_init_attaches_manager_to_state() -> None:
    app = HawkAPI(openapi_url=None, docs_url=None, redoc_url=None, scalar_url=None)
    mgr = init_websockets(app)
    assert app.state.websockets is mgr
    assert isinstance(mgr, ConnectionManager)


def test_resolve_falls_back_to_last() -> None:
    app = HawkAPI(openapi_url=None, docs_url=None, redoc_url=None, scalar_url=None)
    init_websockets(app)
    assert resolve_manager(None) is not None


def test_get_manager_dep_returns_manager() -> None:
    app = HawkAPI(openapi_url=None, docs_url=None, redoc_url=None, scalar_url=None)
    init_websockets(app)

    @app.get("/stats")
    async def stats(m: ConnectionManager = Depends(get_manager)) -> dict[str, Any]:
        return {"connections": m.total_connections}

    client = TestClient(app)
    r = client.get("/stats")
    assert r.status_code == 200
    assert r.json() == {"connections": 0}


def test_get_manager_500_when_missing() -> None:
    app = HawkAPI(openapi_url=None, docs_url=None, redoc_url=None, scalar_url=None)

    @app.get("/x")
    async def x(m: ConnectionManager = Depends(get_manager)) -> dict[str, Any]:
        return {"ok": True}

    import hawkapi_websockets._plugin as _p

    saved = _p._LAST[0]
    _p._LAST[0] = None
    _p._ACTIVE.pop(app, None)
    try:
        r = TestClient(app).get("/x")
        assert r.status_code == 500
    finally:
        _p._LAST[0] = saved
