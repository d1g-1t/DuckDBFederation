from __future__ import annotations

from typing import Annotated

from fastapi import Depends

from parquet_federation.config import Settings, get_settings
from parquet_federation.core.duck import DuckDBPool
from parquet_federation.federation.engine import FederationEngine

_pool: DuckDBPool | None = None
_engine: FederationEngine | None = None


def init_pool(settings: Settings) -> None:
    global _pool, _engine
    _pool = DuckDBPool(settings)
    _engine = FederationEngine(_pool, settings)


def shutdown_pool() -> None:
    global _pool, _engine
    if _pool:
        _pool.close_all()
    _pool = None
    _engine = None


def get_engine() -> FederationEngine:
    if _engine is None:
        raise RuntimeError("FederationEngine not initialized")
    return _engine


EngineDep = Annotated[FederationEngine, Depends(get_engine)]
SettingsDep = Annotated[Settings, Depends(get_settings)]
