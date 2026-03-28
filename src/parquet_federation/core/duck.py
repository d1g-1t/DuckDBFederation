from __future__ import annotations

from contextlib import contextmanager
from threading import Lock
from typing import Generator

import duckdb
import structlog

from parquet_federation.config import Settings
from parquet_federation.core.extensions import apply_performance_settings, load_extensions
from parquet_federation.core.secrets import attach_postgresql, setup_s3_secret

log = structlog.get_logger()


class DuckDBPool:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pool: list[duckdb.DuckDBPyConnection] = []
        self._lock = Lock()
        for _ in range(settings.DUCKDB_POOL_SIZE):
            self._pool.append(self._create_connection())
        log.info("duckdb_pool_ready", size=settings.DUCKDB_POOL_SIZE)

    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(":memory:")
        load_extensions(conn)
        apply_performance_settings(conn, self._settings)
        setup_s3_secret(conn, self._settings)
        try:
            attach_postgresql(conn, self._settings)
        except Exception:
            log.warning("pg_attach_skipped")
        return conn

    @contextmanager
    def acquire(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        conn: duckdb.DuckDBPyConnection | None = None
        with self._lock:
            if self._pool:
                conn = self._pool.pop()
        if conn is None:
            conn = self._create_connection()
        try:
            yield conn
        finally:
            with self._lock:
                self._pool.append(conn)

    def close_all(self) -> None:
        with self._lock:
            for conn in self._pool:
                try:
                    conn.close()
                except Exception:
                    pass
            self._pool.clear()
        log.info("duckdb_pool_closed")
