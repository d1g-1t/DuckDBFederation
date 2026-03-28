from __future__ import annotations

import duckdb
import structlog

from parquet_federation.config import Settings

log = structlog.get_logger()


def load_extensions(conn: duckdb.DuckDBPyConnection) -> None:
    for ext in ("httpfs", "postgres_scanner"):
        try:
            conn.install_extension(ext)
            conn.load_extension(ext)
        except Exception:
            log.warning("extension_load_failed", ext=ext)


def apply_performance_settings(conn: duckdb.DuckDBPyConnection, settings: Settings) -> None:
    conn.execute(f"SET threads={settings.DUCKDB_THREADS};")
    conn.execute(f"SET memory_limit='{settings.DUCKDB_MEMORY_LIMIT}';")
    conn.execute(f"SET temp_directory='{settings.DUCKDB_SPILL_DIR}';")
    conn.execute("SET http_retries=3;")
    conn.execute("SET http_retry_wait_ms=100;")
