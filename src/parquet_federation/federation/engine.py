from __future__ import annotations

import asyncio
import time
from typing import Any, AsyncGenerator

import structlog

from parquet_federation.config import Settings
from parquet_federation.core.duck import DuckDBPool
from parquet_federation.exceptions import QueryTimeoutError
from parquet_federation.federation.query_builder import SafeQueryBuilder
from parquet_federation.federation.result import serialize_result
from parquet_federation.schemas.query import FederationQuery, OutputFormat, SourceConfig
from parquet_federation.schemas.result import QueryMeta, QueryResult

log = structlog.get_logger()


class FederationEngine:
    def __init__(self, pool: DuckDBPool, settings: Settings) -> None:
        self._pool = pool
        self._settings = settings
        self._builder = SafeQueryBuilder()

    async def execute(self, query: FederationQuery) -> QueryResult:
        loop = asyncio.get_event_loop()
        try:
            return await asyncio.wait_for(
                loop.run_in_executor(None, self._execute_sync, query),
                timeout=query.timeout_seconds,
            )
        except asyncio.TimeoutError:
            raise QueryTimeoutError(f"Query exceeded {query.timeout_seconds}s limit")

    def _execute_sync(self, query: FederationQuery) -> QueryResult:
        sql = self._builder.enforce_row_limit(query.sql, self._settings.MAX_RESULT_ROWS)
        setup_stmts = self._builder.build_setup_statements(query.sources)

        with self._pool.acquire() as conn:
            for stmt in setup_stmts:
                conn.execute(stmt)

            start = time.perf_counter()
            rel = conn.execute(sql)
            elapsed_ms = (time.perf_counter() - start) * 1000

            data, row_count = serialize_result(rel, query.output_format)

            log.info(
                "query_executed",
                elapsed_ms=round(elapsed_ms, 2),
                rows=row_count,
                sources=len(query.sources),
            )

            return QueryResult(
                data=data,
                meta=QueryMeta(
                    elapsed_ms=round(elapsed_ms, 2),
                    row_count=row_count,
                    output_format=query.output_format,
                    source_count=len(query.sources),
                ),
            )

    async def explain(self, query: FederationQuery) -> str:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._explain_sync, query)

    def _explain_sync(self, query: FederationQuery) -> str:
        setup_stmts = self._builder.build_setup_statements(query.sources)
        with self._pool.acquire() as conn:
            for stmt in setup_stmts:
                conn.execute(stmt)
            result = conn.execute(f"EXPLAIN {query.sql}")
            rows = result.fetchall()
            return "\n".join(str(r) for r in rows)

    async def execute_batched(
        self,
        query: FederationQuery,
        batch_size: int = 1000,
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        loop = asyncio.get_event_loop()
        sql = self._builder.enforce_row_limit(query.sql, self._settings.MAX_RESULT_ROWS)
        setup_stmts = self._builder.build_setup_statements(query.sources)

        def _fetch_all() -> list[dict[str, Any]]:
            with self._pool.acquire() as conn:
                for stmt in setup_stmts:
                    conn.execute(stmt)
                rel = conn.execute(sql)
                df = rel.fetchdf()
                return df.to_dict(orient="records")

        all_rows = await loop.run_in_executor(None, _fetch_all)
        for i in range(0, len(all_rows), batch_size):
            yield all_rows[i : i + batch_size]

    def check_health(self) -> dict[str, str]:
        statuses: dict[str, str] = {}
        with self._pool.acquire() as conn:
            try:
                conn.execute("SELECT 1")
                statuses["duckdb"] = "ok"
            except Exception as e:
                statuses["duckdb"] = str(e)

            if self._settings.FEDERATION_PG_DSN:
                try:
                    conn.execute("SELECT 1 FROM pg_source.pg_catalog.pg_class LIMIT 1")
                    statuses["postgresql"] = "ok"
                except Exception as e:
                    statuses["postgresql"] = str(e)
            else:
                statuses["postgresql"] = "not_configured"

        return statuses
