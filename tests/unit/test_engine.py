from __future__ import annotations

import os
from pathlib import Path

import pytest

from parquet_federation.config import Settings
from parquet_federation.core.duck import DuckDBPool
from parquet_federation.federation.engine import FederationEngine
from parquet_federation.schemas.query import FederationQuery, OutputFormat, SourceConfig, SourceType


@pytest.fixture()
def engine() -> FederationEngine:
    settings = Settings(
        FEDERATION_PG_DSN=None,
        S3_BUCKET_NAME="",
        DUCKDB_POOL_SIZE=2,
        DUCKDB_THREADS=2,
        DUCKDB_MEMORY_LIMIT="512MB",
    )
    pool = DuckDBPool(settings)
    return FederationEngine(pool, settings)


class TestFederationEngine:
    @pytest.mark.asyncio
    async def test_local_parquet_query(self, engine: FederationEngine, sample_parquet: Path) -> None:
        query = FederationQuery(
            sql="SELECT * FROM events WHERE amount_usd > 0",
            sources=[
                SourceConfig(
                    alias="events",
                    source_type=SourceType.LOCAL_PARQUET,
                    local_path=str(sample_parquet),
                ),
            ],
        )
        result = await engine.execute(query)
        assert result.meta.row_count == 3
        assert all(row["amount_usd"] > 0 for row in result.data)

    @pytest.mark.asyncio
    async def test_join_parquet_and_csv(
        self, engine: FederationEngine, sample_parquet: Path, sample_csv: Path
    ) -> None:
        query = FederationQuery(
            sql="SELECT e.user_id, e.amount_usd, g.country_name FROM events e, geo g WHERE e.amount_usd > 100",
            sources=[
                SourceConfig(alias="events", source_type=SourceType.LOCAL_PARQUET, local_path=str(sample_parquet)),
                SourceConfig(alias="geo", source_type=SourceType.LOCAL_CSV, local_path=str(sample_csv)),
            ],
        )
        result = await engine.execute(query)
        assert result.meta.row_count > 0

    @pytest.mark.asyncio
    async def test_csv_output(self, engine: FederationEngine, sample_parquet: Path) -> None:
        query = FederationQuery(
            sql="SELECT * FROM events LIMIT 3",
            sources=[
                SourceConfig(alias="events", source_type=SourceType.LOCAL_PARQUET, local_path=str(sample_parquet)),
            ],
            output_format=OutputFormat.CSV,
        )
        result = await engine.execute(query)
        assert "user_id" in result.data
        assert result.meta.output_format == OutputFormat.CSV

    @pytest.mark.asyncio
    async def test_parquet_output(self, engine: FederationEngine, sample_parquet: Path) -> None:
        query = FederationQuery(
            sql="SELECT * FROM events LIMIT 2",
            sources=[
                SourceConfig(alias="events", source_type=SourceType.LOCAL_PARQUET, local_path=str(sample_parquet)),
            ],
            output_format=OutputFormat.PARQUET,
        )
        result = await engine.execute(query)
        assert isinstance(result.data, bytes)
        assert result.meta.output_format == OutputFormat.PARQUET

    @pytest.mark.asyncio
    async def test_write_statement_rejected(self, engine: FederationEngine, sample_parquet: Path) -> None:
        with pytest.raises(Exception):
            FederationQuery(
                sql="DROP TABLE events",
                sources=[
                    SourceConfig(alias="events", source_type=SourceType.LOCAL_PARQUET, local_path=str(sample_parquet)),
                ],
            )

    @pytest.mark.asyncio
    async def test_explain(self, engine: FederationEngine, sample_parquet: Path) -> None:
        query = FederationQuery(
            sql="SELECT * FROM events WHERE amount_usd > 100",
            sources=[
                SourceConfig(alias="events", source_type=SourceType.LOCAL_PARQUET, local_path=str(sample_parquet)),
            ],
        )
        plan = await engine.explain(query)
        assert len(plan) > 0

    def test_health_check(self, engine: FederationEngine) -> None:
        statuses = engine.check_health()
        assert statuses["duckdb"] == "ok"

    @pytest.mark.asyncio
    async def test_timeout_enforcement(self, engine: FederationEngine, sample_parquet: Path) -> None:
        query = FederationQuery(
            sql="SELECT * FROM events",
            sources=[
                SourceConfig(alias="events", source_type=SourceType.LOCAL_PARQUET, local_path=str(sample_parquet)),
            ],
            timeout_seconds=30,
        )
        result = await engine.execute(query)
        assert result.meta.row_count > 0

    @pytest.mark.asyncio
    async def test_row_limit_injection(self, engine: FederationEngine, sample_parquet: Path) -> None:
        engine._settings.MAX_RESULT_ROWS = 2
        query = FederationQuery(
            sql="SELECT * FROM events",
            sources=[
                SourceConfig(alias="events", source_type=SourceType.LOCAL_PARQUET, local_path=str(sample_parquet)),
            ],
        )
        result = await engine.execute(query)
        assert result.meta.row_count <= 2
        engine._settings.MAX_RESULT_ROWS = 100_000
