from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from parquet_federation.federation.query_builder import SafeQueryBuilder
from parquet_federation.schemas.query import SourceConfig, SourceType


class TestSafeQueryBuilder:
    def setup_method(self) -> None:
        self.builder = SafeQueryBuilder()

    def test_local_parquet_view(self, sample_parquet: Path) -> None:
        src = SourceConfig(
            alias="events",
            source_type=SourceType.LOCAL_PARQUET,
            local_path=str(sample_parquet),
        )
        stmts = self.builder.build_setup_statements([src])
        assert len(stmts) == 1
        assert "CREATE OR REPLACE VIEW events" in stmts[0]
        assert str(sample_parquet) in stmts[0]

    def test_http_csv_view(self) -> None:
        src = SourceConfig(
            alias="geo",
            source_type=SourceType.HTTP_CSV,
            url="https://example.com/data.csv",
        )
        stmts = self.builder.build_setup_statements([src])
        assert "read_csv" in stmts[0]
        assert "example.com" in stmts[0]

    def test_postgres_table_view(self) -> None:
        src = SourceConfig(
            alias="users",
            source_type=SourceType.POSTGRES,
            pg_schema="public",
            pg_table="users",
        )
        stmts = self.builder.build_setup_statements([src])
        assert "pg_source.public.users" in stmts[0]

    def test_postgres_query_view(self) -> None:
        src = SourceConfig(
            alias="active_users",
            source_type=SourceType.POSTGRES,
            pg_query="SELECT id, name FROM users WHERE is_active = true",
        )
        stmts = self.builder.build_setup_statements([src])
        assert "postgres_query" in stmts[0]

    def test_enforce_row_limit_adds_limit(self) -> None:
        sql = "SELECT * FROM events"
        result = self.builder.enforce_row_limit(sql, 1000)
        assert "LIMIT 1000" in result

    def test_enforce_row_limit_preserves_existing(self) -> None:
        sql = "SELECT * FROM events LIMIT 50"
        result = self.builder.enforce_row_limit(sql, 1000)
        assert result == sql

    def test_alias_injection_rejected(self) -> None:
        with pytest.raises(Exception):
            SourceConfig(
                alias="users; DROP TABLE --",
                source_type=SourceType.LOCAL_PARQUET,
                local_path="/tmp/x.parquet",
            )

    def test_multiple_sources_build(self, sample_parquet: Path) -> None:
        sources = [
            SourceConfig(alias="events", source_type=SourceType.LOCAL_PARQUET, local_path=str(sample_parquet)),
            SourceConfig(alias="geo", source_type=SourceType.HTTP_CSV, url="https://example.com/data.csv"),
        ]
        stmts = self.builder.build_setup_statements(sources)
        assert len(stmts) == 2
