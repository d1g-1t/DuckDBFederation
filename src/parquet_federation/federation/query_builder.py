from __future__ import annotations

from parquet_federation.exceptions import UnsupportedSourceTypeError
from parquet_federation.schemas.query import SourceConfig, SourceType


class SafeQueryBuilder:
    def build_setup_statements(self, sources: list[SourceConfig]) -> list[str]:
        stmts: list[str] = []
        for src in sources:
            view_sql = self._source_to_view(src)
            stmts.append(f"CREATE OR REPLACE VIEW {src.alias} AS {view_sql}")
        return stmts

    def _source_to_view(self, source: SourceConfig) -> str:
        match source.source_type:
            case SourceType.S3_PARQUET:
                path = f"s3://{source.s3_bucket}/{source.s3_prefix}/**/*.parquet"
                hive = str(source.hive_partitioning).lower()
                return f"SELECT * FROM read_parquet('{path}', hive_partitioning={hive})"
            case SourceType.S3_CSV:
                path = f"s3://{source.s3_bucket}/{source.s3_prefix}"
                return f"SELECT * FROM read_csv('{path}', auto_detect=true, header=true)"
            case SourceType.HTTP_CSV:
                return f"SELECT * FROM read_csv('{source.url}', auto_detect=true, header=true)"
            case SourceType.HTTP_PARQUET:
                return f"SELECT * FROM read_parquet('{source.url}')"
            case SourceType.POSTGRES:
                if source.pg_query:
                    escaped = source.pg_query.replace("'", "''")
                    return f"SELECT * FROM postgres_query('pg_source', '{escaped}')"
                return f"SELECT * FROM pg_source.{source.pg_schema}.{source.pg_table}"
            case SourceType.LOCAL_PARQUET:
                return f"SELECT * FROM read_parquet('{source.local_path}')"
            case SourceType.LOCAL_CSV:
                return f"SELECT * FROM read_csv('{source.local_path}', auto_detect=true, header=true)"
            case _:
                raise UnsupportedSourceTypeError(source.source_type)

    def enforce_row_limit(self, sql: str, max_rows: int) -> str:
        if "LIMIT" not in sql.upper():
            return f"SELECT * FROM ({sql}) AS _limited LIMIT {max_rows}"
        return sql
