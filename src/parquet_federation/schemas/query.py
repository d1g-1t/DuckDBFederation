from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, Field, HttpUrl, model_validator


class SourceType(StrEnum):
    S3_PARQUET = "s3_parquet"
    S3_CSV = "s3_csv"
    POSTGRES = "postgres"
    HTTP_CSV = "http_csv"
    HTTP_PARQUET = "http_parquet"
    LOCAL_PARQUET = "local_parquet"
    LOCAL_CSV = "local_csv"


class OutputFormat(StrEnum):
    JSON = "json"
    PARQUET = "parquet"
    CSV = "csv"


class SourceConfig(BaseModel):
    alias: str = Field(..., pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$")
    source_type: SourceType

    s3_bucket: str | None = None
    s3_prefix: str | None = None
    hive_partitioning: bool = True

    url: HttpUrl | None = None

    pg_schema: str | None = "public"
    pg_table: str | None = None
    pg_query: str | None = None

    local_path: str | None = None


class FederationQuery(BaseModel):
    sql: str = Field(..., min_length=10, max_length=50_000)
    sources: list[SourceConfig] = Field(..., min_length=1, max_length=20)
    output_format: OutputFormat = OutputFormat.JSON
    timeout_seconds: Annotated[int, Field(ge=1, le=300)] = 30

    @model_validator(mode="after")
    def validate_no_write_statements(self) -> FederationQuery:
        forbidden = ("DROP", "DELETE", "TRUNCATE", "INSERT", "UPDATE", "CREATE", "ALTER")
        upper_sql = self.sql.upper()
        for kw in forbidden:
            if kw in upper_sql:
                raise ValueError(f"Write statement '{kw}' is not allowed in federation queries")
        return self
