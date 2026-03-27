from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from parquet_federation.schemas.query import OutputFormat


class QueryMeta(BaseModel):
    elapsed_ms: float
    row_count: int
    output_format: OutputFormat
    source_count: int = 0


class QueryResult(BaseModel):
    data: Any = None
    meta: QueryMeta


class HealthStatus(BaseModel):
    status: str
    sources: dict[str, str]


class UseCase(BaseModel):
    title: str
    description: str


class UseCaseResponse(BaseModel):
    use_cases: list[UseCase]
