from __future__ import annotations

import json
from typing import AsyncGenerator

from fastapi import APIRouter
from starlette.responses import StreamingResponse, Response

from parquet_federation.api.dependencies import EngineDep
from parquet_federation.schemas.query import FederationQuery, OutputFormat
from parquet_federation.schemas.result import (
    HealthStatus,
    QueryResult,
    UseCase,
    UseCaseResponse,
)

router = APIRouter(prefix="/api/v1/federation", tags=["federation"])


@router.post("/query", response_model=QueryResult)
async def execute_query(query: FederationQuery, engine: EngineDep) -> QueryResult | Response:
    result = await engine.execute(query)
    if query.output_format == OutputFormat.PARQUET:
        return Response(
            content=result.data,
            media_type="application/octet-stream",
            headers={"Content-Disposition": "attachment; filename=result.parquet"},
        )
    if query.output_format == OutputFormat.CSV:
        return Response(
            content=result.data,
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=result.csv"},
        )
    return result


@router.post("/query/stream")
async def stream_query(query: FederationQuery, engine: EngineDep) -> StreamingResponse:
    async def generate() -> AsyncGenerator[bytes, None]:
        async for batch in engine.execute_batched(query, batch_size=1000):
            for row in batch:
                yield (json.dumps(row, default=str) + "\n").encode()

    return StreamingResponse(
        generate(),
        media_type="application/x-ndjson",
        headers={"X-Query-Source-Count": str(len(query.sources))},
    )


@router.post("/explain")
async def explain_query(query: FederationQuery, engine: EngineDep) -> dict[str, str]:
    plan = await engine.explain(query)
    return {"plan": plan}


@router.get("/health", response_model=HealthStatus)
async def health_check(engine: EngineDep) -> HealthStatus:
    statuses = engine.check_health()
    overall = "healthy" if all(v == "ok" for v in statuses.values() if v != "not_configured") else "degraded"
    return HealthStatus(status=overall, sources=statuses)


@router.get("/sources")
async def list_sources() -> dict[str, list[str]]:
    return {
        "supported_source_types": [
            "s3_parquet",
            "s3_csv",
            "postgres",
            "http_csv",
            "http_parquet",
            "local_parquet",
            "local_csv",
        ]
    }


@router.get("/use-cases", response_model=UseCaseResponse)
async def use_cases() -> UseCaseResponse:
    return UseCaseResponse(
        use_cases=[
            UseCase(
                title="FinTech: Real-time transaction enrichment",
                description="JOIN S3 transaction Parquet (billions of rows, hive-partitioned by date) with live PostgreSQL customer profiles and HTTP CSV exchange rates — in a single query, under 2 seconds",
            ),
            UseCase(
                title="Legal Tech: Court data federation",
                description="JOIN local bankruptcy case Parquet exports with live PostgreSQL CRM and HTTP CSV court fee schedules — generate reports without ETL pipelines",
            ),
            UseCase(
                title="E-commerce: Cross-vendor analytics",
                description="JOIN sales Parquet from S3 with partner HTTP CSV catalog and PostgreSQL inventory — answer 'which products sold via partners are out of stock?' instantly",
            ),
            UseCase(
                title="Data Engineering: Zero-ETL migration",
                description="During DB migration, JOIN old PostgreSQL with new S3 Parquet exports to validate data consistency without full data copy",
            ),
            UseCase(
                title="SaaS Analytics: Multi-tenant federation",
                description="Each tenant's data in separate S3 prefix. One federated query across all tenants for global aggregations. SCOPE secrets ensure tenant isolation",
            ),
        ]
    )
