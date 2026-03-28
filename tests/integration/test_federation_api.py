from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


class TestFederationAPI:
    def test_root(self, test_client: TestClient) -> None:
        r = test_client.get("/")
        assert r.status_code == 200
        data = r.json()
        assert data["service"] == "ParquetFederation"

    def test_health(self, test_client: TestClient) -> None:
        r = test_client.get("/api/v1/federation/health")
        assert r.status_code == 200
        data = r.json()
        assert data["sources"]["duckdb"] == "ok"

    def test_sources(self, test_client: TestClient) -> None:
        r = test_client.get("/api/v1/federation/sources")
        assert r.status_code == 200
        data = r.json()
        assert "s3_parquet" in data["supported_source_types"]

    def test_use_cases(self, test_client: TestClient) -> None:
        r = test_client.get("/api/v1/federation/use-cases")
        assert r.status_code == 200
        data = r.json()
        assert len(data["use_cases"]) == 5

    def test_query_local_parquet(self, test_client: TestClient, sample_parquet: Path) -> None:
        payload = {
            "sql": "SELECT * FROM events WHERE amount_usd > 0",
            "sources": [
                {
                    "alias": "events",
                    "source_type": "local_parquet",
                    "local_path": str(sample_parquet),
                }
            ],
        }
        r = test_client.post("/api/v1/federation/query", json=payload)
        assert r.status_code == 200
        data = r.json()
        assert data["meta"]["row_count"] > 0

    def test_query_stream(self, test_client: TestClient, sample_parquet: Path) -> None:
        payload = {
            "sql": "SELECT * FROM events LIMIT 3",
            "sources": [
                {
                    "alias": "events",
                    "source_type": "local_parquet",
                    "local_path": str(sample_parquet),
                }
            ],
        }
        r = test_client.post("/api/v1/federation/query/stream", json=payload)
        assert r.status_code == 200
        lines = [line for line in r.text.strip().split("\n") if line]
        assert len(lines) == 3
        for line in lines:
            parsed = json.loads(line)
            assert "user_id" in parsed

    def test_explain(self, test_client: TestClient, sample_parquet: Path) -> None:
        payload = {
            "sql": "SELECT * FROM events WHERE amount_usd > 100",
            "sources": [
                {
                    "alias": "events",
                    "source_type": "local_parquet",
                    "local_path": str(sample_parquet),
                }
            ],
        }
        r = test_client.post("/api/v1/federation/explain", json=payload)
        assert r.status_code == 200
        assert "plan" in r.json()

    def test_write_sql_rejected(self, test_client: TestClient) -> None:
        payload = {
            "sql": "DROP TABLE users",
            "sources": [
                {
                    "alias": "users",
                    "source_type": "local_parquet",
                    "local_path": "/tmp/x.parquet",
                }
            ],
        }
        r = test_client.post("/api/v1/federation/query", json=payload)
        assert r.status_code == 422

    def test_invalid_alias_rejected(self, test_client: TestClient) -> None:
        payload = {
            "sql": "SELECT 1 FROM test_table",
            "sources": [
                {
                    "alias": "users; DROP TABLE--",
                    "source_type": "local_parquet",
                    "local_path": "/tmp/x.parquet",
                }
            ],
        }
        r = test_client.post("/api/v1/federation/query", json=payload)
        assert r.status_code == 422
