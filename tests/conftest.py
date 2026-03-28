from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    return Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def sample_parquet(tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp("data") / "events.parquet"
    table = pa.table({
        "user_id": [1, 2, 3, 4, 5],
        "event_type": ["purchase", "view", "purchase", "view", "purchase"],
        "amount_usd": [150.0, 0.0, 75.5, 0.0, 200.0],
        "created_at": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
    })
    pq.write_table(table, path)
    return path


@pytest.fixture(scope="session")
def sample_csv(tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp("data") / "geo.csv"
    rows = [
        "country_code,country_name,region",
        "US,United States,North America",
        "GB,United Kingdom,Europe",
    ]
    path.write_text("\n".join(rows) + "\n")
    return path


@pytest.fixture()
def test_client() -> TestClient:
    os.environ.setdefault("FEDERATION_PG_DSN", "")
    os.environ.setdefault("S3_BUCKET_NAME", "")
    from parquet_federation.main import app
    with TestClient(app) as client:
        yield client
