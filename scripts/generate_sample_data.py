from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq


def generate_sample_parquet(path: str) -> None:
    table = pa.table({
        "user_id": [1, 2, 3, 4, 5, 1, 2, 3],
        "event_type": ["purchase", "view", "purchase", "view", "purchase", "view", "purchase", "view"],
        "amount_usd": [150.0, 0.0, 75.5, 0.0, 200.0, 0.0, 310.0, 0.0],
        "created_at": [
            "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04",
            "2024-01-05", "2024-01-06", "2024-01-07", "2024-01-08",
        ],
    })
    pq.write_table(table, path)


def generate_sample_csv(path: str) -> None:
    rows = [
        "country_code,country_name,region",
        "US,United States,North America",
        "GB,United Kingdom,Europe",
        "DE,Germany,Europe",
        "JP,Japan,Asia",
        "BR,Brazil,South America",
    ]
    with open(path, "w") as f:
        f.write("\n".join(rows) + "\n")


if __name__ == "__main__":
    generate_sample_parquet("tests/fixtures/sample.parquet")
    generate_sample_csv("tests/fixtures/sample.csv")
    print("fixtures generated")
