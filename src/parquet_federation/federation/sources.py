from __future__ import annotations

S3_PARQUET_HIVE = """
    read_parquet(
        's3://{bucket}/{prefix}/**/*.parquet',
        hive_partitioning = true,
        filename = true
    )
"""

S3_PARQUET_GLOB = "read_parquet('s3://{bucket}/{prefix}/*.parquet')"

HTTP_CSV = """
    read_csv(
        '{url}',
        header = true,
        auto_detect = true,
        ignore_errors = false
    )
"""

HTTP_PARQUET = "read_parquet('{url}')"

POSTGRES_TABLE = "pg_source.{schema}.{table}"

POSTGRES_QUERY = "postgres_query('pg_source', '{sql}')"
