from __future__ import annotations

import duckdb
import structlog

from parquet_federation.config import Settings

log = structlog.get_logger()


def setup_s3_secret(conn: duckdb.DuckDBPyConnection, settings: Settings) -> None:
    if settings.AWS_ACCESS_KEY_ID and settings.AWS_SECRET_ACCESS_KEY:
        conn.execute(f"""
            CREATE OR REPLACE SECRET s3_main (
                TYPE s3,
                KEY_ID '{settings.AWS_ACCESS_KEY_ID}',
                SECRET '{settings.AWS_SECRET_ACCESS_KEY.get_secret_value()}',
                REGION '{settings.AWS_REGION}',
                SCOPE 's3://{settings.S3_BUCKET_NAME}'
            );
        """)
        log.info("s3_secret_configured", provider="explicit", bucket=settings.S3_BUCKET_NAME)
    elif settings.S3_BUCKET_NAME:
        conn.execute(f"""
            CREATE OR REPLACE SECRET s3_main (
                TYPE s3,
                PROVIDER CREDENTIAL_CHAIN,
                REGION '{settings.AWS_REGION}',
                SCOPE 's3://{settings.S3_BUCKET_NAME}'
            );
        """)
        log.info("s3_secret_configured", provider="credential_chain", bucket=settings.S3_BUCKET_NAME)

    for extra in settings.EXTRA_S3_SOURCES:
        conn.execute(f"""
            CREATE OR REPLACE SECRET {extra.secret_name} (
                TYPE s3,
                KEY_ID '{extra.key_id}',
                SECRET '{extra.secret.get_secret_value()}',
                REGION '{extra.region}',
                SCOPE '{extra.scope}'
            );
        """)
        log.info("extra_s3_secret_configured", name=extra.secret_name)


def attach_postgresql(conn: duckdb.DuckDBPyConnection, settings: Settings) -> None:
    if not settings.FEDERATION_PG_DSN:
        log.warning("pg_dsn_not_set")
        return
    dsn = settings.FEDERATION_PG_DSN.get_secret_value()
    conn.execute(f"ATTACH '{dsn}' AS pg_source (TYPE postgres, READ_ONLY);")
    log.info("pg_attached", dsn_preview=dsn[:30] + "...")
