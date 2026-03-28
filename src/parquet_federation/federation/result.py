from __future__ import annotations

import io
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from parquet_federation.schemas.query import OutputFormat


def serialize_result(
    rel: Any,
    output_format: OutputFormat,
) -> tuple[Any, int]:
    match output_format:
        case OutputFormat.JSON:
            df = rel.fetchdf()
            data = df.to_dict(orient="records")
            return data, len(df)
        case OutputFormat.PARQUET:
            arrow_table = rel.to_arrow_table()
            buf = io.BytesIO()
            pq.write_table(arrow_table, buf)
            return buf.getvalue(), arrow_table.num_rows
        case OutputFormat.CSV:
            df = rel.fetchdf()
            return df.to_csv(index=False), len(df)
        case _:
            rows = rel.fetchall()
            return rows, len(rows)
