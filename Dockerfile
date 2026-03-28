FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir ".[dev]"

RUN mkdir -p /tmp/duckdb_spill tests/fixtures && \
    python scripts/generate_sample_data.py && \
    python -c "import duckdb; c=duckdb.connect(); c.install_extension('httpfs'); c.install_extension('postgres_scanner'); print('extensions installed')"

EXPOSE 8000

CMD ["uvicorn", "parquet_federation.main:app", "--host", "0.0.0.0", "--port", "8000"]
