## Commands

```bash
# System deps (required before creating venv)
sudo apt install default-jdk-headless libgdal-dev   # Java for PySpark; GDAL for geopandas

# Virtual environment — MUST be on Linux filesystem, not /mnt/c/ (NTFS breaks symlinks)
python3 -m venv ~/harbour-venv
source ~/harbour-venv/bin/activate
pip install -r requirements.txt

# Run pipeline (all five phases in order)
python run.py phase1
python run.py phase2
python run.py phase3
python run.py phase4
python run.py phase5

# Run a single phase with overrides
python run.py phase1 --raw-glob "data/raw/2024/**/*.parquet"
python run.py phase5 --existing-db data/reference/existing_harbours.parquet

# Tests (self-contained, no AIS files needed)
pytest

# Lint (line-length 88, E/F/W rules — config in ruff.toml)
ruff check .

# Streamlit GUI
streamlit run app.py

# Docker
docker build -t myregistry.io/harbour-detector:1.0.0 .
docker push myregistry.io/harbour-detector:1.0.0
```

## Architecture

Five-phase pipeline: stop extraction (Phase 1, **Spark** — `applyInPandas` per MMSI) → H3 aggregation (Phase 2, pandas) → cluster formation (Phase 3, BFS with configurable `cluster_ring_size` to bridge gaps) → enrichment (Phase 4, geopandas + reverse_geocoder) → ID matching/export (Phase 5, deterministic `CC-hex8` IDs e.g. `DE-b8d7e3a2`).

All config lives in `config/settings.yaml`, baked into the Docker image. Any key is overridable at runtime via `SECTION__KEY` env vars — no rebuild needed.

Entry points:
- `run.py` — local CLI (run phases individually)
- `run_pipeline.py` — Docker / Kubernetes entrypoint (runs all five phases sequentially)
- `deploy/spark_job.yaml` — Spark Operator `SparkApplication` manifest (recommended for production)
- `deploy/job.yaml` — plain Kubernetes Job manifest (small datasets / testing)
- `deploy/secret.yaml` — S3 credentials Secret template

Key utilities:
- `utils/config.py` — shared config loader (YAML + env var overrides, dotenv)
- `utils/s3.py` — credential resolution, DuckDB httpfs setup, s3fs filesystem factory, path helpers
- `utils/spark.py` — SparkSession factory with S3A / MinIO config; auto-detects local vs K8s mode

Phase 1 split: `pipeline/extract_stops.py` holds the per-vessel pandas logic (reused as Spark UDF); `pipeline/extract_stops_spark.py` holds the Spark orchestration.

## Gotchas

- `pathlib.Path` collapses `s3://bucket` → `s3:/bucket`. Never use `Path()` for S3 paths. Use `utils.s3.path_join()` for all path joins that may touch S3 URIs.

- Config resolution order (highest wins): env vars → `.env` file → `config/settings.yaml`. Any YAML key is overridable via `SECTION__KEY` env vars (e.g. `PHASE3__CLUSTER_RING_SIZE=5`, `S3__ENDPOINT_URL=http://minio:9000`). Legacy flat vars `RAW_GLOB`, `INTERIM_DIR`, `OUTPUT_DIR`, `EXISTING_DB` also still work. S3 credentials use the standard AWS env vars (`AWS_ACCESS_KEY_ID` etc.).

- `libgdal-dev` must be installed at the OS level (`apt`) for geopandas/fiona — included in the Dockerfile, required on dev hosts too.

- `pytest` cache is redirected to `/tmp` (`pytest.ini`) so test runs leave no state in the working tree.

- `reverse_geocoder` downloads its GeoNames dataset on first import. The Dockerfile pre-warms it during the build so the container needs no outbound internet at runtime.

- MinIO requires `endpoint_url` without a trailing slash and `s3_url_style='path'`. `configure_duckdb_s3()` in `utils/s3.py` handles this automatically. For the Spark path (Phase 1), MinIO also needs `spark.hadoop.fs.s3a.path.style.access=true` — set in `deploy/spark_job.yaml` `sparkConf`.

- Raw AIS timestamps are stored as **integer seconds** (Unix epoch). Always use `pd.to_datetime(col, unit='s', utc=True)` when converting — omitting `unit='s'` silently produces wrong dates.

- Hadoop S3A JARs (`hadoop-aws-3.3.4.jar`, `aws-java-sdk-bundle-1.12.262.jar`) are downloaded into PySpark's `jars/` directory at Docker build time. If you upgrade PySpark, verify the bundled Hadoop version matches (`python3 -c "import pyspark; print(pyspark.__version__)"` then check `pyspark/jars/hadoop-client-runtime-*.jar`).

- Hadoop S3A does not support `**` glob patterns. `extract_stops_spark.py` handles this via `_base_dir(glob)` (strips glob chars from the path) and `recursiveFileLookup=true` on the Spark reader. Do not pass a `**` glob directly to `spark.read`.
