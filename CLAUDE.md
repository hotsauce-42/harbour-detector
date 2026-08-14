## Commands

```bash
# System deps (required before creating venv)
sudo apt install default-jdk-headless   # Java for PySpark — the only OS-level dep

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

# Convert a DMA AIS CSV dump (web.ais.dk/aisdata) into time-sliced Parquet
python3 scripts/convert_aisdk_csv.py ~/Downloads/aisdk-YYYY-MM-DD.zip --out-dir data/raw
python3 scripts/convert_aisdk_csv.py <zip> --limit-rows 2000000   # quick smoke test

# Tests (self-contained, no AIS files needed)
pytest

# Lint (line-length 88, E/F/W rules — config in ruff.toml)
ruff check .

# Streamlit GUI
streamlit run app.py

# Docker — two images: Spark (Phase 1) and enrichment (Phases 2-5)
docker build -f Dockerfile.spark  -t myregistry.io/harbour-detector-spark:1.0.0  .
docker build -f Dockerfile.enrich -t myregistry.io/harbour-detector-enrich:1.0.0 .
docker push myregistry.io/harbour-detector-spark:1.0.0
docker push myregistry.io/harbour-detector-enrich:1.0.0
```

## Architecture

Five-phase pipeline: stop extraction (Phase 1, **Spark** — `applyInPandas` per MMSI) → H3 aggregation (Phase 2, pandas) → cluster formation (Phase 3, BFS with configurable `cluster_ring_size` to bridge gaps) → enrichment (Phase 4, shapely + reverse_geocoder) → ID matching/export (Phase 5, deterministic `CC-hex8` IDs e.g. `DE-b8d7e3a2`).

Phase 5 writes three files: `harbours.geojson` (closed harbour outline), `harbours_cells.geojson` (exact H3-cell union), `harbours.parquet` (both, as `outline_wkt` / `geometry_wkt`). The outline is a morphological closing — `utils/geo.outline_polygon()`.

All config lives in `config/settings.yaml`, baked into both Docker images. Any key is overridable at runtime via `SECTION__KEY` env vars — no rebuild needed.

Two images, mirroring the Spark / non-Spark split:
- `Dockerfile.spark` → `harbour-detector-spark`: JVM + PySpark + S3A JARs, installs `requirements-spark.txt`. Default entrypoint `run_phase1.py`; carries the full code, so `run_pipeline.py` (all 5 phases, `deploy/job.yaml`) works too.
- `Dockerfile.enrich` → `harbour-detector-enrich`: no JVM, no PySpark, installs `requirements-base.txt`. Default entrypoint `run_enrich.py`; `run.py phaseN` works for phases 2–5 only.

Requirements split: `requirements-base.txt` (shared runtime) ← `requirements-spark.txt` (+ pyspark) ← `requirements.txt` (+ Streamlit GUI + dev tooling, local dev only).

Entry points:
- `run.py` — local CLI (run phases individually)
- `run_pipeline.py` — container entrypoint, all five phases sequentially (`deploy/job.yaml`)
- `run_phase1.py` — container entrypoint, Phase 1 (Spark) only (`deploy/spark_job.yaml`)
- `run_enrich.py` — container entrypoint, Phases 2–5 (no Spark) (`deploy/job_enrich.yaml`)
- `deploy/spark_job.yaml` — Spark Operator `SparkApplication` manifest (recommended for production)
- `deploy/job.yaml` — plain Kubernetes Job manifest (small datasets / testing)
- `deploy/job_enrich.yaml` — plain Job for Phases 2–5; pairs with `spark_job.yaml`, sequenced externally
- `deploy/secret.yaml` — S3 credentials Secret template

Key utilities:
- `utils/config.py` — shared config loader (YAML + env var overrides, dotenv)
- `utils/s3.py` — credential resolution, DuckDB httpfs setup, s3fs filesystem factory, path helpers
- `utils/spark.py` — SparkSession factory with S3A / MinIO config; auto-detects local vs K8s mode

Phase 1 split: `pipeline/extract_stops.py` holds the per-vessel pandas logic (reused as Spark UDF); `pipeline/extract_stops_spark.py` holds the Spark orchestration.

Only Phase 1 uses Spark; Phases 2–5 are plain pandas/shapely/DuckDB. Phases communicate only through S3 (`interim_dir`/`output_dir`), so they can run in separate pods — `run_phase1.py` (Spark) then `run_enrich.py` (plain pod), ordered by an external orchestrator.

## Gotchas

- `pathlib.Path` collapses `s3://bucket` → `s3:/bucket`. Never use `Path()` for S3 paths. Use `utils.s3.path_join()` for all path joins that may touch S3 URIs.

- Config resolution order (highest wins): env vars → `.env` file → `config/settings.yaml`. Any YAML key is overridable via `SECTION__KEY` env vars (e.g. `PHASE3__CLUSTER_RING_SIZE=5`, `S3__ENDPOINT_URL=http://minio:9000`). Legacy flat vars `RAW_GLOB`, `INTERIM_DIR`, `OUTPUT_DIR`, `EXISTING_DB` also still work. S3 credentials use the standard AWS env vars (`AWS_ACCESS_KEY_ID` etc.).

- No geopandas, no GDAL. Geometry is shapely only and GeoJSON is written with `json.dumps` + `shapely.geometry.mapping`, so neither image installs `libgdal-dev` and the enrichment image installs no OS packages at all. Don't reach for `gpd.read_file`/`to_file` — adding geopandas back drags in fiona and the GDAL system library.

- `pytest` cache is redirected to `/tmp` (`pytest.ini`) so test runs leave no state in the working tree.

- `reverse_geocoder` downloads its GeoNames dataset on first import. Both Dockerfiles pre-warm it during the build so the containers need no outbound internet at runtime.

- MinIO requires `endpoint_url` without a trailing slash and `s3_url_style='path'`. `configure_duckdb_s3()` in `utils/s3.py` handles this automatically. For the Spark path (Phase 1), MinIO also needs `spark.hadoop.fs.s3a.path.style.access=true` — set in `deploy/spark_job.yaml` `sparkConf`.

- Raw AIS timestamps are stored as **integer seconds** (Unix epoch). Always use `pd.to_datetime(col, unit='s', utc=True)` when converting — omitting `unit='s'` silently produces wrong dates.

- pandas 3 parses datetimes at **microsecond** resolution (pandas 2 used nanoseconds). When *writing* epoch seconds use `(ts - EPOCH) // pd.Timedelta(1, "s")` — `.astype("int64") // 1e9` silently yields 1970 dates. (The read side is the `unit='s'` gotcha above.)

- To exercise Phase 1 without starting a JVM, call `_group_into_segments` / `_label_detection_method` / `_join_type5_data` directly — the same functions the Spark UDF runs, and much faster in tests.

- `ruff check .` has ~65 pre-existing errors (mostly E501 in aligned `pa.table({...})` blocks). To prove you added none, compare counts across `git stash` / `git stash pop` — edits shift line numbers, so diffing the messages gives false positives.

- `phase4.outline_simplify_meters` must stay `0`: it is the only step that can pull the outline inside a trafficked cell, and a 10 m tolerance bites up to ~50 m — a whole res-11 cell.

- Streamlit `AppTest`: `st.segmented_control` is reached via `at.button_group`, and `set_value()` needs a **scalar** — a list is silently ignored, so the test passes while the widget never changed.

- pyspark 4.0.0 bundles Hadoop **3.4.1** (`pyspark/jars/hadoop-client-*-3.4.1.jar`), but `Dockerfile.spark` downloads `hadoop-aws-3.3.4.jar` and comments that 3.3.4 is bundled. Verify this before trusting the Spark S3A path.

- No `unzip` in the base shell — use Python's `zipfile` for archives.

- Hadoop S3A JARs (`hadoop-aws-3.3.4.jar`, `aws-java-sdk-bundle-1.12.262.jar`) are downloaded into PySpark's `jars/` directory at Docker build time. If you upgrade PySpark, verify the bundled Hadoop version matches (`python3 -c "import pyspark; print(pyspark.__version__)"` then check `pyspark/jars/hadoop-client-runtime-*.jar`).

- The base shell has no `python` on PATH — use `python3`. `ruff` and `pytest` are installed only inside `~/harbour-venv`, not the base shell; `source ~/harbour-venv/bin/activate` first (or invoke their full venv paths).

- Hadoop S3A does not support `**` glob patterns. `extract_stops_spark.py` handles this via `_base_dir(glob)` (strips glob chars from the path) and `recursiveFileLookup=true` on the Spark reader. Do not pass a `**` glob directly to `spark.read`.
