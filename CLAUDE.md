## Commands

```bash
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

# Streamlit GUI
streamlit run app.py

# Docker
docker build -t myregistry.io/harbour-detector:1.0.0 .
docker push myregistry.io/harbour-detector:1.0.0
```

## Architecture

Five-phase pipeline: stop extraction (Phase 1, DuckDB) → H3 aggregation (Phase 2, pandas) → cluster formation (Phase 3, BFS) → enrichment (Phase 4, geopandas + reverse_geocoder) → ID matching/export (Phase 5, UUID5).

All config lives in `config/settings.yaml`, baked into the Docker image and overridable via env vars at runtime (`RAW_GLOB`, `INTERIM_DIR`, `OUTPUT_DIR`, `EXISTING_DB`).

Entry points:
- `run.py` — local CLI (run phases individually)
- `run_pipeline.py` — Docker / Kubernetes entrypoint (runs all five phases sequentially)
- `deploy/job.yaml` — Kubernetes Job manifest (namespace: `ais`)
- `deploy/secret.yaml` — S3 credentials Secret template

S3 utilities centralised in `utils/s3.py`: credential resolution, DuckDB httpfs setup, s3fs filesystem factory, path helpers.

## Gotchas

- `pathlib.Path` collapses `s3://bucket` → `s3:/bucket`. Never use `Path()` for S3 paths. Use `utils.s3.path_join()` for all path joins that may touch S3 URIs.

- Credential resolution order (highest wins): `config/settings.yaml` `[s3]` section → env vars (`AWS_ACCESS_KEY_ID` etc.) → `.env` file (loaded with `override=False`, never overwrites env).

- `libgdal-dev` must be installed at the OS level (`apt`) for geopandas/fiona — included in the Dockerfile, required on dev hosts too.

- `pytest` cache is redirected to `/tmp` (`pytest.ini`) so test runs leave no state in the working tree.

- `reverse_geocoder` downloads its GeoNames dataset on first import. The Dockerfile pre-warms it during the build so the container needs no outbound internet at runtime.

- MinIO requires `endpoint_url` without a trailing slash and `s3_url_style='path'`. `configure_duckdb_s3()` in `utils/s3.py` handles this automatically.
