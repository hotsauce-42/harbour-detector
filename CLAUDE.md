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

# Build the nearest-city gazetteer (once; 421 MB download, ~50 s, 1 GB RSS)
python3 scripts/prepare_gazetteer.py --countries DK SE DE NO PL

# Streamlit GUI
streamlit run app.py

# Vendor Leaflet into static/vendor/ for offline use (needs internet once)
python3 scripts/vendor_map_assets.py

# Docker — two images: Spark (Phase 1) and enrichment (Phases 2-5)
docker build -f Dockerfile.spark  -t myregistry.io/harbour-detector-spark:1.0.0  .
docker build -f Dockerfile.enrich -t myregistry.io/harbour-detector-enrich:1.0.0 .
docker push myregistry.io/harbour-detector-spark:1.0.0
docker push myregistry.io/harbour-detector-enrich:1.0.0
```

## Architecture

Five-phase pipeline: stop extraction (Phase 1, **Spark** — `applyInPandas` per MMSI) → H3 aggregation (Phase 2, pandas) → cluster formation (Phase 3, BFS with configurable `cluster_ring_size` to bridge gaps) → enrichment (Phase 4, shapely + gazetteer) → ID matching/export (Phase 5, deterministic `CC-hex8` IDs e.g. `DE-b8d7e3a2`).

Phase 4's `nearest_city` comes from `utils/gazetteer.py`, not `reverse_geocoder`: a k-d tree in ECEF coordinates over a GeoNames dump prepared by `scripts/prepare_gazetteer.py` (`phase4.gazetteer_path`), with a population floor that scales with `n_cells`. `country_iso2` and `admin1` come from the same tree, with `reverse_geocoder` kept only as a fallback — see the gotcha below.

Phase 5 writes three files: `harbours.geojson` (closed harbour outline), `harbours_cells.geojson` (exact H3-cell union), `harbours.parquet` (both, as `outline_wkt` / `geometry_wkt`). The outline is a morphological closing — `utils/geo.outline_polygon()`.

Manual outline edits: the GUI stores what an operator drew in `manual_outline_wkt` and Phase 4's shape in `detected_outline_wkt`; `outline_wkt` is the union of the two (`utils/geo.merge_outlines()`, applied in `id_matching._apply_manual_outlines`). The drawn outline is a **floor** — a re-run can grow a harbour but never shrink it inside the drawn shape, so an inward edit is undone by the next run. `manual_outline_wkt` is never rewritten by the pipeline; it stays the frozen baseline. Outline edits are geometry only — cells, counts, centroid and the Phase 5 matching all stay derived from detected data.

All config lives in `config/settings.yaml`, baked into both Docker images. Any key is overridable at runtime via `SECTION__KEY` env vars — no rebuild needed.

Two images, mirroring the Spark / non-Spark split:
- `Dockerfile.spark` → `harbour-detector-spark`: JVM + PySpark + S3A JARs, installs `requirements-spark.txt`. Default entrypoint `run_phase1.py`; carries the full code, so `run_pipeline.py` (all 5 phases, `deploy/job.yaml`) works too.
- `Dockerfile.enrich` → `harbour-detector-enrich`: no JVM, no PySpark, installs `requirements-base.txt`. Default entrypoint `run_enrich.py`; `run.py phaseN` works for phases 2–5 only.

Requirements split: `requirements-base.txt` (shared runtime, **exact pins** so both images agree on the libraries they exchange Parquet through) ← `requirements-spark.txt` (+ pyspark, Phase 1 only) ← `requirements.txt` (+ Streamlit GUI + pytest/ruff/mypy, local dev and CI). Bumping a shared pin means rebuilding both images from the same commit.

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
- `utils/s3.py` — credential resolution, s3fs filesystem factory, path helpers
- `utils/spark.py` — SparkSession factory with S3A / MinIO config; auto-detects local vs K8s mode

Phase 1 split: `pipeline/extract_stops.py` holds the per-vessel pandas logic (run as Spark UDF) and nothing else — no reading, no writing, so it imports no pyspark; `pipeline/extract_stops_spark.py` holds the Spark orchestration and is the only way to run Phase 1.

Only Phase 1 uses Spark; Phases 2–5 are plain pandas/pyarrow/shapely. Phases communicate only through S3 (`interim_dir`/`output_dir`), so they can run in separate pods — `run_phase1.py` (Spark) then `run_enrich.py` (plain pod), ordered by an external orchestrator.

## Gotchas

- `pathlib.Path` collapses `s3://bucket` → `s3:/bucket`. Never use `Path()` for S3 paths. Use `utils.s3.path_join()` for all path joins that may touch S3 URIs.

- Config resolution order (highest wins): env vars → `.env` file → `config/settings.yaml`. Any YAML key is overridable via `SECTION__KEY` env vars (e.g. `PHASE3__CLUSTER_RING_SIZE=5`, `S3__ENDPOINT_URL=http://minio:9000`). Legacy flat vars `RAW_GLOB`, `INTERIM_DIR`, `OUTPUT_DIR`, `EXISTING_DB` also still work. S3 credentials use the standard AWS env vars (`AWS_ACCESS_KEY_ID` etc.).

- No geopandas, no GDAL. Geometry is shapely only and GeoJSON is written with `json.dumps` + `shapely.geometry.mapping`, so neither image installs `libgdal-dev` and the enrichment image installs no OS packages at all. Don't reach for `gpd.read_file`/`to_file` — adding geopandas back drags in fiona and the GDAL system library.

- `pytest` cache is redirected to `/tmp` (`pytest.ini`) so test runs leave no state in the working tree.

- `reverse_geocoder` downloads its GeoNames dataset on first import. Both Dockerfiles pre-warm it during the build so the containers need no outbound internet at runtime.

- MinIO requires `endpoint_url` without a trailing slash. For the Spark path (Phase 1), MinIO also needs `spark.hadoop.fs.s3a.path.style.access=true` — set in `deploy/spark_job.yaml` `sparkConf` and by `_apply_s3a_conf()` in `utils/spark.py` for local mode.

- Raw AIS timestamps are stored as **integer seconds** (Unix epoch). Always use `pd.to_datetime(col, unit='s', utc=True)` when converting — omitting `unit='s'` silently produces wrong dates.

- pandas 3 parses datetimes at **microsecond** resolution (pandas 2 used nanoseconds). When *writing* epoch seconds use `(ts - EPOCH) // pd.Timedelta(1, "s")` — `.astype("int64") // 1e9` silently yields 1970 dates. (The read side is the `unit='s'` gotcha above.)

- To exercise Phase 1 without starting a JVM, call `_group_into_segments` / `_label_detection_method` / `_join_type5_data` directly — the same functions the Spark UDF runs, and much faster in tests. The end-to-end path (read → filter → UDF → write) needs a real session: `tests/test_phase1.py` has session-scoped `spark` / `spark_stops` fixtures that run the Spark job once over a fixture file (~35 s, `local[1]`).

- `ruff check .` is clean — keep it that way; a single new E501 now stands out instead of hiding in a backlog.

- `country_iso2` comes from the gazetteer's **nearest place at floor 0** — not from the population-floored city, since a floor can select a town tens of km away that is across a border while the village beside the quay is not. `reverse_geocoder` remains the fallback when no place is within `max_city_dist_km`, which is what keeps a regionally-built gazetteer safe; Phase 4 logs a warning naming the count. This was forbidden until 2026-09-05 because `make_harbour_id()` is `{country_iso2}-{uuid5(centroid_h3_r8)[:8]}` — the country is *inside* the ID, so a country change re-IDs any harbour Phase 5 does not match against the existing DB. It was measured before switching: 1 country changed of 328 (a Koster-archipelago harbour rg placed in Norway from 14.8 km away) and **0 IDs**, because 255 of 328 matched the existing DB and no *unmatched* harbour changed country. That zero is a property of that dataset, not a guarantee — re-measure before assuming it elsewhere.

- `reverse_geocoder` is wrong in two ways that `utils/gazetteer.py` exists to fix. Its k-d tree is built on raw (lat, lon) **degrees**, so it minimises Euclidean distance in degree space — at 56°N a degree of longitude is 0.56× a degree of latitude, so places east/west are penalised ~1.8× and the "nearest" city often is not (38 of 331 harbours on a Danish run). It also bundles cities1000 (population > 1000), so no village is in it at all (52 of 331 harbours were >10 km from their city, worst 51 km). The library ships a `geodetic_in_ecef()` that would fix the first — but nothing calls it, and it passes degrees where radians belong.

- GeoNames feature class `P` is not all settlements, and Phase 4's city depends on filtering it in two stages (`utils/gazetteer.py`). Stage one, `is_settlement`, is row-at-a-time and runs in both `prepare_gazetteer.py` and `drop_non_settlements` on load, so a file built before the rule existed is corrected without a 421 MB re-download: it drops `PPLQ`/`PPLW`/`PPLH`/`PPLCH` (gone) and any `PPLX` with no population — a district record positioned at the district, which for a waterfront district beats its own town centre on distance and names the harbour "Altstadt" or "Holmen" instead of Heiligenhafen or Copenhagen. Stage two, `drop_overshadowed_districts`, cannot be done row-at-a-time so it is load-only: a surviving district is kept only if no more populous place lies within `DISTRICT_ISOLATION_KM` (5 km). That is the stand-in for the parent-city link the dump does not carry — Warnemünde's nearest bigger place is Rostock at 12 km so it stays, Christiania's is Copenhagen at 2 km so it goes. On a DK/DE/PL extract it keeps 18 districts of 159, all absorbed towns: Travemünde, Warnemünde, Vegesack, Harburg, Bergedorf, Dąbie.

- Phase 3 decides connectivity on **res-9 parent cells**, so two res-11 cells hundreds of metres apart land in one cluster whenever their parents touch — nothing else reconsiders fine-cell membership. `_prune_detached_cells` is the counterweight: it re-runs `_build_adjacency`/`_connected_components` on the fine cells and drops a stranded group only when it is *both* ≤`max_detached_cells` and under `max_detached_event_share` of the cluster's events. Both halves are load-bearing — 233 of 331 harbours are fragmented at single-ring adjacency and a detached group can hold half the traffic, so size alone deletes a third of every cell and traffic share alone eats outlying terminals. It must stay between `_parent_components` and `_cluster_stats`: everything derived (counts, the event-weighted centroid, `centroid_h3_r8`, bbox) is computed after it, and `_filter_clusters` re-applies the cluster minimums to the pruned numbers. On real data it removes 45 of 2413 cells across 29 of 331 harbours and shifts exactly one `centroid_h3_r8`.

- `phase4.outline_simplify_meters` must stay `0`: it is the only step that can pull the outline inside a trafficked cell, and a 10 m tolerance bites up to ~50 m — a whole res-11 cell.

- Streamlit `AppTest`: `st.segmented_control` is reached via `at.button_group`, and `set_value()` needs a **scalar** — a list is silently ignored, so the test passes while the widget never changed.

- `AppTest` ignores a button's `disabled=True` and runs the click handler anyway (`Button` in the testing API exposes no `.disabled` to assert on either). Never let `disabled=` be the only thing standing between a stray click and a destructive write — guard inside the handler too. This is how the "Save outline" button was found silently clearing a stored outline when nothing was drawn.

- After `unary_union`, do **not** assert `result.covers(input)`: GEOS shifts boundary coordinates by a few ULPs, and at harbour scale (~1e-6 deg²) that is enough to make the predicate False over a zero-area sliver. Assert `input.difference(result).area` is ~0 instead — that is the invariant `merge_outlines` actually guarantees.

- streamlit-folium's `last_active_drawing` is **not** how you detect a click on a `folium.GeoJson` layer — its `onLayerClick` only fills it when the event carries `.layer`, which a plain Leaflet path click does not. What a layer click does set is `last_object_clicked` (a lat/lng), and it fires for *any* layer including the basemap tiles — so a click means nothing until the point is tested against the geometries. That is `app.harbour_at_click()`, which is how the city view maps a click to a harbour.

- The harbour table keeps reporting its selected row on every rerun, so `st.dataframe`'s selection would silently overrule a map click (or any other selection change) on the rerun that follows it. `main()` compares the reported rows against `st.session_state["table_rows"]` and lets a row win only when it is *newly* clicked; `selected_harbour_id` is the actual source of truth.

- Leaflet.Draw's edit toolbar only touches `L.Polygon` layers in the `FeatureGroup` handed to `Draw(feature_group=…)`. `folium.GeoJson` renders an `L.GeoJSON` group, whose contents the toolbar ignores — so the editable outline is rebuilt as `folium.Polygon` per part (`app._editable_polygons`). streamlit-folium then renames that group to `window.drawnItems` (a regex in its `_get_map_string`) and reports it back as `all_drawings`; if that rename ever stops matching, editing silently returns nothing.

- folium's rendered page pulls Leaflet, Leaflet.Draw and four unused libraries (jquery, bootstrap, glyphicons, fontawesome/awesome-markers) from four public CDNs, so a private tile server alone does **not** make the GUI offline-capable — without Leaflet the map is a blank box. `utils/map_assets.use_local_assets()` repoints folium at `static/vendor/` and drops the four unused ones; `gui.local_map_assets` turns it on. The URLs are read out of `folium.Map`/`Draw.default_js`/`default_css`, never hardcoded, so a folium bump is picked up by re-running `scripts/vendor_map_assets.py`.

- `GUI__MAP_TILES` cannot work, despite the "any key is overridable" rule: `_coerce` (`utils/config.py`) splits a list env var on commas and coerces each item against `existing[0]`, which for `map_tiles` is a dict — so you get strings and `t["name"]` raises. Edit/mount the YAML instead. (The GUI also loads config with plain `yaml.safe_load`, not `utils.config`, so `GUI__*` overrides don't reach it at all — `local_map_assets` reads its env var by hand.)

- pandas 3 stores a column of strings as `str` dtype, so a `None` you assign comes back as **NaN**, not None. Assert with `pd.isna(...)`, and treat "is it a string?" as the reliable null test for WKT columns (`utils.overrides.manual_outline`).

- pyspark 4.0.0 bundles Hadoop **3.4.1** (`pyspark/jars/hadoop-client-*-3.4.1.jar`), but `Dockerfile.spark` downloads `hadoop-aws-3.3.4.jar` and comments that 3.3.4 is bundled. Verify this before trusting the Spark S3A path.

- `spark.driver.memory` **cannot** be set through `SparkSession.builder.config()` in local mode. PySpark has already launched the driver JVM by then, from `PYSPARK_SUBMIT_ARGS` (`pyspark/java_gateway.py:72`); the property is accepted and silently ignored, leaving the 1 GB default — which in local mode is the executor heap too. `utils.spark.set_driver_memory()` sets the env var instead, and leaves an existing one alone. The other knobs (`local_threads`, shuffle partitions, Arrow batch size, split size) are ordinary `builder.config()` calls in `_apply_local_conf`, applied only outside Kubernetes.

- `msg_type == 5` is not a rare message in a DMA-converted file — it is **98.6%** of the rows. `convert_aisdk_csv.py` synthesises the column and marks any row carrying voyage data as type 5, and the DMA export puts voyage data on nearly every record. So Phase 1's union feeds 62.7M rows (19.4M positional + 43.3M type-5) into `groupBy(mmsi).applyInPandas` to extract ~12.5k distinct `(mmsi, draught, destination, ship_type)` tuples — 3454× redundancy, and the reason Phase 1 needs a real machine. `phase1.use_type5_data: false` drops that leg for local runs; `ship_type` is then rebuilt by `_ship_type_by_mmsi`, a per-MMSI aggregate that stays in the JVM, and only draught and destination are lost. Its `asc(ship_type)` tie-break deliberately matches pandas `.mode().iloc[0]` so both paths agree.

- No `unzip` in the base shell — use Python's `zipfile` for archives.

- Hadoop S3A JARs (`hadoop-aws-3.3.4.jar`, `aws-java-sdk-bundle-1.12.262.jar`) are downloaded into PySpark's `jars/` directory at Docker build time. If you upgrade PySpark, verify the bundled Hadoop version matches (`python3 -c "import pyspark; print(pyspark.__version__)"` then check `pyspark/jars/hadoop-client-runtime-*.jar`).

- The base shell has no `python` on PATH — use `python3`. `ruff` and `pytest` are installed only inside `~/harbour-venv`, not the base shell; `source ~/harbour-venv/bin/activate` first (or invoke their full venv paths).

- Hadoop S3A does not support `**` glob patterns. `extract_stops_spark.py` handles this via `_base_dir(glob)` (strips glob chars from the path) and `recursiveFileLookup=true` on the Spark reader. Do not pass a `**` glob directly to `spark.read`.
