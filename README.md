![Harbour Detector](harbour-detector-banner.png)
# Harbour Detector

Detects and maps harbours worldwide from historical AIS data. The pipeline processes decoded AIS position and voyage messages, clusters ship-stopping events spatially using H3 hexagons, enriches each cluster with country/city metadata, and outputs a GeoJSON file with stable harbour IDs.

---

## How it works

### Overview

1. **Phase 1 — Stop extraction**: Scan every AIS Parquet file and find "stop events" — periods where a vessel was stationary (low speed, or moored/at-anchor nav status). Join voyage data (draught, ship type, destination) from Type 5 messages.
2. **Phase 2 — H3 aggregation**: Project each stop event onto an H3 hexagonal grid at resolution 11 (~25 m cell edge). Count events, unique vessels, and vessel-type distribution per cell.
3. **Phase 3 — Cluster formation**: Build connected components from adjacent hot H3 cells (BFS). Each component becomes one harbour candidate.
4. **Phase 4 — Enrichment**: Generate a polygon (GeoJSON geometry) from the H3 cell set. Reverse-geocode the centroid to find country and nearest city.
5. **Phase 5 — ID matching**: Assign each harbour a deterministic UUID5 based on its coarse H3 centroid cell (resolution 8). Optionally match against an existing harbour database to preserve historical IDs.

### Stop detection logic

A vessel is considered stopped when:
- Speed over ground (**SOG**) is below the configured threshold (default 0.5 kn), **or**
- The AIS navigational status is "moored" (5) or "at anchor" (1)

Consecutive messages within `max_gap_minutes` of each other are grouped into a stop segment. Segments are discarded if they are shorter than `min_stop_duration_minutes`, contain fewer than `min_messages_per_stop` messages, or if the vessel's positions show excessive spread (`positional_variance_max_meters`).

Draught changes between arrival and departure are detected by joining Type 5 messages recorded within `draught_lookup_hours` of the stop.

### Harbour ID stability

Harbour IDs are UUID5 values derived from the H3 cell at resolution 8 that contains the harbour centroid. Because resolution-8 cells cover ~0.7 km², the same harbour will always receive the same ID across pipeline re-runs, even if the exact set of H3 cells shifts slightly.

When an existing harbour database is provided, Phase 5 tries to match each detected harbour by:
1. **H3 Jaccard overlap** — proportion of shared cells (primary, requires `h3_cells` in the existing DB)
2. **Centroid distance** — fallback if the existing DB has no cell list

If a match is found above the configured thresholds, the existing ID is reused.

---

## Installation

> **Important:** Create the virtual environment on the Linux filesystem, not on the Windows-mounted drive (`/mnt/c/...`). NTFS does not support Unix symlinks or executable bits, so `pip` and `streamlit` will not work from a venv created there.

```bash
python3 -m venv ~/harbour-venv
source ~/harbour-venv/bin/activate
pip install -r requirements.txt
```

---

## Configuration

All parameters live in `config/settings.yaml`.

### `data` — File paths

| Key | Default | Description |
|-----|---------|-------------|
| `raw_glob` | `data/raw/**/*.parquet` | Glob pattern for raw AIS Parquet files |
| `interim_dir` | `data/interim` | Intermediate per-phase outputs |
| `reference_dir` | `data/reference` | Reference databases (existing harbours, etc.) |
| `output_dir` | `data/output` | Final pipeline output |

### `columns` — Parquet column mapping

Adjust these if your Parquet files use different column names.

| Key | Default |
|-----|---------|
| `mmsi` | `mmsi` |
| `timestamp` | `timestamp` |
| `lat` | `lat` |
| `lon` | `lon` |
| `sog` | `sog` |
| `nav_status` | `nav_status` |
| `msg_type` | `msg_type` |
| `draught` | `draught` |
| `destination` | `destination` |
| `ship_type` | `ship_type` |

### `phase1` — Stop extraction

| Key | Default | Description |
|-----|---------|-------------|
| `moored_nav_statuses` | `[1, 5]` | AIS nav status codes meaning moored/at anchor |
| `sog_threshold_knots` | `0.5` | Max speed to consider a vessel stopped |
| `max_gap_minutes` | `15` | Max gap between messages still belonging to the same stop |
| `min_stop_duration_minutes` | `30` | Discard stops shorter than this |
| `min_messages_per_stop` | `3` | Discard stops with too few position messages |
| `positional_variance_max_meters` | `300` | Discard stops where the vessel was still drifting |
| `mmsi_min` / `mmsi_max` | `100000000` / `999999999` | Filter out invalid MMSI numbers |
| `draught_lookup_hours` | `6` | Window around a stop to look for Type 5 voyage messages |

### `phase2` — H3 aggregation

| Key | Default | Description |
|-----|---------|-------------|
| `h3_resolution` | `11` | H3 resolution for spatial indexing (~25 m cell edge) |
| `min_unique_mmsi` | `5` | Minimum distinct vessels for a cell to be considered a harbour cell |

### `phase3` — Cluster formation

| Key | Default | Description |
|-----|---------|-------------|
| `min_cells_per_harbour` | `1` | Minimum H3 cells for a connected component to be kept |

### `phase4` — Enrichment

| Key | Default | Description |
|-----|---------|-------------|
| `city_min_population` | `1000` | Minimum city population for reverse geocoding lookup |

### `phase5` — ID matching

| Key | Default | Description |
|-----|---------|-------------|
| `h3_jaccard_threshold` | `0.3` | Minimum H3 cell overlap ratio to match an existing harbour |
| `centroid_match_distance_meters` | `500` | Fallback: centroid distance threshold for a match |

### `s3` — S3 / MinIO storage

All three data path keys (`raw_glob`, `interim_dir`, `output_dir`) accept `s3://` URIs in addition to local paths. When any path is an S3 URI the pipeline automatically routes I/O through the `s3fs` library (reads) and DuckDB's built-in `httpfs` extension (Phase 1 raw scans).

| Key | Default | Description |
|-----|---------|-------------|
| `access_key_id` | `""` | AWS access key ID. Overrides `AWS_ACCESS_KEY_ID` env var |
| `secret_access_key` | `""` | AWS secret access key. Overrides `AWS_SECRET_ACCESS_KEY` env var |
| `region` | `""` | AWS region (defaults to `us-east-1` when blank). Overrides `AWS_DEFAULT_REGION` |
| `endpoint_url` | `""` | Custom S3-compatible endpoint, e.g. `http://localhost:9000` for MinIO. Overrides `S3_ENDPOINT_URL` |

**Credential precedence** (highest wins):

1. Values set in `config/settings.yaml` under `[s3]`
2. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `S3_ENDPOINT_URL`)
3. `.env` file in the project root (loaded automatically if present; never overwrites already-set env vars)

Leave all four YAML fields blank to rely entirely on environment variables or IAM instance roles.

### `gui` — Streamlit app

| Key | Default | Description |
|-----|---------|-------------|
| `output_file` | `data/output/harbours.geojson` | GeoJSON file the GUI reads |
| `default_tile` | `OpenStreetMap` | Which tile layer is selected on startup |
| `map_tiles` | (4 built-in layers) | List of `{name, url, attribution}` tile server definitions |

To add a custom tile server, append to `map_tiles`:

```yaml
map_tiles:
  - name: "My Server"
    url: "https://mytiles.example.com/{z}/{x}/{y}.png"
    attribution: "© My Company"
```

---

## Running the pipeline

```bash
# Activate the venv first
source ~/harbour-venv/bin/activate

# Run all phases in order
python run.py phase1
python run.py phase2
python run.py phase3
python run.py phase4
python run.py phase5

# Phase 1: override the raw file glob
python run.py phase1 --raw-glob "data/raw/2024/**/*.parquet"

# Phase 5: match against an existing harbour database
python run.py phase5 --existing-db data/reference/existing_harbours.parquet
# Also accepts .geojson
python run.py phase5 --existing-db data/reference/existing_harbours.geojson
```

Each phase reads from `data/interim/` and writes its output there. Phase 5 additionally writes the final files to `data/output/`.

---

## Using S3 (or MinIO) for storage

Any combination of local and S3 paths is valid. You can read raw Parquet from S3 while keeping intermediate files local, or route the entire pipeline through S3.

### Quick start — AWS S3

**1. Set credentials** (choose one method):

Option A — `.env` file (recommended for local development):
```bash
cp .env.example .env
# Edit .env and fill in your credentials
```

Option B — environment variables:
```bash
export AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
export AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export AWS_DEFAULT_REGION="eu-west-1"
```

Option C — `config/settings.yaml`:
```yaml
s3:
  access_key_id:     "AKIAIOSFODNN7EXAMPLE"
  secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  region:            "eu-west-1"
  endpoint_url:      ""
```

**2. Set S3 paths in `config/settings.yaml`:**
```yaml
data:
  raw_glob:    "s3://my-bucket/ais/**/*.parquet"
  interim_dir: "s3://my-bucket/harbour-detector/interim"
  output_dir:  "s3://my-bucket/harbour-detector/output"
```

**3. Run the pipeline exactly as before:**
```bash
python run.py phase1
python run.py phase2
python run.py phase3
python run.py phase4
python run.py phase5
```

### Quick start — MinIO

MinIO uses path-style URLs and requires a custom endpoint. The pipeline sets `s3_url_style=path` and disables SSL automatically when `endpoint_url` is an `http://` address.

```yaml
s3:
  access_key_id:     "minioadmin"
  secret_access_key: "minioadmin"
  region:            "us-east-1"   # MinIO ignores this but it must be non-empty for some clients
  endpoint_url:      "http://localhost:9000"

data:
  raw_glob:    "s3://my-bucket/ais/**/*.parquet"
  interim_dir: "s3://my-bucket/harbour-detector/interim"
  output_dir:  "s3://my-bucket/harbour-detector/output"
```

> MinIO with HTTPS: set `endpoint_url: "https://minio.internal:9000"` — SSL is enabled automatically when the scheme is `https://`.

### Mixing local and S3 paths

Each path is independently switchable. For example, read raw data from S3 but keep intermediate files local:

```yaml
data:
  raw_glob:    "s3://my-bucket/ais/**/*.parquet"   # read from S3
  interim_dir: "data/interim"                       # local
  output_dir:  "data/output"                        # local
```

Or write only the final output to S3:
```yaml
data:
  raw_glob:    "data/raw/**/*.parquet"              # local
  interim_dir: "data/interim"                       # local
  output_dir:  "s3://my-bucket/harbour-detector/output"   # write results to S3
```

### Existing harbour database on S3

The `--existing-db` flag also accepts `s3://` URIs:
```bash
python run.py phase5 --existing-db s3://my-bucket/reference/existing_harbours.parquet
```

### IAM instance roles (no explicit credentials)

Leave all four `[s3]` YAML fields blank and do not set the corresponding environment variables. The underlying AWS SDK will pick up credentials from the EC2/ECS instance role, EKS service account, or `~/.aws/credentials` automatically.

```yaml
s3:
  access_key_id:     ""
  secret_access_key: ""
  region:            ""
  endpoint_url:      ""
```

### Troubleshooting S3 connections

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `NoCredentialsError` | No credentials found anywhere | Set `access_key_id` / `secret_access_key` in YAML or env vars |
| `EndpointResolutionError` / connection refused | Wrong endpoint or MinIO not running | Check `endpoint_url` — must not have a trailing slash |
| `403 Forbidden` | Bucket policy or wrong credentials | Verify key/secret and that the bucket allows the operation |
| `NoSuchKey` when reading interim files | Previous phase not run yet | Run phases in order (phase1 → phase2 → … → phase5) |
| DuckDB `IO Error: Unable to connect` | httpfs extension not installed | Run `pip install duckdb --upgrade`; ensure outbound HTTPS is allowed |

---

## Running the GUI

```bash
source ~/harbour-venv/bin/activate
streamlit run app.py
```

Or with a custom config path:

```bash
streamlit run app.py -- --config config/settings.yaml
```

The app opens in your browser. Use the sidebar to switch tile layers, search by city or country, and sort the harbour list. Click any row in the table to show that harbour on the map.

### Testing the GUI without pipeline data

Generate 20 realistic dummy harbours (real H3 polygons, real-world ports):

```bash
python scripts/generate_dummy_harbours.py
```

This writes `data/output/harbours.geojson` so the GUI has something to display immediately.

---

## Building the Docker image

The `Dockerfile` in the project root packages the full pipeline into a self-contained image. The `reverse_geocoder` GeoNames dataset is pre-warmed during the build so the container never needs outbound internet access at runtime.

### Build

```bash
docker build -t myregistry.io/harbour-detector:1.0.0 .
```

### Push

```bash
docker push myregistry.io/harbour-detector:1.0.0
```

Replace `myregistry.io/harbour-detector:1.0.0` with your actual registry and tag.

### Local smoke test

Verify the image works before deploying to the cluster:

```bash
docker run --rm \
  -e RAW_GLOB="s3://my-bucket/ais/**/*.parquet" \
  -e INTERIM_DIR="s3://my-bucket/harbour-detector/interim" \
  -e OUTPUT_DIR="s3://my-bucket/harbour-detector/output" \
  -e AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE" \
  -e AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -e AWS_DEFAULT_REGION="eu-west-1" \
  myregistry.io/harbour-detector:1.0.0
```

For MinIO add `-e S3_ENDPOINT_URL="http://host.docker.internal:9000"`.

---

## Deploying as a Kubernetes Job

The manifests in `deploy/` run the full five-phase pipeline as a single Kubernetes Job in the `ais` namespace. Each phase writes its intermediate output to S3, so a failed pod can be retried without re-running earlier phases.

### 1. Create the namespace (once)

```bash
kubectl create namespace ais
```

### 2. Create the S3 credentials secret

Edit `deploy/secret.yaml` and fill in your real credentials:

```yaml
stringData:
  access-key-id:     "AKIAIOSFODNN7EXAMPLE"
  secret-access-key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  region:            "eu-west-1"
```

Then apply it:

```bash
kubectl apply -f deploy/secret.yaml
```

> **Never commit `secret.yaml` with real credentials.** Use a secrets manager (Vault, Sealed Secrets, AWS Secrets Manager) to generate it at deploy time.

For MinIO, add an `endpoint-url` key to the Secret and uncomment the `S3_ENDPOINT_URL` block in `deploy/job.yaml`.

### 3. Configure data paths

Open `deploy/job.yaml` and set the three environment variables to point at your data:

```yaml
- name: RAW_GLOB
  value: "s3://my-bucket/ais/**/*.parquet"
- name: INTERIM_DIR
  value: "s3://my-bucket/harbour-detector/interim"
- name: OUTPUT_DIR
  value: "s3://my-bucket/harbour-detector/output"
```

### 4. Submit the Job

```bash
kubectl apply -f deploy/job.yaml
```

### 5. Follow progress

```bash
kubectl logs -f job/harbour-detector -n ais
```

### 6. Clean up after completion

```bash
kubectl delete job harbour-detector -n ais
```

### Environment variable overrides

The container entry point (`run_pipeline.py`) loads `config/settings.yaml` baked into the image and applies the following environment variable overrides at startup:

| Variable | Config key | Description |
|----------|-----------|-------------|
| `RAW_GLOB` | `data.raw_glob` | Glob pattern for raw AIS Parquet files |
| `INTERIM_DIR` | `data.interim_dir` | Directory for intermediate per-phase outputs |
| `OUTPUT_DIR` | `data.output_dir` | Directory for the final GeoJSON and Parquet output |
| `EXISTING_DB` | `phase5.existing_db_path` | Optional: existing harbour DB for stable ID preservation |

S3 credentials are injected from the `harbour-detector-s3` Kubernetes Secret:

| Environment variable | Secret key |
|----------------------|------------|
| `AWS_ACCESS_KEY_ID` | `access-key-id` |
| `AWS_SECRET_ACCESS_KEY` | `secret-access-key` |
| `AWS_DEFAULT_REGION` | `region` |
| `S3_ENDPOINT_URL` _(MinIO only)_ | `endpoint-url` |

---

## Output format

`data/output/harbours.geojson` is a GeoJSON `FeatureCollection`. Each feature represents one harbour.

**Geometry**: a Polygon or MultiPolygon derived from the union of all H3 cells in the cluster.

**Properties**:

| Field | Type | Description |
|-------|------|-------------|
| `harbour_id` | string (UUID) | Stable identifier; same harbour always gets the same ID |
| `h3_cells` | array of strings | All H3 cell addresses at resolution 11 |
| `n_cells` | integer | Number of H3 cells |
| `n_events` | integer | Total stop events recorded in this harbour |
| `n_unique_mmsi_approx` | integer | Approximate number of distinct vessels |
| `n_draught_changes` | integer | Stop events with a measurable draught change |
| `n_cargo` | integer | Stop events by cargo vessels |
| `n_tanker` | integer | Stop events by tankers |
| `n_passenger` | integer | Stop events by passenger vessels |
| `n_fishing` | integer | Stop events by fishing vessels |
| `n_recreational` | integer | Stop events by recreational craft |
| `centroid_lat` / `centroid_lon` | float | Traffic-weighted centroid |
| `country_iso2` | string | ISO 3166-1 alpha-2 country code |
| `country_name` | string | Full country name |
| `nearest_city` | string | Nearest city name (from GeoNames) |
| `nearest_city_dist_km` | float | Distance to that city in km |
| `admin1` | string | First-level administrative region |
| `top_destination_locode` | string | Most common UN/LOCODE in AIS destination strings |
| `matched_existing` | boolean | Whether the ID was taken from an existing harbour DB |

---

## Project structure

```
harbour-detector/
├── config/
│   └── settings.yaml          # All configuration (baked into the Docker image)
├── deploy/
│   ├── job.yaml               # Kubernetes Job manifest
│   └── secret.yaml            # Kubernetes Secret template for S3 credentials
├── pipeline/
│   ├── extract_stops.py       # Phase 1: stop event extraction
│   ├── h3_aggregation.py      # Phase 2: H3 cell aggregation
│   ├── cluster_formation.py   # Phase 3: connected-component clustering
│   ├── enrichment.py          # Phase 4: polygon + geocoding
│   └── id_matching.py         # Phase 5: ID assignment and DB matching
├── models/
│   └── stop_event.py          # Pydantic model for stop events
├── utils/
│   ├── geo.py                 # Haversine distance, positional variance
│   └── s3.py                  # S3 credential loading, path helpers, DuckDB httpfs setup
├── tests/                     # Pytest unit tests for all phases
├── scripts/
│   └── generate_dummy_harbours.py  # GUI test data generator
├── data/
│   ├── raw/                   # Input Parquet files (not committed)
│   ├── interim/               # Per-phase intermediate outputs (not committed)
│   ├── reference/             # Reference databases (not committed)
│   └── output/                # Final GeoJSON and Parquet output (not committed)
├── app.py                     # Streamlit GUI
├── run.py                     # CLI entry point (local)
├── run_pipeline.py            # Docker / Kubernetes entry point
├── Dockerfile
├── .dockerignore
├── requirements.txt
└── .env.example               # Template for local S3 credentials
```

---

## Running tests

```bash
source ~/harbour-venv/bin/activate
pytest
```

Tests are self-contained and use synthetic in-memory data — no raw AIS files needed.
