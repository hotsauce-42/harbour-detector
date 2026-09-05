![Harbour Detector](harbour-detector-banner.png)
# Harbour Detector

Detects and maps harbours worldwide from historical AIS data. The pipeline processes decoded AIS position and voyage messages, clusters ship-stopping events spatially using H3 hexagons, enriches each cluster with country/city metadata, and outputs a GeoJSON file with stable harbour IDs.

---

## How it works

### Overview

1. **Phase 1 — Stop extraction** *(Spark)*: Scan every AIS Parquet file and find "stop events" — periods where a vessel was stationary (low speed, or moored/at-anchor nav status). Join voyage data (draught, ship type, destination) from Type 5 messages. Distributed across a Spark cluster — one executor task per MMSI — so a full year of data fits without memory issues.
2. **Phase 2 — H3 aggregation**: Project each stop event onto an H3 hexagonal grid at resolution 11 (~25 m cell edge). Count events, unique vessels, and vessel-type distribution per cell.
3. **Phase 3 — Cluster formation**: Build connected components from H3 cells that lie within `cluster_ring_size` rings of each other (BFS). The ring gap bridges cold cells between parts of the same harbour complex. Each component becomes one harbour candidate.
4. **Phase 4 — Enrichment**: Generate a polygon (GeoJSON geometry) from the H3 cell set. Reverse-geocode the centroid to find country and nearest city.
5. **Phase 5 — ID matching**: Assign each harbour a deterministic UUID5 based on its coarse H3 centroid cell (resolution 8). Optionally match against an existing harbour database to preserve historical IDs and any manual metadata corrections made in the GUI.

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

If a match is found above the configured thresholds, the existing ID is reused — together with any city/region/country corrections the existing record marks as manually edited, and any outline drawn for it in the GUI. See [Keeping manual edits across a re-run](#keeping-manual-edits-across-a-re-run) and [Adjusting a harbour's outline](#adjusting-a-harbours-outline).

---

## Installation

> **Important:** Create the virtual environment on the Linux filesystem, not on the Windows-mounted drive (`/mnt/c/...`). NTFS does not support Unix symlinks or executable bits, so `pip` and `streamlit` will not work from a venv created there.

**Prerequisites:**
- Python 3.10+
- Java 11 or 17 (required by PySpark) — `sudo apt install default-jdk-headless`

No other OS-level packages are needed; every Python dependency installs from a wheel.

```bash
python3 -m venv ~/harbour-venv
source ~/harbour-venv/bin/activate
pip install -r requirements.txt
```

Phase 1 runs on **Spark**. For local development `local[*]` mode is used automatically (no cluster needed). For production, deploy via the Spark Operator — see [Deploying on Kubernetes (Spark)](#deploying-on-kubernetes-spark).

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
| `use_type5_data` | `true` | Process Type 5 messages at all. Turn off to run Phase 1 on a workstation — see [Running Phase 1 on a laptop](#running-phase-1-on-a-laptop) |

### `phase2` — H3 aggregation

| Key | Default | Description |
|-----|---------|-------------|
| `h3_resolution` | `11` | H3 resolution for spatial indexing (~25 m cell edge) |
| `min_unique_mmsi` | `5` | Minimum distinct vessels for a cell to be considered a harbour cell |

### `phase3` — Cluster formation

| Key | Default | Description |
|-----|---------|-------------|
| `min_cells_per_harbour` | `1` | Minimum H3 cells for a connected component to be kept |
| `cluster_ring_size` | `3` | H3 rings to search for neighbours. `1` = touching only; `3` bridges ~75 m gaps between hot cells, merging fragmented harbour complexes. Increase for large port areas with cold cells between berths. |
| `prune_detached_cells` | `true` | Drop stray cells stranded off the harbour body — see [Detached cells](#detached-cells) |
| `detached_ring_size` | `2` | How far a cell may be from the rest and still count as attached. At res 11 (~50 m cells) two rings is ~105 m |
| `max_detached_cells` | `2` | A stranded group larger than this is a real berth, and is kept |
| `max_detached_event_share` | `0.05` | …and so is one carrying at least this share of the harbour's events |

#### Detached cells

Clusters are connected through res-9 **parent** cells, so two res-11 cells hundreds of metres apart join the same harbour whenever their parents happen to touch. That admits the occasional speck out in the water — a single anchoring, GPS scatter — which is then exported as its own polygon part. Tunø By carried two, 205 m and 369 m out, 2 AIS events each against the harbour's 107.

Phase 3 therefore re-checks connectivity on the fine cells and drops what is left stranded — but only when the group is **both** small and quiet. Either test alone is wrong: harbours really do have detached berths (most are fragmented at single-ring adjacency, and a detached group can hold half the traffic), so a size test alone would delete a third of every cell in the output, while a traffic test alone would swallow whole outlying terminals.

On a 331-harbour Danish run this removes 45 of 2 413 cells across 29 harbours, the largest single loss being 15 events, and moves exactly one harbour's centroid into a different res-8 cell. Set `prune_detached_cells: false` for the old behaviour.

> Pruning shrinks `h3_cells` and shifts the event-weighted centroid, and `harbour_id` is `{country_iso2}-{uuid5(centroid_h3_r8)[:8]}` — so a harbour whose centroid crosses a res-8 boundary is re-issued an ID unless Phase 5 matches it against `existing_db_path`.

### `phase4` — Enrichment

| Key | Default | Description |
|-----|---------|-------------|
| `gazetteer_path` | `""` | GeoNames places file from `scripts/prepare_gazetteer.py`. Empty falls back to the cities1000 dataset bundled with `reverse_geocoder` — see [Getting the city right](#getting-the-city-right) |
| `city_population_tiers` | 0 / 100 cells → 1 000 / 1 000 cells → 15 000 | Population a harbour of a given `n_cells` demands of its city. Highest tier reached wins; below the first there is no floor. Administrative seats (GeoNames `PPLA*`/`PPLC`) clear any floor |
| `max_city_dist_km` | `50` | Past this the floor is dropped and the nearest place of any size wins |
| `outline_buffer_meters` | `75` | Closing radius for the harbour outline. Fills gaps narrower than 2× this (~150 m, three res-11 cells) without pushing the boundary more than ~1 buffer past the outermost cell. Raise it to merge terminals that are further apart into a single polygon. |
| `outline_simplify_meters` | `0` | Vertex thinning tolerance for the outline. Off by default — it is the only step that can pull the boundary inside a trafficked cell (a 10 m tolerance already bites up to ~50 m, a whole res-11 cell). Raise it to shrink the output ~5× if that trade is acceptable. |
| `outline_fill_holes` | `true` | Drop interior rings, so untrafficked cells inside a harbour leave no holes |

#### Getting the city right

Out of the box, `nearest_city` comes from the GeoNames **cities1000** dataset bundled inside `reverse_geocoder` — places with a population over 1 000. Nothing smaller is in the file, so a harbour beside a village of 300 gets named after a town tens of kilometres away. On a 331-harbour Danish run, 107 harbours sat more than 5 km from their assigned city and the worst sat 51 km away, on the far side of a sea.

Build the dense gazetteer once and point `gazetteer_path` at it:

```bash
python3 scripts/prepare_gazetteer.py                      # global: 421 MB download, ~50 s
python3 scripts/prepare_gazetteer.py --countries DK SE DE NO PL   # or just your region
python3 scripts/prepare_gazetteer.py --source ~/allCountries.zip --no-download
```

It turns a GeoNames dump into `data/reference/geonames/places.parquet` — 5.2 M populated places, 115 MB, sorted by latitude. On the same Danish run:

| | bundled cities1000 | dense gazetteer |
|---|---|---|
| median harbour → city | 2.3 km | **0.6 km** |
| p90 | 12.8 km | **1.5 km** |
| worst | 51.1 km | 11.3 km |
| harbours >10 km from their city | 52 | **1** |

```
Grenaa (51.1 km)            →  Anholt (2.4 km)      the island's own harbour
Nykobing Falster (22.1 km)  →  Gedser (0.5 km)      the ferry port
Kalundborg (22.5 km)        →  Sejerby (0.6 km)
```

Cost at runtime is small because Phase 4 filters the file to a bounding box around the run's own centroids before indexing it: ~0.6 s and ~100 MB RSS for the Danish box (49 932 of 5.2 M places). Preparing the file peaks at 1.0 GB RSS, once.

Two things this deliberately does **not** change:

- **The country comes from the nearest place, not from the city.** A population floor can pick a town tens of kilometres away, and that town may be across a border while the village beside the quay is not — so `country_iso2` is resolved separately, at floor 0. `reverse_geocoder` stays as the fallback for when no place is within `max_city_dist_km`, which matters if you built a regional gazetteer; Phase 4 warns when that happens.

  Note that `harbour_id` is `{country_iso2}-{hash}`, so the country is *inside* the ID: a harbour whose country changes and which Phase 5 does *not* match against your existing database will be issued a new ID. On the reference run this affected 1 harbour of 328 and 0 IDs — a harbour in the Swedish Koster archipelago that `reverse_geocoder` had placed in Norway, from a town 14.8 km away rather than the village 1.0 km away. Re-measure on your own data before assuming the same.
- **A district is not a city — unless it is a town in its own right.** GeoNames records city districts as their own entries (`PPLX`) at the district rather than the town centre, which for a waterfront district is nearer the harbour. Left alone they name a harbour `Altstadt` or `Holmen` instead of Heiligenhafen or Copenhagen. A district is kept only if GeoNames counts people in it *and* no more populous place sits within 5 km — the stand-in for the parent-city link the dump does not carry. Warnemünde's nearest bigger place is Rostock, 12 km off, so a harbour there is still called Warnemünde; Christiania's is Copenhagen, 2 km off, so those harbours are called Copenhagen. Abandoned and destroyed places are dropped outright.
- **Large harbours keep their city.** With every village in the gazetteer, the nearest place to a major port is often a hamlet on its edge. `city_population_tiers` raises the population floor as `n_cells` grows, so a port gets its city and a marina gets its village. Administrative seats are exempt from the floor, since GeoNames often records a small population for a county town.

If nobody prepares the file, Phase 4 logs a warning and falls back to cities1000 — with the distance bug fixed, which by itself corrected 38 of those 331 harbours.

### `phase5` — ID matching

| Key | Default | Description |
|-----|---------|-------------|
| `h3_jaccard_threshold` | `0.3` | Minimum H3 cell overlap ratio to match an existing harbour |
| `centroid_match_distance_meters` | `500` | Fallback: centroid distance threshold for a match |
| `existing_db_path` | `data/existing_db/harbours.geojson` | Existing harbour database to match against (`.geojson` or `.parquet`, local or `s3://`). Also the file manual GUI corrections and drawn outlines are read back from — see [Keeping manual edits across a re-run](#keeping-manual-edits-across-a-re-run). Overridable per run with `--existing-db`. |

Phase 5 also reads `phase4.outline_fill_holes`, so merging a drawn outline treats interior voids the way the detected outline was built.

### `spark` — Spark session

| Key | Default | Description |
|-----|---------|-------------|
| `app_name` | `"harbour-detector"` | Spark application name shown in the Spark UI |
| `local_threads` | `2` | Local-mode task slots. Each is a Python worker holding its own pandas frame, so Spark's `local[*]` means one per core |
| `local_driver_memory` | `"3g"` | Local-mode JVM heap. In local mode the driver *is* the executor, and Spark's default is 1 GB |
| `local_shuffle_partitions` | `64` | Width of the `groupBy(mmsi)` shuffle (Spark's default, 200, is sized for a cluster) |
| `local_arrow_batch_size` | `5000` | Rows per Arrow batch handed to a Python worker |
| `local_max_partition_bytes` | `"32m"` | Parquet read split size (Spark default `128m`) |

Every `local_*` key applies **only outside Kubernetes**. In a cluster the Spark Operator supplies these, and the settings that matter there (executor count, memory, cores) live in `deploy/spark_job.yaml` under `sparkConf` / `driver` / `executor`.

### Running Phase 1 on a laptop

Phase 1 is the only phase that needs Spark, and the only one sized for a server. On a one-day Danish extract (288 files, 659 MB, 43.9 M rows) it unions **19.4 M positional candidates with 43.3 M Type 5 rows** and shuffles all 62.7 M through `groupBy(mmsi).applyInPandas`.

That Type 5 volume is an artefact of the source format, not of AIS: `scripts/convert_aisdk_csv.py` marks any row carrying voyage data as `msg_type = 5`, and the DMA export puts voyage data on nearly every record — 98.6% of the file. Those 43.3 M rows contain just **12,534 distinct** `(mmsi, draught, destination, ship_type)` combinations.

So for local runs, switch the Type 5 leg off:

```bash
echo "PHASE1__USE_TYPE5_DATA=false" >> .env     # or set it per-run
python run.py phase1
```

Production keeps `use_type5_data: true` in `config/settings.yaml` and is unaffected — the env override is what changes it here, so no image or config change ships.

What you give up, and what you keep:

| | with Type 5 | without |
|---|---|---|
| rows through the shuffle | 62.7 M | **19.4 M** |
| `draught_arrival` / `_departure` / `_delta` | filled | empty |
| `destination_raw` / `destination_locode` | filled | empty |
| `ship_type` → `n_cargo`…`n_tug_pilot` | filled | **still filled** |

`ship_type` survives because it is a vessel attribute rather than a per-message one, so it is recomputed by `_ship_type_by_mmsi` — a per-MMSI aggregate that never leaves the JVM. Draught and destination cannot survive: `_join_type5_data` needs a Type 5 record within `draught_lookup_hours` of each individual stop.

On the reference dataset, draught is nearly absent anyway (34 of 2,678 cells; 10 of 331 harbours), while the vessel-type breakdown covers 2,253 of 2,678 cells — which is why the split falls where it does.

### `s3` — S3 / MinIO storage

All three data path keys (`raw_glob`, `interim_dir`, `output_dir`) accept `s3://` URIs in addition to local paths. Phase 1 (Spark) accesses S3 via the Hadoop S3A connector; all other phases use `s3fs`.

| Key | Default | Description |
|-----|---------|-------------|
| `access_key_id` | `""` | AWS access key ID |
| `secret_access_key` | `""` | AWS secret access key |
| `region` | `""` | AWS region (defaults to `us-east-1` when blank) |
| `endpoint_url` | `""` | Custom S3-compatible endpoint, e.g. `http://localhost:9000` for MinIO |

**Configuration precedence** (highest wins):

1. **Environment variables** (including those loaded from `.env`)
2. **`.env` file** in the project root — loaded automatically; never overwrites already-set env vars
3. **`config/settings.yaml`** — baked-in defaults

Leave all four YAML fields blank and set the standard AWS env vars instead. This is the recommended approach for Kubernetes deployments where you never want to rebuild the image just to change a credential.

### `gui` — Streamlit app

| Key | Default | Description |
|-----|---------|-------------|
| `output_file` | `data/output/harbours.geojson` | GeoJSON file the GUI reads — the harbour outlines |
| `cells_file` | `data/output/harbours_cells.geojson` | H3-cell geometry behind the map's **H3 cells** / **Both** toggle. Defaults to `output_file` with a `_cells` suffix; if the file is absent the GUI shows outlines only. |
| `default_tile` | `OpenStreetMap` | Which tile layer is selected on startup |
| `map_tiles` | (4 built-in layers) | List of `{name, url, attribution}` tile server definitions |
| `local_map_assets` | `false` | Load Leaflet from `static/vendor/` instead of the public CDNs — see [Offline / air-gapped use](#offline--air-gapped-use) |
| `map_assets_url` | `/app/static/vendor` | Where those files are served from, if not this app's own `static/` directory |

To add a custom tile server, append to `map_tiles`:

```yaml
map_tiles:
  - name: "My Server"
    url: "https://mytiles.example.com/{z}/{x}/{y}.png"
    attribution: "© My Company"
```

### Offline / air-gapped use

The GUI reaches the internet in two independent ways, and both have to be dealt
with — a private tile server on its own is not enough.

**1. Tiles.** Replace the four public entries in `map_tiles` with your own
server. Leaving them in place costs a DNS timeout each time one is selected.

**2. Leaflet itself.** folium's page loads six libraries from four public CDNs.
Offline, none of them arrive and the map is a blank box — `leaflet.draw` among
them, which is the outline editor. Download the two that this app actually uses
(the other four are unreferenced and get dropped):

```bash
python3 scripts/vendor_map_assets.py          # → static/vendor/, needs internet
```

Then turn it on:

```yaml
gui:
  local_map_assets: true       # or GUI__LOCAL_MAP_ASSETS=true
```

Streamlit serves `static/` at `/app/static/` — already enabled in
`.streamlit/config.toml`. `static/vendor/` is gitignored, so on a sealed
network run the script on a connected machine and copy the directory across (or
run it during an image build). Set `map_assets_url` instead to serve the files
from another host, e.g. the web server that already serves your tiles.

Versions come from folium, so a folium upgrade is picked up by re-running the
script — no URLs to edit by hand.

---

## Running the pipeline

> **Phase 1 requires Java.** PySpark starts a local JVM when no Spark cluster is available. Make sure `java` is on your `PATH` (`java -version` should work) before running phase1.

```bash
# Activate the venv first
source ~/harbour-venv/bin/activate

# Run all phases in order
python run.py phase1   # Spark (local[*] mode on dev machines)
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

### Overriding config without editing YAML

Any `config/settings.yaml` key can be overridden with a `SECTION__KEY` environment variable (double-underscore separator, case-insensitive):

```bash
# Tune clustering without rebuilding the image
PHASE3__CLUSTER_RING_SIZE=5 python run.py phase3

# Lower the stop detection threshold
PHASE1__SOG_THRESHOLD_KNOTS=0.3 python run.py phase1

# Override the MinIO endpoint
S3__ENDPOINT_URL=http://localhost:9000 python run.py phase1
```

The legacy flat variables (`RAW_GLOB`, `INTERIM_DIR`, `OUTPUT_DIR`, `EXISTING_DB`) still work as before.

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
| Spark `No FileSystem for scheme: s3a` (Phase 1 only) | Hadoop S3A JARs missing from the classpath | Use the Spark image, which downloads them at build time; locally, add `hadoop-aws` + `aws-java-sdk-bundle` to `pyspark/jars/` |

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

### Seeing a whole city at once

A large port is usually several harbours, and the table gives no sense of how they sit relative to each other. When the selected harbour shares its city with others, a **Show all N harbours in {city}** toggle appears above the map: the rest of the group is drawn in grey around the selected one, and the view fits the whole group.

**Click any grey harbour to select it.** Everything below follows the click — the metrics row, **Edit location details**, the outline editor, and **Full properties**, which opens automatically on the harbour you clicked. So the group view is also the way to work through a port harbour by harbour without going back to the table each time.

Two details worth knowing:

- Grouping is by city **and** country. Hamburg (DE) and Hamburg (NY) are different places, and putting both on one map would zoom out to the Atlantic. A harbour with an empty `nearest_city` is a group of one — a missing value is not a place that harbours share.
- Click-to-select pauses while **Edit outline** is on, so a stray click cannot swap harbours out from under an unsaved outline. The group stays visible as context; turn editing off to move on to the next harbour.

### Editing a harbour's location details

Reverse geocoding gets the country right nearly always, but `nearest_city` is simply whatever GeoNames has closest to the centroid — for a large port that is often a suburb rather than the port city, and `admin1` can be wrong near a regional border. Select a harbour and open **Edit location details** to correct the city, region and country by hand. The harbour ID is not editable — it is what the matching keys on.

Saving writes the new values straight into `harbours.geojson` **and** `harbours_cells.geojson` (both files carry the same properties, so they would otherwise drift apart), and records which fields you touched in a `manual_overrides` array:

```json
"nearest_city":     "Hamburg-Altona",
"admin1":           "Hamburg",
"country_name":     "Germany",
"manual_overrides": ["nearest_city"]
```

Editing **Country** also updates `country_iso2` when the name resolves to an ISO 3166-1 code (`Netherlands` → `NL`), so the pair cannot contradict each other. If the name does not resolve, the existing code is left untouched and the app says so.

**Clear manual flags** drops the marker without changing the values, letting the next pipeline run re-derive those fields normally.

### Keeping manual edits across a re-run

Phase 5 reads `manual_overrides` back off the existing harbour database. When a freshly detected cluster matches an existing harbour, the fields listed there are restored from the existing record in place of the freshly geocoded values — every other field still refreshes as usual. A correction therefore survives every later run, while auto-derived values stay free to improve.

Matching itself always runs against the fresh data, so an override can never change *which* harbour a cluster matches — only the metadata it ends up with.

> **The edits must reach the database Phase 5 actually reads.** The GUI writes to `gui.output_file`, but Phase 5 matches against `phase5.existing_db_path`. When those are different files, copy the output across before the next run, or point `existing_db_path` at the output file. The app shows a warning whenever the two paths differ.

### Adjusting a harbour's outline

The detected outline is a morphological closing of the cells that saw traffic, so it can miss a quay that had no stop events in the window, or bulge into open water. Turn on **Edit outline** above the map to fix it by hand:

- The ✏️ tool makes the outline's vertices draggable; its midpoint handles add new ones.
- The ▱ tool draws a replacement polygon — useful when the detected shape is badly wrong. Delete the old one with 🗑 and everything left on the map is saved as one outline (so a multi-part harbour keeps its parts).
- **Save outline** writes it; **Revert to detected** drops it again.

Saving records three things:

```json
"manual_outline_wkt":   "POLYGON ((9.93 53.54, …))",   // exactly what you drew
"detected_outline_wkt": "POLYGON ((9.94 53.54, …))",   // what Phase 4 detected
```

and the feature geometry becomes the union of the two. Both GeoJSON files carry the properties; only `harbours.geojson` gets the new geometry, since `harbours_cells.geojson` holds the H3-cell union, which drawing an outline does not change.

**The drawn outline is a floor, not a replacement.** On the next run Phase 5 sets `outline_wkt = detected ∪ manual`, so a harbour can still grow as new stop events light up cells outside your shape, but can never shrink back inside it. The flip side: dragging the boundary *inwards* lasts only until the next run unions the detected area back in — the app warns you at save time when an edit trims detected area away.

`manual_outline_wkt` is never rewritten by the pipeline. It stays the baseline you drew however far the harbour grows around it, which is what makes **Revert to detected** exact and keeps "what a human drew" separable from "what the pipeline added".

An outline edit is geometry only: `h3_cells`, `n_events`, `n_unique_mmsi` and the centroid keep describing the detected data, and matching still runs on cells and centroid — so drawing a bigger outline never changes which cluster matches which harbour.

### Testing the GUI without pipeline data

Generate 20 realistic dummy harbours (real H3 polygons, real-world ports):

```bash
python scripts/generate_dummy_harbours.py
```

This writes `data/output/harbours.geojson` so the GUI has something to display immediately.

---

## Building the Docker images

There are two images, matching the two halves of the pipeline:

| Dockerfile | Image | Phases | Default entrypoint | Contents |
|---|---|---|---|---|
| `Dockerfile.spark` | `harbour-detector-spark` | 1 (and 1–5 if you want one pod) | `run_phase1.py` | JVM + PySpark + S3A JARs + full pipeline code |
| `Dockerfile.enrich` | `harbour-detector-enrich` | 2–5 | `run_enrich.py` | pandas / pyarrow / shapely only — no JVM, no PySpark, no OS packages |

Both images include:
- Their default configuration (`config/settings.yaml`), overridable at runtime via environment variables
- The `reverse_geocoder` GeoNames dataset, pre-warmed so no outbound internet is needed at runtime

The Spark image additionally includes the Hadoop S3A connector JARs (`hadoop-aws` + `aws-java-sdk-bundle`), downloaded at build time so the container never needs Maven access at runtime.

Neither image bakes in the **Phase 4 gazetteer** (`data/reference/geonames/places.parquet`, 115 MB) — it is data, not code, and rebuilding an image to refresh a GeoNames dump is the wrong shape. Supply it one of two ways:

```yaml
# Mount it and point Phase 4 at the mount
env:
  - name: PHASE4__GAZETTEER_PATH
    value: /data/geonames/places.parquet
```

```bash
# …or keep it on S3 alongside the rest of the pipeline's data
PHASE4__GAZETTEER_PATH=s3://my-bucket/reference/geonames/places.parquet
```

An S3 URI uses the same credentials as every other path. If the file is missing or unreachable, Phase 4 logs a warning and falls back to the bundled cities1000 dataset rather than failing the run.

Dependencies are split to match:

| File | Contents |
|---|---|
| `requirements-base.txt` | Shared runtime deps — installed by the enrichment image |
| `requirements-spark.txt` | `requirements-base.txt` + `pyspark` (Phase 1 only) — installed by the Spark image |
| `requirements.txt` | `requirements-spark.txt` + Streamlit GUI + `pytest` / `ruff` / `mypy` — local development and CI |

The shared runtime deps are **pinned exactly**. The two images are built separately and hand data over as Parquet, so a version floor would let them resolve different majors at their own build times — a Phase 1 image on pandas 2 feeding a Phase 2–5 image on pandas 3 disagrees about datetime resolution with no obvious symptom. Bump the pin, rebuild **both** images from the same commit, run `pytest`.

### Build

```bash
docker build -f Dockerfile.spark  -t myregistry.io/harbour-detector-spark:1.0.0  .
docker build -f Dockerfile.enrich -t myregistry.io/harbour-detector-enrich:1.0.0 .
```

### Push

```bash
docker push myregistry.io/harbour-detector-spark:1.0.0
docker push myregistry.io/harbour-detector-enrich:1.0.0
```

Replace `myregistry.io/...` with your actual registry and tag.

### Local smoke test

Verify the images work before deploying to the cluster:

```bash
# Phase 1 (Spark image, its default entrypoint)
docker run --rm \
  -e RAW_GLOB="s3://my-bucket/ais/**/*.parquet" \
  -e INTERIM_DIR="s3://my-bucket/harbour-detector/interim" \
  -e OUTPUT_DIR="s3://my-bucket/harbour-detector/output" \
  -e AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE" \
  -e AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -e AWS_DEFAULT_REGION="eu-west-1" \
  myregistry.io/harbour-detector-spark:1.0.0

# Phases 2-5 (enrichment image, its default entrypoint)
docker run --rm \
  -e INTERIM_DIR="s3://my-bucket/harbour-detector/interim" \
  -e OUTPUT_DIR="s3://my-bucket/harbour-detector/output" \
  -e AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE" \
  -e AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -e AWS_DEFAULT_REGION="eu-west-1" \
  myregistry.io/harbour-detector-enrich:1.0.0
```

For MinIO add `-e S3_ENDPOINT_URL="http://host.docker.internal:9000"`.

### Running a single phase

Both images ship `run.py`, so any individual phase can be run by overriding the entrypoint — useful for re-running just Phase 4 after a config change:

```bash
docker run --rm --entrypoint python \
  myregistry.io/harbour-detector-enrich:1.0.0 run.py phase4
```

The enrichment image covers phases 2–5; `run.py phase1` needs the Spark image (the enrichment image has no `pyspark`). All five phases in one container is `run_pipeline.py` on the Spark image.

---

## Deploying on Kubernetes (Spark)

Only Phase 1 uses Spark. The recommended production deployment **splits the pipeline into two steps** that hand off through S3:

1. **Phase 1** runs on the **Spark Operator** (`deploy/spark_job.yaml`, entrypoint `run_phase1.py`), distributing stop extraction across executor pods.
2. **Phases 2–5** run in a **plain pod** (`deploy/job_enrich.yaml`, entrypoint `run_enrich.py`) — pandas / pyarrow / shapely only, no Spark, no executors, so the pod is small.

The two steps share no state beyond the S3 paths (`INTERIM_DIR` / `OUTPUT_DIR`), so an external orchestrator just has to run them in order. All intermediate data is written to S3, so failed pods can be retried without re-running earlier phases.

> Prefer a single pod? `deploy/job.yaml` (entrypoint `run_pipeline.py`) still runs all five phases in one container with Spark in `local[*]` mode — see [Deploying as a plain Kubernetes Job](#deploying-as-a-plain-kubernetes-job-small-datasets--testing).

### Prerequisites

- [Spark Operator](https://github.com/kubeflow/spark-operator) installed in the cluster
- Spark 4.0 cluster available

### 1. Create the namespace and service account (once)

```bash
kubectl create namespace ais

# The driver pod needs RBAC to create/delete executor pods
kubectl create serviceaccount spark -n ais
kubectl create clusterrolebinding spark-role \
  --clusterrole=edit \
  --serviceaccount=ais:spark \
  --namespace=ais
```

### 2. Create the S3 credentials secret

Edit `deploy/secret.yaml` and fill in your real credentials. For MinIO, uncomment the `endpoint-url` line:

```yaml
stringData:
  access-key-id:     "AKIAIOSFODNN7EXAMPLE"
  secret-access-key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  region:            "eu-west-1"
  endpoint-url:      "http://minio.minio.svc.cluster.local:9000"   # MinIO only
```

```bash
kubectl apply -f deploy/secret.yaml
```

> **Never commit `secret.yaml` with real credentials.** Use a secrets manager (Vault, Sealed Secrets, AWS Secrets Manager) to generate it at deploy time.

### 3. Configure `deploy/spark_job.yaml`

Set the `CHANGE_ME` env vars in the `driver` section and the MinIO endpoint in `sparkConf`. Phase 1 only reads `RAW_GLOB` and writes `INTERIM_DIR` — `OUTPUT_DIR` and `EXISTING_DB` belong to the enrichment step (`job_enrich.yaml`):

```yaml
sparkConf:
  "spark.hadoop.fs.s3a.endpoint": "http://minio.minio.svc.cluster.local:9000"

driver:
  env:
    - name: RAW_GLOB
      value: "s3://my-bucket/ais/**/*.parquet"
    - name: INTERIM_DIR
      value: "s3://my-bucket/harbour-detector/interim"
```

Adjust executor sizing to your cluster:
```yaml
executor:
  cores: 4
  instances: 4      # one task per MMSI batch; more instances = faster Phase 1
  memory: "16g"
```

### 4. Submit

```bash
kubectl apply -f deploy/spark_job.yaml
```

### 5. Follow progress

```bash
# Driver log (Phase 1)
kubectl logs -f \
  $(kubectl get pod -n ais -l spark-role=driver -o name) \
  -n ais

# Spark UI (port-forward to the driver pod)
kubectl port-forward -n ais \
  $(kubectl get pod -n ais -l spark-role=driver -o name | sed 's/pod\///') \
  4040:4040
# then open http://localhost:4040
```

### 6. Run the enrichment step (Phases 2–5)

After Phase 1 completes, configure `deploy/job_enrich.yaml` (set `INTERIM_DIR` to the same value used above, plus `OUTPUT_DIR` and any optional `EXISTING_DB`) and apply it. Your orchestrator sequences the two steps; by hand:

```bash
# Wait for Phase 1 to finish
kubectl wait -n ais \
  --for=jsonpath='{.status.applicationState.state}'=COMPLETED \
  sparkapplication/harbour-detector --timeout=24h

# Run Phases 2–5 in a plain pod (no Spark)
kubectl apply -f deploy/job_enrich.yaml
kubectl logs -f job/harbour-detector-enrich -n ais
```

### 7. Clean up after completion

```bash
kubectl delete sparkapplication harbour-detector -n ais
kubectl delete -f deploy/job_enrich.yaml
```

### Environment variable overrides

Any `config/settings.yaml` key can be overridden without rebuilding the Docker image. Use the `SECTION__KEY` convention in the `driver.env` block:

```yaml
driver:
  env:
    - name: PHASE3__CLUSTER_RING_SIZE
      value: "5"
    - name: PHASE2__MIN_UNIQUE_MMSI
      value: "3"
    - name: PHASE1__SOG_THRESHOLD_KNOTS
      value: "0.3"
```

The full override reference:

| Pattern | Example | Equivalent YAML key |
|---------|---------|---------------------|
| `DATA__RAW_GLOB` | `s3://bucket/ais/**/*.parquet` | `data.raw_glob` |
| `DATA__INTERIM_DIR` | `s3://bucket/interim` | `data.interim_dir` |
| `DATA__OUTPUT_DIR` | `s3://bucket/output` | `data.output_dir` |
| `PHASE1__SOG_THRESHOLD_KNOTS` | `0.3` | `phase1.sog_threshold_knots` |
| `PHASE1__MIN_STOP_DURATION_MINUTES` | `20` | `phase1.min_stop_duration_minutes` |
| `PHASE2__MIN_UNIQUE_MMSI` | `3` | `phase2.min_unique_mmsi` |
| `PHASE3__CLUSTER_RING_SIZE` | `5` | `phase3.cluster_ring_size` |
| `S3__ENDPOINT_URL` | `http://minio:9000` | `s3.endpoint_url` |
| `SPARK__APP_NAME` | `harbour-prod` | `spark.app_name` |
| `EXISTING_DB` _(legacy)_ | `s3://bucket/ref/harbours.parquet` | `phase5.existing_db_path` |

S3 credentials are injected from the `harbour-detector-s3` Kubernetes Secret and available as standard AWS env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`) in both the driver and all executor pods.

---

## Deploying as a plain Kubernetes Job (small datasets / testing)

For small datasets that fit in a single pod's memory, `deploy/job.yaml` runs the pipeline without Spark. Phase 1 will still use Spark in `local[*]` mode on the pod itself, so the pod needs enough RAM for one day of filtered data.

### 1–2. Namespace, secret — same as above

### 3. Configure `deploy/job.yaml`

```yaml
env:
  - name: RAW_GLOB
    value: "s3://my-bucket/ais/2024/01/**/*.parquet"
  - name: INTERIM_DIR
    value: "s3://my-bucket/harbour-detector/interim"
  - name: OUTPUT_DIR
    value: "s3://my-bucket/harbour-detector/output"
```

Adjust `resources.limits.memory` to match your dataset size.

### 4. Submit and follow

```bash
kubectl apply -f deploy/job.yaml
kubectl logs -f job/harbour-detector -n ais
kubectl delete job harbour-detector -n ais   # clean up
```

---

## Output format

Phase 5 writes three files, all covering the same harbours with the same properties:

| File | Geometry |
|------|----------|
| `data/output/harbours.geojson` | The **harbour outline** — one closed polygon per harbour |
| `data/output/harbours_cells.geojson` | The exact union of the harbour's hot H3 cells |
| `data/output/harbours.parquet` | Both, as the `outline_wkt` and `geometry_wkt` columns |

GeoJSON allows only one geometry per feature, which is why the two shapes are split
across two files; every feature carries a `geometry_kind` property (`outline` or
`cells`) saying which one it holds.

The outline is the cell union morphologically **closed** — dilated by
`phase4.outline_buffer_meters` and eroded again — then stripped of interior rings.
That fills the gaps between berths and the holes left by cells that saw no traffic,
giving the shape of the harbour rather than the shape of the data. Closing never
extends the boundary more than about one buffer past the outermost cell, so the
outline does not cover water or land the vessels never visited. The outline always
contains every cell that saw traffic. Terminals further apart than 2× the buffer
stay separate, so an outline can still be a MultiPolygon — raise
`outline_buffer_meters` to merge them; Phase 4 logs how many stayed multi-part.

`data/output/harbours.geojson` is a GeoJSON `FeatureCollection`. Each feature represents one harbour.

**Properties**:

| Field | Type | Description |
|-------|------|-------------|
| `harbour_id` | string (UUID) | Stable identifier; same harbour always gets the same ID |
| `geometry_kind` | string | `outline` or `cells` — which geometry this file holds |
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
| `manual_overrides` | array of strings | Fields corrected by hand in the GUI (`nearest_city`, `admin1`, `country_name`) and restored on the next run instead of being re-geocoded. Empty for untouched harbours. |

---

## Project structure

```
harbour-detector/
├── config/
│   └── settings.yaml              # All configuration (baked into the Docker image)
├── deploy/
│   ├── spark_job.yaml             # SparkApplication for Spark Operator — Phase 1 (recommended)
│   ├── job_enrich.yaml            # Plain Kubernetes Job — Phases 2-5 (pairs with spark_job.yaml)
│   ├── job.yaml                   # Plain Kubernetes Job — all 5 phases (small datasets / testing)
│   └── secret.yaml                # Kubernetes Secret template for S3 credentials
├── pipeline/
│   ├── extract_stops.py           # Phase 1: per-vessel stop detection logic (reused by Spark UDF)
│   ├── extract_stops_spark.py     # Phase 1: Spark orchestration (applyInPandas per MMSI)
│   ├── h3_aggregation.py          # Phase 2: H3 cell aggregation
│   ├── cluster_formation.py       # Phase 3: connected-component clustering
│   ├── enrichment.py              # Phase 4: polygon + geocoding
│   └── id_matching.py             # Phase 5: ID assignment and DB matching
├── models/
│   └── stop_event.py              # Pydantic model for stop events
├── utils/
│   ├── config.py                  # Shared config loader (YAML + env var overrides)
│   ├── geo.py                     # Haversine distance, positional variance
│   ├── overrides.py               # Manual GUI corrections shared by app.py and Phase 5
│   ├── s3.py                      # S3 credential loading, path helpers, s3fs filesystem factory
│   └── spark.py                   # SparkSession factory with S3A / MinIO configuration
├── tests/                         # Pytest unit tests for all phases
├── scripts/
│   └── generate_dummy_harbours.py # GUI test data generator
├── data/
│   ├── raw/                       # Input Parquet files (not committed)
│   ├── interim/                   # Per-phase intermediate outputs (not committed)
│   ├── reference/                 # Reference databases (not committed)
│   └── output/                    # Final GeoJSON and Parquet output (not committed)
├── app.py                         # Streamlit GUI
├── run.py                         # CLI entry point (local, per-phase)
├── run_pipeline.py                # Container entry point — all 5 phases (job.yaml)
├── run_phase1.py                  # Container entry point — Phase 1 only (spark_job.yaml)
├── run_enrich.py                  # Container entry point — Phases 2-5 (job_enrich.yaml)
├── Dockerfile.spark                # Image for Phase 1 — JVM + PySpark + S3A JARs
├── Dockerfile.enrich               # Image for Phases 2-5 — no JVM, no PySpark
├── .dockerignore
├── requirements-base.txt           # Shared runtime deps (enrichment image)
├── requirements-spark.txt          # Base + pyspark (Spark image)
├── requirements.txt                # Base + spark + GUI + dev tooling (local)
└── .env.example                   # Template for local S3 credentials
```

---

## Running tests

```bash
source ~/harbour-venv/bin/activate
pytest
```

Tests are self-contained and use synthetic in-memory data — no raw AIS files needed.
