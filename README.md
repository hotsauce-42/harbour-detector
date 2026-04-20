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
│   └── settings.yaml          # All configuration
├── pipeline/
│   ├── extract_stops.py       # Phase 1: stop event extraction
│   ├── h3_aggregation.py      # Phase 2: H3 cell aggregation
│   ├── cluster_formation.py   # Phase 3: connected-component clustering
│   ├── enrichment.py          # Phase 4: polygon + geocoding
│   └── id_matching.py         # Phase 5: ID assignment and DB matching
├── models/
│   └── stop_event.py          # Pydantic model for stop events
├── utils/
│   └── geo.py                 # Haversine distance, positional variance
├── tests/                     # Pytest unit tests for all phases
├── scripts/
│   └── generate_dummy_harbours.py  # GUI test data generator
├── data/
│   ├── raw/                   # Input Parquet files (not committed)
│   ├── interim/               # Per-phase intermediate outputs (not committed)
│   ├── reference/             # Reference databases (not committed)
│   └── output/                # Final GeoJSON and Parquet output (not committed)
├── app.py                     # Streamlit GUI
├── run.py                     # CLI entry point
└── requirements.txt
```

---

## Running tests

```bash
source ~/harbour-venv/bin/activate
pytest
```

Tests are self-contained and use synthetic in-memory data — no raw AIS files needed.
