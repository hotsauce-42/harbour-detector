"""
Phase 5: ID Matching + GeoJSON Export

For each enriched harbour cluster:
  1. Match against the existing harbour database (if supplied) using:
       a) H3 cell Jaccard overlap  (preferred, precise)
       b) Centroid distance        (fallback when existing DB has no h3_cells)
  2. Assign the matched harbour_id, or generate a new deterministic one
     (UUID5 of the centroid H3 cell at resolution 8 — stable across re-runs).
  3. Write data/output/harbours.geojson and data/output/harbours.parquet.

Existing harbour database format (Parquet or GeoJSON):
  Required : harbour_id  (string)
  Preferred: h3_cells    (list of H3 cell strings at any resolution)
  Fallback : centroid_lat, centroid_lon  (float)
"""

import json
import logging
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import h3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import mapping
from shapely.wkt import loads as from_wkt

from utils.geo import haversine_meters

logger = logging.getLogger(__name__)

# Fixed namespace — keeps harbour_id stable across pipeline re-runs
_HARBOUR_NS = uuid.UUID("b8d7e3a2-5f1c-4e8b-9a6d-3c7f2e1b4a5d")

OUTPUT_SCHEMA = pa.schema([
    pa.field("harbour_id",             pa.string()),
    pa.field("cluster_id",             pa.int32()),
    pa.field("h3_cells",               pa.list_(pa.string())),
    pa.field("n_cells",                pa.int32()),
    pa.field("n_events",               pa.int32()),
    pa.field("n_unique_mmsi_approx",   pa.int32()),
    pa.field("n_draught_changes",      pa.int32()),
    pa.field("centroid_lat",           pa.float64()),
    pa.field("centroid_lon",           pa.float64()),
    pa.field("country_iso2",           pa.string()),
    pa.field("country_name",           pa.string()),
    pa.field("nearest_city",           pa.string()),
    pa.field("nearest_city_dist_km",   pa.float32()),
    pa.field("admin1",                 pa.string()),
    pa.field("geometry_wkt",           pa.string()),
    pa.field("matched_existing",       pa.bool_()),  # True = reused existing harbour_id
])


@dataclass
class Phase5Config:
    interim_dir: str
    output_dir: str
    existing_db_path: Optional[str] = None
    h3_jaccard_threshold: float = 0.3
    centroid_match_distance_meters: float = 500.0

    @classmethod
    def from_yaml(cls, cfg: dict) -> "Phase5Config":
        p5 = cfg.get("phase5", {})
        data = cfg.get("data", {})
        return cls(
            interim_dir=data.get("interim_dir", "data/interim"),
            output_dir=data.get("output_dir", "data/output"),
            existing_db_path=p5.get("existing_db_path"),
            h3_jaccard_threshold=p5.get("h3_jaccard_threshold", 0.3),
            centroid_match_distance_meters=p5.get("centroid_match_distance_meters", 500.0),
        )


# ---------------------------------------------------------------------------
# Deterministic ID generation
# ---------------------------------------------------------------------------

def make_harbour_id(centroid_h3_r8: str) -> str:
    """Stable UUID5 derived from the resolution-8 H3 cell of the centroid."""
    return str(uuid.uuid5(_HARBOUR_NS, centroid_h3_r8))


# ---------------------------------------------------------------------------
# Load existing harbour database
# ---------------------------------------------------------------------------

def _load_existing_db(path: str) -> pd.DataFrame:
    """
    Load an existing harbour database from Parquet or GeoJSON.
    Returns a DataFrame with at minimum: harbour_id, centroid_lat, centroid_lon.
    h3_cells column (list<str>) is used when present.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Existing harbour DB not found: {path}")

    suffix = p.suffix.lower()

    if suffix == ".parquet":
        df = pd.read_parquet(p)

    elif suffix in (".geojson", ".json"):
        with open(p) as f:
            fc = json.load(f)
        rows = []
        for feat in fc.get("features", []):
            props = feat.get("properties", {})
            geom  = feat.get("geometry")
            row = dict(props)
            # Derive centroid from geometry if not present in properties
            if "centroid_lat" not in row and geom:
                from shapely.geometry import shape as _shape
                try:
                    c = _shape(geom).centroid
                    row.setdefault("centroid_lat", c.y)
                    row.setdefault("centroid_lon", c.x)
                except Exception:
                    pass
            rows.append(row)
        df = pd.DataFrame(rows)

    else:
        raise ValueError(f"Unsupported existing DB format: {suffix}. Use .parquet or .geojson")

    required = {"harbour_id"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Existing DB is missing required columns: {missing}")

    logger.info("Loaded existing harbour DB: %d harbours from %s", len(df), path)
    return df


# ---------------------------------------------------------------------------
# Build lookup indexes from existing DB
# ---------------------------------------------------------------------------

def _build_indexes(
    existing: pd.DataFrame,
) -> tuple[dict[str, str], list[dict]]:
    """
    Returns:
      cell_index   : h3_cell → harbour_id  (for Jaccard matching)
      centroid_list: list of {harbour_id, centroid_lat, centroid_lon, h3_cells}
    """
    cell_index: dict[str, str] = {}
    centroid_list: list[dict] = []

    has_cells = "h3_cells" in existing.columns

    for _, row in existing.iterrows():
        hid = row["harbour_id"]
        cells: list[str] = []

        if has_cells and isinstance(row["h3_cells"], (list, tuple)):
            cells = list(row["h3_cells"])
            for cell in cells:
                cell_index[cell] = hid

        clat = float(row["centroid_lat"]) if "centroid_lat" in row and pd.notna(row.get("centroid_lat")) else None
        clon = float(row["centroid_lon"]) if "centroid_lon" in row and pd.notna(row.get("centroid_lon")) else None

        centroid_list.append({
            "harbour_id":   hid,
            "centroid_lat": clat,
            "centroid_lon": clon,
            "h3_cells":     set(cells),
        })

    return cell_index, centroid_list


# ---------------------------------------------------------------------------
# Matching logic
# ---------------------------------------------------------------------------

def _jaccard(set_a: set, set_b: set) -> float:
    union = set_a | set_b
    return len(set_a & set_b) / len(union) if union else 0.0


def _find_match(
    new_cells: set[str],
    new_lat: float,
    new_lon: float,
    cell_index: dict[str, str],
    centroid_list: list[dict],
    config: Phase5Config,
) -> Optional[str]:
    """
    Return the matched existing harbour_id, or None if no match is found.

    Strategy A — H3 Jaccard:
      Use the cell_index to collect candidate existing harbour_ids (any cell overlap),
      compute Jaccard for the best candidate, accept if >= threshold.

    Strategy B — centroid distance:
      Walk centroid_list and accept the first entry within the distance threshold.
    """
    # --- Strategy A ---
    if cell_index:
        votes: dict[str, int] = {}
        for cell in new_cells:
            if cell in cell_index:
                eid = cell_index[cell]
                votes[eid] = votes.get(eid, 0) + 1

        if votes:
            best_id = max(votes, key=votes.__getitem__)
            # Find full record for best candidate
            best = next((e for e in centroid_list if e["harbour_id"] == best_id), None)
            if best and best["h3_cells"]:
                score = _jaccard(new_cells, best["h3_cells"])
                if score >= config.h3_jaccard_threshold:
                    return best_id

    # --- Strategy B ---
    for entry in centroid_list:
        if entry["centroid_lat"] is None or entry["centroid_lon"] is None:
            continue
        dist = haversine_meters(new_lat, new_lon, entry["centroid_lat"], entry["centroid_lon"])
        if dist <= config.centroid_match_distance_meters:
            return entry["harbour_id"]

    return None


# ---------------------------------------------------------------------------
# Assign harbour_ids to all clusters
# ---------------------------------------------------------------------------

def _assign_ids(
    enriched: pd.DataFrame,
    cell_index: dict[str, str],
    centroid_list: list[dict],
    config: Phase5Config,
) -> pd.DataFrame:
    harbour_ids    = []
    matched_flags  = []

    n_matched = 0
    n_new     = 0

    for _, row in enriched.iterrows():
        cells   = set(row["h3_cells"]) if isinstance(row["h3_cells"], (list, tuple)) else set()
        clat    = float(row["centroid_lat"])
        clon    = float(row["centroid_lon"])

        existing_id = _find_match(cells, clat, clon, cell_index, centroid_list, config)

        if existing_id:
            harbour_ids.append(existing_id)
            matched_flags.append(True)
            n_matched += 1
        else:
            harbour_ids.append(make_harbour_id(row["centroid_h3_r8"]))
            matched_flags.append(False)
            n_new += 1

    logger.info(
        "ID assignment: %d matched existing, %d new harbour_ids generated",
        n_matched, n_new,
    )

    enriched = enriched.copy()
    enriched["harbour_id"]       = harbour_ids
    enriched["matched_existing"] = matched_flags
    return enriched


# ---------------------------------------------------------------------------
# Write outputs
# ---------------------------------------------------------------------------

def _write_parquet(df: pd.DataFrame, out_dir: Path) -> Path:
    out_path = out_dir / "harbours.parquet"
    h3_cells_array = pa.array(df["h3_cells"].tolist(), type=pa.list_(pa.string()))

    table = pa.table(
        {
            "harbour_id":           pa.array(df["harbour_id"],                    type=pa.string()),
            "cluster_id":           pa.array(df["cluster_id"],                    type=pa.int32()),
            "h3_cells":             h3_cells_array,
            "n_cells":              pa.array(df["n_cells"],                       type=pa.int32()),
            "n_events":             pa.array(df["n_events"],                      type=pa.int32()),
            "n_unique_mmsi_approx": pa.array(df["n_unique_mmsi_approx"],         type=pa.int32()),
            "n_draught_changes":    pa.array(df["n_draught_changes"],             type=pa.int32()),
            "centroid_lat":         pa.array(df["centroid_lat"],                  type=pa.float64()),
            "centroid_lon":         pa.array(df["centroid_lon"],                  type=pa.float64()),
            "country_iso2":         pa.array(df["country_iso2"],                  type=pa.string()),
            "country_name":         pa.array(df["country_name"],                  type=pa.string()),
            "nearest_city":         pa.array(df["nearest_city"],                  type=pa.string()),
            "nearest_city_dist_km": pa.array(df["nearest_city_dist_km"].astype("float32"), type=pa.float32()),
            "admin1":               pa.array(df["admin1"],                        type=pa.string()),
            "geometry_wkt":         pa.array(df["geometry_wkt"],                  type=pa.string()),
            "matched_existing":     pa.array(df["matched_existing"],              type=pa.bool_()),
        },
        schema=OUTPUT_SCHEMA,
    )
    pq.write_table(table, out_path, compression="snappy")
    logger.info("Wrote harbours.parquet → %s", out_path)
    return out_path


def _write_geojson(df: pd.DataFrame, out_dir: Path) -> Path:
    out_path = out_dir / "harbours.geojson"

    features = []
    for _, row in df.iterrows():
        # Geometry
        geom = None
        if pd.notna(row.get("geometry_wkt")):
            try:
                geom = mapping(from_wkt(row["geometry_wkt"]))
            except Exception as exc:
                logger.warning("Could not parse WKT for harbour %s: %s", row["harbour_id"], exc)

        cells = list(row["h3_cells"]) if isinstance(row["h3_cells"], (list, tuple)) else []

        feature = {
            "type": "Feature",
            "geometry": geom,
            "properties": {
                "harbour_id":             row["harbour_id"],
                "h3_cells":               cells,
                "n_cells":                int(row["n_cells"]),
                "n_events":               int(row["n_events"]),
                "n_unique_mmsi_approx":   int(row["n_unique_mmsi_approx"]),
                "n_draught_changes":      int(row["n_draught_changes"]),
                "centroid_lat":           float(row["centroid_lat"]),
                "centroid_lon":           float(row["centroid_lon"]),
                "country_iso2":           row["country_iso2"] or "",
                "country_name":           row["country_name"] or "",
                "nearest_city":           row["nearest_city"] or "",
                "nearest_city_dist_km":   round(float(row["nearest_city_dist_km"]), 3),
                "admin1":                 row["admin1"] or "",
                "matched_existing":       bool(row["matched_existing"]),
            },
        }
        features.append(feature)

    geojson = {"type": "FeatureCollection", "features": features}

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    logger.info("Wrote harbours.geojson (%d features) → %s", len(features), out_path)
    return out_path


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_phase5(config: Phase5Config) -> tuple[Path, Path]:
    """
    Returns (parquet_path, geojson_path).
    """
    enriched_path = Path(config.interim_dir) / "harbours_enriched.parquet"
    if not enriched_path.exists():
        raise FileNotFoundError(
            f"harbours_enriched.parquet not found at {enriched_path} — run phase4 first"
        )

    logger.info("Phase 5: reading %s …", enriched_path)
    enriched = pd.read_parquet(enriched_path)
    logger.info("  loaded %d enriched clusters", len(enriched))

    # Load existing harbour DB (optional)
    cell_index:    dict[str, str] = {}
    centroid_list: list[dict]     = []

    if config.existing_db_path:
        existing = _load_existing_db(config.existing_db_path)
        cell_index, centroid_list = _build_indexes(existing)
    else:
        logger.info("No existing harbour DB supplied — all IDs will be newly generated.")

    # Assign IDs
    result = _assign_ids(enriched, cell_index, centroid_list, config)

    # Write outputs
    out_dir = Path(config.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = _write_parquet(result, out_dir)
    geojson_path = _write_geojson(result, out_dir)

    return parquet_path, geojson_path
