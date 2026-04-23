"""
Phase 2: H3 Aggregation

Reads stops.parquet, maps each stop to an H3 cell (resolution 11),
aggregates per cell, and filters cells below the minimum unique-vessel
threshold.

Output: data/interim/h3_counts.parquet
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path

import h3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from utils.s3 import (
    build_s3_config,
    ensure_dir,
    get_s3_filesystem,
    get_s3_storage_options,
    is_s3_path,
    path_join,
)

logger = logging.getLogger(__name__)

H3_COUNTS_SCHEMA = pa.schema([
    pa.field("h3_cell",                pa.string()),
    pa.field("n_unique_mmsi",          pa.int32()),
    pa.field("n_events",               pa.int32()),
    pa.field("total_duration_minutes", pa.float32()),
    pa.field("mean_duration_minutes",  pa.float32()),
    pa.field("cell_lat",               pa.float64()),
    pa.field("cell_lon",               pa.float64()),
    pa.field("n_draught_changes",      pa.int32()),
    # Return-visit indicators
    pa.field("max_visits_per_mmsi",    pa.int32()),     # max times any single vessel visited
    pa.field("mean_visits_per_mmsi",   pa.float32()),   # average visits per vessel
    # Vessel type distribution
    pa.field("n_cargo",                pa.int32()),
    pa.field("n_tanker",               pa.int32()),
    pa.field("n_passenger",            pa.int32()),
    pa.field("n_fishing",              pa.int32()),
    pa.field("n_recreational",         pa.int32()),
    pa.field("n_tug_pilot",            pa.int32()),
    # Destination signal
    pa.field("top_destination_locode", pa.string()),    # most-reported LOCODE before arrival
])


@dataclass
class Phase2Config:
    interim_dir: str
    h3_resolution: int = 11
    min_unique_mmsi: int = 5
    draught_change_threshold_m: float = 0.3  # metres — minimum |delta| to count as a change
    s3_cfg: dict = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, cfg: dict) -> "Phase2Config":
        p2 = cfg.get("phase2", {})
        return cls(
            interim_dir=cfg.get("data", {}).get("interim_dir", "data/interim"),
            h3_resolution=p2.get("h3_resolution", 11),
            min_unique_mmsi=p2.get("min_unique_mmsi", 5),
            draught_change_threshold_m=p2.get("draught_change_threshold_m", 0.3),
            s3_cfg=build_s3_config(cfg.get("s3", {})),
        )


# AIS ship type → simplified category
def _classify_ship_type(ship_type) -> str:
    try:
        t = int(ship_type)
    except (TypeError, ValueError):
        return "unknown"
    if t in (30, 31, 32):
        return "fishing"
    if t in (36, 37):
        return "recreational"
    if t in (50, 51, 52):
        return "tug_pilot"
    if 60 <= t <= 69:
        return "passenger"
    if 70 <= t <= 79:
        return "cargo"
    if 80 <= t <= 89:
        return "tanker"
    return "other"


# ---------------------------------------------------------------------------
# Step 1: map stops → H3 cells
# ---------------------------------------------------------------------------

def _assign_h3_cells(stops: pd.DataFrame, resolution: int) -> pd.DataFrame:
    """Add an h3_cell column to the stops DataFrame."""
    logger.info("Assigning H3 cells (resolution %d) to %d stops …", resolution, len(stops))

    stops = stops.copy()
    stops["h3_cell"] = [
        h3.latlng_to_cell(lat, lon, resolution)
        for lat, lon in zip(stops["lat"], stops["lon"])
    ]
    return stops


# ---------------------------------------------------------------------------
# Step 2: aggregate per cell
# ---------------------------------------------------------------------------

def _aggregate(stops: pd.DataFrame, config: Phase2Config) -> pd.DataFrame:
    """Group stops by H3 cell and compute per-cell statistics."""
    logger.info("Aggregating stops by H3 cell …")

    stops = stops.copy()

    # Draught change flag
    stops["_draught_change"] = (
        pd.to_numeric(stops["draught_delta"], errors="coerce").abs() >= config.draught_change_threshold_m
        if "draught_delta" in stops.columns
        else pd.Series(False, index=stops.index)
    ).fillna(False)

    # Vessel type flags
    if "ship_type" in stops.columns:
        stops["_vessel_cat"] = stops["ship_type"].apply(_classify_ship_type)
    else:
        stops["_vessel_cat"] = "unknown"
    stops["_is_cargo"]       = stops["_vessel_cat"] == "cargo"
    stops["_is_tanker"]      = stops["_vessel_cat"] == "tanker"
    stops["_is_passenger"]   = stops["_vessel_cat"] == "passenger"
    stops["_is_fishing"]     = stops["_vessel_cat"] == "fishing"
    stops["_is_recreational"]= stops["_vessel_cat"] == "recreational"
    stops["_is_tug_pilot"]   = stops["_vessel_cat"] == "tug_pilot"

    # Base aggregation
    agg = (
        stops.groupby("h3_cell")
        .agg(
            n_unique_mmsi         =("mmsi",              "nunique"),
            n_events              =("mmsi",              "count"),
            total_duration_minutes=("duration_minutes",  "sum"),
            mean_duration_minutes =("duration_minutes",  "mean"),
            n_draught_changes     =("_draught_change",   "sum"),
            n_cargo               =("_is_cargo",         "sum"),
            n_tanker              =("_is_tanker",        "sum"),
            n_passenger           =("_is_passenger",     "sum"),
            n_fishing             =("_is_fishing",       "sum"),
            n_recreational        =("_is_recreational",  "sum"),
            n_tug_pilot           =("_is_tug_pilot",     "sum"),
        )
        .reset_index()
    )

    # Return-visit stats: visits per (cell, mmsi) → max and mean per cell
    visits = (
        stops.groupby(["h3_cell", "mmsi"])
        .size()
        .reset_index(name="visits")
        .groupby("h3_cell")["visits"]
        .agg(max_visits_per_mmsi="max", mean_visits_per_mmsi="mean")
        .reset_index()
    )
    agg = agg.merge(visits, on="h3_cell", how="left")

    # Top destination LOCODE: most frequently reported LOCODE among stops in this cell
    if "destination_locode" in stops.columns:
        dest_stops = stops[stops["destination_locode"].notna() & (stops["destination_locode"] != "")]
        if not dest_stops.empty:
            top_dest = (
                dest_stops.groupby("h3_cell")["destination_locode"]
                .agg(lambda s: s.mode().iloc[0])
                .reset_index()
                .rename(columns={"destination_locode": "top_destination_locode"})
            )
            agg = agg.merge(top_dest, on="h3_cell", how="left")
        else:
            agg["top_destination_locode"] = None
    else:
        agg["top_destination_locode"] = None

    # Deterministic cell centre
    cell_centres = [h3.cell_to_latlng(c) for c in agg["h3_cell"]]
    agg["cell_lat"] = [c[0] for c in cell_centres]
    agg["cell_lon"] = [c[1] for c in cell_centres]

    int_cols   = ("n_unique_mmsi", "n_events", "n_draught_changes",
                  "n_cargo", "n_tanker", "n_passenger", "n_fishing",
                  "n_recreational", "n_tug_pilot", "max_visits_per_mmsi")
    float_cols = ("total_duration_minutes", "mean_duration_minutes", "mean_visits_per_mmsi")
    for col in int_cols:
        agg[col] = agg[col].astype("int32")
    for col in float_cols:
        agg[col] = agg[col].astype("float32")

    logger.info("  produced %d unique H3 cells", len(agg))
    return agg


# ---------------------------------------------------------------------------
# Step 3: filter noise
# ---------------------------------------------------------------------------

def _filter_cells(agg: pd.DataFrame, config: Phase2Config) -> pd.DataFrame:
    """Keep only cells visited by at least min_unique_mmsi distinct vessels."""
    before = len(agg)
    agg = agg[agg["n_unique_mmsi"] >= config.min_unique_mmsi].reset_index(drop=True)
    logger.info(
        "Filtered cells: %d → %d  (min_unique_mmsi=%d)",
        before, len(agg), config.min_unique_mmsi,
    )
    return agg


# ---------------------------------------------------------------------------
# Step 4: write output
# ---------------------------------------------------------------------------

def _write_h3_counts(agg: pd.DataFrame, config: Phase2Config) -> str:
    out_path = path_join(config.interim_dir, "h3_counts.parquet")
    # Ensure new string column is properly typed even when all-null
    agg = agg.copy()
    agg["top_destination_locode"] = agg["top_destination_locode"].astype(object).where(
        agg["top_destination_locode"].notna(), other=None
    )
    table = pa.Table.from_pandas(agg, schema=H3_COUNTS_SCHEMA, safe=False)
    if is_s3_path(config.interim_dir):
        fs = get_s3_filesystem(config.s3_cfg)
        with fs.open(out_path, "wb") as fh:
            pq.write_table(table, fh, compression="snappy")
    else:
        pq.write_table(table, out_path, compression="snappy")
    logger.info("Wrote %d H3 cells → %s", len(agg), out_path)
    return out_path


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_phase2(config: Phase2Config) -> str:
    stops_path = path_join(config.interim_dir, "stops.parquet")
    if not is_s3_path(config.interim_dir) and not Path(stops_path).exists():
        raise FileNotFoundError(f"stops.parquet not found at {stops_path} — run phase1 first")

    logger.info("Phase 2: reading %s …", stops_path)
    if is_s3_path(config.interim_dir):
        stops = pd.read_parquet(stops_path, storage_options=get_s3_storage_options(config.s3_cfg))
    else:
        stops = pd.read_parquet(stops_path)
    logger.info("  loaded %d stop events", len(stops))

    stops  = _assign_h3_cells(stops, config.h3_resolution)
    agg    = _aggregate(stops, config)
    agg    = _filter_cells(agg, config)
    return _write_h3_counts(agg, config)
