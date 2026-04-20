"""Unit tests for Phase 2 H3 aggregation."""

from datetime import timezone
from pathlib import Path

import h3
import pandas as pd
import pytest

from pipeline.h3_aggregation import (
    Phase2Config,
    _aggregate,
    _assign_h3_cells,
    _classify_ship_type,
    _filter_cells,
    run_phase2,
)

RES = 11

# Hamburg harbour area
LAT, LON = 53.54, 9.97


def _make_stops(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["timestamp_start"] = pd.Timestamp("2024-01-01", tz="UTC")
    df["timestamp_end"]   = pd.Timestamp("2024-01-01 02:00", tz="UTC")
    df["n_messages"]           = 20
    df["pos_variance_meters"]  = 10.0
    df["nav_status"]           = 5
    df["detection_method"]     = "nav_status"
    # New optional columns — default to None if not provided in rows
    for col in ("ship_type", "destination_locode", "destination_raw"):
        if col not in df.columns:
            df[col] = None
    return df


def _base_config(tmp_path: Path) -> Phase2Config:
    return Phase2Config(interim_dir=str(tmp_path), h3_resolution=RES, min_unique_mmsi=2)


# ---------------------------------------------------------------------------

def test_h3_cell_assigned():
    stops = _make_stops([{"mmsi": 111111111, "lat": LAT, "lon": LON,
                          "duration_minutes": 60.0, "draught_delta": None}])
    result = _assign_h3_cells(stops, RES)
    assert "h3_cell" in result.columns
    cell = result.iloc[0]["h3_cell"]
    assert h3.get_resolution(cell) == RES


def test_cell_centre_matches_h3():
    stops = _make_stops([{"mmsi": 111111111, "lat": LAT, "lon": LON,
                          "duration_minutes": 60.0, "draught_delta": None}])
    stops = _assign_h3_cells(stops, RES)
    config = Phase2Config(interim_dir="", h3_resolution=RES)
    agg = _aggregate(stops, config)

    expected_lat, expected_lon = h3.cell_to_latlng(agg.iloc[0]["h3_cell"])
    assert abs(agg.iloc[0]["cell_lat"] - expected_lat) < 1e-9
    assert abs(agg.iloc[0]["cell_lon"] - expected_lon) < 1e-9


def test_unique_mmsi_counted_correctly():
    # 3 stops from 2 distinct vessels at the same location
    stops = _make_stops([
        {"mmsi": 111111111, "lat": LAT, "lon": LON, "duration_minutes": 60.0, "draught_delta": None},
        {"mmsi": 111111111, "lat": LAT, "lon": LON, "duration_minutes": 45.0, "draught_delta": None},
        {"mmsi": 222222222, "lat": LAT, "lon": LON, "duration_minutes": 90.0, "draught_delta": None},
    ])
    stops = _assign_h3_cells(stops, RES)
    config = Phase2Config(interim_dir="", h3_resolution=RES)
    agg = _aggregate(stops, config)

    assert agg.iloc[0]["n_unique_mmsi"] == 2
    assert agg.iloc[0]["n_events"]      == 3


def test_filter_removes_low_traffic_cells():
    stops = _make_stops([
        {"mmsi": 111111111, "lat": LAT,       "lon": LON,       "duration_minutes": 60.0, "draught_delta": None},
        # busy cell: 3 vessels nearby (same H3 cell)
        {"mmsi": 222222222, "lat": LAT,       "lon": LON,       "duration_minutes": 60.0, "draught_delta": None},
        {"mmsi": 333333333, "lat": LAT,       "lon": LON,       "duration_minutes": 60.0, "draught_delta": None},
        # quiet cell: 1 vessel far away
        {"mmsi": 444444444, "lat": LAT + 5.0, "lon": LON + 5.0, "duration_minutes": 60.0, "draught_delta": None},
    ])
    stops = _assign_h3_cells(stops, RES)
    config = Phase2Config(interim_dir="", h3_resolution=RES, min_unique_mmsi=2)
    agg    = _aggregate(stops, config)
    result = _filter_cells(agg, config)

    assert len(result) == 1
    assert result.iloc[0]["n_unique_mmsi"] == 3


def test_draught_change_counted():
    stops = _make_stops([
        {"mmsi": 111111111, "lat": LAT, "lon": LON, "duration_minutes": 60.0, "draught_delta":  1.2},
        {"mmsi": 222222222, "lat": LAT, "lon": LON, "duration_minutes": 60.0, "draught_delta":  0.1},
        {"mmsi": 333333333, "lat": LAT, "lon": LON, "duration_minutes": 60.0, "draught_delta": -0.8},
    ])
    stops = _assign_h3_cells(stops, RES)
    config = Phase2Config(interim_dir="", h3_resolution=RES, draught_change_threshold_m=0.3)
    agg = _aggregate(stops, config)

    # 1.2 and 0.8 exceed threshold, 0.1 does not
    assert agg.iloc[0]["n_draught_changes"] == 2


# ---------------------------------------------------------------------------
# Return-visit tests
# ---------------------------------------------------------------------------

def test_max_visits_per_mmsi_single_vessel_multiple_stops():
    # Same vessel stops 3 times in same cell, another vessel once
    stops = _make_stops([
        {"mmsi": 111111111, "lat": LAT, "lon": LON, "duration_minutes": 60.0, "draught_delta": None},
        {"mmsi": 111111111, "lat": LAT, "lon": LON, "duration_minutes": 60.0, "draught_delta": None},
        {"mmsi": 111111111, "lat": LAT, "lon": LON, "duration_minutes": 60.0, "draught_delta": None},
        {"mmsi": 222222222, "lat": LAT, "lon": LON, "duration_minutes": 60.0, "draught_delta": None},
    ])
    stops = _assign_h3_cells(stops, RES)
    config = Phase2Config(interim_dir="", h3_resolution=RES)
    agg = _aggregate(stops, config)
    assert agg.iloc[0]["max_visits_per_mmsi"] == 3


def test_mean_visits_per_mmsi():
    # 3 stops from vessel A, 1 from vessel B → mean = (3+1)/2 = 2.0
    stops = _make_stops([
        {"mmsi": 111111111, "lat": LAT, "lon": LON, "duration_minutes": 60.0, "draught_delta": None},
        {"mmsi": 111111111, "lat": LAT, "lon": LON, "duration_minutes": 60.0, "draught_delta": None},
        {"mmsi": 111111111, "lat": LAT, "lon": LON, "duration_minutes": 60.0, "draught_delta": None},
        {"mmsi": 222222222, "lat": LAT, "lon": LON, "duration_minutes": 60.0, "draught_delta": None},
    ])
    stops = _assign_h3_cells(stops, RES)
    config = Phase2Config(interim_dir="", h3_resolution=RES)
    agg = _aggregate(stops, config)
    assert abs(agg.iloc[0]["mean_visits_per_mmsi"] - 2.0) < 0.01


# ---------------------------------------------------------------------------
# Vessel type distribution tests
# ---------------------------------------------------------------------------

def test_classify_ship_type_categories():
    assert _classify_ship_type(70)  == "cargo"
    assert _classify_ship_type(85)  == "tanker"
    assert _classify_ship_type(60)  == "passenger"
    assert _classify_ship_type(30)  == "fishing"
    assert _classify_ship_type(36)  == "recreational"
    assert _classify_ship_type(52)  == "tug_pilot"
    assert _classify_ship_type(0)   == "other"
    assert _classify_ship_type(None)== "unknown"


def test_vessel_type_counts_in_aggregation():
    stops = _make_stops([
        {"mmsi": 111111111, "lat": LAT, "lon": LON, "duration_minutes": 60.0,
         "draught_delta": None, "ship_type": 70},   # cargo
        {"mmsi": 222222222, "lat": LAT, "lon": LON, "duration_minutes": 60.0,
         "draught_delta": None, "ship_type": 80},   # tanker
        {"mmsi": 333333333, "lat": LAT, "lon": LON, "duration_minutes": 60.0,
         "draught_delta": None, "ship_type": 60},   # passenger
        {"mmsi": 444444444, "lat": LAT, "lon": LON, "duration_minutes": 60.0,
         "draught_delta": None, "ship_type": 70},   # cargo
    ])
    stops = _assign_h3_cells(stops, RES)
    config = Phase2Config(interim_dir="", h3_resolution=RES)
    agg = _aggregate(stops, config)
    assert agg.iloc[0]["n_cargo"]     == 2
    assert agg.iloc[0]["n_tanker"]    == 1
    assert agg.iloc[0]["n_passenger"] == 1
    assert agg.iloc[0]["n_fishing"]   == 0


# ---------------------------------------------------------------------------
# Destination LOCODE tests
# ---------------------------------------------------------------------------

def test_top_destination_locode():
    stops = _make_stops([
        {"mmsi": 111111111, "lat": LAT, "lon": LON, "duration_minutes": 60.0,
         "draught_delta": None, "destination_locode": "DEHAM"},
        {"mmsi": 222222222, "lat": LAT, "lon": LON, "duration_minutes": 60.0,
         "draught_delta": None, "destination_locode": "DEHAM"},
        {"mmsi": 333333333, "lat": LAT, "lon": LON, "duration_minutes": 60.0,
         "draught_delta": None, "destination_locode": "NLRTM"},
    ])
    stops = _assign_h3_cells(stops, RES)
    config = Phase2Config(interim_dir="", h3_resolution=RES)
    agg = _aggregate(stops, config)
    assert agg.iloc[0]["top_destination_locode"] == "DEHAM"


def test_top_destination_locode_null_when_absent():
    stops = _make_stops([
        {"mmsi": 111111111, "lat": LAT, "lon": LON, "duration_minutes": 60.0,
         "draught_delta": None},
    ])
    stops = _assign_h3_cells(stops, RES)
    config = Phase2Config(interim_dir="", h3_resolution=RES)
    agg = _aggregate(stops, config)
    assert agg.iloc[0]["top_destination_locode"] is None


# ---------------------------------------------------------------------------
# End-to-end
# ---------------------------------------------------------------------------

def test_run_phase2_end_to_end(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pipeline.extract_stops import STOP_SCHEMA

    stops = _make_stops([
        {"mmsi": 111111111, "lat": LAT, "lon": LON, "duration_minutes": 60.0,
         "draught_delta": 0.5, "ship_type": 70, "destination_locode": "DEHAM"},
        {"mmsi": 222222222, "lat": LAT, "lon": LON, "duration_minutes": 90.0,
         "draught_delta": None, "ship_type": 80, "destination_locode": "DEHAM"},
        {"mmsi": 333333333, "lat": LAT, "lon": LON, "duration_minutes": 45.0,
         "draught_delta": None, "ship_type": None, "destination_locode": None},
    ])
    stops["draught_arrival"]    = None
    stops["draught_departure"]  = None
    stops["destination_raw"]    = None
    stops["timestamp_start"]    = pd.to_datetime(stops["timestamp_start"]).dt.tz_convert("UTC")
    stops["timestamp_end"]      = pd.to_datetime(stops["timestamp_end"]).dt.tz_convert("UTC")
    pq.write_table(pa.Table.from_pandas(stops, schema=STOP_SCHEMA, safe=False),
                   tmp_path / "stops.parquet")

    config = _base_config(tmp_path)
    out    = run_phase2(config)

    result = pd.read_parquet(out)
    assert len(result) == 1
    row = result.iloc[0]
    assert row["n_unique_mmsi"]          >= 2
    assert row["max_visits_per_mmsi"]    == 1
    assert row["n_cargo"]                == 1
    assert row["n_tanker"]               == 1
    assert row["top_destination_locode"] == "DEHAM"
