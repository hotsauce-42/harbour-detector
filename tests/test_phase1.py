"""
Unit tests for Phase 1 stop extraction.
Uses synthetic in-memory Parquet data — no real AIS files needed.
"""

from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline.extract_stops import (
    Phase1Config,
    _group_into_segments,
    _join_type5_data,
    _label_detection_method,
    _parse_locode,
)


def _ts(hour: int) -> pd.Timestamp:
    return pd.Timestamp(f"2024-01-01 {hour:02d}:00:00", tz="UTC")


def _make_position_rows(
    mmsi: int,
    start_hour: int,
    n_messages: int,
    sog: float,
    nav_status: int,
    lat: float = 53.5,
    lon: float = 9.9,
) -> list[dict]:
    rows = []
    for i in range(n_messages):
        rows.append({
            "mmsi": mmsi,
            "timestamp": _ts(start_hour) + timedelta(minutes=i * 5),
            "lat": lat + np.random.uniform(-0.0001, 0.0001),
            "lon": lon + np.random.uniform(-0.0001, 0.0001),
            "sog": sog,
            "nav_status": nav_status,
            "msg_type": 1,
            "draught": None,
        })
    return rows


_EPOCH = pd.Timestamp("1970-01-01", tz="UTC")


def _write_parquet(rows: list[dict], path: Path) -> None:
    """
    Write a raw-AIS fixture file. Timestamps are stored as integer Unix
    seconds, matching the real raw data (see the Gotchas in CLAUDE.md).

    The subtraction-and-divide keeps this independent of the datetime64
    resolution pandas happens to parse into (ns on pandas 2, us on pandas 3).
    """
    df = pd.DataFrame(rows)
    ts = pd.to_datetime(df["timestamp"], utc=True)
    df["timestamp"] = ((ts - _EPOCH) // pd.Timedelta(1, "s")).astype("int64")
    pq.write_table(pa.Table.from_pandas(df), path)


def _build_fixture(tmp_path: Path) -> Phase1Config:
    """Write a small raw-AIS fixture and return the matching Phase 1 config."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    # Vessel A: moored at Hamburg for 2 hours (24 messages × 5min)
    rows = _make_position_rows(123456789, start_hour=0, n_messages=24,
                               sog=0.1, nav_status=5, lat=53.54, lon=9.97)
    # Vessel B: moving, should NOT produce a stop
    rows += _make_position_rows(234567890, start_hour=0, n_messages=24,
                                sog=8.0, nav_status=0, lat=53.0, lon=9.0)
    # Vessel C: slow but status=0 (sustained-speed detection only)
    rows += _make_position_rows(345678901, start_hour=0, n_messages=12,
                                sog=0.2, nav_status=0, lat=51.9, lon=4.47)

    _write_parquet(rows, raw_dir / "2024-01-01_00.parquet")

    return Phase1Config(
        raw_glob=str(raw_dir / "*.parquet"),
        interim_dir=str(tmp_path / "interim"),
        sog_threshold_knots=0.5,
        min_stop_duration_minutes=30.0,
        min_messages_per_stop=3,
        positional_variance_max_meters=300.0,
        max_gap_minutes=15.0,
        moored_nav_statuses=[1, 5],
    )


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession for the Phase 1 end-to-end tests (needs a JVM)."""
    pyspark = pytest.importorskip("pyspark")
    session = (
        pyspark.sql.SparkSession.builder
        .appName("harbour-detector-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def spark_stops(spark, tmp_path_factory):
    """Run Phase 1 once over the shared fixture data and return the stops."""
    from pipeline.extract_stops_spark import run_phase1

    tmp_path = tmp_path_factory.mktemp("phase1")
    config = _build_fixture(tmp_path)
    out = run_phase1(config, spark)
    return pd.read_parquet(out)


def test_moored_vessel_produces_stop(spark_stops):
    vessel_a = spark_stops[spark_stops["mmsi"] == 123456789]
    assert len(vessel_a) == 1
    assert vessel_a.iloc[0]["duration_minutes"] >= 30


def test_moving_vessel_excluded(spark_stops):
    assert 234567890 not in spark_stops["mmsi"].values


def test_sustained_speed_vessel_detected(spark_stops):
    vessel_c = spark_stops[spark_stops["mmsi"] == 345678901]
    assert len(vessel_c) == 1
    assert vessel_c.iloc[0]["detection_method"] == "sustained_speed"


def test_detection_method_label():
    stops = pd.DataFrame([
        {"mmsi": 1, "nav_status": 5, "lat": 0.0, "lon": 0.0,
         "timestamp_start": _ts(0), "timestamp_end": _ts(2),
         "duration_minutes": 120.0, "n_messages": 10, "pos_variance_meters": 5.0},
        {"mmsi": 2, "nav_status": 0, "lat": 0.0, "lon": 0.0,
         "timestamp_start": _ts(0), "timestamp_end": _ts(2),
         "duration_minutes": 120.0, "n_messages": 10, "pos_variance_meters": 5.0},
    ])
    config = Phase1Config(raw_glob="", interim_dir="", moored_nav_statuses=[1, 5])
    result = _label_detection_method(stops, config)
    assert result.loc[0, "detection_method"] == "nav_status"
    assert result.loc[1, "detection_method"] == "sustained_speed"


def test_draught_join():
    stops = pd.DataFrame([{
        "mmsi": 123456789,
        "timestamp_start": _ts(2),
        "timestamp_end":   _ts(6),
    }])
    type5 = pd.DataFrame([
        {"mmsi": 123456789, "timestamp": _ts(1), "draught": 5.2,
         "destination": "DEHAM", "ship_type": 70},
        {"mmsi": 123456789, "timestamp": _ts(8), "draught": 4.1,
         "destination": "DEHAM", "ship_type": 70},
    ])
    type5["timestamp"] = pd.to_datetime(type5["timestamp"], utc=True)

    config = Phase1Config(raw_glob="", interim_dir="", draught_lookup_hours=6)
    result = _join_type5_data(stops, type5, config)

    assert abs(result.iloc[0]["draught_arrival"]   - 5.2) < 0.01
    assert abs(result.iloc[0]["draught_departure"] - 4.1) < 0.01
    assert abs(result.iloc[0]["draught_delta"]     - (-1.1)) < 0.01


def test_destination_locode_joined():
    stops = pd.DataFrame([{
        "mmsi": 123456789,
        "timestamp_start": _ts(2),
        "timestamp_end":   _ts(6),
    }])
    type5 = pd.DataFrame([
        {"mmsi": 123456789, "timestamp": _ts(1), "draught": 5.0,
         "destination": "DEHAM", "ship_type": 70},
    ])
    type5["timestamp"] = pd.to_datetime(type5["timestamp"], utc=True)
    config = Phase1Config(raw_glob="", interim_dir="", draught_lookup_hours=6)
    result = _join_type5_data(stops, type5, config)

    assert result.iloc[0]["destination_raw"]    == "DEHAM"
    assert result.iloc[0]["destination_locode"] == "DEHAM"


def test_ship_type_joined_as_mode():
    stops = pd.DataFrame([
        {"mmsi": 111111111, "timestamp_start": _ts(2), "timestamp_end": _ts(4)},
        {"mmsi": 111111111, "timestamp_start": _ts(10), "timestamp_end": _ts(12)},
    ])
    type5 = pd.DataFrame([
        {"mmsi": 111111111, "timestamp": _ts(0), "draught": 5.0,
         "destination": None, "ship_type": 70},
        {"mmsi": 111111111, "timestamp": _ts(6), "draught": 5.0,
         "destination": None, "ship_type": 70},
        {"mmsi": 111111111, "timestamp": _ts(8), "draught": 5.0,
         "destination": None, "ship_type": 71},
    ])
    type5["timestamp"] = pd.to_datetime(type5["timestamp"], utc=True)
    config = Phase1Config(raw_glob="", interim_dir="", draught_lookup_hours=6)
    result = _join_type5_data(stops, type5, config)
    # mode of [70, 70, 71] = 70 for both stops
    assert int(result.iloc[0]["ship_type"]) == 70
    assert int(result.iloc[1]["ship_type"]) == 70


# LOCODE parsing tests
def test_parse_locode_exact():
    assert _parse_locode("DEHAM") == "DEHAM"

def test_parse_locode_spaced():
    assert _parse_locode("DE HAM") == "DEHAM"

def test_parse_locode_embedded():
    assert _parse_locode("EN ROUTE DEHAM") == "DEHAM"

def test_parse_locode_with_city():
    assert _parse_locode("DEHAM-HAMBURG") == "DEHAM"

def test_parse_locode_free_text_no_locode():
    assert _parse_locode("HAMBURG") is None

def test_parse_locode_empty():
    assert _parse_locode("") is None
    assert _parse_locode(None) is None


def test_positional_variance_rejects_drifting_vessel():
    """A vessel drifting slowly over a large area should not produce a stop."""
    rows = []
    for i in range(20):
        rows.append({
            "mmsi": 111111111,
            "timestamp": _ts(0) + timedelta(minutes=i * 5),
            "lat": 53.0 + i * 0.01,   # drifting ~1km per step
            "lon": 9.0,
            "sog": 0.3,
            "nav_status": 0,
        })
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    config = Phase1Config(
        raw_glob="", interim_dir="",
        positional_variance_max_meters=300.0,
        min_stop_duration_minutes=30.0,
        min_messages_per_stop=3,
        max_gap_minutes=15.0,
    )
    segments = _group_into_segments(df, config)
    assert len(segments) == 0
