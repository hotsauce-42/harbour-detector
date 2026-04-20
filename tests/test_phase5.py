"""Unit tests for Phase 5 ID matching and GeoJSON export."""

import json
import uuid
from pathlib import Path

import h3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from shapely.wkt import loads as from_wkt

from pipeline.enrichment import ENRICHED_SCHEMA
from pipeline.id_matching import (
    Phase5Config,
    _assign_ids,
    _build_indexes,
    _find_match,
    _jaccard,
    _write_geojson,
    make_harbour_id,
    run_phase5,
)
from utils.geo import haversine_meters

RES = 11
HAMBURG_LAT,   HAMBURG_LON   = 53.54,  9.97
ROTTERDAM_LAT, ROTTERDAM_LON = 51.90,  4.47

_HARBOUR_NS = uuid.UUID("b8d7e3a2-5f1c-4e8b-9a6d-3c7f2e1b4a5d")


def _cells(lat: float, lon: float, rings: int = 1) -> list[str]:
    seed = h3.latlng_to_cell(lat, lon, RES)
    return sorted(h3.grid_disk(seed, rings))


def _enriched_row(cluster_id: int, lat: float, lon: float) -> dict:
    cells = _cells(lat, lon)
    return {
        "cluster_id":           cluster_id,
        "h3_cells":             cells,
        "n_cells":              len(cells),
        "n_events":             50,
        "n_unique_mmsi_approx": 15,
        "n_draught_changes":    2,
        "centroid_lat":         lat,
        "centroid_lon":         lon,
        "centroid_h3_r8":       h3.latlng_to_cell(lat, lon, 8),
        "bbox_min_lat":         lat - 0.001,
        "bbox_max_lat":         lat + 0.001,
        "bbox_min_lon":         lon - 0.001,
        "bbox_max_lon":         lon + 0.001,
        "geometry_wkt":         None,
        "country_iso2":         "DE",
        "country_name":         "Germany",
        "nearest_city":         "Hamburg",
        "nearest_city_lat":     lat,
        "nearest_city_lon":     lon,
        "nearest_city_dist_km": 1.0,
        "admin1":               "Hamburg",
    }


def _write_enriched(rows: list[dict], path: Path) -> None:
    df = pd.DataFrame(rows)
    h3_arr = pa.array(df["h3_cells"].tolist(), type=pa.list_(pa.string()))
    table = pa.table(
        {c: pa.array(df[c].tolist()) for c in df.columns if c != "h3_cells"} |
        {"h3_cells": h3_arr},
        schema=ENRICHED_SCHEMA,
    )
    pq.write_table(table, path)


def _base_config(tmp_path: Path, existing_db: str = None) -> Phase5Config:
    return Phase5Config(
        interim_dir=str(tmp_path),
        output_dir=str(tmp_path / "output"),
        existing_db_path=existing_db,
        h3_jaccard_threshold=0.3,
        centroid_match_distance_meters=500.0,
    )


# ---------------------------------------------------------------------------

def test_make_harbour_id_is_deterministic():
    cell = h3.latlng_to_cell(HAMBURG_LAT, HAMBURG_LON, 8)
    assert make_harbour_id(cell) == make_harbour_id(cell)


def test_make_harbour_id_differs_for_different_cells():
    cell_a = h3.latlng_to_cell(HAMBURG_LAT,   HAMBURG_LON,   8)
    cell_b = h3.latlng_to_cell(ROTTERDAM_LAT, ROTTERDAM_LON, 8)
    assert make_harbour_id(cell_a) != make_harbour_id(cell_b)


def test_make_harbour_id_is_valid_uuid():
    cell = h3.latlng_to_cell(HAMBURG_LAT, HAMBURG_LON, 8)
    uid = make_harbour_id(cell)
    parsed = uuid.UUID(uid)   # raises if not valid
    assert parsed.version == 5


def test_jaccard_identical_sets():
    s = {"a", "b", "c"}
    assert _jaccard(s, s) == 1.0


def test_jaccard_disjoint_sets():
    assert _jaccard({"a"}, {"b"}) == 0.0


def test_jaccard_partial_overlap():
    assert abs(_jaccard({"a", "b"}, {"b", "c"}) - 1/3) < 1e-9


def test_build_indexes_maps_cells():
    cells = _cells(HAMBURG_LAT, HAMBURG_LON)
    existing = pd.DataFrame([{
        "harbour_id":   "existing-123",
        "centroid_lat": HAMBURG_LAT,
        "centroid_lon": HAMBURG_LON,
        "h3_cells":     cells,
    }])
    cell_idx, centroid_list = _build_indexes(existing)
    for cell in cells:
        assert cell_idx[cell] == "existing-123"


def test_find_match_by_jaccard():
    cells = set(_cells(HAMBURG_LAT, HAMBURG_LON))
    existing = pd.DataFrame([{
        "harbour_id":   "existing-123",
        "centroid_lat": HAMBURG_LAT,
        "centroid_lon": HAMBURG_LON,
        "h3_cells":     list(cells),
    }])
    cell_idx, centroid_list = _build_indexes(existing)
    config = Phase5Config(interim_dir="", output_dir="",
                          h3_jaccard_threshold=0.3,
                          centroid_match_distance_meters=500.0)

    result = _find_match(cells, HAMBURG_LAT, HAMBURG_LON, cell_idx, centroid_list, config)
    assert result == "existing-123"


def test_find_match_by_centroid_distance():
    # No h3_cells in existing — falls through to distance match
    existing = pd.DataFrame([{
        "harbour_id":   "existing-456",
        "centroid_lat": HAMBURG_LAT + 0.001,   # ~100m away
        "centroid_lon": HAMBURG_LON,
    }])
    cell_idx, centroid_list = _build_indexes(existing)
    config = Phase5Config(interim_dir="", output_dir="",
                          centroid_match_distance_meters=500.0)

    result = _find_match(set(), HAMBURG_LAT, HAMBURG_LON, cell_idx, centroid_list, config)
    assert result == "existing-456"


def test_find_match_returns_none_when_too_far():
    existing = pd.DataFrame([{
        "harbour_id":   "existing-far",
        "centroid_lat": ROTTERDAM_LAT,
        "centroid_lon": ROTTERDAM_LON,
    }])
    cell_idx, centroid_list = _build_indexes(existing)
    config = Phase5Config(interim_dir="", output_dir="",
                          centroid_match_distance_meters=500.0)

    result = _find_match(set(), HAMBURG_LAT, HAMBURG_LON, cell_idx, centroid_list, config)
    assert result is None


def test_assign_ids_reuses_existing():
    row = _enriched_row(0, HAMBURG_LAT, HAMBURG_LON)
    enriched = pd.DataFrame([row])

    existing = pd.DataFrame([{
        "harbour_id":   "existing-abc",
        "centroid_lat": HAMBURG_LAT,
        "centroid_lon": HAMBURG_LON,
        "h3_cells":     row["h3_cells"],
    }])
    cell_idx, centroid_list = _build_indexes(existing)

    config = Phase5Config(interim_dir="", output_dir="")
    result = _assign_ids(enriched, cell_idx, centroid_list, config)
    assert result.iloc[0]["harbour_id"] == "existing-abc"
    assert result.iloc[0]["matched_existing"] == True


def test_assign_ids_generates_new_when_no_match():
    row = _enriched_row(0, HAMBURG_LAT, HAMBURG_LON)
    enriched = pd.DataFrame([row])
    config = Phase5Config(interim_dir="", output_dir="")

    result = _assign_ids(enriched, {}, [], config)
    expected = make_harbour_id(row["centroid_h3_r8"])
    assert result.iloc[0]["harbour_id"] == expected
    assert result.iloc[0]["matched_existing"] == False


def test_geojson_structure(tmp_path):
    from shapely.geometry import mapping
    from shapely.wkt import dumps as to_wkt
    import h3 as _h3

    cells = _cells(HAMBURG_LAT, HAMBURG_LON)
    geo   = _h3.cells_to_geo(cells)
    from shapely.geometry import shape
    wkt = to_wkt(shape(geo))

    row = _enriched_row(0, HAMBURG_LAT, HAMBURG_LON)
    row["geometry_wkt"] = wkt
    df  = pd.DataFrame([row])
    df["harbour_id"]       = "test-harbour-id"
    df["matched_existing"] = False

    out = _write_geojson(df, tmp_path)
    with open(out) as f:
        fc = json.load(f)

    assert fc["type"] == "FeatureCollection"
    feat = fc["features"][0]
    assert feat["type"] == "Feature"
    assert feat["geometry"]["type"] in ("Polygon", "MultiPolygon")
    props = feat["properties"]
    assert props["harbour_id"] == "test-harbour-id"
    assert isinstance(props["h3_cells"], list)
    assert "country_name" in props
    assert "nearest_city" in props


def test_run_phase5_no_existing_db(tmp_path):
    rows = [
        _enriched_row(0, HAMBURG_LAT,   HAMBURG_LON),
        _enriched_row(1, ROTTERDAM_LAT, ROTTERDAM_LON),
    ]
    _write_enriched(rows, tmp_path / "harbours_enriched.parquet")

    config = _base_config(tmp_path)
    parquet_path, geojson_path = run_phase5(config)

    df = pd.read_parquet(parquet_path)
    assert len(df) == 2
    assert df["harbour_id"].nunique() == 2
    assert df["matched_existing"].sum() == 0

    with open(geojson_path) as f:
        fc = json.load(f)
    assert len(fc["features"]) == 2


def test_run_phase5_with_existing_db_geojson(tmp_path):
    cells = _cells(HAMBURG_LAT, HAMBURG_LON)
    rows = [_enriched_row(0, HAMBURG_LAT, HAMBURG_LON)]
    _write_enriched(rows, tmp_path / "harbours_enriched.parquet")

    # Write a minimal existing-db GeoJSON
    existing_geojson = {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "geometry": None,
            "properties": {
                "harbour_id":   "legacy-hh-001",
                "centroid_lat": HAMBURG_LAT,
                "centroid_lon": HAMBURG_LON,
                "h3_cells":     cells,
            },
        }],
    }
    db_path = tmp_path / "existing.geojson"
    with open(db_path, "w") as f:
        json.dump(existing_geojson, f)

    config = _base_config(tmp_path, existing_db=str(db_path))
    parquet_path, _ = run_phase5(config)

    df = pd.read_parquet(parquet_path)
    assert df.iloc[0]["harbour_id"] == "legacy-hh-001"
    assert df.iloc[0]["matched_existing"] == True
