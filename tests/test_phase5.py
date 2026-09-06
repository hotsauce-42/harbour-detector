"""Unit tests for Phase 5 ID matching and GeoJSON export."""

import json
import uuid
from pathlib import Path
from typing import Optional

import h3
import numpy as np
import pandas as pd
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import box, shape
from shapely.wkt import dumps as to_wkt
from shapely.wkt import loads as from_wkt

from pipeline.enrichment import ENRICHED_SCHEMA
from pipeline.id_matching import (
    ManualState,
    _apply_manual_lock_area,
    _apply_manual_transit,
    _load_existing_db,
    cell_list,
    Phase5Config,
    _apply_manual_outlines,
    _assign_ids,
    _build_indexes,
    _find_match,
    _jaccard,
    _write_geojson,
    make_harbour_id,
    run_phase5,
)
from utils.overrides import (
    DETECTED_OUTLINE_KEY,
    MANUAL_LOCK_AREA_KEY,
    DETECTED_TRANSIT_KEY,
    MANUAL_OUTLINE_KEY,
    MANUAL_TRANSIT_KEY,
)

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
        "n_unique_mmsi":        15,
        "n_draught_changes":    2,
        # Carried through from Phase 4; an ordinary harbour, not a transit site.
        "mean_dwell_minutes":   400.0,
        "max_visits_per_mmsi":  8,
        "n_cargo":              0,
        "n_tanker":             0,
        "n_passenger":          0,
        "n_fishing":            0,
        "n_recreational":       30,
        "n_tug_pilot":          0,
        "transit_like":         False,
        "centroid_lat":         lat,
        "centroid_lon":         lon,
        "centroid_id_cell":       h3.latlng_to_cell(lat, lon, 8),
        "bbox_min_lat":         lat - 0.001,
        "bbox_max_lat":         lat + 0.001,
        "bbox_min_lon":         lon - 0.001,
        "bbox_max_lon":         lon + 0.001,
        "geometry_wkt":         None,
        "outline_wkt":          None,
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


def _base_config(tmp_path: Path, existing_db: Optional[str] = None) -> Phase5Config:
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


def test_make_harbour_id_format():
    cell = h3.latlng_to_cell(HAMBURG_LAT, HAMBURG_LON, 8)
    hid = make_harbour_id(cell, "DE")
    country, _, hex8 = hid.partition("-")
    assert country == "DE"
    assert len(hex8) == 8
    int(hex8, 16)   # raises if not valid hex
    # deterministic across calls; ZZ fallback when country unknown
    assert make_harbour_id(cell, "DE") == hid
    assert make_harbour_id(cell).startswith("ZZ-")


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
    cell_idx, centroid_list, *_ = _build_indexes(existing)
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
    cell_idx, centroid_list, *_ = _build_indexes(existing)
    config = Phase5Config(interim_dir="", output_dir="",
                          h3_jaccard_threshold=0.3,
                          centroid_match_distance_meters=500.0)

    result = _find_match(cells, HAMBURG_LAT, HAMBURG_LON,
                         cell_idx, centroid_list, config)
    assert result == "existing-123"


def test_find_match_by_centroid_distance():
    # No h3_cells in existing — falls through to distance match
    existing = pd.DataFrame([{
        "harbour_id":   "existing-456",
        "centroid_lat": HAMBURG_LAT + 0.001,   # ~100m away
        "centroid_lon": HAMBURG_LON,
    }])
    cell_idx, centroid_list, *_ = _build_indexes(existing)
    config = Phase5Config(interim_dir="", output_dir="",
                          centroid_match_distance_meters=500.0)

    result = _find_match(set(), HAMBURG_LAT, HAMBURG_LON,
                         cell_idx, centroid_list, config)
    assert result == "existing-456"


def test_find_match_returns_none_when_too_far():
    existing = pd.DataFrame([{
        "harbour_id":   "existing-far",
        "centroid_lat": ROTTERDAM_LAT,
        "centroid_lon": ROTTERDAM_LON,
    }])
    cell_idx, centroid_list, *_ = _build_indexes(existing)
    config = Phase5Config(interim_dir="", output_dir="",
                          centroid_match_distance_meters=500.0)

    result = _find_match(set(), HAMBURG_LAT, HAMBURG_LON,
                         cell_idx, centroid_list, config)
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
    cell_idx, centroid_list, *_ = _build_indexes(existing)

    config = Phase5Config(interim_dir="", output_dir="")
    result = _assign_ids(enriched, cell_idx, centroid_list, config)
    assert result.iloc[0]["harbour_id"] == "existing-abc"
    assert result.iloc[0]["matched_existing"]


def test_assign_ids_generates_new_when_no_match():
    row = _enriched_row(0, HAMBURG_LAT, HAMBURG_LON)
    enriched = pd.DataFrame([row])
    config = Phase5Config(interim_dir="", output_dir="")

    result = _assign_ids(enriched, {}, [], config)
    expected = make_harbour_id(row["centroid_id_cell"], row["country_iso2"])
    assert result.iloc[0]["harbour_id"] == expected
    assert not result.iloc[0]["matched_existing"]


def test_geojson_structure(tmp_path):
    from shapely.wkt import dumps as to_wkt
    import h3 as _h3

    cells = _cells(HAMBURG_LAT, HAMBURG_LON)
    geo   = _h3.cells_to_geo(cells)
    from shapely.geometry import shape
    wkt = to_wkt(shape(geo))

    row = _enriched_row(0, HAMBURG_LAT, HAMBURG_LON)
    row["geometry_wkt"] = wkt
    row["outline_wkt"]  = wkt
    df  = pd.DataFrame([row])
    df["harbour_id"]       = "test-harbour-id"
    df["matched_existing"] = False

    out = _write_geojson(df, str(tmp_path), {})
    with open(out) as f:
        fc = json.load(f)

    assert fc["type"] == "FeatureCollection"
    feat = fc["features"][0]
    assert feat["type"] == "Feature"
    assert feat["geometry"]["type"] in ("Polygon", "MultiPolygon")
    props = feat["properties"]
    assert props["harbour_id"] == "test-harbour-id"
    assert props["geometry_kind"] == "outline"
    assert isinstance(props["h3_cells"], list)
    assert "country_name" in props
    assert "nearest_city" in props


def test_geojson_geometry_column_selects_source(tmp_path):
    """_write_geojson takes its geometry from the requested WKT column."""
    from shapely.geometry import shape
    from shapely.wkt import dumps as to_wkt

    cells_wkt = to_wkt(shape(h3.cells_to_geo(_cells(HAMBURG_LAT, HAMBURG_LON))))

    row = _enriched_row(0, HAMBURG_LAT, HAMBURG_LON)
    row["geometry_wkt"] = cells_wkt
    row["outline_wkt"]  = None          # only the cell union is available
    df  = pd.DataFrame([row])
    df["harbour_id"]       = "test-harbour-id"
    df["matched_existing"] = False

    out = _write_geojson(df, str(tmp_path), {},
                         geometry_col="geometry_wkt",
                         filename="harbours_cells.geojson")
    assert Path(out).name == "harbours_cells.geojson"
    with open(out) as f:
        feat = json.load(f)["features"][0]

    assert feat["properties"]["geometry_kind"] == "cells"
    assert feat["geometry"] is not None


def test_run_phase5_no_existing_db(tmp_path):
    rows = [
        _enriched_row(0, HAMBURG_LAT,   HAMBURG_LON),
        _enriched_row(1, ROTTERDAM_LAT, ROTTERDAM_LON),
    ]
    _write_enriched(rows, tmp_path / "harbours_enriched.parquet")

    config = _base_config(tmp_path)
    parquet_path, geojson_path, cells_path = run_phase5(config)

    df = pd.read_parquet(parquet_path)
    assert len(df) == 2
    assert df["harbour_id"].nunique() == 2
    assert df["matched_existing"].sum() == 0

    with open(geojson_path) as f:
        fc = json.load(f)
    assert len(fc["features"]) == 2

    # Both geometry flavours are exported, each into its own file.
    assert Path(geojson_path).name == "harbours.geojson"
    assert Path(cells_path).name   == "harbours_cells.geojson"
    with open(cells_path) as f:
        cells_fc = json.load(f)
    assert len(cells_fc["features"]) == 2
    assert fc["features"][0]["properties"]["geometry_kind"]       == "outline"
    assert cells_fc["features"][0]["properties"]["geometry_kind"] == "cells"
    assert {"geometry_wkt", "outline_wkt"} <= set(df.columns)


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
    parquet_path, _, _ = run_phase5(config)

    df = pd.read_parquet(parquet_path)
    assert df.iloc[0]["harbour_id"] == "legacy-hh-001"
    assert df.iloc[0]["matched_existing"]


# ---------------------------------------------------------------------------
# Manual overrides carried across a re-run
# ---------------------------------------------------------------------------

def _existing_db_geojson(path: Path, props: dict) -> None:
    fc = {
        "type": "FeatureCollection",
        "features": [{"type": "Feature", "geometry": None, "properties": props}],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(fc, f)


def test_assign_ids_applies_manual_overrides_on_match():
    """A matched harbour keeps the corrected city instead of the geocoded one."""
    row = _enriched_row(0, HAMBURG_LAT, HAMBURG_LON)
    enriched = pd.DataFrame([row])
    existing = pd.DataFrame([{
        "harbour_id":       "existing-abc",
        "centroid_lat":     HAMBURG_LAT,
        "centroid_lon":     HAMBURG_LON,
        "h3_cells":         row["h3_cells"],
        "nearest_city":     "Hamburg-Altona",
        "manual_overrides": ["nearest_city"],
    }])
    cell_idx, centroid_list, manual = _build_indexes(existing)
    overrides = manual.overrides

    config = Phase5Config(interim_dir="", output_dir="")
    result = _assign_ids(enriched, cell_idx, centroid_list, config,
                         ManualState(overrides=overrides))

    assert result.iloc[0]["nearest_city"] == "Hamburg-Altona"
    assert list(result.iloc[0]["manual_overrides"]) == ["nearest_city"]
    # Unmarked fields still come from Phase 4's geocoding.
    assert result.iloc[0]["admin1"] == "Hamburg"


def test_assign_ids_ignores_unmarked_existing_values():
    """Existing values that were never edited must not freeze the fresh ones."""
    row = _enriched_row(0, HAMBURG_LAT, HAMBURG_LON)
    enriched = pd.DataFrame([row])
    existing = pd.DataFrame([{
        "harbour_id":   "existing-abc",
        "centroid_lat": HAMBURG_LAT,
        "centroid_lon": HAMBURG_LON,
        "h3_cells":     row["h3_cells"],
        "nearest_city": "Stale Name",       # present, but not marked
    }])
    cell_idx, centroid_list, manual = _build_indexes(existing)
    overrides = manual.overrides

    config = Phase5Config(interim_dir="", output_dir="")
    result = _assign_ids(enriched, cell_idx, centroid_list, config,
                         ManualState(overrides=overrides))

    assert result.iloc[0]["nearest_city"] == "Hamburg"
    assert list(result.iloc[0]["manual_overrides"]) == []


def test_assign_ids_no_overrides_for_unmatched_harbour():
    row = _enriched_row(0, HAMBURG_LAT, HAMBURG_LON)
    result = _assign_ids(pd.DataFrame([row]), {}, [],
                         Phase5Config(interim_dir="", output_dir=""))
    assert list(result.iloc[0]["manual_overrides"]) == []


def test_assign_ids_override_applies_only_to_matching_row():
    """With several clusters, a correction must not leak onto its neighbours."""
    hh = _enriched_row(0, HAMBURG_LAT, HAMBURG_LON)
    rt = _enriched_row(1, ROTTERDAM_LAT, ROTTERDAM_LON)
    enriched = pd.DataFrame([hh, rt])

    existing = pd.DataFrame([{
        "harbour_id":       "existing-rotterdam",
        "centroid_lat":     ROTTERDAM_LAT,
        "centroid_lon":     ROTTERDAM_LON,
        "h3_cells":         rt["h3_cells"],
        "nearest_city":     "Rotterdam-Maasvlakte",
        "manual_overrides": ["nearest_city"],
    }])
    cell_idx, centroid_list, manual = _build_indexes(existing)
    overrides = manual.overrides

    config = Phase5Config(interim_dir="", output_dir="")
    result = _assign_ids(enriched, cell_idx, centroid_list, config,
                         ManualState(overrides=overrides))

    by_cluster = result.set_index("cluster_id")
    assert by_cluster.loc[1, "nearest_city"] == "Rotterdam-Maasvlakte"
    assert by_cluster.loc[0, "nearest_city"] == "Hamburg"   # untouched


def test_run_phase5_survives_manual_edit_round_trip(tmp_path):
    """
    End-to-end: a GUI correction stored in the existing DB reappears in the
    freshly generated output, and stays marked for the run after that.
    """
    cells = _cells(HAMBURG_LAT, HAMBURG_LON)
    rows  = [_enriched_row(0, HAMBURG_LAT, HAMBURG_LON)]
    _write_enriched(rows, tmp_path / "harbours_enriched.parquet")

    db_path = tmp_path / "existing.geojson"
    _existing_db_geojson(db_path, {
        "harbour_id":       "legacy-hh-001",
        "centroid_lat":     HAMBURG_LAT,
        "centroid_lon":     HAMBURG_LON,
        "h3_cells":         cells,
        "nearest_city":     "Hamburg-Altona",   # hand-corrected in the GUI
        "country_name":     "Germany",
        "country_iso2":     "DE",
        "manual_overrides": ["nearest_city", "country_name"],
    })

    config = _base_config(tmp_path, existing_db=str(db_path))
    parquet_path, geojson_path, cells_path = run_phase5(config)

    df = pd.read_parquet(parquet_path)
    assert df.iloc[0]["harbour_id"] == "legacy-hh-001"
    assert df.iloc[0]["nearest_city"] == "Hamburg-Altona"
    assert sorted(df.iloc[0]["manual_overrides"]) == ["country_name",
                                                      "nearest_city"]

    # The marker must round-trip through both GeoJSON files, otherwise the
    # correction is lost the next time one of them is used as the existing DB.
    for path in (geojson_path, cells_path):
        with open(path, encoding="utf-8") as f:
            props = json.load(f)["features"][0]["properties"]
        assert props["nearest_city"] == "Hamburg-Altona"
        assert sorted(props["manual_overrides"]) == ["country_name",
                                                     "nearest_city"]


# ---------------------------------------------------------------------------
# Manually drawn outlines — a floor the pipeline may grow, never shrink
# ---------------------------------------------------------------------------

def _detected_outline(lat: float, lon: float) -> str:
    """The outline Phase 4 would hand Phase 5 for this harbour."""
    return to_wkt(shape(h3.cells_to_geo(_cells(lat, lon))))


def _drawn_extension(detected_wkt: str) -> str:
    """A quay an operator drew onto the east side, overlapping the outline."""
    _, miny, maxx, maxy = from_wkt(detected_wkt).bounds
    return to_wkt(box(maxx - 0.0005, miny, maxx + 0.003, maxy))


def _assert_covers(merged_wkt: str, original_wkt: str) -> None:
    """No area of `original` was given up — see utils.geo.merge_outlines."""
    merged, original = from_wkt(merged_wkt), from_wkt(original_wkt)
    assert original.difference(merged).area <= original.area * 1e-9


def _outline_frame(detected: Optional[str], drawn: Optional[str]) -> pd.DataFrame:
    return pd.DataFrame([{
        "harbour_id":       "DE-abcd1234",
        "outline_wkt":      detected,
        MANUAL_OUTLINE_KEY: drawn,
    }])


def test_manual_outline_extends_the_detected_one():
    detected = _detected_outline(HAMBURG_LAT, HAMBURG_LON)
    drawn    = _drawn_extension(detected)

    result = _apply_manual_outlines(_outline_frame(detected, drawn))
    merged = result.iloc[0]["outline_wkt"]

    _assert_covers(merged, detected)
    _assert_covers(merged, drawn)
    assert from_wkt(merged).area > from_wkt(detected).area


def test_manual_outline_cannot_shrink_the_detected_one():
    """An inward edit is unioned away — the harbour never loses detected area."""
    detected = _detected_outline(HAMBURG_LAT, HAMBURG_LON)
    minx, miny, maxx, maxy = from_wkt(detected).bounds
    drawn = to_wkt(box(minx, miny, (minx + maxx) / 2, (miny + maxy) / 2))

    result = _apply_manual_outlines(_outline_frame(detected, drawn))

    _assert_covers(result.iloc[0]["outline_wkt"], detected)


def test_detected_outline_is_kept_alongside_the_merged_one():
    detected = _detected_outline(HAMBURG_LAT, HAMBURG_LON)
    drawn    = _drawn_extension(detected)

    result = _apply_manual_outlines(_outline_frame(detected, drawn))

    assert result.iloc[0][DETECTED_OUTLINE_KEY] == detected
    assert result.iloc[0]["outline_wkt"] != detected


def test_manual_outline_is_never_rewritten():
    """The drawn baseline stays frozen however far the harbour grows around it."""
    detected = _detected_outline(HAMBURG_LAT, HAMBURG_LON)
    drawn    = _drawn_extension(detected)

    once  = _apply_manual_outlines(_outline_frame(detected, drawn))
    twice = _apply_manual_outlines(once)

    assert once.iloc[0][MANUAL_OUTLINE_KEY] == drawn
    assert twice.iloc[0][MANUAL_OUTLINE_KEY] == drawn


def test_harbour_without_a_manual_outline_is_untouched():
    detected = _detected_outline(HAMBURG_LAT, HAMBURG_LON)

    result = _apply_manual_outlines(_outline_frame(detected, None))

    assert result.iloc[0]["outline_wkt"] == detected
    assert result.iloc[0][DETECTED_OUTLINE_KEY] == detected


def test_unparseable_manual_outline_keeps_the_detected_one():
    """One bad polygon in the existing DB must not cost a whole run."""
    detected = _detected_outline(HAMBURG_LAT, HAMBURG_LON)

    result = _apply_manual_outlines(_outline_frame(detected, "POLYGON ((not wkt))"))

    assert result.iloc[0]["outline_wkt"] == detected


def test_manual_outline_stands_in_when_nothing_was_detected():
    drawn = to_wkt(box(9.9, 53.5, 9.91, 53.51))

    result = _apply_manual_outlines(_outline_frame(None, drawn))

    _assert_covers(result.iloc[0]["outline_wkt"], drawn)


def test_build_indexes_collects_manual_outlines():
    drawn = to_wkt(box(9.9, 53.5, 9.91, 53.51))
    existing = pd.DataFrame([
        {"harbour_id": "DE-drawn", MANUAL_OUTLINE_KEY: drawn},
        {"harbour_id": "DE-plain", MANUAL_OUTLINE_KEY: None},
    ])

    *_, manual = _build_indexes(existing)
    outlines = manual.outlines

    assert outlines == {"DE-drawn": drawn}


def test_assign_ids_attaches_a_manual_outline_only_to_the_matched_harbour():
    hh = _enriched_row(0, HAMBURG_LAT,   HAMBURG_LON)
    rt = _enriched_row(1, ROTTERDAM_LAT, ROTTERDAM_LON)
    enriched = pd.DataFrame([hh, rt])

    existing = pd.DataFrame([{
        "harbour_id":       "legacy-hh-001",
        "centroid_lat":     HAMBURG_LAT,
        "centroid_lon":     HAMBURG_LON,
        "h3_cells":         hh["h3_cells"],
        MANUAL_OUTLINE_KEY: to_wkt(box(9.9, 53.5, 9.91, 53.51)),
    }])
    cell_idx, centroid_list, manual = _build_indexes(existing)
    overrides, outlines = manual.overrides, manual.outlines

    config = Phase5Config(interim_dir="", output_dir="")
    result = _assign_ids(enriched, cell_idx, centroid_list, config,
                         ManualState(overrides=overrides, outlines=outlines))

    by_cluster = result.set_index("cluster_id")
    assert by_cluster.loc[0, MANUAL_OUTLINE_KEY] == outlines["legacy-hh-001"]
    # pandas 3 stores the column as `str`, so the unmatched harbour's None
    # comes back as NaN rather than None.
    assert pd.isna(by_cluster.loc[1, MANUAL_OUTLINE_KEY])


def test_run_phase5_merges_a_manual_outline_end_to_end(tmp_path):
    """
    A drawn outline stored in the existing DB widens the exported harbour and
    comes back out verbatim, ready for the run after that.
    """
    row = _enriched_row(0, HAMBURG_LAT, HAMBURG_LON)
    detected = _detected_outline(HAMBURG_LAT, HAMBURG_LON)
    row["geometry_wkt"] = detected
    row["outline_wkt"]  = detected
    _write_enriched([row], tmp_path / "harbours_enriched.parquet")

    drawn   = _drawn_extension(detected)
    db_path = tmp_path / "existing.geojson"
    _existing_db_geojson(db_path, {
        "harbour_id":       "legacy-hh-001",
        "centroid_lat":     HAMBURG_LAT,
        "centroid_lon":     HAMBURG_LON,
        "h3_cells":         row["h3_cells"],
        MANUAL_OUTLINE_KEY: drawn,
    })

    config = _base_config(tmp_path, existing_db=str(db_path))
    parquet_path, geojson_path, cells_path = run_phase5(config)

    df = pd.read_parquet(parquet_path)
    assert df.iloc[0]["harbour_id"] == "legacy-hh-001"
    _assert_covers(df.iloc[0]["outline_wkt"], drawn)
    _assert_covers(df.iloc[0]["outline_wkt"], detected)
    assert df.iloc[0][MANUAL_OUTLINE_KEY] == drawn
    assert df.iloc[0][DETECTED_OUTLINE_KEY] == detected

    # The exported outline geometry is the merged one …
    with open(geojson_path, encoding="utf-8") as f:
        outline_feat = json.load(f)["features"][0]
    _assert_covers(to_wkt(shape(outline_feat["geometry"])), drawn)

    # … and both files carry the baseline, so either can be the next existing DB.
    for path in (geojson_path, cells_path):
        with open(path, encoding="utf-8") as f:
            props = json.load(f)["features"][0]["properties"]
        assert props[MANUAL_OUTLINE_KEY] == drawn
        assert props[DETECTED_OUTLINE_KEY] == detected


def test_run_phase5_leaves_the_outline_columns_null_for_new_harbours(tmp_path):
    row = _enriched_row(0, HAMBURG_LAT, HAMBURG_LON)
    row["geometry_wkt"] = row["outline_wkt"] = _detected_outline(HAMBURG_LAT,
                                                                 HAMBURG_LON)
    _write_enriched([row], tmp_path / "harbours_enriched.parquet")

    parquet_path, geojson_path, _ = run_phase5(_base_config(tmp_path))

    df = pd.read_parquet(parquet_path)
    assert pd.isna(df.iloc[0][MANUAL_OUTLINE_KEY])
    assert df.iloc[0][DETECTED_OUTLINE_KEY] == row["outline_wkt"]

    with open(geojson_path, encoding="utf-8") as f:
        props = json.load(f)["features"][0]["properties"]
    assert props[MANUAL_OUTLINE_KEY] is None


# ---------------------------------------------------------------------------
# The operator's lock verdict, round-tripped
# ---------------------------------------------------------------------------

def _transit_run(tmp_path, *, detected: bool, stored) -> pd.Series:
    """Run Phase 5 with a stored verdict over a given detected one."""
    row = _enriched_row(0, HAMBURG_LAT, HAMBURG_LON)
    outline = _detected_outline(HAMBURG_LAT, HAMBURG_LON)
    row["geometry_wkt"] = outline
    row["outline_wkt"] = outline
    row["transit_like"] = detected
    _write_enriched([row], tmp_path / "harbours_enriched.parquet")

    record = {
        "harbour_id":   "legacy-hh-001",
        "centroid_lat": HAMBURG_LAT,
        "centroid_lon": HAMBURG_LON,
        "h3_cells":     row["h3_cells"],
    }
    if stored is not None:
        record[MANUAL_TRANSIT_KEY] = stored
    db_path = tmp_path / "existing.geojson"
    _existing_db_geojson(db_path, record)

    parquet_path, _, _ = run_phase5(_base_config(tmp_path, existing_db=str(db_path)))
    return pd.read_parquet(parquet_path).iloc[0]


def test_a_stored_verdict_promotes_a_site_the_detector_missed(tmp_path):
    row = _transit_run(tmp_path, detected=False, stored=True)

    assert bool(row["transit_like"]) is True
    assert bool(row[DETECTED_TRANSIT_KEY]) is False   # what Phase 4 thought
    assert bool(row[MANUAL_TRANSIT_KEY]) is True


def test_a_stored_verdict_clears_a_false_positive(tmp_path):
    """
    The direction that matters most: a person overruling a heuristic that
    flagged a real harbour, and having it stay overruled on every future run.
    """
    row = _transit_run(tmp_path, detected=True, stored=False)

    assert bool(row["transit_like"]) is False
    assert bool(row[DETECTED_TRANSIT_KEY]) is True


def test_without_a_stored_verdict_the_detector_stands(tmp_path):
    row = _transit_run(tmp_path, detected=True, stored=None)

    assert bool(row["transit_like"]) is True
    assert pd.isna(row[MANUAL_TRANSIT_KEY])


def test_a_new_harbour_carries_no_verdict(tmp_path):
    """Nothing to inherit when there is no existing record to inherit from."""
    row = _enriched_row(0, HAMBURG_LAT, HAMBURG_LON)
    outline = _detected_outline(HAMBURG_LAT, HAMBURG_LON)
    row["geometry_wkt"] = outline
    row["outline_wkt"] = outline
    row["transit_like"] = True
    _write_enriched([row], tmp_path / "harbours_enriched.parquet")

    parquet_path, _, _ = run_phase5(_base_config(tmp_path))
    result = pd.read_parquet(parquet_path).iloc[0]

    assert pd.isna(result[MANUAL_TRANSIT_KEY])
    assert bool(result["transit_like"]) is True


def test_centroid_match_takes_the_nearest_not_the_first():
    """
    Two existing harbours inside the 500 m radius, the exact match listed
    second. Walking the list and accepting the first hit picked a 496 m
    neighbour over a 0 m match on real data, orphaning one id and giving
    another to two clusters at once.
    """
    lat, lon = HAMBURG_LAT, HAMBURG_LON
    centroid_list = [
        # ~400 m north, and listed first
        {"harbour_id": "near-miss", "centroid_lat": lat + 0.0036,
         "centroid_lon": lon, "h3_cells": []},
        {"harbour_id": "exact", "centroid_lat": lat,
         "centroid_lon": lon, "h3_cells": []},
    ]
    config = _base_config(Path("."))

    assert _find_match(set(), lat, lon, {}, centroid_list, config) == "exact"


def test_centroid_match_still_returns_nothing_when_everything_is_too_far():
    lat, lon = HAMBURG_LAT, HAMBURG_LON
    centroid_list = [
        {"harbour_id": "far", "centroid_lat": lat + 0.05,
         "centroid_lon": lon, "h3_cells": []},
    ]
    assert _find_match(set(), lat, lon, {}, centroid_list,
                       _base_config(Path("."))) is None


# ---------------------------------------------------------------------------
# h3_cells survives a Parquet round-trip
# ---------------------------------------------------------------------------

def test_cell_list_accepts_what_parquet_actually_hands_back():
    """
    The shape that broke everything: a Parquet round-trip returns a numpy
    array, and `isinstance(ndarray, (list, tuple))` is False. Testing for
    list/tuple treated every Parquet-loaded harbour as cell-less, which fed
    `_find_match` an empty set (so Jaccard matching could never fire) and wrote
    `"h3_cells": []` into both GeoJSON outputs.
    """
    cells = ["8b1f05908259fff", "8b1f0590824afff"]

    assert cell_list(np.array(cells, dtype=object)) == cells
    assert cell_list(np.array(cells)) == cells          # numpy str dtype
    assert cell_list(cells) == cells
    assert cell_list(tuple(cells)) == cells


def test_cell_list_treats_every_shape_of_missing_as_no_cells():
    assert cell_list(None) == []
    assert cell_list(float("nan")) == []
    assert cell_list([]) == []


def test_the_geojson_carries_the_cells_it_was_built_from(tmp_path):
    """
    Both outputs are documented as usable as the existing database, so cells
    have to reach them — otherwise a database built from one silently falls
    back to centroid-only matching.
    """
    row = _enriched_row(0, HAMBURG_LAT, HAMBURG_LON)
    outline = _detected_outline(HAMBURG_LAT, HAMBURG_LON)
    row["geometry_wkt"] = outline
    row["outline_wkt"] = outline
    _write_enriched([row], tmp_path / "harbours_enriched.parquet")

    _, geojson_path, cells_path = run_phase5(_base_config(tmp_path))

    for path in (geojson_path, cells_path):
        feature = json.loads(Path(path).read_text())["features"][0]
        assert feature["properties"]["h3_cells"] == sorted(row["h3_cells"])


def test_a_database_written_by_phase5_can_drive_jaccard_matching(tmp_path):
    """The round trip that matters: output → existing DB → cell index."""
    row = _enriched_row(0, HAMBURG_LAT, HAMBURG_LON)
    outline = _detected_outline(HAMBURG_LAT, HAMBURG_LON)
    row["geometry_wkt"] = outline
    row["outline_wkt"] = outline
    _write_enriched([row], tmp_path / "harbours_enriched.parquet")
    _, geojson_path, _ = run_phase5(_base_config(tmp_path))

    cell_index, _, *_ = _build_indexes(_load_existing_db(str(geojson_path), {}))

    assert len(cell_index) == len(row["h3_cells"])


# ---------------------------------------------------------------------------
# An existing harbour can only be claimed once
# ---------------------------------------------------------------------------

def _two_clusters_near(lat: float, lon: float, offset: float) -> pd.DataFrame:
    """Two clusters, the second `offset` degrees north of the first."""
    return pd.DataFrame([
        _enriched_row(0, lat, lon),
        _enriched_row(1, lat + offset, lon),
    ])


def test_two_clusters_cannot_share_one_existing_harbour():
    """
    500 m is generous next to how close real harbours get — two Hellerup
    basins are 442 m apart — so both fall inside one existing harbour's radius.
    Letting both match hands them the same id and seeds a duplicate that
    re-matches both clusters on every later run.
    """
    enriched = _two_clusters_near(HAMBURG_LAT, HAMBURG_LON, 0.003)   # ~330 m
    centroid_list = [{"harbour_id": "existing", "centroid_lat": HAMBURG_LAT,
                      "centroid_lon": HAMBURG_LON, "h3_cells": []}]

    result = _assign_ids(enriched, {}, centroid_list, _base_config(Path(".")))

    assert result["harbour_id"].nunique() == 2
    # The nearer cluster keeps it; the other is issued a fresh id.
    assert result.iloc[0]["harbour_id"] == "existing"
    assert bool(result.iloc[0]["matched_existing"]) is True
    assert result.iloc[1]["harbour_id"] != "existing"
    assert bool(result.iloc[1]["matched_existing"]) is False


def test_the_closer_cluster_wins_whichever_order_they_arrive_in():
    """The winner is decided by match quality, not by row order."""
    enriched = pd.DataFrame([
        _enriched_row(0, HAMBURG_LAT + 0.003, HAMBURG_LON),   # further away
        _enriched_row(1, HAMBURG_LAT, HAMBURG_LON),           # exact
    ])
    centroid_list = [{"harbour_id": "existing", "centroid_lat": HAMBURG_LAT,
                      "centroid_lon": HAMBURG_LON, "h3_cells": []}]

    result = _assign_ids(enriched, {}, centroid_list, _base_config(Path(".")))

    assert result.iloc[1]["harbour_id"] == "existing"
    assert result.iloc[0]["harbour_id"] != "existing"


def test_an_h3_overlap_claim_outranks_a_centroid_only_claim():
    """
    Sharing cells with an existing harbour is stronger evidence than merely
    being near its centroid, so it wins the contest even from further away.
    """
    far = _enriched_row(0, HAMBURG_LAT + 0.004, HAMBURG_LON)   # ~445 m, shares cells
    near = _enriched_row(1, HAMBURG_LAT, HAMBURG_LON)          # 0 m, no overlap
    enriched = pd.DataFrame([far, near])

    cell_index = {cell: "existing" for cell in far["h3_cells"]}
    centroid_list = [{"harbour_id": "existing",
                      "centroid_lat": HAMBURG_LAT + 0.004,
                      "centroid_lon": HAMBURG_LON,
                      "h3_cells": far["h3_cells"]}]

    result = _assign_ids(enriched, cell_index, centroid_list,
                         _base_config(Path(".")))

    assert result.iloc[0]["harbour_id"] == "existing"
    assert result.iloc[1]["harbour_id"] != "existing"


def test_uncontested_matching_is_unchanged():
    """Two clusters, two existing harbours, one each — nothing to resolve."""
    enriched = _two_clusters_near(HAMBURG_LAT, HAMBURG_LON, 0.05)   # far apart
    centroid_list = [
        {"harbour_id": "a", "centroid_lat": HAMBURG_LAT,
         "centroid_lon": HAMBURG_LON, "h3_cells": []},
        {"harbour_id": "b", "centroid_lat": HAMBURG_LAT + 0.05,
         "centroid_lon": HAMBURG_LON, "h3_cells": []},
    ]

    result = _assign_ids(enriched, {}, centroid_list, _base_config(Path(".")))

    assert list(result["harbour_id"]) == ["a", "b"]
    assert [bool(v) for v in result["matched_existing"]] == [True, True]


# ---------------------------------------------------------------------------
# A lock drawn over part of a harbour
# ---------------------------------------------------------------------------

BRUNS_TUG = ["8b1f1590510dfff", "8b1f15905166fff", "8b1f15905175fff"]
BRUNS_LOCK = ["8b1f15905c46fff", "8b1f15905c6afff"]
LOCK_BOX = to_wkt(box(9.140, 53.892, 9.150, 53.897))


def _lock_frame(area=None, manual_verdict=None, detected=False) -> pd.DataFrame:
    return pd.DataFrame([{
        "harbour_id": "DE-bruns",
        "h3_cells": BRUNS_LOCK + BRUNS_TUG,
        MANUAL_LOCK_AREA_KEY: area,
        MANUAL_TRANSIT_KEY: manual_verdict,
        "transit_like": detected,
        DETECTED_TRANSIT_KEY: detected,
    }])


def test_lock_cells_are_the_cells_inside_the_drawn_area():
    """
    Brunsbüttel's lock and its tug berth are 406 m apart in one harbour record.
    The drawn area is what says which cells are which.
    """
    result = _apply_manual_lock_area(_lock_frame(LOCK_BOX)).iloc[0]

    assert sorted(result["lock_cells"]) == sorted(BRUNS_LOCK)
    assert result["lock_cell_share"] == pytest.approx(2 / 5)


def test_no_drawn_area_leaves_the_cells_empty():
    result = _apply_manual_lock_area(_lock_frame(None)).iloc[0]

    assert list(result["lock_cells"]) == []
    assert result["lock_cell_share"] == 0.0


def test_unparseable_lock_geometry_is_skipped_not_fatal():
    """One bad polygon in the database must not cost a whole run."""
    result = _apply_manual_lock_area(_lock_frame("NOT WKT")).iloc[0]

    assert list(result["lock_cells"]) == []


def test_a_drawn_area_marks_the_site_as_transit():
    frame = _apply_manual_lock_area(_lock_frame(LOCK_BOX, detected=False))

    assert bool(_apply_manual_transit(frame).iloc[0]["transit_like"]) is True


def test_an_explicit_verdict_outranks_a_drawn_area():
    frame = _apply_manual_lock_area(
        _lock_frame(LOCK_BOX, manual_verdict=False, detected=True)
    )

    assert bool(_apply_manual_transit(frame).iloc[0]["transit_like"]) is False


def test_a_lock_area_survives_a_phase5_run_without_changing_the_id(tmp_path):
    """
    The whole design in one assertion: marking a sub-area annotates, it does
    not split, so the harbour keeps its identity.
    """
    row = _enriched_row(0, HAMBURG_LAT, HAMBURG_LON)
    outline = _detected_outline(HAMBURG_LAT, HAMBURG_LON)
    row["geometry_wkt"] = outline
    row["outline_wkt"] = outline
    _write_enriched([row], tmp_path / "harbours_enriched.parquet")

    # An area covering part of the harbour's cells.
    lat, lon = h3.cell_to_latlng(row["h3_cells"][0])
    area = to_wkt(box(lon - 0.0002, lat - 0.0002, lon + 0.0002, lat + 0.0002))
    db_path = tmp_path / "existing.geojson"
    _existing_db_geojson(db_path, {
        "harbour_id": "legacy-hh-001",
        "centroid_lat": HAMBURG_LAT,
        "centroid_lon": HAMBURG_LON,
        "h3_cells": row["h3_cells"],
        MANUAL_LOCK_AREA_KEY: area,
    })

    parquet_path, _, _ = run_phase5(
        _base_config(tmp_path, existing_db=str(db_path))
    )
    result = pd.read_parquet(parquet_path).iloc[0]

    assert result["harbour_id"] == "legacy-hh-001"       # id untouched
    assert result[MANUAL_LOCK_AREA_KEY] == area          # drawn area verbatim
    assert len(result["lock_cells"]) >= 1
    assert len(result["lock_cells"]) < len(row["h3_cells"])
    assert bool(result["transit_like"]) is True
