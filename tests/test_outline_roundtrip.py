"""
End-to-end test for the manually adjusted harbour outline.

Covers the loop the feature exists for: run the pipeline, adjust an outline in
the GUI, run the pipeline again — the adjustment has to come back, and the
harbour must never shrink inside it.
"""

import json
from pathlib import Path

import h3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import box, mapping, shape
from shapely.wkt import dumps as to_wkt
from shapely.wkt import loads as from_wkt

import app
from pipeline.enrichment import ENRICHED_SCHEMA
from pipeline.id_matching import Phase5Config, run_phase5
from utils.geo import merge_outlines, outline_polygon
from utils.overrides import DETECTED_OUTLINE_KEY, MANUAL_OUTLINE_KEY

RES = 11
HAMBURG_LAT, HAMBURG_LON = 53.54, 9.97


def _enriched(tmp_path: Path, rings: int) -> Path:
    """One harbour, as Phase 4 would hand it over. More rings = more traffic."""
    cells = sorted(h3.grid_disk(h3.latlng_to_cell(HAMBURG_LAT, HAMBURG_LON, RES),
                                rings))
    cell_geom = shape(h3.cells_to_geo(cells))
    row = {
        "cluster_id":           0,
        "h3_cells":             cells,
        "n_cells":              len(cells),
        "n_events":             100,
        "n_unique_mmsi":        20,
        "n_draught_changes":    3,
        "centroid_lat":         HAMBURG_LAT,
        "centroid_lon":         HAMBURG_LON,
        "centroid_h3_r8":       h3.latlng_to_cell(HAMBURG_LAT, HAMBURG_LON, 8),
        "bbox_min_lat":         HAMBURG_LAT - 0.001,
        "bbox_max_lat":         HAMBURG_LAT + 0.001,
        "bbox_min_lon":         HAMBURG_LON - 0.001,
        "bbox_max_lon":         HAMBURG_LON + 0.001,
        "geometry_wkt":         to_wkt(cell_geom),
        "outline_wkt":          to_wkt(outline_polygon(cell_geom)),
        "country_iso2":         "DE",
        "country_name":         "Germany",
        "nearest_city":         "Hamburg",
        "nearest_city_lat":     HAMBURG_LAT,
        "nearest_city_lon":     HAMBURG_LON,
        "nearest_city_dist_km": 1.0,
        "admin1":               "Hamburg",
    }
    df = pd.DataFrame([row])
    table = pa.table(
        {c: pa.array(df[c].tolist()) for c in df.columns if c != "h3_cells"} |
        {"h3_cells": pa.array(df["h3_cells"].tolist(), type=pa.list_(pa.string()))},
        schema=ENRICHED_SCHEMA,
    )
    path = tmp_path / "harbours_enriched.parquet"
    pq.write_table(table, path)
    return path


def _config(tmp_path: Path, existing_db: str | None = None) -> Phase5Config:
    return Phase5Config(interim_dir=str(tmp_path),
                        output_dir=str(tmp_path / "output"),
                        existing_db_path=existing_db)


def _feature(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)["features"][0]


def test_outline_drawn_in_the_gui_survives_the_next_run(tmp_path):
    _enriched(tmp_path, rings=1)
    _, geojson_path, cells_path = run_phase5(_config(tmp_path))

    # ── The operator extends the outline eastwards and saves ────────────────
    feat     = _feature(geojson_path)
    hid      = feat["properties"]["harbour_id"]
    detected = app.detected_geometry(feat)
    _, miny, maxx, maxy = detected.bounds
    drawn = box(maxx - 0.0005, miny, maxx + 0.002, maxy)

    written = app.save_harbour_outline(
        [geojson_path, cells_path], geojson_path, hid,
        to_wkt(drawn), to_wkt(detected),
        mapping(merge_outlines(detected, drawn)),
    )
    assert len(written) == 2

    # ── Next run: same harbour, more traffic, the edited file as existing DB ─
    _enriched(tmp_path, rings=2)
    parquet_path, geojson_path, _ = run_phase5(_config(tmp_path, geojson_path))

    df = pd.read_parquet(parquet_path)
    assert df.iloc[0]["harbour_id"] == hid          # same harbour, same ID
    outline = from_wkt(df.iloc[0]["outline_wkt"])

    # The drawn quay is still covered …
    assert drawn.difference(outline).area <= drawn.area * 1e-9
    # … the harbour grew with the new traffic …
    assert outline.area > from_wkt(df.iloc[0][DETECTED_OUTLINE_KEY]).area
    assert outline.area > merge_outlines(detected, drawn).area
    # … and the baseline is untouched, ready for the run after that.
    assert df.iloc[0][MANUAL_OUTLINE_KEY] == to_wkt(drawn)
    assert _feature(geojson_path)["properties"][MANUAL_OUTLINE_KEY] == to_wkt(drawn)


def test_reverting_in_the_gui_gives_the_detected_outline_back(tmp_path):
    _enriched(tmp_path, rings=1)
    _, geojson_path, cells_path = run_phase5(_config(tmp_path))

    feat     = _feature(geojson_path)
    hid      = feat["properties"]["harbour_id"]
    detected = app.detected_geometry(feat)
    drawn    = box(9.9, 53.5, 9.99, 53.59)

    app.save_harbour_outline([geojson_path, cells_path], geojson_path, hid,
                             to_wkt(drawn), to_wkt(detected),
                             mapping(merge_outlines(detected, drawn)))
    app.save_harbour_outline([geojson_path, cells_path], geojson_path, hid,
                             None, to_wkt(detected), mapping(detected))

    # Re-running with the reverted file must not resurrect the drawn outline.
    _enriched(tmp_path, rings=1)
    parquet_path, _, _ = run_phase5(_config(tmp_path, geojson_path))

    df = pd.read_parquet(parquet_path)
    assert pd.isna(df.iloc[0][MANUAL_OUTLINE_KEY])
    assert from_wkt(df.iloc[0]["outline_wkt"]).area < drawn.area
