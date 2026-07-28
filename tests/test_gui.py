"""
Unit tests for the Streamlit GUI's geometry layers.

Only the pure helpers are exercised — no Streamlit runtime is needed, so these
run in the normal pytest sweep alongside the pipeline tests.
"""

import json
from pathlib import Path

import h3
import pytest
from shapely.geometry import mapping, shape

import app

RES = 11
HAMBURG_LAT, HAMBURG_LON = 53.54, 9.97


def _feature(harbour_id: str, geom, kind: str) -> dict:
    return {
        "type": "Feature",
        "geometry": mapping(geom),
        "properties": {
            "harbour_id":    harbour_id,
            "geometry_kind": kind,
            "nearest_city":  "Hamburg",
            "country_name":  "Germany",
            "centroid_lat":  HAMBURG_LAT,
            "centroid_lon":  HAMBURG_LON,
            "n_events":      100,
            "n_unique_mmsi": 20,
            "n_cells":       7,
        },
    }


@pytest.fixture
def outputs(tmp_path):
    """Write an outline/cells GeoJSON pair like Phase 5 does."""
    cells = sorted(h3.grid_disk(h3.latlng_to_cell(HAMBURG_LAT, HAMBURG_LON, RES), 1))
    cell_geom = shape(h3.cells_to_geo(cells))
    outline_geom = cell_geom.convex_hull      # any enclosing shape will do here

    pairs = [
        ("harbours.geojson",       outline_geom, "outline"),
        ("harbours_cells.geojson", cell_geom,    "cells"),
    ]
    for name, geom, kind in pairs:
        fc = {"type": "FeatureCollection",
              "features": [_feature("DE-abcd1234", geom, kind)]}
        (tmp_path / name).write_text(json.dumps(fc), encoding="utf-8")
    return tmp_path


# ---------------------------------------------------------------------------
# Companion-file resolution
# ---------------------------------------------------------------------------

def test_cells_path_derived_from_output_file():
    assert app._cells_path("data/output/harbours.geojson", {}) == str(
        Path("data/output/harbours_cells.geojson")
    )


def test_cells_path_explicit_override_wins():
    got = app._cells_path("data/output/harbours.geojson",
                          {"cells_file": "/elsewhere/cells.geojson"})
    assert got == "/elsewhere/cells.geojson"


# ---------------------------------------------------------------------------
# Geometry index
# ---------------------------------------------------------------------------

def test_geometry_indexed_by_harbour_id(outputs):
    by_id = app.load_geometry_by_id.__wrapped__(str(outputs / "harbours_cells.geojson"))
    assert set(by_id) == {"DE-abcd1234"}
    assert by_id["DE-abcd1234"]["type"] in ("Polygon", "MultiPolygon")


def test_missing_cells_file_returns_empty(tmp_path):
    """A missing companion file must degrade to 'outline only', not raise."""
    assert app.load_geometry_by_id.__wrapped__(str(tmp_path / "absent.geojson")) == {}
    assert app.load_geometry_by_id.__wrapped__("") == {}


# ---------------------------------------------------------------------------
# Map layers
# ---------------------------------------------------------------------------

def _layer_count(feat, cells_geom, show):
    m = app._build_map(feat, "https://tiles/{z}/{x}/{y}.png", "attr", "tiles",
                       cells_geom=cells_geom, show=show)
    return m.get_root().render().count("L.geoJson")


@pytest.mark.parametrize("show,expected", [
    (app.SHOW_OUTLINE, 1),
    (app.SHOW_CELLS,   1),
    (app.SHOW_BOTH,    2),
])
def test_toggle_draws_expected_layer_count(outputs, show, expected):
    feats = app.load_features.__wrapped__(str(outputs / "harbours.geojson"))
    cells = app.load_geometry_by_id.__wrapped__(str(outputs / "harbours_cells.geojson"))
    geom  = cells[feats[0]["properties"]["harbour_id"]]
    assert _layer_count(feats[0], geom, show) == expected


def test_both_without_cells_falls_back_to_outline(outputs):
    """Selecting 'Both' when no cell geometry exists still draws the outline."""
    feats = app.load_features.__wrapped__(str(outputs / "harbours.geojson"))
    assert _layer_count(feats[0], None, app.SHOW_BOTH) == 1
