"""
Tests for scripts/map_port_areas.py — the HTML map of Phase 6's port areas.
"""

import json
import sys
from pathlib import Path

import pytest
from shapely.geometry import MultiPolygon, box, mapping

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

import map_port_areas as cli  # noqa: E402


def _write(path: Path, features: list[dict]) -> Path:
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features}),
                    encoding="utf-8")
    return path


def _area(area_id: str, geom) -> dict:
    return {"type": "Feature", "geometry": mapping(geom), "properties": {
        "port_area_id": area_id, "name": "Kiel", "n_harbours": 2,
        "n_cells": 3, "h3_cells": ["8b1f00000000fff"] * 3}}


@pytest.fixture
def inputs(tmp_path):
    two_pieces = MultiPolygon([box(10.0, 54.0, 10.01, 54.01),
                               box(10.03, 54.0, 10.04, 54.01)])
    areas = _write(tmp_path / "port_areas.geojson",
                   [_area("PA-DE-1", two_pieces),
                    _area("PA-DE-2", box(10.1, 54.1, 10.11, 54.11))])
    harbours = _write(tmp_path / "harbours.geojson", [{
        "type": "Feature", "geometry": mapping(box(10.0, 54.0, 10.005, 54.005)),
        "properties": {"harbour_id": "DE-1", "nearest_city": "Kiel",
                       "h3_cells": ["x"] * 100}}])
    return areas, harbours


def test_area_layer_counts_pieces_and_drops_cell_lists(inputs):
    areas, _ = inputs
    props = [f["properties"] for f in cli._area_layer(areas)["features"]]
    assert [p["pieces"] for p in props] == [2, 1]
    assert all("h3_cells" not in p for p in props)


def test_the_map_has_every_layer(inputs, tmp_path):
    areas, harbours = inputs
    out = cli.build_map(areas, harbours, tmp_path / "map.html",
                        compare=areas, compare_label="with land cut")
    page = out.read_text(encoding="utf-8")
    assert "PA-DE-2" in page and "DE-1" in page
    assert "with land cut" in page


def test_the_offline_map_loads_nothing_from_the_internet(
        inputs, tmp_path, monkeypatch):
    """
    `use_local_assets` mutates folium's class attributes globally and cannot be
    undone, so the originals are restored here — otherwise this test poisons
    every folium map built later in the session, including test_gui's.
    """
    folium = pytest.importorskip("folium")
    from folium.plugins import Draw

    saved = [(cls, attr, list(getattr(cls, attr)))
             for cls in (folium.Map, Draw)
             for attr in ("default_js", "default_css")]
    monkeypatch.setattr(cli, "is_vendored", lambda *a, **k: True)
    monkeypatch.setattr(cli.shutil, "copytree", lambda *a, **k: None)
    areas, harbours = inputs
    try:
        out = cli.build_map(areas, harbours, tmp_path / "map.html", offline=True)
        page = out.read_text(encoding="utf-8")
    finally:
        for cls, attr, value in saved:
            setattr(cls, attr, value)

    assert 'src="vendor/leaflet.js"' in page
    # The default OpenStreetMap basemap is dropped offline, not left to time out.
    assert "https://" not in page
