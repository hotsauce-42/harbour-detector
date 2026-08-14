"""
Unit tests for the Streamlit GUI's geometry layers.

Only the pure helpers are exercised — no Streamlit runtime is needed, so these
run in the normal pytest sweep alongside the pipeline tests.
"""

import json
from pathlib import Path

import h3
import pytest
import yaml
from shapely.geometry import mapping, shape
from streamlit.testing.v1 import AppTest

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


# ---------------------------------------------------------------------------
# Manual property edits
# ---------------------------------------------------------------------------

def _props(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))["features"][0]["properties"]


def test_plan_edits_marks_only_changed_fields():
    props = {"nearest_city": "Hamburg", "admin1": "Hamburg",
             "country_name": "Germany"}
    updates, overrides, _ = app.plan_edits(props, {
        "nearest_city": "Hamburg-Altona",
        "admin1":       "Hamburg",        # unchanged
        "country_name": "Germany",        # unchanged
    })
    assert updates == {"nearest_city": "Hamburg-Altona"}
    assert overrides == ["nearest_city"]


def test_plan_edits_no_changes_is_a_noop():
    props = {"nearest_city": "Hamburg", "admin1": "", "country_name": ""}
    updates, _, _ = app.plan_edits(props, {"nearest_city": "Hamburg",
                                           "admin1": "", "country_name": ""})
    assert updates == {}


def test_plan_edits_ignores_surrounding_whitespace():
    props = {"nearest_city": "Hamburg"}
    updates, _, _ = app.plan_edits(props, {"nearest_city": "  Hamburg  "})
    assert updates == {}


def test_plan_edits_keeps_existing_markers():
    """Editing a second field must not drop the first field's marker."""
    props = {"nearest_city": "Hamburg-Altona", "admin1": "Hamburg",
             app.OVERRIDES_KEY: ["nearest_city"]}
    _, overrides, _ = app.plan_edits(props, {"nearest_city": "Hamburg-Altona",
                                             "admin1": "Schleswig-Holstein"})
    assert overrides == ["nearest_city", "admin1"]


def test_plan_edits_resolves_country_iso2():
    props = {"country_name": "Germany", "country_iso2": "DE"}
    updates, _, note = app.plan_edits(props, {"country_name": "Netherlands"})
    assert updates["country_name"] == "Netherlands"
    assert updates["country_iso2"] == "NL"
    assert "NL" in note


def test_plan_edits_leaves_iso2_alone_when_unresolvable():
    props = {"country_name": "Germany", "country_iso2": "DE"}
    updates, _, note = app.plan_edits(props, {"country_name": "Freedonia"})
    assert updates["country_name"] == "Freedonia"
    assert "country_iso2" not in updates
    assert "DE" in note


def test_save_writes_both_geojson_files(outputs):
    """Outline and cells files carry the same properties and must stay in step."""
    outline, cells = outputs / "harbours.geojson", outputs / "harbours_cells.geojson"
    written = app.save_harbour_edits(
        [str(outline), str(cells)], "DE-abcd1234",
        {"nearest_city": "Hamburg-Altona"}, ["nearest_city"],
    )
    assert len(written) == 2
    for path in (outline, cells):
        props = _props(path)
        assert props["nearest_city"] == "Hamburg-Altona"
        assert props[app.OVERRIDES_KEY] == ["nearest_city"]


def test_save_leaves_geometry_and_id_untouched(outputs):
    outline = outputs / "harbours.geojson"
    before = json.loads(outline.read_text(encoding="utf-8"))["features"][0]
    app.save_harbour_edits([str(outline)], "DE-abcd1234",
                           {"admin1": "Hamburg"}, ["admin1"])
    after = json.loads(outline.read_text(encoding="utf-8"))["features"][0]
    assert after["geometry"] == before["geometry"]
    assert after["properties"]["harbour_id"] == "DE-abcd1234"


def test_save_ignores_unknown_harbour(outputs):
    outline = outputs / "harbours.geojson"
    before = outline.read_text(encoding="utf-8")
    written = app.save_harbour_edits([str(outline)], "XX-nosuchid",
                                     {"admin1": "Nowhere"}, ["admin1"])
    assert written == []
    assert outline.read_text(encoding="utf-8") == before


def test_save_skips_missing_companion_file(tmp_path, outputs):
    """A missing cells file must not break saving the outline."""
    written = app.save_harbour_edits(
        [str(outputs / "harbours.geojson"), str(tmp_path / "absent.geojson")],
        "DE-abcd1234", {"admin1": "Hamburg"}, ["admin1"],
    )
    assert len(written) == 1


def test_clearing_markers_removes_the_property(outputs):
    outline = outputs / "harbours.geojson"
    app.save_harbour_edits([str(outline)], "DE-abcd1234",
                           {"admin1": "Hamburg"}, ["admin1"])
    assert app.OVERRIDES_KEY in _props(outline)

    app.save_harbour_edits([str(outline)], "DE-abcd1234", {}, [])
    props = _props(outline)
    assert app.OVERRIDES_KEY not in props
    assert props["admin1"] == "Hamburg"      # value stays, only the flag goes


def test_save_leaves_no_temp_files_behind(outputs):
    outline = outputs / "harbours.geojson"
    app.save_harbour_edits([str(outline)], "DE-abcd1234",
                           {"admin1": "Hamburg"}, ["admin1"])
    assert [p.name for p in outputs.iterdir() if p.name.endswith(".tmp")] == []


def test_saved_file_is_valid_geojson_for_reload(outputs):
    """The GUI reloads what it just wrote, so the file must stay parseable."""
    outline = outputs / "harbours.geojson"
    app.save_harbour_edits([str(outline)], "DE-abcd1234",
                           {"nearest_city": "Ünïcødé Città"}, ["nearest_city"])
    feats = app.load_features.__wrapped__(str(outline))
    assert feats[0]["properties"]["nearest_city"] == "Ünïcødé Città"


# ---------------------------------------------------------------------------
# Full widget round-trip (AppTest drives the real form)
# ---------------------------------------------------------------------------

def _app_on(outputs: Path) -> AppTest:
    """Run app.main() against a throwaway config pointing at `outputs`."""
    cfg = {"gui": {
        "output_file":  str(outputs / "harbours.geojson"),
        "cells_file":   str(outputs / "harbours_cells.geojson"),
        "default_tile": "OSM",
        "map_tiles": [{"name": "OSM", "url": "https://t/{z}/{x}/{y}.png",
                       "attribution": "a"}],
    }}
    cfg_path = outputs / "cfg.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")

    script = (
        "import app\n"
        f"app.CONFIG_PATH = {str(cfg_path)!r}\n"
        "app.load_config.clear()\n"
        "app.load_features.clear()\n"
        "app.load_geometry_by_id.clear()\n"
        "app.main()\n"
    )
    return AppTest.from_string(script, default_timeout=120).run()


def _submit(at, label: str):
    return [b for b in at.button if b.label == label][0].click().run()


def test_edit_form_saves_through_the_ui(outputs):
    """Drives the real widgets: type into the form, hit Save, check the file."""
    at = _app_on(outputs)
    assert not at.exception
    assert at.text_input(key="edit_DE-abcd1234_nearest_city").value == "Hamburg"

    at.text_input(key="edit_DE-abcd1234_nearest_city").set_value("Hamburg-Altona")
    at.text_input(key="edit_DE-abcd1234_country_name").set_value("Netherlands")
    at = _submit(at, "Save changes")

    assert not at.exception
    for name in ("harbours.geojson", "harbours_cells.geojson"):
        props = _props(outputs / name)
        assert props["nearest_city"] == "Hamburg-Altona"
        assert props["country_name"] == "Netherlands"
        assert props["country_iso2"] == "NL"     # resolved from the new name
        assert sorted(props[app.OVERRIDES_KEY]) == ["country_name", "nearest_city"]


def test_edit_form_reflects_saved_values_after_rerun(outputs):
    """The save clears the cache, so the reloaded form shows the new value."""
    at = _app_on(outputs)
    at.text_input(key="edit_DE-abcd1234_admin1").set_value("Schleswig-Holstein")
    at = _submit(at, "Save changes")
    assert at.text_input(key="edit_DE-abcd1234_admin1").value == "Schleswig-Holstein"


def test_edit_form_rejects_nothing_when_unchanged(outputs):
    at = _app_on(outputs)
    at = _submit(at, "Save changes")
    assert not at.exception
    assert app.OVERRIDES_KEY not in _props(outputs / "harbours.geojson")


def test_row_for_harbour_finds_selection_after_rerun(outputs):
    feats = app.load_features.__wrapped__(str(outputs / "harbours.geojson"))
    df = app._build_display_df(feats)
    assert app._row_for_harbour(df, feats, "DE-abcd1234") == 0
    assert app._row_for_harbour(df, feats, "XX-unknown") == 0
    assert app._row_for_harbour(df, feats, None) == 0
