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
from shapely.geometry import (
    LineString,
    MultiPolygon,
    Polygon,
    box,
    mapping,
    shape,
)
from shapely.wkt import dumps as to_wkt
from shapely.wkt import loads as from_wkt
from streamlit.testing.v1 import AppTest

import app
from utils.geo import merge_outlines

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


# ---------------------------------------------------------------------------
# Manual outline edits
# ---------------------------------------------------------------------------

def _drawing(geom) -> dict:
    """One entry as st_folium hands it back in all_drawings."""
    return {"type": "Feature", "properties": {}, "geometry": mapping(geom)}


def _first_feature(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))["features"][0]


def test_outline_from_drawings_returns_the_drawn_polygon():
    drawn = box(9.9, 53.5, 9.91, 53.51)
    got = app.outline_from_drawings([_drawing(drawn)])
    assert from_wkt(got).equals(drawn)


def test_outline_from_drawings_unions_every_layer():
    """Whatever is left on the draw layer — seeded parts and new ones — is one
    outline."""
    left, right = box(0, 0, 2, 1), box(1, 0, 3, 1)
    got = from_wkt(app.outline_from_drawings([_drawing(left), _drawing(right)]))
    assert got.equals(box(0, 0, 3, 1))


def test_outline_from_drawings_repairs_a_self_intersection():
    bowtie = Polygon([(0, 0), (1, 1), (1, 0), (0, 1)])
    got = from_wkt(app.outline_from_drawings([_drawing(bowtie)]))
    assert got.is_valid
    assert got.area == 0.5


def test_outline_from_drawings_ignores_non_areal_layers():
    line = LineString([(0, 0), (1, 1)])
    assert app.outline_from_drawings([_drawing(line)]) is None


def test_outline_from_drawings_with_nothing_drawn():
    """Deleting every layer is how an operator clears the outline."""
    assert app.outline_from_drawings(None) is None
    assert app.outline_from_drawings([]) is None


def test_detected_geometry_prefers_the_stored_detected_outline():
    detected = box(0, 0, 1, 1)
    feat = {
        "geometry": mapping(box(0, 0, 5, 5)),      # already merged with an edit
        "properties": {app.DETECTED_OUTLINE_KEY: to_wkt(detected)},
    }
    assert app.detected_geometry(feat).equals(detected)


def test_detected_geometry_falls_back_to_the_feature_geometry():
    """Output written before this feature existed carries no detected outline."""
    geom = box(0, 0, 1, 1)
    feat = {"geometry": mapping(geom), "properties": {}}
    assert app.detected_geometry(feat).equals(geom)


def test_detected_geometry_survives_unparseable_wkt():
    geom = box(0, 0, 1, 1)
    feat = {"geometry": mapping(geom),
            "properties": {app.DETECTED_OUTLINE_KEY: "POLYGON ((nope))"}}
    assert app.detected_geometry(feat).equals(geom)


def test_drawn_geometry_reads_the_stored_manual_outline():
    drawn = box(0, 0, 1, 1)
    feat = {"properties": {app.MANUAL_OUTLINE_KEY: to_wkt(drawn)}}
    assert app.drawn_geometry(feat).equals(drawn)
    assert app.drawn_geometry({"properties": {}}) is None


# ── Saving ─────────────────────────────────────────────────────────────────

def test_save_outline_writes_the_baseline_to_both_files(outputs):
    outline, cells = outputs / "harbours.geojson", outputs / "harbours_cells.geojson"
    detected = app.detected_geometry(_first_feature(outline))
    drawn    = box(9.9, 53.5, 9.98, 53.58)
    merged   = merge_outlines(detected, drawn)

    written = app.save_harbour_outline(
        [str(outline), str(cells)], str(outline), "DE-abcd1234",
        to_wkt(drawn), to_wkt(detected), mapping(merged),
    )

    assert len(written) == 2
    for path in (outline, cells):
        props = _props(path)
        assert from_wkt(props[app.MANUAL_OUTLINE_KEY]).equals(drawn)
        assert from_wkt(props[app.DETECTED_OUTLINE_KEY]).equals(detected)


def test_save_outline_only_changes_the_outline_file_geometry(outputs):
    """The cells file holds the H3-cell union, which a drawn outline never touches."""
    outline, cells = outputs / "harbours.geojson", outputs / "harbours_cells.geojson"
    cells_before = _first_feature(cells)["geometry"]
    drawn = box(9.9, 53.5, 9.98, 53.58)

    app.save_harbour_outline([str(outline), str(cells)], str(outline),
                             "DE-abcd1234", to_wkt(drawn), None, mapping(drawn))

    assert _first_feature(cells)["geometry"] == cells_before
    assert shape(_first_feature(outline)["geometry"]).equals(drawn)


def test_save_outline_keeps_property_overrides(outputs):
    """An outline edit must not disturb a city/region/country correction."""
    outline = outputs / "harbours.geojson"
    app.save_harbour_edits([str(outline)], "DE-abcd1234",
                           {"nearest_city": "Hamburg-Altona"}, ["nearest_city"])

    drawn = box(9.9, 53.5, 9.98, 53.58)
    app.save_harbour_outline([str(outline)], str(outline), "DE-abcd1234",
                             to_wkt(drawn), None, mapping(drawn))

    props = _props(outline)
    assert props["nearest_city"] == "Hamburg-Altona"
    assert props[app.OVERRIDES_KEY] == ["nearest_city"]


def test_reverting_clears_the_manual_outline(outputs):
    outline = outputs / "harbours.geojson"
    detected = app.detected_geometry(_first_feature(outline))
    drawn = box(9.9, 53.5, 9.98, 53.58)
    app.save_harbour_outline([str(outline)], str(outline), "DE-abcd1234",
                             to_wkt(drawn), to_wkt(detected),
                             mapping(merge_outlines(detected, drawn)))

    app.save_harbour_outline([str(outline)], str(outline), "DE-abcd1234",
                             None, to_wkt(detected), mapping(detected))

    props = _props(outline)
    assert app.MANUAL_OUTLINE_KEY not in props
    assert shape(_first_feature(outline)["geometry"]).equals(detected)


def test_save_outline_ignores_an_unknown_harbour(outputs):
    outline = outputs / "harbours.geojson"
    before = outline.read_text(encoding="utf-8")
    written = app.save_harbour_outline([str(outline)], str(outline), "XX-nosuchid",
                                       to_wkt(box(0, 0, 1, 1)), None, None)
    assert written == []
    assert outline.read_text(encoding="utf-8") == before


# ── Feedback ───────────────────────────────────────────────────────────────

def test_note_is_quiet_for_an_inward_edit():
    # The stored outline is the union of both shapes on purpose, so drawing
    # inside the detected one is normal use, not something to warn about.
    detected = box(0, 0, 10, 10)
    assert app.outline_edit_note(detected, box(0, 0, 5, 10)) is None


def test_note_is_quiet_for_a_pure_extension():
    detected = box(0, 0, 10, 10)
    assert app.outline_edit_note(detected, box(0, 0, 12, 10)) is None


def test_note_warns_about_a_wildly_oversized_outline():
    detected = box(0, 0, 1, 1)
    note = app.outline_edit_note(detected, box(0, 0, 100, 100))
    assert note and "neighbouring harbour" in note


# ── The editable map ───────────────────────────────────────────────────────

def _leaflet(feat, **kwargs) -> str:
    """The JS streamlit-folium actually ships to the browser for this map."""
    from streamlit_folium import _get_map_string

    m = app._build_map(feat, "https://tiles/{z}/{x}/{y}.png", "attr", "tiles",
                       **kwargs)
    return _get_map_string(m)


def test_edit_mode_hands_the_outline_to_the_draw_control(outputs):
    """
    The whole interaction hangs on this: Leaflet.Draw edits the feature group
    it is given, and streamlit-folium reports that same group back as
    all_drawings only if it renamed it to `drawnItems`.
    """
    feats = app.load_features.__wrapped__(str(outputs / "harbours.geojson"))
    js = _leaflet(feats[0], editable=True)

    assert "options.edit.featureGroup = drawnItems;" in js
    assert "L.Control.Draw" in js
    # The outline is seeded as a real polygon layer — an L.geoJson group would
    # render fine but the edit toolbar would not touch it.
    assert "L.polygon(" in js


def test_edit_mode_does_not_draw_the_outline_twice(outputs):
    feats = app.load_features.__wrapped__(str(outputs / "harbours.geojson"))
    assert _leaflet(feats[0], editable=True).count("L.geoJson") == 0
    assert _leaflet(feats[0], editable=False).count("L.geoJson") == 1


def test_view_mode_has_no_draw_control(outputs):
    feats = app.load_features.__wrapped__(str(outputs / "harbours.geojson"))
    assert "L.Control.Draw" not in _leaflet(feats[0], editable=False)


def test_editable_polygons_keep_every_part_and_hole():
    ring = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)],
                   holes=[[(4, 4), (6, 4), (6, 6), (4, 6)]])
    apart = box(20, 20, 21, 21)
    polygons = app._editable_polygons(MultiPolygon([ring, apart]), app.OUTLINE_STYLE)

    assert len(polygons) == 2
    # locations = [exterior, *holes], each a list of (lat, lon) pairs
    assert len(polygons[0].locations) == 2
    assert len(polygons[1].locations) == 1


def test_editable_polygons_use_lat_lon_order():
    """GeoJSON is (lon, lat) and Leaflet is (lat, lon) — the classic mix-up."""
    polygons = app._editable_polygons(box(9.9, 53.5, 9.91, 53.51),
                                      app.OUTLINE_STYLE)
    lats = [lat for lat, _ in polygons[0].locations[0]]
    lons = [lon for _, lon in polygons[0].locations[0]]
    assert min(lats) > 53 and max(lats) < 54
    assert min(lons) > 9 and max(lons) < 10


def test_outline_controls_appear_only_in_edit_mode(outputs):
    at = _app_on(outputs)
    assert not at.exception
    assert "Save outline" not in [b.label for b in at.button]

    at = at.toggle(key="outline_mode_DE-abcd1234").set_value(True).run()

    assert not at.exception
    labels = [b.label for b in at.button]
    assert "Save outline" in labels
    assert "Revert to detected" in labels


def test_saving_is_blocked_until_something_is_drawn(outputs):
    """st_folium reports no drawings under AppTest, which is the 'nothing drawn'
    case: the button has to stay inert rather than save an empty outline."""
    at = _app_on(outputs)
    at = at.toggle(key="outline_mode_DE-abcd1234").set_value(True).run()

    before = (outputs / "harbours.geojson").read_text(encoding="utf-8")
    at = _submit(at, "Save outline")

    assert not at.exception
    assert (outputs / "harbours.geojson").read_text(encoding="utf-8") == before


def test_revert_is_offered_for_a_harbour_with_a_manual_outline(outputs):
    outline = outputs / "harbours.geojson"
    drawn = box(9.9, 53.5, 9.98, 53.58)
    app.save_harbour_outline([str(outline)], str(outline), "DE-abcd1234",
                             to_wkt(drawn), None, mapping(drawn))

    at = _app_on(outputs)
    at = at.toggle(key="outline_mode_DE-abcd1234").set_value(True).run()

    assert not at.exception
    # The toggle's own label doubles as the "this harbour was edited" marker.
    assert "manually adjusted" in at.toggle(key="outline_mode_DE-abcd1234").label
