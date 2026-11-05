"""Tests for the harbour quiz (quiz.py) and scripts/apply_quiz_verdicts.py."""

import csv
import json
import random
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import h3
import pytest
import yaml
from shapely.geometry import mapping, shape
from streamlit.testing.v1 import AppTest

import app
import quiz
from utils import map_assets
from utils.overrides import MANUAL_OFFSHORE_KEY, MANUAL_TRANSIT_KEY

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
import apply_quiz_verdicts  # noqa: E402
import calibrate_offshore  # noqa: E402

T0 = datetime(2026, 11, 5, 12, tzinfo=timezone.utc)


def _props(hid: str, **extra) -> dict:
    return {"harbour_id": hid, "nearest_city": "Kiel", "country_iso2": "DE",
            "country_name": "Germany", "centroid_lat": 54.3, "centroid_lon": 10.15,
            "n_events": 10, "n_unique_mmsi": 5, "n_cells": 7, **extra}


def _features(n: int, **extra) -> list[dict]:
    return [{"type": "Feature", "geometry": None,
             "properties": _props(f"DE-{i:08x}", **extra)} for i in range(n)]


@pytest.fixture
def outputs(tmp_path):
    """A three-harbour outline/cells pair and a config pointing at it."""
    feats = []
    for i, lat in enumerate((54.30, 54.35, 54.40)):
        cells = sorted(h3.grid_disk(h3.latlng_to_cell(lat, 10.15, 11), 1))
        geom = shape(h3.cells_to_geo(cells))
        feats.append((cells, geom, _props(f"DE-0000000{i}", centroid_lat=lat,
                                          transit_like=(i == 1))))
    for name, use_hull in (("harbours.geojson", True),
                           ("harbours_cells.geojson", False)):
        fc = {"type": "FeatureCollection", "features": [
            {"type": "Feature",
             "geometry": mapping(g.convex_hull if use_hull else g),
             "properties": {**p, "h3_cells": c}}
            for c, g, p in feats]}
        (tmp_path / name).write_text(json.dumps(fc), encoding="utf-8")
    return tmp_path


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("props, expected", [
    ({}, "harbour"),
    ({"transit_like": True}, "lock"),
    ({"offshore_like": True}, "offshore"),
    ({"transit_like": True, "offshore_like": True}, "lock+offshore"),
    # A manual verdict outranks the detector, as in app.py.
    ({"transit_like": True, MANUAL_TRANSIT_KEY: False}, "harbour"),
])
def test_detector_category_follows_the_effective_verdict(props, expected):
    assert quiz.detector_category(props) == expected


def test_agreement():
    assert quiz.agrees("lock", "lock") is True
    assert quiz.agrees("lock+offshore", "offshore") is True
    assert quiz.agrees("harbour", "lock") is False
    assert quiz.agrees("harbour", "unsure") is None


def test_a_round_takes_unreviewed_sites_first():
    feats = _features(30)
    reviewed = {f"DE-{i:08x}" for i in range(25)}
    picked = quiz.pick_round(feats, 10, reviewed, random.Random(1))
    assert len(picked) == len(set(picked)) == 10
    fresh = [p for p in picked if p not in reviewed]
    assert len(fresh) == 5          # all five unreviewed, topped up with five


def test_a_round_never_exceeds_the_database():
    assert len(quiz.pick_round(_features(3), 20, set(), random.Random())) == 3


def test_manual_verdicts_and_quiz_files_count_as_reviewed(tmp_path):
    feats = _features(3)
    feats[0]["properties"][MANUAL_OFFSHORE_KEY] = False
    quiz.append_answer(tmp_path / "quiz_a.csv",
                       quiz.answer_row(feats[1]["properties"], "unsure", T0))
    assert quiz.reviewed_ids(str(tmp_path), feats) == {"DE-00000000", "DE-00000001"}


def test_the_latest_answer_wins_across_files(tmp_path):
    p = _props("DE-aaaaaaaa")
    quiz.append_answer(tmp_path / "quiz_b.csv",
                       quiz.answer_row(p, "offshore", T0 + timedelta(hours=1)))
    quiz.append_answer(tmp_path / "quiz_a.csv", quiz.answer_row(p, "harbour", T0))
    quiz.append_answer(tmp_path / "quiz_a.csv",
                       quiz.answer_row(_props("DE-bbbbbbbb"), "lock", T0))

    answers = quiz.read_answers(quiz.quiz_files(str(tmp_path)))
    assert answers["DE-aaaaaaaa"]["answer"] == "offshore"
    assert answers["DE-bbbbbbbb"]["answer"] == "lock"
    with open(tmp_path / "quiz_a.csv", newline="") as f:
        assert len(list(csv.DictReader(f))) == 2      # one header, two rows


def test_answer_row_records_the_detector_category():
    row = quiz.answer_row(_props("X", transit_like=True, coast_dist_km=None),
                          "harbour", T0)
    assert row["detector_category"] == "lock"
    assert row["coast_dist_km"] == ""
    assert set(row) == set(quiz.CSV_FIELDS)


# ---------------------------------------------------------------------------
# apply_quiz_verdicts / calibrate_offshore
# ---------------------------------------------------------------------------

def _answers(tmp_path, pairs) -> Path:
    path = tmp_path / "quiz_x.csv"
    for hid, answer in pairs:
        quiz.append_answer(path, quiz.answer_row(_props(hid), answer, T0))
    return path


def test_apply_maps_answers_to_manual_verdicts(outputs):
    path = _answers(outputs, [("DE-00000000", "harbour"), ("DE-00000001", "lock"),
                              ("DE-00000002", "unsure"), ("DE-ffffffff", "offshore")])
    changes = apply_quiz_verdicts.plan(quiz.read_answers([path]))
    assert "DE-00000002" not in changes

    files = [str(outputs / "harbours.geojson"), str(outputs / "harbours_cells.geojson")]
    updated, missing = apply_quiz_verdicts.apply(changes, files)
    assert updated == ["DE-00000000", "DE-00000001"] and missing == ["DE-ffffffff"]

    for name in ("harbours.geojson", "harbours_cells.geojson"):
        props = {f["properties"]["harbour_id"]: f["properties"] for f in
                 json.loads((outputs / name).read_text())["features"]}
        assert props["DE-00000000"][MANUAL_TRANSIT_KEY] is False
        assert props["DE-00000000"][MANUAL_OFFSHORE_KEY] is False
        assert props["DE-00000001"][MANUAL_TRANSIT_KEY] is True
        assert MANUAL_OFFSHORE_KEY not in props["DE-00000001"]
        assert MANUAL_TRANSIT_KEY not in props["DE-00000002"]


def test_dry_run_writes_nothing(outputs, monkeypatch, capsys):
    path = _answers(outputs, [("DE-00000000", "lock")])
    before = (outputs / "harbours.geojson").read_text()
    monkeypatch.setattr(sys, "argv", ["x", str(path), "--dry-run",
                                      "--geojson", str(outputs / "harbours.geojson")])
    assert apply_quiz_verdicts.main() == 0
    assert (outputs / "harbours.geojson").read_text() == before
    assert "DE-00000000" in capsys.readouterr().out


def test_calibration_reads_quiz_answers(tmp_path):
    path = _answers(tmp_path, [("A", "offshore"), ("B", "lock"), ("C", "harbour"),
                               ("D", "unsure")])
    offshore, harbour = calibrate_offshore.quiz_examples([path])
    assert offshore == {"A"} and harbour == {"B", "C"}


# ---------------------------------------------------------------------------
# The Streamlit page
# ---------------------------------------------------------------------------

def _quiz_on(outputs: Path, round_size: int = 3) -> AppTest:
    cfg = {"gui": {
        "output_file": str(outputs / "harbours.geojson"),
        "cells_file": str(outputs / "harbours_cells.geojson"),
        "quiz_dir": str(outputs / "quiz"),
        "quiz_round_size": round_size,
        "default_tile": "Own",
        "map_tiles": [{"name": "Own", "url": "http://tiles.internal/{z}/{x}/{y}.png",
                       "attribution": "a"}],
    }}
    cfg_path = outputs / "cfg.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    script = (
        "import app, quiz\n"
        f"app.CONFIG_PATH = {str(cfg_path)!r}\n"
        "app.load_config.clear()\n"
        "app.load_features.clear()\n"
        "app.load_geometry_by_id.clear()\n"
        "quiz.main()\n"
    )
    return AppTest.from_string(script, default_timeout=120).run()


def test_a_full_round_through_the_ui(outputs):
    at = _quiz_on(outputs)
    assert not at.exception
    for key in ("harbour", "lock", "unsure"):
        at = at.button(key=f"answer_{key}").click().run()
        assert not at.exception

    files = quiz.quiz_files(str(outputs / "quiz"))
    assert len(files) == 1
    with open(files[0], newline="") as f:
        rows = list(csv.DictReader(f))
    assert [r["answer"] for r in rows] == ["harbour", "lock", "unsure"]
    assert len({r["harbour_id"] for r in rows}) == 3
    # The overview replaces the buttons.
    assert any("Round complete" in s.value for s in at.subheader)
    assert not [b for b in at.button if b.key and b.key.startswith("answer_")]


def test_the_quiz_map_needs_nothing_but_the_tile_server(outputs):
    saved = [(cls, attr, list(getattr(cls, attr)))
             for cls in map_assets._HOLDERS for attr in map_assets._ATTRS]
    try:
        map_assets.use_local_assets()
        feat = app.load_features.__wrapped__(str(outputs / "harbours.geojson"))[0]
        cells = app.load_geometry_by_id.__wrapped__(
            str(outputs / "harbours_cells.geojson"))
        m = app._build_map(feat, "http://tiles.internal/{z}/{x}/{y}.png", "a", "t",
                           cells_geom=cells[feat["properties"]["harbour_id"]],
                           show=app.SHOW_BOTH)
        page = m.get_root().render()
        jquery = map_assets.asset_urls()["jquery"]
    finally:
        for cls, attr, value in saved:
            setattr(cls, attr, value)
    assert set(re.findall(r"https?://[^\"' )]+", page)) == {
        "http://tiles.internal/{z}/{x}/{y}.png"}
    # The popups need jquery offline (see test_gui.py), so it must be local.
    assert jquery in page and jquery.startswith("/app/static/vendor")


def test_changing_an_answer_from_the_overview(outputs):
    at = _quiz_on(outputs)
    for key in ("harbour", "lock", "unsure"):
        at = at.button(key=f"answer_{key}").click().run()
    second = at.session_state["quiz_ids"][1]

    # What a click on the overview's second row does (_open_from_overview).
    at.session_state["quiz_pos"] = 1
    at.session_state["quiz_revisit"] = True
    at = at.run()
    assert any("you said" in i.value for i in at.info)
    at = at.button(key="answer_offshore").click().run()
    assert not at.exception

    # Straight back to the overview, with the new answer winning.
    assert any("Round complete" in s.value for s in at.subheader)
    assert at.session_state["quiz_answers"][second] == "offshore"
    files = quiz.quiz_files(str(outputs / "quiz"))
    assert quiz.read_answers(files)[second]["answer"] == "offshore"
    with open(files[0], newline="") as f:
        assert len(list(csv.DictReader(f))) == 4      # appended, not rewritten


def test_stat_tiles_carry_the_full_value_as_a_tooltip():
    page = quiz.stat_tiles_html([("Mean dwell", "1,234 min"), ("City", "A<b>&c")])
    assert 'title="Mean dwell: 1,234 min"' in page
    assert "A&lt;b&gt;&amp;c" in page and "A<b>" not in page
