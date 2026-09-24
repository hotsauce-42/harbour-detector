"""
Pins for comparing the pipeline against a foreign harbour database.

Two databases rarely agree on what a harbour *polygon* is — ours is the
trafficked water, someone else's may be the whole administrative port area. The
tests that matter most here are the ones holding that difference open: a pair
must still link when their IoU is near zero, areas must be in m² rather than
degrees², and a group the two sides split differently must survive as one group
rather than decomposing into a disappearance and an invention.

The rest pin the quiet failures — a name field detected by presence instead of
by fill, a Danish city name shredded by Unicode normalisation, a diagnosis that
claims to know something the interim files cannot actually show.
"""

import json
import sys
from pathlib import Path

import numpy as np
import pytest
from shapely.geometry import Polygon, box, mapping

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from compare_harbour_dbs import main  # noqa: E402
from pipeline.cluster_formation import _connected_components  # noqa: E402
from utils.compare import (  # noqa: E402
    Record,
    area_m2,
    calibrate,
    components,
    coverage_cells,
    detect_field,
    diagnose_only_in_old,
    in_coverage,
    link_metrics,
    links,
    load_interim_layers,
    load_old,
    metric_frame,
    normalise_country,
    normalise_name,
    score,
)

# A stretch of Danish coast, at a latitude where a degree of longitude is only
# 0.56 × a degree of latitude — which is the whole reason areas are computed in
# a metric frame rather than in degrees.
LAT, LON = 55.70, 12.60
EQUATOR_LAT, EQUATOR_LON = 0.00, 12.60


def _box(lat, lon, width_m, height_m=None, dx_m=0.0, dy_m=0.0):
    """A rectangle of a given size in metres, offset from (lat, lon)."""
    height_m = width_m if height_m is None else height_m
    _, to_deg = metric_frame(lat, lon)
    x0, y0 = to_deg(dx_m - width_m / 2, dy_m - height_m / 2)
    x1, y1 = to_deg(dx_m + width_m / 2, dy_m + height_m / 2)
    return box(float(x0), float(y0), float(x1), float(y1))


def _rec(side, rec_id, geom, **props):
    return Record(side=side, rec_id=rec_id, geom=geom,
                  lat=geom.centroid.y, lon=geom.centroid.x,
                  name=props.pop("name", None),
                  country=props.pop("country", None),
                  props=props)


def _feature(geom, **props):
    return {"type": "Feature", "geometry": mapping(geom), "properties": props}


def _write_fc(path, features):
    path.write_text(json.dumps(
        {"type": "FeatureCollection", "features": features}), encoding="utf-8")
    return path


def _comps(old, new, link_list, max_component=8):
    return components(old, new, link_list,
                      find_components=_connected_components,
                      max_component=max_component)


def _links(old, new, min_coverage=0.5, max_gap_m=250.0, min_overlap_m2=2500.0):
    return links(old, new, min_coverage=min_coverage, max_gap_m=max_gap_m,
                 min_overlap_m2=min_overlap_m2)


# ---------------------------------------------------------------------------
# Geometry — the numbers everything else is built on
# ---------------------------------------------------------------------------

def test_areas_are_metres_squared_not_degrees():
    """A degrees² area would be ~1e-10 of this, and vary with latitude."""
    rec = _rec("old", "A", _box(LAT, LON, 100.0))
    assert area_m2(rec) == pytest.approx(10_000.0, rel=0.01)


def test_the_metric_frame_does_not_stretch_longitude_at_high_latitude():
    """
    The bug this avoids is the one `reverse_geocoder` has: measuring in raw
    degrees makes an east-west extent count for ~1.8× at 56°N.
    """
    north = area_m2(_rec("old", "N", _box(LAT, LON, 100.0)))
    equator = area_m2(_rec("old", "E", _box(EQUATOR_LAT, EQUATOR_LON, 100.0)))
    assert north == pytest.approx(equator, rel=0.01)


def test_a_polygon_pair_that_overlaps_is_linked_even_when_iou_is_low():
    """
    The load-bearing case: a whole-port polygon against a trafficked-water one.
    Linking on IoU would call a perfect detection a disagreement.
    """
    old = [_rec("old", "O1", _box(LAT, LON, 2000.0))]
    new = [_rec("new", "N1", _box(LAT, LON, 100.0))]

    lk = link_metrics(old[0], new[0])
    assert lk.iou < 0.01
    assert lk.coverage_new == pytest.approx(1.0, rel=0.01)
    assert len(_links(old, new)) == 1


def test_polygons_touching_at_a_corner_share_no_area_but_still_link():
    """
    A corner touch encloses nothing, so it must not count as shared area — but
    zero gap is well inside the gap rule, which is deliberately generous.
    """
    old = [_rec("old", "O1", _box(LAT, LON, 100.0))]
    new = [_rec("new", "N1", _box(LAT, LON, 100.0, dx_m=100.0, dy_m=100.0))]

    lk = link_metrics(old[0], new[0])
    assert lk.inter_m2 == pytest.approx(0.0, abs=1.0)
    assert lk.iou == pytest.approx(0.0, abs=1e-6)
    assert len(_links(old, new)) == 1


def test_polygons_further_apart_than_the_gap_rule_do_not_link():
    old = [_rec("old", "O1", _box(LAT, LON, 100.0))]
    new = [_rec("new", "N1", _box(LAT, LON, 100.0, dx_m=5_000.0))]

    assert _links(old, new) == []
    assert score(old, new, _comps(old, new, []))["only_in_old"] == 1


# ---------------------------------------------------------------------------
# Correspondence — components, not a forced one-to-one
# ---------------------------------------------------------------------------

def test_two_new_harbours_inside_one_old_port_form_one_group():
    """
    A port the pipeline split into two basins is one disagreement about
    extent. Forcing one-to-one would report it as one harbour lost and one
    invented, which is two wrong statements instead of none.
    """
    old = [_rec("old", "O1", _box(LAT, LON, 2000.0))]
    new = [_rec("new", "N1", _box(LAT, LON, 100.0, dx_m=-400.0)),
           _rec("new", "N2", _box(LAT, LON, 100.0, dx_m=400.0))]

    comps = _comps(old, new, _links(old, new))
    assert len(comps) == 1
    assert comps[0].cardinality == "1:N"

    stats = score(old, new, comps)
    assert (stats["only_in_old"], stats["only_in_new"]) == (0, 0)


def test_one_new_harbour_spanning_two_old_polygons_is_reported_as_a_merge():
    old = [_rec("old", "O1", _box(LAT, LON, 200.0, dx_m=-300.0)),
           _rec("old", "O2", _box(LAT, LON, 200.0, dx_m=300.0))]
    new = [_rec("new", "N1", _box(LAT, LON, 1000.0))]

    comps = _comps(old, new, _links(old, new))
    assert len(comps) == 1
    assert comps[0].cardinality == "N:1"


def test_an_unlinked_old_polygon_lands_in_only_in_old():
    """Seeding the graph with every record is what makes this fall out."""
    old = [_rec("old", "O1", _box(LAT, LON, 100.0)),
           _rec("old", "O2", _box(LAT, LON, 100.0, dx_m=50_000.0))]
    new = [_rec("new", "N1", _box(LAT, LON, 100.0))]

    comps = _comps(old, new, _links(old, new))
    stats = score(old, new, comps)
    assert stats["only_in_old"] == 1
    assert stats["agreement_old"] == pytest.approx(0.5)
    assert stats["cardinality"]["1:0"] == 1


def test_an_oversized_group_is_marked_tangled_and_left_out_of_calibration():
    """
    Big polygons chain — old A meets new X, new X meets old B — and one chain
    can swallow a port city. A tangled group is still reported, but must not
    move a median on its own.
    """
    old = [_rec("old", "O1", _box(LAT, LON, 100.0))]
    new = [_rec("new", "N1", _box(LAT, LON, 100.0))]
    link_list = _links(old, new)

    comps = _comps(old, new, link_list, max_component=1)
    assert comps[0].tangled is True
    assert score(old, new, comps)["n_tangled"] == 1
    assert calibrate(link_list, comps).n_pairs == 0


# ---------------------------------------------------------------------------
# Calibration
# ---------------------------------------------------------------------------

def test_calibration_names_a_containment_regime_rather_than_scoring_it():
    """
    When one side draws whole ports, IoU is structurally capped. The tool has
    to say so instead of reporting the cap as a quality number.
    """
    old, new = [], []
    for i in range(5):
        offset = i * 10_000.0
        old.append(_rec("old", f"O{i}", _box(LAT, LON, 2000.0, dx_m=offset)))
        new.append(_rec("new", f"N{i}", _box(LAT, LON, 200.0, dx_m=offset)))

    link_list = _links(old, new)
    cal = calibrate(link_list, _comps(old, new, link_list))
    assert cal.regime == "old_is_superset"
    assert cal.headline == "coverage_new"
    assert cal.median_iou < 0.05
    assert "reference only" in cal.note


def test_calibration_calls_two_identical_databases_comparable():
    old, new = [], []
    for i in range(5):
        geom = _box(LAT, LON, 300.0, dx_m=i * 10_000.0)
        old.append(_rec("old", f"O{i}", geom))
        new.append(_rec("new", f"N{i}", geom))

    link_list = _links(old, new)
    cal = calibrate(link_list, _comps(old, new, link_list))
    assert cal.regime == "comparable"
    assert cal.headline == "iou"
    assert cal.median_iou == pytest.approx(1.0, rel=1e-6)


# ---------------------------------------------------------------------------
# Loading a foreign database
# ---------------------------------------------------------------------------

def test_the_name_field_is_detected_by_fill_rate_not_by_first_key():
    """
    A database carrying an empty `name` beside a populated `PORT_NAME` would
    otherwise be read through the empty one, losing every name while nothing
    looked wrong.
    """
    features = [
        _feature(_box(LAT, LON, 100.0, dx_m=i * 5000.0),
                 name="", PORT_NAME=f"Harbour {i}")
        for i in range(4)
    ]
    key, fill = detect_field(features, ("name", "PORT_NAME"))
    assert (key, fill) == ("PORT_NAME", 1.0)


def test_a_three_letter_code_and_a_country_name_both_normalise_to_iso2():
    assert normalise_country("DNK") == "DK"
    assert normalise_country("de") == "DE"
    assert normalise_country("Germany") == "DE"
    assert normalise_country("") is None
    assert normalise_country(None) is None


def test_a_danish_city_survives_name_normalisation():
    """
    NFKD leaves ø, æ and å intact — they are letters, not accented vowels — so
    stripping non-ASCII afterwards split København into 'k benhavn' and the
    city stopped matching itself.
    """
    assert normalise_name("København") == "kobenhavn"
    assert normalise_name("Port of København") == "kobenhavn"
    assert normalise_name("Ærøskøbing") == "aeroskobing"
    # Removed as a whole token only: Frederikshavn is a city, not a compound.
    assert normalise_name("Frederikshavn") == "frederikshavn"


def test_a_synthesised_old_id_is_stable_for_the_same_file(tmp_path):
    """Positional, so re-running against the same file reproduces it."""
    path = _write_fc(tmp_path / "old.geojson", [
        _feature(_box(LAT, LON, 100.0, dx_m=i * 5000.0)) for i in range(3)
    ])
    first, choice, _ = load_old(path)
    second, _, _ = load_old(path)

    assert choice.id_field is None
    assert [r.rec_id for r in first] == ["OLD-0001", "OLD-0002", "OLD-0003"]
    assert [r.rec_id for r in first] == [r.rec_id for r in second]


def test_a_self_intersecting_old_polygon_is_repaired_rather_than_dropped(
        tmp_path):
    """
    Foreign polygon databases are routinely invalid, and `.area` on an invalid
    polygon is garbage. Never assert `covers()` on a repaired geometry: GEOS
    shifts boundary coordinates by a few ULPs.
    """
    bowtie = Polygon([(12.60, 55.70), (12.61, 55.71),
                      (12.60, 55.71), (12.61, 55.70)])
    path = _write_fc(tmp_path / "old.geojson", [_feature(bowtie)])

    records, _, invalid = load_old(path)
    assert invalid == []
    assert len(records) == 1
    assert records[0].geom.is_valid
    assert records[0].geom.area > 0


def test_an_old_polygon_with_no_area_is_counted_not_silently_skipped(tmp_path):
    path = _write_fc(tmp_path / "old.geojson", [
        _feature(_box(LAT, LON, 100.0)),
        {"type": "Feature", "properties": {},
         "geometry": {"type": "LineString",
                      "coordinates": [[12.6, 55.7], [12.7, 55.8]]}},
    ])
    records, _, invalid = load_old(path)
    assert len(records) == 1
    assert len(invalid) == 1


# ---------------------------------------------------------------------------
# Coverage
# ---------------------------------------------------------------------------

def test_the_coverage_gate_splits_on_where_vessels_actually_stopped():
    cells = coverage_cells([LAT], [LON], resolution=5, dilate=1)
    near = _rec("old", "NEAR", _box(LAT, LON, 100.0))
    far = _rec("old", "FAR", _box(LAT + 8.0, LON + 8.0, 100.0))

    inside, outside = in_coverage([near, far], cells, resolution=5)
    assert [r.rec_id for r in inside] == ["NEAR"]
    assert [r.rec_id for r in outside] == ["FAR"]


def test_no_coverage_cells_means_everything_stays_in_scope():
    """An empty gate must not silently delete the whole comparison."""
    recs = [_rec("old", "A", _box(LAT, LON, 100.0))]
    inside, outside = in_coverage(recs, set(), resolution=5)
    assert (len(inside), len(outside)) == (1, 0)


# ---------------------------------------------------------------------------
# Diagnosis
# ---------------------------------------------------------------------------

def _layers(stops=(), cells=(), clusters=()):
    def arr(points):
        if not points:
            return np.array([]), np.array([])
        return (np.array([p[0] for p in points]),
                np.array([p[1] for p in points]))
    return {"stops": arr(stops), "cells": arr(cells),
            "clusters": arr(clusters)}


def test_an_old_harbour_with_no_stops_inside_it_is_diagnosed_as_no_stops():
    rec = _rec("old", "O1", _box(LAT, LON, 200.0))
    diag = diagnose_only_in_old([rec], [], _layers())
    assert diag["O1"].bucket == "no_stops"
    assert diag["O1"].n_stops == 0


def test_an_old_harbour_with_cells_but_no_cluster_is_diagnosed_by_elimination():
    """
    `harbour_clusters.parquet` holds only the clusters that survived Phase 3,
    so a cluster that formed and was then dropped leaves no trace. The bucket
    is inferred, and the report has to say so.
    """
    rec = _rec("old", "O1", _box(LAT, LON, 200.0))
    diag = diagnose_only_in_old(
        [rec], [], _layers(stops=[(LAT, LON)], cells=[(LAT, LON)]))
    assert diag["O1"].bucket == "below_cluster_floor"
    assert (diag["O1"].n_stops, diag["O1"].n_clusters) == (1, 0)


def test_a_harbour_with_a_pipeline_neighbour_is_a_disagreement_about_extent():
    """Nearby-but-unlinked is a different statement from undetected."""
    rec = _rec("old", "O1", _box(LAT, LON, 200.0))
    neighbour = _rec("new", "N1", _box(LAT, LON, 200.0, dx_m=600.0))
    diag = diagnose_only_in_old([rec], [neighbour], _layers())
    assert diag["O1"].bucket == "unpaired_nearby"
    assert diag["O1"].nearest_id == "N1"


def test_the_diagnosis_degrades_to_unknown_when_the_interim_files_are_absent(
        tmp_path):
    rec = _rec("old", "O1", _box(LAT, LON, 200.0))
    layers = load_interim_layers(tmp_path)
    assert layers == {}

    diag = diagnose_only_in_old([rec], [], layers)
    assert diag["O1"].bucket == "unknown"


# ---------------------------------------------------------------------------
# The CLI
# ---------------------------------------------------------------------------

def _two_db_fixture(tmp_path, transit_extra=True):
    """Two old harbours, both matched, plus an unmatched transit_like new one."""
    old = [
        _feature(_box(LAT, LON, 300.0), name="Alpha", country="DK"),
        _feature(_box(LAT, LON, 300.0, dx_m=10_000.0), name="Beta",
                 country="DK"),
    ]
    new = [
        _feature(_box(LAT, LON, 300.0), harbour_id="DK-aaaa1111",
                 nearest_city="Alpha", country_iso2="DK", centroid_lat=LAT,
                 centroid_lon=LON, n_events=100, n_unique_mmsi=10,
                 transit_like=False),
        _feature(_box(LAT, LON, 300.0, dx_m=10_000.0),
                 harbour_id="DK-bbbb2222", nearest_city="Beta",
                 country_iso2="DK", n_events=50, n_unique_mmsi=5,
                 transit_like=False),
    ]
    if transit_extra:
        new.append(_feature(
            _box(LAT, LON, 300.0, dx_m=80_000.0), harbour_id="DK-cccc3333",
            nearest_city="Lock", country_iso2="DK", n_events=20,
            n_unique_mmsi=4, transit_like=True))
    return (_write_fc(tmp_path / "old.geojson", old),
            _write_fc(tmp_path / "new.geojson", new))


def _run(tmp_path, *extra):
    old, new = _two_db_fixture(tmp_path)
    out = tmp_path / "out"
    code = main(["--old", str(old), "--new", str(new), "--out-dir", str(out),
                 "--no-map", "--no-diagnose", *extra])
    summary = json.loads((out / "summary.json").read_text())
    return code, summary


def test_the_report_exits_non_zero_when_the_old_database_has_no_polygons(
        tmp_path):
    _, new = _two_db_fixture(tmp_path)
    empty = _write_fc(tmp_path / "empty.geojson", [])
    code = main(["--old", str(empty), "--new", str(new),
                 "--out-dir", str(tmp_path / "out"), "--no-map"])
    assert code == 1


def test_a_run_reports_both_agreement_rates_separately(tmp_path):
    code, summary = _run(tmp_path)
    assert code == 0
    assert summary["scores"]["agreement_old"] == pytest.approx(1.0)
    assert summary["scores"]["agreement_new"] == pytest.approx(2 / 3)
    assert "f1" not in summary["scores"]


def test_excluding_transit_like_sites_changes_only_the_new_side_agreement(
        tmp_path):
    """
    Only 2 of 331 harbours are transit_like in the reference run, so the real
    swing is a fraction of a point — this pins the direction, not a magnitude.
    """
    _, plain = _run(tmp_path)
    _, without = _run(tmp_path, "--exclude-transit")

    assert without["scores"]["agreement_old"] == \
        plain["scores"]["agreement_old"]
    assert without["scores"]["agreement_new"] > \
        plain["scores"]["agreement_new"]
    assert without["scores"]["agreement_new"] == pytest.approx(1.0)


def test_the_coverage_gate_is_off_unless_it_is_asked_for(tmp_path):
    """Dropping rows by default would quietly inflate the agreement rate."""
    _, summary = _run(tmp_path)
    assert "coverage gate" not in summary["excluded"]
    assert summary["out_of_coverage"] == []


def test_a_country_allowlist_says_how_much_it_excluded(tmp_path):
    """A gate that silently removed rows would inflate the agreement rate."""
    old = _write_fc(tmp_path / "old.geojson", [
        _feature(_box(LAT, LON, 300.0), name="Alpha", country="DK"),
        _feature(_box(LAT, LON, 300.0, dx_m=10_000.0), name="Hansa",
                 country="DE"),
    ])
    new = _write_fc(tmp_path / "new.geojson", [
        _feature(_box(LAT, LON, 300.0), harbour_id="DK-aaaa1111",
                 nearest_city="Alpha", country_iso2="DK", n_events=100),
    ])
    out = tmp_path / "out"
    assert main(["--old", str(old), "--new", str(new), "--out-dir", str(out),
                 "--no-map", "--no-diagnose", "--countries", "DK"]) == 0

    summary = json.loads((out / "summary.json").read_text())
    assert summary["excluded"]["country allowlist"] == 1
    assert summary["scores"]["n_old"] == 1


def test_the_map_page_links_its_leaflet_assets_relatively_when_offline(
        tmp_path, monkeypatch):
    """
    `use_local_assets` mutates folium's class attributes globally and cannot be
    undone, so the originals are restored here — otherwise this test poisons
    every folium map built later in the session, including test_gui's.
    """
    folium = pytest.importorskip("folium")
    from folium.plugins import Draw

    import compare_harbour_dbs as cli
    saved = [(cls, attr, list(getattr(cls, attr)))
             for cls in (folium.Map, Draw)
             for attr in ("default_js", "default_css")]
    # The vendored files need not actually be on disk for the page to be
    # rendered against them; only the URL rewriting is under test.
    monkeypatch.setattr(cli, "is_vendored", lambda *a, **k: True)
    monkeypatch.setattr(cli.shutil, "copytree", lambda *a, **k: None)
    try:
        old, new = _two_db_fixture(tmp_path)
        out = tmp_path / "out"
        assert main(["--old", str(old), "--new", str(new),
                     "--out-dir", str(out), "--offline",
                     "--no-diagnose"]) == 0
        page = (out / "compare_map.html").read_text(encoding="utf-8")
    finally:
        for cls, attr, value in saved:
            setattr(cls, attr, value)

    assert 'src="vendor/leaflet.js"' in page
    assert "https://cdn.jsdelivr.net" not in page
    # folium's Popup template builds its content with `$(...)`, and every
    # harbour here carries a popup — without jquery the whole map dies.
    assert "$(" in page
    assert "vendor/jquery" in page
