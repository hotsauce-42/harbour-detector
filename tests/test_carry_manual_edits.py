"""
Carrying manual edits across a re-id.

Changing `phase3.centroid_id_resolution` re-issues every harbour_id, which
breaks the link Phase 5 uses to carry an operator's work forward. These pin
that the migration tool re-establishes it from geometry instead.
"""

import json
import sys
from pathlib import Path

from shapely.geometry import box, mapping

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from carry_manual_edits import carry, main  # noqa: E402

DRAWN = "POLYGON ((9.9 53.5, 9.91 53.5, 9.91 53.51, 9.9 53.51, 9.9 53.5))"


def _feature(hid: str, cells: list[str], lat=53.5, lon=9.9, **props) -> dict:
    base = {
        "harbour_id": hid,
        "h3_cells": cells,
        "centroid_lat": lat,
        "centroid_lon": lon,
        "nearest_city": "Hamburg",
    }
    base.update(props)
    geom = box(lon, lat, lon + 0.01, lat + 0.01)
    return {"type": "Feature", "geometry": mapping(geom), "properties": base}


def test_manual_work_follows_the_geometry_not_the_id():
    """The id changed; the harbour did not."""
    cells = ["8b1f05908259fff", "8b1f0590824afff"]
    old = [_feature("OLD-1", cells,
                    manual_outline_wkt=DRAWN,
                    nearest_city="Kappeln",
                    manual_overrides=["nearest_city"])]
    new = [_feature("NEW-9", cells)]

    moved, lost = carry(old, new)

    assert lost == []
    assert moved[0][0] == "OLD-1" and moved[0][1] == "NEW-9"
    props = new[0]["properties"]
    assert props["manual_outline_wkt"] == DRAWN
    assert props["nearest_city"] == "Kappeln"          # the corrected value …
    assert props["manual_overrides"] == ["nearest_city"]  # … and its marker


def test_a_lock_verdict_travels_too():
    cells = ["8b1f05908259fff"]
    old = [_feature("OLD-1", cells, manual_transit_like=True)]
    new = [_feature("NEW-9", cells)]

    carry(old, new)

    assert new[0]["properties"]["manual_transit_like"] is True


def test_harbours_without_manual_work_are_left_alone():
    cells = ["8b1f05908259fff"]
    old = [_feature("OLD-1", cells)]
    new = [_feature("NEW-9", cells, nearest_city="Hamburg")]

    moved, lost = carry(old, new)

    assert (moved, lost) == ([], [])
    assert "manual_outline_wkt" not in new[0]["properties"]


def test_it_matches_the_right_harbour_among_several():
    a, b = ["8b1f05908259fff"], ["8b1f0590824afff"]
    old = [_feature("OLD-B", b, lat=54.0, manual_outline_wkt=DRAWN)]
    new = [_feature("NEW-A", a), _feature("NEW-B", b, lat=54.0)]

    carry(old, new)

    assert "manual_outline_wkt" not in new[0]["properties"]
    assert new[1]["properties"]["manual_outline_wkt"] == DRAWN


def test_an_unmatchable_edit_is_reported_rather_than_guessed():
    """
    Attaching someone's hand-drawn outline to the wrong harbour is worse than
    telling them it could not be placed.
    """
    old = [_feature("OLD-1", ["8b1f05908259fff"], lat=53.5, lon=9.9,
                    manual_outline_wkt=DRAWN)]
    new = [_feature("NEW-9", ["8b0000000000fff"], lat=-33.9, lon=18.4)]

    moved, lost = carry(old, new)

    assert moved == []
    assert lost[0][0] == "OLD-1"
    assert "manual_outline_wkt" not in new[0]["properties"]


def _write(path: Path, features: list[dict]) -> None:
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))


def test_dry_run_writes_nothing(tmp_path, monkeypatch, capsys):
    cells = ["8b1f05908259fff"]
    src, dst = tmp_path / "old.geojson", tmp_path / "new.geojson"
    _write(src, [_feature("OLD-1", cells, manual_outline_wkt=DRAWN)])
    _write(dst, [_feature("NEW-9", cells)])
    before = dst.read_text()

    monkeypatch.setattr(sys, "argv", ["carry", "--from", str(src),
                                      "--to", str(dst), "--dry-run"])
    assert main() == 0
    assert dst.read_text() == before


def test_an_unmatched_edit_exits_non_zero_and_writes_nothing(tmp_path, monkeypatch):
    src, dst = tmp_path / "old.geojson", tmp_path / "new.geojson"
    _write(src, [_feature("OLD-1", ["8b1f05908259fff"], manual_outline_wkt=DRAWN)])
    _write(dst, [_feature("NEW-9", ["8b0000000000fff"], lat=-33.9, lon=18.4)])
    before = dst.read_text()

    monkeypatch.setattr(sys, "argv", ["carry", "--from", str(src), "--to", str(dst)])
    assert main() == 1
    assert dst.read_text() == before


def test_a_normal_run_writes_the_merged_file(tmp_path, monkeypatch):
    cells = ["8b1f05908259fff"]
    src, dst = tmp_path / "old.geojson", tmp_path / "new.geojson"
    _write(src, [_feature("OLD-1", cells, manual_outline_wkt=DRAWN)])
    _write(dst, [_feature("NEW-9", cells)])

    monkeypatch.setattr(sys, "argv", ["carry", "--from", str(src), "--to", str(dst)])
    assert main() == 0

    written = json.loads(dst.read_text())["features"][0]["properties"]
    assert written["manual_outline_wkt"] == DRAWN
