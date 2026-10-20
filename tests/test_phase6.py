"""
Unit tests for Phase 6 — grouping harbours into port areas.

Self-contained: harbours are built from H3 cells around chosen points, land is
a shapely box handed straight to `Land`, and municipalities are passed in, so
nothing needs a gazetteer, a land file or a pipeline run.
"""

import json
import math

import h3
import pandas as pd
import pytest
from shapely.geometry import box, shape
from shapely.wkt import dumps as to_wkt
from shapely.wkt import loads as from_wkt

from pipeline.h3_aggregation import VESSEL_COUNTS
from pipeline.port_areas import (
    Phase6Config,
    _cut_land,
    build_port_areas,
    run_phase6,
)
from utils.coastline import Land

LAT, LON = 56.0, 10.0
M_PER_DEG_LON = 111_320 * math.cos(math.radians(LAT))


def _harbour(harbour_id: str, east_m: float = 0.0, commercial: bool = False,
             rings: int = 1, n_events: int = 50) -> dict:
    """A harbour of res-11 cells around a point `east_m` metres east of LON."""
    lon = LON + east_m / M_PER_DEG_LON
    cells = sorted(h3.grid_disk(h3.latlng_to_cell(LAT, lon, 11), rings))
    outline = shape(h3.cells_to_h3shape(cells).__geo_interface__)
    counts = {c: 0 for c in VESSEL_COUNTS}
    counts["n_cargo" if commercial else "n_recreational"] = 10
    return {
        "harbour_id": harbour_id,
        "h3_cells": cells,
        "n_cells": len(cells),
        "n_events": n_events,
        "centroid_lat": LAT,
        "centroid_lon": lon,
        "nearest_city": f"City {harbour_id}",
        "country_iso2": "DK",
        "outline_wkt": to_wkt(outline),
        **counts,
    }


def _config(**overrides) -> Phase6Config:
    params = dict(output_dir="unused", max_gap_meters=1000.0,
                  commercial_max_gap_meters=2500.0, commercial_min_share=0.3,
                  same_municipality=False)
    params.update(overrides)
    return Phase6Config(**params)


def _areas(harbours: list[dict], config: Phase6Config | None = None,
           municipality: list | None = None, land: Land | None = None):
    df = pd.DataFrame(harbours)
    return build_port_areas(
        df, config or _config(),
        municipality=municipality or [None] * len(df),
        land=land or Land([box(50, 50, 51, 51)]),   # land far away: no cut
    )


def _members(areas: pd.DataFrame) -> list[list[str]]:
    return sorted(sorted(ids) for ids in areas["harbour_ids"])


# -- linking ------------------------------------------------------------------

def test_a_harbour_without_neighbours_is_an_area_of_its_own():
    areas = _areas([_harbour("DK-aaaa0001")])
    assert len(areas) == 1
    row = areas.iloc[0]
    assert row["port_area_id"] == "PA-DK-aaaa0001"
    assert list(row["harbour_ids"]) == ["DK-aaaa0001"]
    assert row["name"] == "City DK-aaaa0001"


def test_harbours_within_the_gap_share_an_area_and_further_ones_do_not():
    areas = _areas([_harbour("DK-a", 0), _harbour("DK-b", 700),
                    _harbour("DK-c", 2500)])
    assert _members(areas) == [["DK-a", "DK-b"], ["DK-c"]]


def test_linking_is_transitive():
    areas = _areas([_harbour("DK-a", 0), _harbour("DK-b", 900),
                    _harbour("DK-c", 1800)])
    assert _members(areas) == [["DK-a", "DK-b", "DK-c"]]


def test_two_commercial_harbours_reach_across_the_larger_gap():
    far = [_harbour("DK-a", 0, commercial=True),
           _harbour("DK-b", 2000, commercial=True)]
    assert _members(_areas(far)) == [["DK-a", "DK-b"]]

    one_marina = [_harbour("DK-a", 0, commercial=True), _harbour("DK-b", 2000)]
    assert _members(_areas(one_marina)) == [["DK-a"], ["DK-b"]]


def test_the_commercial_gap_switches_off_when_set_to_the_general_gap():
    far = [_harbour("DK-a", 0, commercial=True),
           _harbour("DK-b", 2000, commercial=True)]
    config = _config(commercial_max_gap_meters=1000.0)
    assert _members(_areas(far, config)) == [["DK-a"], ["DK-b"]]


def test_different_municipalities_are_never_linked():
    pair = [_harbour("DK-a", 0), _harbour("DK-b", 500)]
    split = _areas(pair, municipality=[("DK", "17", "101"), ("DK", "17", "157")])
    assert _members(split) == [["DK-a"], ["DK-b"]]


def test_an_unknown_municipality_does_not_veto_a_link():
    pair = [_harbour("DK-a", 0), _harbour("DK-b", 500)]
    areas = _areas(pair, municipality=[("DK", "17", "101"), None])
    assert _members(areas) == [["DK-a", "DK-b"]]


# -- the area -----------------------------------------------------------------

def test_the_area_bridges_the_water_between_its_harbours():
    # Compact harbours at almost the full gap: a closing alone erodes the
    # neck it grows between them, so this is what the link hulls are for.
    areas = _areas([_harbour("DK-a", 0), _harbour("DK-b", 1050)])
    assert len(areas) == 1
    cells = from_wkt(areas.iloc[0]["geometry_wkt"])
    assert cells.geom_type == "Polygon"          # one piece, gap closed


def test_every_member_cell_is_in_its_area():
    harbours = [_harbour("DK-a", 0, rings=3), _harbour("DK-b", 700)]
    areas = _areas(harbours)
    cells = set(areas.iloc[0]["h3_cells"])
    assert all(set(h["h3_cells"]) <= cells for h in harbours)
    assert areas.iloc[0]["n_cells"] == len(cells)


def test_land_is_cut_away_but_member_cells_on_it_are_kept():
    pair = [_harbour("DK-a", 0, rings=2), _harbour("DK-b", 600, rings=2)]
    # Land south of a line through both centres: the quay half of each.
    land = Land([box(LON - 0.1, LAT - 0.1, LON + 0.1, LAT)])
    areas = _areas(pair, land=land)
    row = areas.iloc[0]
    assert row["land_cut"]
    own = set(pair[0]["h3_cells"]) | set(pair[1]["h3_cells"])
    assert own <= set(row["h3_cells"])
    assert any(h3.cell_to_latlng(c)[0] < LAT for c in own)

    extra = set(row["h3_cells"]) - own
    assert extra, "the bridge added no water at all"
    assert all(h3.cell_to_latlng(c)[0] >= LAT for c in extra)


def test_water_cut_off_from_every_member_is_dropped():
    area = box(0.0, 0.0, 3.0, 1.0)
    members = box(0.2, 0.2, 0.4, 0.4)
    strait_of_land = box(1.0, -1.0, 2.0, 2.0)    # splits the area in two
    kept = _cut_land(area, members, Land([strait_of_land]))
    assert kept.bounds[2] <= 1.0 + 1e-9          # the east part is gone


def test_switching_the_land_cut_off_keeps_the_land():
    pair = [_harbour("DK-a", 0, rings=2), _harbour("DK-b", 600, rings=2)]
    land = Land([box(LON - 0.1, LAT - 0.1, LON + 0.1, LAT)])
    on = _areas(pair, land=land).iloc[0]
    off = _areas(pair, _config(cut_land=False), land=land).iloc[0]
    assert not off["land_cut"]
    assert set(on["h3_cells"]) < set(off["h3_cells"])


def test_a_site_outside_the_land_files_coverage_is_not_cut():
    land = Land([box(LON - 0.1, LAT - 0.1, LON + 0.1, LAT)],
                coverage=(0.0, 0.0, 1.0, 1.0))
    areas = _areas([_harbour("DK-a", 0, rings=2)], land=land)
    assert not areas.iloc[0]["land_cut"]


def test_no_cell_belongs_to_two_areas():
    # Vetoed apart, but close enough that both closings reach the same water.
    pair = [_harbour("DK-a", 0, rings=3), _harbour("DK-b", 450, rings=3)]
    areas = _areas(pair, municipality=[("DK", "1"), ("DK", "2")])
    assert len(areas) == 2
    a, b = (set(c) for c in areas["h3_cells"])
    assert not a & b
    assert set(pair[0]["h3_cells"]) <= a or set(pair[0]["h3_cells"]) <= b


def test_cells_at_a_coarser_resolution_still_cover_every_member():
    harbour = _harbour("DK-a", 0)
    areas = _areas([harbour], _config(cell_resolution=9))
    row = areas.iloc[0]
    assert row["h3_resolution"] == 9
    parents = {h3.cell_to_parent(c, 9) for c in harbour["h3_cells"]}
    assert parents <= set(row["h3_cells"])


# -- ids ----------------------------------------------------------------------

def test_the_area_is_named_after_its_largest_member():
    areas = _areas([_harbour("DK-small", 0, rings=1, n_events=900),
                    _harbour("DK-large", 600, rings=3, n_events=10)])
    row = areas.iloc[0]
    assert row["anchor_harbour_id"] == "DK-large"
    assert row["port_area_id"] == "PA-DK-large"


def test_a_repeated_harbour_id_still_yields_unique_area_ids():
    areas = _areas([_harbour("DK-same", 0), _harbour("DK-same", 5000)])
    assert sorted(areas["port_area_id"]) == ["PA-DK-same", "PA-DK-same-2"]


# -- config + run -------------------------------------------------------------

def test_config_reads_its_keys_and_shares_phase4s_files():
    cfg = {
        "data": {"output_dir": "out"},
        "phase4": {"gazetteer_path": "g.parquet", "coastline_path": "l.parquet"},
        "phase6": {"max_gap_meters": 800, "commercial_max_gap_meters": 3000,
                   "commercial_min_share": 0.5, "same_municipality": False,
                   "cell_resolution": 10},
    }
    config = Phase6Config.from_yaml(cfg)
    assert (config.max_gap_meters, config.commercial_max_gap_meters) == (800, 3000)
    assert config.commercial_min_share == 0.5
    assert not config.same_municipality
    assert config.cell_resolution == 10
    assert (config.gazetteer_path, config.coastline_path) == ("g.parquet",
                                                              "l.parquet")


def test_run_writes_every_harbour_into_exactly_one_feature(tmp_path):
    harbours = pd.DataFrame([_harbour("DK-a", 0), _harbour("DK-b", 700),
                             _harbour("DK-c", 5000)])
    harbours.to_parquet(tmp_path / "harbours.parquet")
    config = _config(output_dir=str(tmp_path), coastline_path="")

    parquet_path, geojson_path = run_phase6(config)

    with open(geojson_path, encoding="utf-8") as fh:
        features = json.load(fh)["features"]
    ids = [i for f in features for i in f["properties"]["harbour_ids"]]
    assert sorted(ids) == ["DK-a", "DK-b", "DK-c"]
    for f in features:
        cells = f["properties"]["h3_cells"]
        geom = shape(f["geometry"])
        union = shape(h3.cells_to_h3shape(cells).__geo_interface__)
        assert geom.symmetric_difference(union).area == pytest.approx(0, abs=1e-12)
    assert len(pd.read_parquet(parquet_path)) == len(features)
