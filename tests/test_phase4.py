"""Unit tests for Phase 4 enrichment."""

from pathlib import Path

import h3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.wkt import loads as from_wkt

from pipeline.cluster_formation import CLUSTER_SCHEMA
from pipeline.enrichment import (
    Phase4Config,
    _add_geocoding,
    _add_polygons,
    _country_name,
    _make_polygon_wkt,
    population_floor,
    run_phase4,
)
from utils.gazetteer import GAZETTEER_SCHEMA

RES = 11
HAMBURG_LAT,    HAMBURG_LON    = 53.54,  9.97
ROTTERDAM_LAT,  ROTTERDAM_LON  = 51.90,  4.47
SINGAPORE_LAT,  SINGAPORE_LON  =  1.29, 103.85


def _cluster_row(cluster_id: int, lat: float, lon: float, n_cells: int = 7) -> dict:
    seed = h3.latlng_to_cell(lat, lon, RES)
    cells = sorted(h3.grid_disk(seed, 1))[:n_cells]
    return {
        "cluster_id":           cluster_id,
        "h3_cells":             cells,
        "n_cells":              len(cells),
        "n_events":             100,
        "n_unique_mmsi":        30,
        "n_draught_changes":    5,
        "centroid_lat":         lat,
        "centroid_lon":         lon,
        "centroid_h3_r8":       h3.latlng_to_cell(lat, lon, 8),
        "bbox_min_lat":         lat - 0.001,
        "bbox_max_lat":         lat + 0.001,
        "bbox_min_lon":         lon - 0.001,
        "bbox_max_lon":         lon + 0.001,
    }


def _make_clusters_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _write_clusters_parquet(rows: list[dict], path: Path) -> None:
    df = _make_clusters_df(rows)
    h3_cells_array = pa.array(df["h3_cells"].tolist(), type=pa.list_(pa.string()))
    table = pa.table(
        {
            "cluster_id":        pa.array(df["cluster_id"],        type=pa.int32()),
            "h3_cells":          h3_cells_array,
            "n_cells":           pa.array(df["n_cells"],           type=pa.int32()),
            "n_events":          pa.array(df["n_events"],          type=pa.int32()),
            "n_unique_mmsi":     pa.array(df["n_unique_mmsi"],     type=pa.int32()),
            "n_draught_changes": pa.array(df["n_draught_changes"], type=pa.int32()),
            "centroid_lat":      pa.array(df["centroid_lat"],      type=pa.float64()),
            "centroid_lon":      pa.array(df["centroid_lon"],      type=pa.float64()),
            "centroid_h3_r8":    pa.array(df["centroid_h3_r8"],    type=pa.string()),
            "bbox_min_lat":      pa.array(df["bbox_min_lat"],      type=pa.float64()),
            "bbox_max_lat":      pa.array(df["bbox_max_lat"],      type=pa.float64()),
            "bbox_min_lon":      pa.array(df["bbox_min_lon"],      type=pa.float64()),
            "bbox_max_lon":      pa.array(df["bbox_max_lon"],      type=pa.float64()),
        },
        schema=CLUSTER_SCHEMA,
    )
    pq.write_table(table, path)


# ---------------------------------------------------------------------------

def test_polygon_is_valid_wkt():
    seed = h3.latlng_to_cell(HAMBURG_LAT, HAMBURG_LON, RES)
    cells = list(h3.grid_disk(seed, 1))
    wkt = _make_polygon_wkt(cells)
    assert wkt is not None
    geom = from_wkt(wkt)
    assert geom.is_valid
    assert not geom.is_empty


def test_polygon_contains_centroid():
    seed = h3.latlng_to_cell(HAMBURG_LAT, HAMBURG_LON, RES)
    cells = list(h3.grid_disk(seed, 2))
    wkt = _make_polygon_wkt(cells)
    geom = from_wkt(wkt)
    from shapely.geometry import Point
    # H3 cell centre should be inside the polygon
    lat, lon = h3.cell_to_latlng(seed)
    assert geom.contains(Point(lon, lat))


def test_polygon_wkt_empty_cells_returns_none():
    result = _make_polygon_wkt([])
    assert result is None


def _outline_config() -> Phase4Config:
    return Phase4Config(interim_dir="")


def test_add_polygons_column_added():
    df = _make_clusters_df([_cluster_row(0, HAMBURG_LAT, HAMBURG_LON)])
    result = _add_polygons(df, _outline_config())
    assert "geometry_wkt" in result.columns
    assert result.iloc[0]["geometry_wkt"] is not None


def test_add_polygons_outline_column_added():
    df = _make_clusters_df([_cluster_row(0, HAMBURG_LAT, HAMBURG_LON)])
    result = _add_polygons(df, _outline_config())
    assert "outline_wkt" in result.columns
    assert result.iloc[0]["outline_wkt"] is not None


def test_outline_is_hole_free_and_grows():
    """The outline encloses at least the cell union and carries no holes."""
    df = _make_clusters_df([_cluster_row(0, HAMBURG_LAT, HAMBURG_LON)])
    result = _add_polygons(df, _outline_config())

    cells   = from_wkt(result.iloc[0]["geometry_wkt"])
    outline = from_wkt(result.iloc[0]["outline_wkt"])

    assert outline.area >= cells.area
    polys = outline.geoms if outline.geom_type == "MultiPolygon" else [outline]
    assert all(len(p.interiors) == 0 for p in polys)


def test_outline_covers_every_cell():
    """
    Without vertex thinning the outline strictly contains the cell union —
    no cell that saw traffic may fall outside the harbour it belongs to.
    """
    from pipeline.enrichment import _cells_to_geom
    from utils.geo import outline_polygon

    cells = sorted(h3.grid_disk(h3.latlng_to_cell(HAMBURG_LAT, HAMBURG_LON, RES), 1))
    geom  = _cells_to_geom(cells)

    outline = outline_polygon(geom, buffer_meters=75.0, simplify_meters=0)
    # 1e-12° is ~0.1 µm — absorbs the float noise of the degrees→m→degrees
    # round-trip, while staying far below any real geometric deviation.
    assert outline.buffer(1e-12).covers(geom)


def test_outline_bridges_gap_between_cell_groups():
    """Two cell groups ~200 m apart close into a single polygon at 75 m."""
    from pipeline.enrichment import _cells_to_geom
    from utils.geo import outline_polygon

    left  = h3.latlng_to_cell(HAMBURG_LAT, HAMBURG_LON, RES)
    # ~200 m east, leaving a cold gap of roughly 50 m between the two groups.
    right = h3.latlng_to_cell(HAMBURG_LAT, HAMBURG_LON + 0.0030, RES)
    cells = sorted(set(h3.grid_disk(left, 1)) | set(h3.grid_disk(right, 1)))

    geom = _cells_to_geom(cells)
    assert geom.geom_type == "MultiPolygon"          # starts as two islands

    outline = outline_polygon(geom, buffer_meters=75.0)
    assert outline.geom_type == "Polygon"            # closed into one


def test_outline_keeps_distant_groups_separate():
    """Groups ~400 m apart stay separate — closing must not invent land."""
    from pipeline.enrichment import _cells_to_geom
    from utils.geo import outline_polygon

    left  = h3.latlng_to_cell(HAMBURG_LAT, HAMBURG_LON, RES)
    right = h3.latlng_to_cell(HAMBURG_LAT, HAMBURG_LON + 0.0060, RES)
    cells = sorted(set(h3.grid_disk(left, 1)) | set(h3.grid_disk(right, 1)))

    outline = outline_polygon(_cells_to_geom(cells), buffer_meters=75.0)
    assert outline.geom_type == "MultiPolygon"


def test_country_name_germany():
    assert _country_name("DE") == "Germany"


def test_country_name_netherlands():
    assert "Netherlands" in _country_name("NL")


def test_country_name_unknown_falls_back_to_code():
    assert _country_name("XX") == "XX"


def test_country_name_empty_string():
    assert _country_name("") == ""


def test_geocoding_hamburg():
    df = _make_clusters_df([_cluster_row(0, HAMBURG_LAT, HAMBURG_LON)])
    result = _add_geocoding(df, Phase4Config(interim_dir=""))
    assert result.iloc[0]["country_iso2"] == "DE"
    assert result.iloc[0]["country_name"] == "Germany"
    assert result.iloc[0]["nearest_city"] != ""


def test_geocoding_singapore():
    df = _make_clusters_df([_cluster_row(0, SINGAPORE_LAT, SINGAPORE_LON)])
    result = _add_geocoding(df, Phase4Config(interim_dir=""))
    assert result.iloc[0]["country_iso2"] == "SG"


def test_geocoding_city_distance_is_positive():
    df = _make_clusters_df([_cluster_row(0, ROTTERDAM_LAT, ROTTERDAM_LON)])
    result = _add_geocoding(df, Phase4Config(interim_dir=""))
    assert result.iloc[0]["nearest_city_dist_km"] >= 0


def test_run_phase4_end_to_end(tmp_path):
    rows = [
        _cluster_row(0, HAMBURG_LAT,   HAMBURG_LON),
        _cluster_row(1, ROTTERDAM_LAT, ROTTERDAM_LON),
        _cluster_row(2, SINGAPORE_LAT, SINGAPORE_LON),
    ]
    _write_clusters_parquet(rows, tmp_path / "harbour_clusters.parquet")

    config = Phase4Config(interim_dir=str(tmp_path))
    out    = run_phase4(config)

    result = pd.read_parquet(out)
    assert len(result) == 3
    assert set(result["country_iso2"]) == {"DE", "NL", "SG"}
    assert result["geometry_wkt"].notna().all()
    assert result["nearest_city"].str.len().gt(0).all()


# ---------------------------------------------------------------------------
# Nearest city — size tiers and the gazetteer
# ---------------------------------------------------------------------------

TIERS = [
    {"min_cells": 0, "min_population": 0},
    {"min_cells": 100, "min_population": 1000},
    {"min_cells": 1000, "min_population": 15000},
]


def test_a_small_harbour_has_no_population_floor():
    """Which is the point: it should take the name of the village beside it."""
    assert population_floor(7, TIERS) == 0
    assert population_floor(99, TIERS) == 0


def test_the_floor_rises_with_the_harbour():
    assert population_floor(100, TIERS) == 1000
    assert population_floor(999, TIERS) == 1000
    assert population_floor(5000, TIERS) == 15000


def test_tiers_may_be_listed_in_any_order():
    """They are config; nobody should have to keep them sorted by hand."""
    assert population_floor(1000, list(reversed(TIERS))) == 15000


def test_no_tiers_means_no_floor():
    assert population_floor(10_000, []) == 0


def _write_gazetteer(path, places) -> None:
    columns = list(zip(*places))
    pq.write_table(pa.table({
        field.name: pa.array(list(values), type=field.type)
        for field, values in zip(GAZETTEER_SCHEMA, columns)
    }, schema=GAZETTEER_SCHEMA), path)


def test_a_configured_gazetteer_names_a_harbour_after_its_village(tmp_path):
    """
    End to end through _add_geocoding: the village is 1 km away and absent from
    cities1000, the town is 20 km away and in it. The small harbour gets the
    village.
    """
    path = tmp_path / "places.parquet"
    _write_gazetteer(path, [
        ("Vejrø",   ROTTERDAM_LAT + 0.009, ROTTERDAM_LON,     0, "PPL", "NL", "Zuid"),
        ("Big City", ROTTERDAM_LAT + 0.18,  ROTTERDAM_LON, 90000, "PPL", "NL", "Zuid"),
    ])
    df = _make_clusters_df([_cluster_row(0, ROTTERDAM_LAT, ROTTERDAM_LON)])

    result = _add_geocoding(df, Phase4Config(interim_dir="", gazetteer_path=str(path)))

    assert result.iloc[0]["nearest_city"] == "Vejrø"
    assert result.iloc[0]["nearest_city_dist_km"] < 2
    assert result.iloc[0]["admin1"] == "Zuid"
    # The country still comes from reverse_geocoder — the harbour ID depends on
    # it, so the gazetteer must not be able to move it.
    assert result.iloc[0]["country_iso2"] == "NL"


def test_a_large_harbour_skips_the_village_for_the_city(tmp_path):
    path = tmp_path / "places.parquet"
    _write_gazetteer(path, [
        ("Vejrø",   ROTTERDAM_LAT + 0.009, ROTTERDAM_LON,     0, "PPL", "NL", "Zuid"),
        ("Big City", ROTTERDAM_LAT + 0.18,  ROTTERDAM_LON, 90000, "PPL", "NL", "Zuid"),
    ])
    df = _make_clusters_df([
        _cluster_row(0, ROTTERDAM_LAT, ROTTERDAM_LON, n_cells=7),
    ])
    df.loc[0, "n_cells"] = 2000          # a port, not a marina

    result = _add_geocoding(
        df, Phase4Config(interim_dir="", gazetteer_path=str(path),
                         city_population_tiers=TIERS))

    assert result.iloc[0]["nearest_city"] == "Big City"


def test_an_unreachable_floor_falls_back_to_the_nearest_place(tmp_path):
    """
    A floor that only matches something 500 km away is worse than no floor —
    better the village next door than a city across the sea.
    """
    path = tmp_path / "places.parquet"
    _write_gazetteer(path, [
        ("Vejrø",   ROTTERDAM_LAT + 0.009, ROTTERDAM_LON,     0, "PPL", "NL", "Zuid"),
        ("Faraway", ROTTERDAM_LAT + 4.5,   ROTTERDAM_LON, 90000, "PPL", "NL", "Zuid"),
    ])
    df = _make_clusters_df([_cluster_row(0, ROTTERDAM_LAT, ROTTERDAM_LON)])
    df.loc[0, "n_cells"] = 2000

    result = _add_geocoding(
        df, Phase4Config(interim_dir="", gazetteer_path=str(path),
                         city_population_tiers=TIERS, max_city_dist_km=50))

    assert result.iloc[0]["nearest_city"] == "Vejrø"


def test_a_missing_gazetteer_still_produces_a_city(tmp_path):
    """Phase 4 must run on a machine where nobody prepared the file."""
    df = _make_clusters_df([_cluster_row(0, HAMBURG_LAT, HAMBURG_LON)])
    result = _add_geocoding(
        df, Phase4Config(interim_dir="",
                         gazetteer_path=str(tmp_path / "absent.parquet")))

    assert result.iloc[0]["nearest_city"] != ""
    assert result.iloc[0]["country_iso2"] == "DE"
