"""Unit tests for Phase 4 enrichment."""

from pathlib import Path

import h3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from shapely.wkt import loads as from_wkt

from pipeline.cluster_formation import CLUSTER_SCHEMA
from pipeline.enrichment import (
    Phase4Config,
    _add_geocoding,
    _add_polygons,
    _country_name,
    _make_polygon_wkt,
    run_phase4,
)

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
        "n_unique_mmsi_approx": 30,
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
            "cluster_id":           pa.array(df["cluster_id"],           type=pa.int32()),
            "h3_cells":             h3_cells_array,
            "n_cells":              pa.array(df["n_cells"],              type=pa.int32()),
            "n_events":             pa.array(df["n_events"],             type=pa.int32()),
            "n_unique_mmsi_approx": pa.array(df["n_unique_mmsi_approx"],type=pa.int32()),
            "n_draught_changes":    pa.array(df["n_draught_changes"],    type=pa.int32()),
            "centroid_lat":         pa.array(df["centroid_lat"],         type=pa.float64()),
            "centroid_lon":         pa.array(df["centroid_lon"],         type=pa.float64()),
            "centroid_h3_r8":       pa.array(df["centroid_h3_r8"],       type=pa.string()),
            "bbox_min_lat":         pa.array(df["bbox_min_lat"],         type=pa.float64()),
            "bbox_max_lat":         pa.array(df["bbox_max_lat"],         type=pa.float64()),
            "bbox_min_lon":         pa.array(df["bbox_min_lon"],         type=pa.float64()),
            "bbox_max_lon":         pa.array(df["bbox_max_lon"],         type=pa.float64()),
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


def test_add_polygons_column_added():
    df = _make_clusters_df([_cluster_row(0, HAMBURG_LAT, HAMBURG_LON)])
    result = _add_polygons(df)
    assert "geometry_wkt" in result.columns
    assert result.iloc[0]["geometry_wkt"] is not None


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
    result = _add_geocoding(df)
    assert result.iloc[0]["country_iso2"] == "DE"
    assert result.iloc[0]["country_name"] == "Germany"
    assert result.iloc[0]["nearest_city"] != ""


def test_geocoding_singapore():
    df = _make_clusters_df([_cluster_row(0, SINGAPORE_LAT, SINGAPORE_LON)])
    result = _add_geocoding(df)
    assert result.iloc[0]["country_iso2"] == "SG"


def test_geocoding_city_distance_is_positive():
    df = _make_clusters_df([_cluster_row(0, ROTTERDAM_LAT, ROTTERDAM_LON)])
    result = _add_geocoding(df)
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
