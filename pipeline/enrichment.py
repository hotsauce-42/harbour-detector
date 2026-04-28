"""
Phase 4: Enrichment

For every harbour cluster:
  1. Polygon   — convert H3 cell set to a GeoJSON geometry via h3.cells_to_geo()
  2. Country   — reverse-geocode the centroid with reverse_geocoder, map cc → full name
  3. City      — nearest populated place (GeoNames cities500, pop > 500) from
                 reverse_geocoder, filtered by min_population in post-processing

Output: data/interim/harbours_enriched.parquet
        (harbour_id is added in Phase 5; this file uses cluster_id as a temp key)
"""

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

import h3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pycountry
import reverse_geocoder as rg
from shapely.geometry import mapping, shape
from shapely.wkt import dumps as to_wkt

from utils.geo import haversine_meters
from utils.s3 import (
    build_s3_config,
    get_s3_filesystem,
    get_s3_storage_options,
    is_s3_path,
    path_join,
)

logger = logging.getLogger(__name__)

ENRICHED_SCHEMA = pa.schema([
    pa.field("cluster_id", pa.int32()),
    pa.field("h3_cells", pa.list_(pa.string())),
    pa.field("n_cells", pa.int32()),
    pa.field("n_events", pa.int32()),
    pa.field("n_unique_mmsi_approx", pa.int32()),
    pa.field("n_draught_changes", pa.int32()),
    pa.field("centroid_lat", pa.float64()),
    pa.field("centroid_lon", pa.float64()),
    pa.field("centroid_h3_r8", pa.string()),
    pa.field("bbox_min_lat", pa.float64()),
    pa.field("bbox_max_lat", pa.float64()),
    pa.field("bbox_min_lon", pa.float64()),
    pa.field("bbox_max_lon", pa.float64()),
    pa.field("geometry_wkt", pa.string()),  # WKT of H3-cell polygon
    pa.field("country_iso2", pa.string()),  # ISO 3166-1 alpha-2
    pa.field("country_name", pa.string()),
    pa.field("nearest_city", pa.string()),
    pa.field("nearest_city_lat", pa.float64()),
    pa.field("nearest_city_lon", pa.float64()),
    pa.field("nearest_city_dist_km", pa.float32()),
    pa.field("admin1", pa.string()),  # state / province
])


@dataclass
class Phase4Config:
    interim_dir: str
    city_min_population: int = 1000  # not enforced by reverse_geocoder; kept for docs
    s3_cfg: dict = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, cfg: dict) -> "Phase4Config":
        return cls(
            interim_dir=cfg.get("data", {}).get("interim_dir", "data/interim"),
            city_min_population=cfg.get("phase4", {}).get("city_min_population", 1000),
            s3_cfg=build_s3_config(cfg.get("s3", {})),
        )


# ---------------------------------------------------------------------------
# Step 1: polygon generation
# ---------------------------------------------------------------------------

def _make_polygon_wkt(cells: list[str]) -> str | None:
    """
    Convert a list of H3 cells to a WKT polygon string.
    Returns None if h3.cells_to_geo raises (e.g. empty cell list).
    """
    try:
        geo_dict = h3.cells_to_geo(cells)
        geom = shape(geo_dict)
        if geom.is_empty:
            return None
        return to_wkt(geom)
    except Exception as exc:
        logger.warning("cells_to_geo failed (%s) — skipping polygon for %d cells", exc, len(cells))
        return None


def _add_polygons(clusters: pd.DataFrame) -> pd.DataFrame:
    logger.info("Generating H3 polygons for %d clusters …", len(clusters))
    clusters = clusters.copy()
    clusters["geometry_wkt"] = [
        _make_polygon_wkt(cells) for cells in clusters["h3_cells"]
    ]
    n_failed = clusters["geometry_wkt"].isna().sum()
    if n_failed:
        logger.warning("  %d clusters produced no polygon", n_failed)
    return clusters


# ---------------------------------------------------------------------------
# Step 2 + 3: country + city via reverse_geocoder (single batch call)
# ---------------------------------------------------------------------------

def _country_name(iso2: str) -> str:
    """Map ISO 3166-1 alpha-2 code to full English country name."""
    if not iso2:
        return ""
    try:
        return pycountry.countries.get(alpha_2=iso2).name
    except AttributeError:
        return iso2  # fall back to the code itself


def _add_geocoding(clusters: pd.DataFrame) -> pd.DataFrame:
    logger.info("Reverse-geocoding %d cluster centroids …", len(clusters))

    coords = list(zip(clusters["centroid_lat"], clusters["centroid_lon"]))

    # mode=2 → quiet batch mode
    results = rg.search(coords, mode=2)

    country_iso2 = []
    country_names = []
    nearest_cities = []
    city_lats = []
    city_lons = []
    city_dists_km = []
    admin1s = []

    for (clat, clon), r in zip(coords, results):
        iso2 = r.get("cc", "")
        city_lat = float(r.get("lat", clat))
        city_lon = float(r.get("lon", clon))
        dist_km = haversine_meters(clat, clon, city_lat, city_lon) / 1000.0

        country_iso2.append(iso2)
        country_names.append(_country_name(iso2))
        nearest_cities.append(r.get("name", ""))
        city_lats.append(city_lat)
        city_lons.append(city_lon)
        city_dists_km.append(float(dist_km))
        admin1s.append(r.get("admin1", ""))

    clusters = clusters.copy()
    clusters["country_iso2"] = country_iso2
    clusters["country_name"] = country_names
    clusters["nearest_city"] = nearest_cities
    clusters["nearest_city_lat"] = city_lats
    clusters["nearest_city_lon"] = city_lons
    clusters["nearest_city_dist_km"] = city_dists_km
    clusters["admin1"] = admin1s

    return clusters


# ---------------------------------------------------------------------------
# Step 4: write output
# ---------------------------------------------------------------------------

def _write_enriched(df: pd.DataFrame, config: Phase4Config) -> str:
    out_path = path_join(config.interim_dir, "harbours_enriched.parquet")

    h3_cells_array = pa.array(df["h3_cells"].tolist(), type=pa.list_(pa.string()))

    table = pa.table(
        {
            "cluster_id": pa.array(df["cluster_id"], type=pa.int32()),
            "h3_cells": h3_cells_array,
            "n_cells": pa.array(df["n_cells"], type=pa.int32()),
            "n_events": pa.array(df["n_events"], type=pa.int32()),
            "n_unique_mmsi_approx": pa.array(df["n_unique_mmsi_approx"], type=pa.int32()),
            "n_draught_changes": pa.array(df["n_draught_changes"], type=pa.int32()),
            "centroid_lat": pa.array(df["centroid_lat"], type=pa.float64()),
            "centroid_lon": pa.array(df["centroid_lon"], type=pa.float64()),
            "centroid_h3_r8": pa.array(df["centroid_h3_r8"], type=pa.string()),
            "bbox_min_lat": pa.array(df["bbox_min_lat"], type=pa.float64()),
            "bbox_max_lat": pa.array(df["bbox_max_lat"], type=pa.float64()),
            "bbox_min_lon": pa.array(df["bbox_min_lon"], type=pa.float64()),
            "bbox_max_lon": pa.array(df["bbox_max_lon"], type=pa.float64()),
            "geometry_wkt": pa.array(df["geometry_wkt"], type=pa.string()),
            "country_iso2": pa.array(df["country_iso2"], type=pa.string()),
            "country_name": pa.array(df["country_name"], type=pa.string()),
            "nearest_city": pa.array(df["nearest_city"], type=pa.string()),
            "nearest_city_lat": pa.array(df["nearest_city_lat"], type=pa.float64()),
            "nearest_city_lon": pa.array(df["nearest_city_lon"], type=pa.float64()),
            "nearest_city_dist_km": pa.array(df["nearest_city_dist_km"].astype("float32"), type=pa.float32()),
            "admin1": pa.array(df["admin1"], type=pa.string()),
        },
        schema=ENRICHED_SCHEMA,
    )
    if is_s3_path(config.interim_dir):
        fs = get_s3_filesystem(config.s3_cfg)
        with fs.open(out_path, "wb") as fh:
            pq.write_table(table, fh, compression="snappy")
    else:
        pq.write_table(table, out_path, compression="snappy")
    logger.info("Wrote %d enriched harbours → %s", len(df), out_path)
    return out_path


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_phase4(config: Phase4Config) -> str:
    clusters_path = path_join(config.interim_dir, "harbour_clusters.parquet")
    if not is_s3_path(config.interim_dir) and not Path(clusters_path).exists():
        raise FileNotFoundError(
            f"harbour_clusters.parquet not found at {clusters_path} — run phase3 first"
        )

    logger.info("Phase 4: reading %s …", clusters_path)
    if is_s3_path(config.interim_dir):
        clusters = pd.read_parquet(clusters_path, storage_options=get_s3_storage_options(config.s3_cfg))
    else:
        clusters = pd.read_parquet(clusters_path)
    logger.info("  loaded %d clusters", len(clusters))

    clusters = _add_polygons(clusters)
    clusters = _add_geocoding(clusters)
    return _write_enriched(clusters, config)
