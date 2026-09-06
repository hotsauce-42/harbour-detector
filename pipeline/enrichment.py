"""
Phase 4: Enrichment

For every harbour cluster:
  1. Polygon   — convert H3 cell set to a GeoJSON geometry via h3.cells_to_geo(),
                 plus a closed outline polygon of the harbour as a whole
  2. Country   — reverse-geocode the centroid with reverse_geocoder, map cc → full name
  3. City      — nearest populated place from a GeoNames gazetteer
                 (utils.gazetteer), with a population floor that scales with the
                 harbour's size

Output: data/interim/harbours_enriched.parquet
        (harbour_id is added in Phase 5; this file uses cluster_id as a temp key)
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path

import h3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pycountry
import reverse_geocoder as rg
from shapely.geometry import shape
from shapely.wkt import dumps as to_wkt

from pipeline.h3_aggregation import VESSEL_COUNTS
from utils.gazetteer import Gazetteer, bbox_around
from utils.geo import haversine_meters, outline_polygon
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
    pa.field("n_unique_mmsi", pa.int32()),
    pa.field("n_draught_changes", pa.int32()),
    pa.field("mean_dwell_minutes", pa.float64()),
    pa.field("max_visits_per_mmsi", pa.int32()),
    pa.field("n_cargo", pa.int32()),
    pa.field("n_tanker", pa.int32()),
    pa.field("n_passenger", pa.int32()),
    pa.field("n_fishing", pa.int32()),
    pa.field("n_recreational", pa.int32()),
    pa.field("n_tug_pilot", pa.int32()),
    # Does this site behave like somewhere vessels pass through rather than
    # stay? See _flag_transit_sites.
    pa.field("transit_like", pa.bool_()),
    pa.field("centroid_lat", pa.float64()),
    pa.field("centroid_lon", pa.float64()),
    pa.field("centroid_id_cell", pa.string()),
    pa.field("bbox_min_lat", pa.float64()),
    pa.field("bbox_max_lat", pa.float64()),
    pa.field("bbox_min_lon", pa.float64()),
    pa.field("bbox_max_lon", pa.float64()),
    pa.field("geometry_wkt", pa.string()),  # WKT of exact H3-cell union
    pa.field("outline_wkt", pa.string()),   # WKT of closed harbour outline
    pa.field("country_iso2", pa.string()),  # ISO 3166-1 alpha-2
    pa.field("country_name", pa.string()),
    pa.field("nearest_city", pa.string()),
    pa.field("nearest_city_lat", pa.float64()),
    pa.field("nearest_city_lon", pa.float64()),
    pa.field("nearest_city_dist_km", pa.float32()),
    pa.field("admin1", pa.string()),  # state / province
])


# Harbours large enough to reach a tier need a place of at least that many
# people, so a major port is named after its city rather than the hamlet on its
# edge. Keyed on n_cells because a harbour's cell count saturates at its
# physical footprint, while n_events keeps growing with every day of AIS added.
DEFAULT_CITY_TIERS = [
    {"min_cells": 0, "min_population": 0},
    {"min_cells": 100, "min_population": 1000},
    {"min_cells": 1000, "min_population": 15000},
]


@dataclass
class Phase4Config:
    interim_dir: str
    # GeoNames gazetteer from scripts/prepare_gazetteer.py. Empty → the
    # cities1000 dataset bundled with reverse_geocoder, which has no place
    # under 1000 people in it.
    gazetteer_path: str = ""
    city_population_tiers: list = field(default_factory=lambda: DEFAULT_CITY_TIERS)
    # Past this, a tier's floor is dropped and the nearest place of any size
    # wins — better a nearby village than a city on the far side of a bay.
    max_city_dist_km: float = 50.0
    # A port belongs to its city: when the nearest place is a hamlet GeoNames
    # records no inhabitants for, prefer an administrative seat of at least
    # port_city_min_population within port_city_max_km, in the same country.
    # 0 km switches the rule off.
    port_city_max_km: float = 8.0
    port_city_min_population: int = 50_000
    port_city_max_hamlet_population: int = 0
    # Transit detection — see _flag_transit_sites. Dwell is an absolute limit
    # because a lock cycle is bounded by physics; the visit thresholds are
    # ratios against this run's median because raw visit counts grow with the
    # length of the AIS window. 0 minutes disables the whole step.
    transit_max_dwell_minutes: float = 120.0
    transit_max_visits_ratio: float = 0.85
    transit_max_repeat_ratio: float = 0.55
    transit_min_commercial_share: float = 0.4
    transit_min_classified_vessels: int = 3
    transit_min_sample: int = 20
    # Outline generation — see utils.geo.outline_polygon
    outline_buffer_meters: float = 75.0
    outline_simplify_meters: float = 0.0
    outline_fill_holes: bool = True
    s3_cfg: dict = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, cfg: dict) -> "Phase4Config":
        p4 = cfg.get("phase4", {})
        return cls(
            interim_dir=cfg.get("data", {}).get("interim_dir", "data/interim"),
            gazetteer_path=p4.get("gazetteer_path", ""),
            city_population_tiers=p4.get("city_population_tiers",
                                         DEFAULT_CITY_TIERS),
            max_city_dist_km=float(p4.get("max_city_dist_km", 50.0)),
            port_city_max_km=float(p4.get("port_city_max_km", 8.0)),
            port_city_min_population=int(
                p4.get("port_city_min_population", 50_000)
            ),
            port_city_max_hamlet_population=int(
                p4.get("port_city_max_hamlet_population", 0)
            ),
            transit_max_dwell_minutes=float(
                p4.get("transit_max_dwell_minutes", 120.0)
            ),
            transit_max_visits_ratio=float(p4.get("transit_max_visits_ratio", 0.85)),
            transit_max_repeat_ratio=float(p4.get("transit_max_repeat_ratio", 0.55)),
            transit_min_commercial_share=float(
                p4.get("transit_min_commercial_share", 0.4)
            ),
            transit_min_classified_vessels=int(
                p4.get("transit_min_classified_vessels", 3)
            ),
            transit_min_sample=int(p4.get("transit_min_sample", 20)),
            outline_buffer_meters=p4.get("outline_buffer_meters", 75.0),
            outline_simplify_meters=p4.get("outline_simplify_meters", 0.0),
            outline_fill_holes=p4.get("outline_fill_holes", True),
            s3_cfg=build_s3_config(cfg.get("s3", {})),
        )


# ---------------------------------------------------------------------------
# Step 1: polygon generation
# ---------------------------------------------------------------------------

def _cells_to_geom(cells: list[str]):
    """
    Union of a list of H3 cells as a Shapely geometry.
    Returns None if h3.cells_to_geo raises (e.g. empty cell list) or is empty.
    """
    try:
        geom = shape(h3.cells_to_geo(cells))
    except Exception as exc:
        logger.warning("cells_to_geo failed (%s) — skipping polygon for %d cells",
                       exc, len(cells))
        return None
    return None if geom.is_empty else geom


def _make_polygon_wkt(cells: list[str]) -> str | None:
    """Convert a list of H3 cells to a WKT polygon string of their exact union."""
    geom = _cells_to_geom(cells)
    return to_wkt(geom) if geom is not None else None


def _add_polygons(clusters: pd.DataFrame, config: Phase4Config) -> pd.DataFrame:
    """
    Attach two geometries per cluster:

    geometry_wkt — the exact union of the harbour's hot H3 cells, holes and all
    outline_wkt  — that union morphologically closed into the harbour outline
    """
    logger.info("Generating H3 polygons and outlines for %d clusters …", len(clusters))
    clusters = clusters.copy()

    geoms = [_cells_to_geom(cells) for cells in clusters["h3_cells"]]
    clusters["geometry_wkt"] = [to_wkt(g) if g is not None else None for g in geoms]
    clusters["outline_wkt"] = [
        to_wkt(outline_polygon(
            g,
            buffer_meters=config.outline_buffer_meters,
            simplify_meters=config.outline_simplify_meters,
            fill_holes=config.outline_fill_holes,
        )) if g is not None else None
        for g in geoms
    ]

    n_failed = clusters["geometry_wkt"].isna().sum()
    if n_failed:
        logger.warning("  %d clusters produced no polygon", n_failed)

    n_multi = sum(
        1 for w in clusters["outline_wkt"] if w and w.startswith("MULTIPOLYGON")
    )
    if n_multi:
        logger.info(
            "  %d outlines remained multi-part — raise phase4.outline_buffer_meters "
            "(currently %.0f m) to merge terminals further apart",
            n_multi, config.outline_buffer_meters,
        )
    return clusters


# ---------------------------------------------------------------------------
# Step 2 + 3: country (reverse_geocoder) + city (GeoNames gazetteer)
# ---------------------------------------------------------------------------

def _country_name(iso2: str) -> str:
    """Map ISO 3166-1 alpha-2 code to full English country name."""
    if not iso2:
        return ""
    country = pycountry.countries.get(alpha_2=iso2)
    if country is None:
        return iso2  # unknown code — fall back to the code itself
    return country.name


def population_floor(n_cells: int, tiers: list[dict]) -> int:
    """
    The population a harbour of this size demands of its city.

    The highest tier the harbour reaches wins, so the tiers can be listed in
    any order. A harbour below every tier has no floor at all — which is the
    point: a small harbour should take the name of the village next to it.
    """
    floor = 0
    for tier in sorted(tiers, key=lambda t: int(t.get("min_cells", 0))):
        if n_cells >= int(tier.get("min_cells", 0)):
            floor = int(tier.get("min_population", 0))
    return floor


def _add_geocoding(clusters: pd.DataFrame, config: Phase4Config) -> pd.DataFrame:
    logger.info("Reverse-geocoding %d cluster centroids …", len(clusters))

    coords = list(zip(clusters["centroid_lat"], clusters["centroid_lon"]))

    # reverse_geocoder is now only the fallback for country and admin1. Its own
    # answer is unreliable for the same reason it was replaced for the city: it
    # minimises Euclidean distance over raw degrees, so at 58.9°N it reached
    # across the Skagerrak and put a harbour in the Swedish Koster archipelago
    # — 1.0 km from Nord-Koster — in Norway, from a town 14.8 km away.
    # mode=2 → quiet batch mode
    results = rg.search(coords, mode=2)

    # Only the places near this run's harbours are worth indexing — the global
    # gazetteer is 5.2M rows, the box around a country's coastline is ~50k.
    gazetteer = Gazetteer.open(
        config.gazetteer_path,
        bbox=bbox_around(clusters["centroid_lat"], clusters["centroid_lon"]),
        s3_cfg=config.s3_cfg,
    )
    tiers = config.city_population_tiers
    if not gazetteer.has_population:
        logger.info(
            "  gazetteer carries no population column — size tiers are ignored "
            "and the nearest place always wins",
        )

    country_iso2 = []
    country_names = []
    nearest_cities = []
    city_lats = []
    city_lons = []
    city_dists_km = []
    admin1s = []

    n_floored = 0
    n_rg_country = 0
    n_port_city = 0
    for (clat, clon), r, n_cells in zip(coords, results, clusters["n_cells"]):
        # Country comes from the *nearest* place, never the population-floored
        # city below: a floor can select a town tens of km away, and that town
        # may be across a border while the village next to the quay is not.
        # The floor-0 tree is already built and cached by Gazetteer._tree_for,
        # so this costs one more query on an existing index.
        country_place = gazetteer.nearest(clat, clon, 0)
        if (country_place is not None
                and country_place.distance_km <= config.max_city_dist_km):
            iso2 = country_place.cc
        else:
            iso2 = r.get("cc", "")
            n_rg_country += 1
        country_iso2.append(iso2)
        country_names.append(_country_name(iso2))

        floor = population_floor(int(n_cells), tiers)
        place = gazetteer.nearest(clat, clon, floor) if floor else country_place
        # A floor that only reaches something implausibly far away is worse
        # than no floor: fall back to whatever is actually next to the harbour,
        # which is the place the country was taken from.
        if place is None or place.distance_km > config.max_city_dist_km:
            place = country_place if country_place is not None else place
        elif floor > 0:
            n_floored += 1

        # A port belongs to its city. When the nearest place is a hamlet
        # GeoNames records no inhabitants for, it is usually not what the
        # harbour is called: Rostock's Überseehafen is 0.6 km from Petersdorf
        # (population 0, and nothing to do with Rostock) and 7.5 km from
        # Rostock itself. A named place — Warnemünde at 8,441 — is left alone.
        if (place is not None
                and config.port_city_max_km > 0
                and place.population <= config.port_city_max_hamlet_population):
            city = gazetteer.nearest_seat(
                clat, clon, config.port_city_min_population,
                config.port_city_max_km, iso2,
            )
            # …but only when the harbour is actually in that city's
            # municipality. Distance alone would hand Hellerup to Copenhagen
            # (Gentofte kommune) and Sandwig to Flensburg (Glücksburg).
            if (city is not None and city.name != place.name
                    and gazetteer.shares_municipality(clat, clon, city)):
                place = city
                n_port_city += 1

        if place is None:
            # Nothing in the gazetteer at all — keep the columns aligned.
            nearest_cities.append(r.get("name", ""))
            city_lats.append(float(r.get("lat", clat)))
            city_lons.append(float(r.get("lon", clon)))
            city_dists_km.append(float(
                haversine_meters(clat, clon,
                                 float(r.get("lat", clat)),
                                 float(r.get("lon", clon))) / 1000.0
            ))
            admin1s.append(r.get("admin1", ""))
            continue

        nearest_cities.append(place.name)
        city_lats.append(place.lat)
        city_lons.append(place.lon)
        city_dists_km.append(place.distance_km)
        # admin1 stays tied to the city, not to the place the country came
        # from. The two can only differ when a population floor actually fires
        # and reaches into another region — no harbour in this dataset gets
        # near the first tier (largest is 47 cells, the tier starts at 100).
        admin1s.append(place.admin1 or r.get("admin1", ""))

    if n_floored:
        logger.info("  %d harbour(s) large enough to require a populated city",
                    n_floored)
    if n_port_city:
        logger.info("  %d harbour(s) named after their city rather than an "
                    "unpopulated hamlet nearer the quay", n_port_city)
    if n_rg_country:
        logger.warning(
            "  %d harbour(s) had no gazetteer place within %.0f km — country "
            "fell back to reverse_geocoder; check phase4.gazetteer_path covers "
            "this region", n_rg_country, config.max_city_dist_km,
        )
    far = sum(1 for d in city_dists_km if d > config.max_city_dist_km)
    if far:
        logger.warning(
            "  %d harbour(s) are more than %.0f km from any populated place — "
            "check phase4.gazetteer_path covers this region",
            far, config.max_city_dist_km,
        )

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
# Step 3b: transit sites (ship locks) masquerading as harbours
# ---------------------------------------------------------------------------

# A lock on a shipping canal is transited by commercial traffic. The share of
# cargo and tankers is what separates it from a marina with the same dwell:
# Kiel-Holtenau is 1.00 and Brunsbüttel 0.50, while the two Cuxhaven harbours
# that otherwise looked identical are 0.00 and 0.29.
COMMERCIAL_COUNTS = ("n_cargo", "n_tanker")


def _flag_transit_sites(clusters: pd.DataFrame, config: Phase4Config) -> pd.DataFrame:
    """
    Mark sites where vessels pass through rather than stay — ship locks.

    A lock produces exactly the signature Phases 1-3 look for, so it cannot be
    excluded earlier. What gives it away is the shape of its traffic:

      * dwell is short and bounded by physics — a lock cycle, piling up just
        above phase1.min_stop_duration_minutes rather than spread over hours
      * each vessel appears about once: it is passing, not berthing
      * the traffic is commercial, which is what distinguishes a canal lock
        from a marina where boats also stop briefly

    The dwell limit is absolute; the two visit limits are **ratios against this
    run's median**, because raw visit counts grow with the length of the AIS
    window — a threshold tuned on one day would quietly stop matching anything
    on a month. The medians make the test self-calibrating.

    Advisory only. Nothing is dropped, and Phase 5 lets an operator overrule
    the verdict either way.
    """
    clusters = clusters.copy()
    clusters["transit_like"] = False
    if config.transit_max_dwell_minutes <= 0:
        return clusters

    if len(clusters) < config.transit_min_sample:
        logger.info(
            "  transit detection skipped: %d harbours is too few for the "
            "median to mean anything (need %d)",
            len(clusters), config.transit_min_sample,
        )
        return clusters

    visits = clusters["n_events"] / clusters["n_unique_mmsi"].clip(lower=1)
    repeat = clusters["max_visits_per_mmsi"]
    classified = sum(clusters[c] for c in VESSEL_COUNTS)
    commercial = sum(clusters[c] for c in COMMERCIAL_COUNTS)
    share = commercial / classified.where(classified > 0)

    flag = (
        (clusters["mean_dwell_minutes"] < config.transit_max_dwell_minutes)
        & (visits < visits.median() * config.transit_max_visits_ratio)
        & (repeat <= repeat.median() * config.transit_max_repeat_ratio)
        & (classified >= config.transit_min_classified_vessels)
        & (share >= config.transit_min_commercial_share)
    )
    clusters["transit_like"] = flag.fillna(False)

    n_unknown = int((classified < config.transit_min_classified_vessels).sum())
    if n_unknown:
        logger.info(
            "  %d harbour(s) have too few vessels of known type to judge — "
            "left unflagged (ship_type comes from phase1's type-5 join)",
            n_unknown,
        )
    n_flagged = int(clusters["transit_like"].sum())
    if n_flagged:
        logger.info(
            "  %d site(s) look like transit points rather than harbours "
            "(dwell < %.0f min, few repeat visits, commercial traffic)",
            n_flagged, config.transit_max_dwell_minutes,
        )
    return clusters


# ---------------------------------------------------------------------------
# Step 4: write output
# ---------------------------------------------------------------------------

def _write_enriched(df: pd.DataFrame, config: Phase4Config) -> str:
    out_path = path_join(config.interim_dir, "harbours_enriched.parquet")

    # Built from ENRICHED_SCHEMA, so a field added to the schema cannot be
    # silently left out of the writer. Two columns need help: h3_cells is a
    # list, and nearest_city_dist_km is float32 where pandas holds float64.
    table = pa.table(
        {
            field.name: (
                pa.array(df["h3_cells"].tolist(), type=field.type)
                if field.name == "h3_cells"
                else pa.array(df[field.name].astype("float32"), type=field.type)
                if field.name == "nearest_city_dist_km"
                else pa.array(df[field.name], type=field.type)
            )
            for field in ENRICHED_SCHEMA
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
        clusters = pd.read_parquet(
            clusters_path, storage_options=get_s3_storage_options(config.s3_cfg)
        )
    else:
        clusters = pd.read_parquet(clusters_path)
    logger.info("  loaded %d clusters", len(clusters))

    clusters = _add_polygons(clusters, config)
    clusters = _add_geocoding(clusters, config)
    clusters = _flag_transit_sites(clusters, config)
    return _write_enriched(clusters, config)
