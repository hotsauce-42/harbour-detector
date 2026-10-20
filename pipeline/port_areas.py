"""
Phase 6: Port areas — harbours grouped into the areas a vessel enters and leaves.

A detected harbour is a basin, a terminal or a marina, so a large port is a
dozen of them with water and quays in between. To ask "did this ship enter the
port of Copenhagen?" a tracker needs one area per port, and that is what this
phase writes. It reads Phase 5's harbours.parquet and changes nothing in it.

  1. Link. Two harbours belong to one port area when the gap between their
     outlines is at most `max_gap_meters` — or `commercial_max_gap_meters`
     when both carry mostly cargo and tanker traffic, since terminals sit
     further apart than marinas. With `same_municipality`, harbours in
     different municipalities are never linked. Port areas are the connected
     components of those links; a harbour without neighbours is an area of
     its own, so the output covers every harbour.
  2. Bridge and close. Every link adds the convex hull of its two harbours'
     outlines — the water between them — and the result is morphologically
     closed with half the area's link gap and its holes filled. A closing
     alone does not bridge: between two compact harbours the neck it grows is
     thinner than the erosion that follows, so it is eroded away again.
  3. Cut land (`cut_land`). Land polygons (phase4.coastline_path) are
     subtracted, but the members' own outlines are always kept: quay cells
     overlap OSM land, and a vessel at its berth must count as inside. Water
     left cut off from every member — a pond behind a closed-over peninsula —
     is dropped.
  4. Cells. The area becomes H3 cells (centre containment) plus every member
     cell. A cell belongs to exactly one area: a contested cell goes to the
     area that owns it as a member cell, else to the nearest area.

Why per-harbour size does not set the gap: a big port is not a big harbour but
many small ones. On the reference run the largest outline is ~540 m across and
the median ~110 m, so scaling the gap with harbour size merged *fewer* harbours
than a fixed gap. Traffic type is what separates a port from a marina cluster.

Output: port_areas.geojson (geometry = the exact cell union, h3_cells in the
properties) and port_areas.parquet (also the smooth area as outline_wkt).
"""

import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import h3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from shapely.geometry import Polygon, mapping, shape
from shapely.ops import transform, unary_union
from shapely.wkt import dumps as to_wkt
from shapely.wkt import loads as from_wkt

from pipeline.cluster_formation import _connected_components
from pipeline.enrichment import COMMERCIAL_COUNTS
from pipeline.h3_aggregation import VESSEL_COUNTS
from pipeline.id_matching import cell_list
from utils.coastline import Land, search_windows
from utils.gazetteer import Gazetteer, bbox_around
from utils.geo import _local_metric_frame, _polygonal_parts, outline_polygon
from utils.s3 import (
    build_s3_config,
    get_s3_filesystem,
    get_s3_storage_options,
    is_s3_path,
    path_join,
)

logger = logging.getLogger(__name__)

PORT_AREA_SCHEMA = pa.schema([
    pa.field("port_area_id",      pa.string()),
    pa.field("name",              pa.string()),
    pa.field("country_iso2",      pa.string()),
    pa.field("anchor_harbour_id", pa.string()),
    pa.field("harbour_ids",       pa.list_(pa.string())),
    pa.field("n_harbours",        pa.int32()),
    pa.field("link_gap_meters",   pa.float32()),
    pa.field("land_cut",          pa.bool_()),
    pa.field("h3_resolution",     pa.int8()),
    pa.field("h3_cells",          pa.list_(pa.string())),
    pa.field("n_cells",           pa.int32()),
    pa.field("centroid_lat",      pa.float64()),
    pa.field("centroid_lon",      pa.float64()),
    pa.field("outline_wkt",       pa.string()),
    pa.field("geometry_wkt",      pa.string()),
])


@dataclass
class Phase6Config:
    output_dir: str
    max_gap_meters: float = 1000.0
    commercial_max_gap_meters: float = 2500.0
    commercial_min_share: float = 0.3
    same_municipality: bool = True
    cut_land: bool = True
    cell_resolution: int = 11
    # Shared with Phase 4 — one gazetteer, one land file.
    gazetteer_path: str = ""
    coastline_path: str = ""
    s3_cfg: dict = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, cfg: dict) -> "Phase6Config":
        p4 = cfg.get("phase4", {})
        p6 = cfg.get("phase6", {})
        data = cfg.get("data", {})
        return cls(
            output_dir=data.get("output_dir", "data/output"),
            max_gap_meters=float(p6.get("max_gap_meters", 1000.0)),
            commercial_max_gap_meters=float(
                p6.get("commercial_max_gap_meters", 2500.0)
            ),
            commercial_min_share=float(p6.get("commercial_min_share", 0.3)),
            same_municipality=bool(p6.get("same_municipality", True)),
            cut_land=bool(p6.get("cut_land", True)),
            cell_resolution=int(p6.get("cell_resolution", 11)),
            gazetteer_path=p4.get("gazetteer_path", "") or "",
            coastline_path=p4.get("coastline_path", "") or "",
            s3_cfg=build_s3_config(cfg.get("s3", {})),
        )


# ---------------------------------------------------------------------------
# Step 1: link harbours into port areas
# ---------------------------------------------------------------------------

def _commercial_share(harbours: pd.DataFrame) -> np.ndarray:
    """Cargo + tanker share of classified vessels; NaN when none are classified.

    The same definition as Phase 4's lock test, so "commercial" means one thing.
    """
    classified = sum(harbours[c].fillna(0) for c in VESSEL_COUNTS)
    commercial = sum(harbours[c].fillna(0) for c in COMMERCIAL_COUNTS)
    return (commercial / classified.where(classified > 0)).to_numpy(dtype=float)


def _gap_meters(a, b) -> float:
    """Shortest distance between two lon/lat geometries, in metres."""
    mid = unary_union([a.centroid, b.centroid]).centroid
    to_meters, _ = _local_metric_frame(mid.y, mid.x)
    return float(transform(to_meters, a).distance(transform(to_meters, b)))


def _candidate_pairs(geoms: list, reach_meters: float) -> list[tuple[int, int]]:
    """Every (i, j), i < j, whose envelopes come within `reach_meters`.

    Envelopes only — the tree works in degrees, so it is used for candidates
    and the real gap is measured in metres afterwards.
    """
    if len(geoms) < 2:
        return []
    windows = search_windows(geoms, reach_meters / 1000.0)
    tree = shapely.STRtree(np.asarray(geoms, dtype=object))
    hits = tree.query(shapely.box(*windows.T), predicate="intersects")
    return sorted({(int(i), int(j)) for i, j in hits.T if i < j})


def _link_harbours(
    geoms: list,
    commercial: np.ndarray,
    municipality: list,
    config: Phase6Config,
) -> tuple[list[list[int]], list[float], list[tuple[int, int]]]:
    """
    Port areas as lists of harbour row positions, each with the largest gap
    that linked two of its members (`max_gap_meters` for a lone harbour), and
    the links themselves.

    The municipality test is a veto only: it blocks a link when both keys are
    known and differ, never when either is unknown.
    """
    commercial_gap = max(config.commercial_max_gap_meters, config.max_gap_meters)
    is_commercial = np.nan_to_num(commercial, nan=0.0) >= config.commercial_min_share

    graph: dict[int, set[int]] = {i: set() for i in range(len(geoms))}
    edge_gap: dict[tuple[int, int], float] = {}
    n_vetoed = 0
    for i, j in _candidate_pairs(geoms, commercial_gap):
        allowed = (commercial_gap if is_commercial[i] and is_commercial[j]
                   else config.max_gap_meters)
        if _gap_meters(geoms[i], geoms[j]) > allowed:
            continue
        mi, mj = municipality[i], municipality[j]
        if mi is not None and mj is not None and mi != mj:
            n_vetoed += 1
            continue
        graph[i].add(j)
        graph[j].add(i)
        edge_gap[(i, j)] = allowed

    if n_vetoed:
        logger.info("  %d links vetoed: harbours in different municipalities",
                    n_vetoed)

    groups = [sorted(c) for c in _connected_components(graph)]
    gaps = []
    for members in groups:
        inside = set(members)
        used = [g for (i, j), g in edge_gap.items() if i in inside and j in inside]
        gaps.append(max(used, default=config.max_gap_meters))
    return groups, gaps, sorted(edge_gap)


def _municipalities(harbours: pd.DataFrame, config: Phase6Config) -> list:
    """One municipality key per harbour, or all None when the guard is off."""
    unknown = [None] * len(harbours)
    if not config.same_municipality or harbours.empty:
        return unknown
    gazetteer = Gazetteer.open(
        config.gazetteer_path,
        bbox=bbox_around(harbours["centroid_lat"], harbours["centroid_lon"]),
        s3_cfg=config.s3_cfg,
    )
    if not gazetteer.has_municipalities:
        logger.warning(
            "  same_municipality is on, but the gazetteer carries no division "
            "codes — harbours are linked on distance alone. Re-run "
            "scripts/prepare_gazetteer.py to build one that does."
        )
        return unknown
    return [gazetteer.municipality_at(lat, lon)
            for lat, lon in zip(harbours["centroid_lat"], harbours["centroid_lon"])]


# ---------------------------------------------------------------------------
# Steps 2-3: the area polygon
# ---------------------------------------------------------------------------

def _area_polygon(geoms: list, members: list[int], links: list, gap_meters: float):
    """Member outlines joined by their links' hulls, then closed."""
    inside = set(members)
    bridges = [unary_union([geoms[i], geoms[j]]).convex_hull
               for i, j in links if i in inside and j in inside]
    joined = unary_union([geoms[i] for i in members] + bridges)
    return outline_polygon(joined, buffer_meters=gap_meters / 2, fill_holes=True)


def _cut_land(area, members, land: Land):
    """
    `area` minus land, plus `members`, minus water that touches no member.

    Members are added back because a quay overlaps the OSM land polygon — a
    berth must stay inside its own port area. Water cut off from every member
    (a lagoon behind a peninsula the closing spanned) is not part of the port.
    """
    hits = land.tree.query(area, predicate="intersects")
    if not len(hits):
        return area
    # A split land tile can hold tens of thousands of vertices; only the part
    # under the area matters, and clipping keeps all of that.
    nearby = shapely.clip_by_rect(land.polygons[hits], *area.bounds)
    water = unary_union([area.difference(unary_union(nearby)), members])
    kept = [p for p in _polygonal_parts(water) if p.intersects(members)]
    return unary_union(kept) if kept else members


# ---------------------------------------------------------------------------
# Step 4: H3 cells
# ---------------------------------------------------------------------------

def _at_resolution(cells: list[str], res: int) -> set[str]:
    """Harbour cells re-expressed at `res`: parents if coarser, children if finer."""
    out: set[str] = set()
    for cell in cells:
        r = h3.get_resolution(cell)
        if r == res:
            out.add(cell)
        elif r > res:
            out.add(h3.cell_to_parent(cell, res))
        else:
            out.update(h3.cell_to_children(cell, res))
    return out


def _cell_distance_meters(cell: str, geom) -> float:
    lat, lon = h3.cell_to_latlng(cell)
    to_meters, _ = _local_metric_frame(lat, lon)
    return float(transform(to_meters, geom).distance(shapely.Point(0.0, 0.0)))


def _make_exclusive(
    area_cells: list[set[str]],
    own_cells: list[set[str]],
    members: list,
) -> int:
    """
    Give every cell claimed by several areas to exactly one of them, in place.

    A tracker asking "which port is this vessel in?" needs one answer. An
    area keeps a contested cell that is one of its own members' cells; between
    equals, the area whose members lie nearest wins. Returns how many cells
    were contested.
    """
    claims: dict[str, list[int]] = defaultdict(list)
    for k, cells in enumerate(area_cells):
        for cell in cells:
            claims[cell].append(k)

    contested = 0
    for cell, owners in claims.items():
        if len(owners) < 2:
            continue
        contested += 1
        own = [k for k in owners if cell in own_cells[k]]
        pool = own or owners
        winner = min(
            pool,
            key=lambda k, cell=cell: (_cell_distance_meters(cell, members[k]), k),
        )
        for k in owners:
            if k != winner:
                area_cells[k].discard(cell)
    return contested


def _cells_geometry(cells: list[str]):
    """The exact union of H3 cells as a shapely (Multi)Polygon."""
    if not cells:
        return Polygon()
    return shape(h3.cells_to_h3shape(cells).__geo_interface__)


# ---------------------------------------------------------------------------
# Build + write
# ---------------------------------------------------------------------------

def _text(value) -> str:
    """A string column value, or "" — pandas 3 reads a null back as NaN."""
    return value if isinstance(value, str) else ""


def _anchor(members: pd.DataFrame) -> pd.Series:
    """
    The member an area is named and identified after.

    The largest footprint, not the busiest: n_cells saturates at the physical
    size of a harbour while n_events grows with every day of AIS, so this is
    the choice that stays put between runs and keeps port_area_id stable.
    """
    ranked = members.sort_values(
        ["n_cells", "n_events", "harbour_id"], ascending=[False, False, True]
    )
    return ranked.iloc[0]


def build_port_areas(
    harbours: pd.DataFrame,
    config: Phase6Config,
    municipality: Optional[list] = None,
    land: Optional[Land] = None,
) -> pd.DataFrame:
    """
    Port areas from a harbours.parquet frame, one row per area.

    `municipality` and `land` are looked up from the configured files when
    not given; tests pass them in.
    """
    harbours = harbours.reset_index(drop=True)
    if harbours.empty:
        return pd.DataFrame(columns=PORT_AREA_SCHEMA.names)

    geoms = [from_wkt(w) for w in harbours["outline_wkt"]]
    if municipality is None:
        municipality = _municipalities(harbours, config)
    groups, gaps, links = _link_harbours(
        geoms, _commercial_share(harbours), municipality, config
    )

    members = [unary_union([geoms[i] for i in g]) for g in groups]
    areas = [_area_polygon(geoms, g, links, gap) for g, gap in zip(groups, gaps)]

    if not config.cut_land:
        land = None
    elif land is None and config.coastline_path:
        land = Land.open(config.coastline_path,
                         windows=search_windows(areas, 0.5), s3_cfg=config.s3_cfg)
    land_cut = [False] * len(areas)
    if not config.cut_land:
        logger.info("  land cut switched off (phase6.cut_land: false)")
    elif land is None:
        logger.warning("  no land polygons — port areas keep the land they span")
    else:
        for k, area in enumerate(areas):
            c = area.centroid
            if land.covers(c.y, c.x):
                areas[k] = _cut_land(area, members[k], land)
                land_cut[k] = True
        if not all(land_cut):
            logger.warning(
                "  %d port areas lie outside the land file's coverage and keep "
                "their land", land_cut.count(False),
            )

    res = config.cell_resolution
    own = [
        _at_resolution([c for i in g for c in cell_list(harbours.at[i, "h3_cells"])],
                       res)
        for g in groups
    ]
    area_cells = [set(h3.geo_to_cells(a, res)) | o for a, o in zip(areas, own)]
    contested = _make_exclusive(area_cells, own, members)
    if contested:
        logger.info("  %d cells claimed by two areas, each given to one", contested)

    rows = []
    for g, gap, area, cells, cut in zip(groups, gaps, areas, area_cells, land_cut):
        group = harbours.iloc[g]
        anchor = _anchor(group)
        ordered = sorted(cells)
        centroid = area.centroid
        rows.append({
            "port_area_id":      f"PA-{anchor['harbour_id']}",
            "name":              _text(anchor.get("nearest_city")),
            "country_iso2":      _text(anchor.get("country_iso2")),
            "anchor_harbour_id": anchor["harbour_id"],
            "harbour_ids":       sorted(group["harbour_id"].astype(str)),
            "n_harbours":        len(g),
            "link_gap_meters":   gap,
            "land_cut":          cut,
            "h3_resolution":     res,
            "h3_cells":          ordered,
            "n_cells":           len(ordered),
            "centroid_lat":      centroid.y,
            "centroid_lon":      centroid.x,
            "outline_wkt":       to_wkt(area),
            "geometry_wkt":      to_wkt(_cells_geometry(ordered)),
        })

    areas_df = pd.DataFrame(rows).sort_values("port_area_id", ignore_index=True)
    # harbour_id is not unique (two harbours whose centroids share an H3 cell),
    # so neither is an id derived from it. Suffix the repeats, in sorted order.
    dup = areas_df.groupby("port_area_id").cumcount()
    areas_df.loc[dup > 0, "port_area_id"] += "-" + (dup[dup > 0] + 1).astype(str)
    return areas_df


def _read_harbours(path: str, s3_cfg: dict) -> pd.DataFrame:
    if is_s3_path(path):
        return pd.read_parquet(path, storage_options=get_s3_storage_options(s3_cfg))
    if not Path(path).exists():
        raise FileNotFoundError(f"{path} not found — run phase5 first")
    return pd.read_parquet(path)


def _write_bytes(path: str, payload: bytes, s3_cfg: dict) -> None:
    if is_s3_path(path):
        with get_s3_filesystem(s3_cfg).open(path, "wb") as fh:
            fh.write(payload)
    else:
        Path(path).write_bytes(payload)


def _write_parquet(areas: pd.DataFrame, out_dir: str, s3_cfg: dict) -> str:
    out_path = path_join(out_dir, "port_areas.parquet")
    table = pa.Table.from_pandas(areas[PORT_AREA_SCHEMA.names],
                                 schema=PORT_AREA_SCHEMA, preserve_index=False)
    if is_s3_path(out_dir):
        with get_s3_filesystem(s3_cfg).open(out_path, "wb") as fh:
            pq.write_table(table, fh, compression="snappy")
    else:
        pq.write_table(table, out_path, compression="snappy")
    logger.info("Wrote port_areas.parquet → %s", out_path)
    return out_path


def _write_geojson(areas: pd.DataFrame, out_dir: str, s3_cfg: dict) -> str:
    """One feature per port area; the geometry is the exact union of its cells."""
    out_path = path_join(out_dir, "port_areas.geojson")
    features = []
    for _, row in areas.iterrows():
        features.append({
            "type": "Feature",
            "geometry": mapping(from_wkt(row["geometry_wkt"])),
            "properties": {
                "port_area_id":      row["port_area_id"],
                "name":              row["name"],
                "country_iso2":      row["country_iso2"],
                "anchor_harbour_id": row["anchor_harbour_id"],
                "harbour_ids":       list(row["harbour_ids"]),
                "n_harbours":        int(row["n_harbours"]),
                "link_gap_meters":   float(row["link_gap_meters"]),
                "land_cut":          bool(row["land_cut"]),
                "h3_resolution":     int(row["h3_resolution"]),
                "h3_cells":          list(row["h3_cells"]),
                "n_cells":           int(row["n_cells"]),
                "centroid_lat":      float(row["centroid_lat"]),
                "centroid_lon":      float(row["centroid_lon"]),
            },
        })
    geojson = {"type": "FeatureCollection", "features": features}
    _write_bytes(out_path,
                 json.dumps(geojson, ensure_ascii=False, indent=2).encode("utf-8"),
                 s3_cfg)
    logger.info("Wrote port_areas.geojson (%d areas) → %s", len(features), out_path)
    return out_path


def run_phase6(config: Phase6Config) -> tuple[str, str]:
    """Returns (parquet_path, geojson_path)."""
    in_path = path_join(config.output_dir, "harbours.parquet")
    logger.info("Phase 6: reading %s …", in_path)
    harbours = _read_harbours(in_path, config.s3_cfg)
    logger.info("  loaded %d harbours", len(harbours))

    areas = build_port_areas(harbours, config)
    multi = areas[areas["n_harbours"] > 1]
    logger.info(
        "  %d port areas, %d of them joining %d harbours; largest: %s (%d)",
        len(areas), len(multi), int(multi["n_harbours"].sum()),
        areas.loc[areas["n_harbours"].idxmax(), "name"] if len(areas) else "-",
        int(areas["n_harbours"].max()) if len(areas) else 0,
    )
    return (_write_parquet(areas, config.output_dir, config.s3_cfg),
            _write_geojson(areas, config.output_dir, config.s3_cfg))
