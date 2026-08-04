"""
Phase 3: Cluster Formation

Reads h3_counts.parquet, connects hot H3 cells into clusters, and computes
per-cluster statistics.

Connectivity is parent-based by default: two hot cells belong to the same
cluster when their parent cells at `connectivity_resolution` (e.g. res 9,
~350 m across) are identical or adjacent. This bridges the cold-cell gaps
between terminals of a large port without needing huge grid_disk rings.
Setting `connectivity_resolution` to null falls back to the legacy
fine-cell ring search (`cluster_ring_size`).

Cluster-level vessel counts are computed exactly by re-joining stops.parquet
(a vessel visiting several cells of one cluster counts once), and the
`min_unique_mmsi_per_cluster` threshold is enforced here — at cluster level —
rather than per cell in Phase 2.

No external graph library is needed — BFS is implemented inline.

Output: data/interim/harbour_clusters.parquet
"""

import logging
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import h3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from utils.s3 import (
    build_s3_config,
    get_s3_filesystem,
    get_s3_storage_options,
    is_s3_path,
    path_join,
)

logger = logging.getLogger(__name__)

CLUSTER_SCHEMA = pa.schema([
    pa.field("cluster_id",           pa.int32()),
    pa.field("h3_cells",             pa.list_(pa.string())),
    pa.field("n_cells",              pa.int32()),
    pa.field("n_events",             pa.int32()),
    # Exact count of distinct vessels across the cluster, computed from
    # stops.parquet. Falls back to the sum of per-cell uniques (over-counts
    # vessels spanning cells) only when stops.parquet is unavailable.
    pa.field("n_unique_mmsi",        pa.int32()),
    pa.field("n_draught_changes",    pa.int32()),
    pa.field("centroid_lat",         pa.float64()),
    pa.field("centroid_lon",         pa.float64()),
    # H3 cell at resolution 8 of the centroid — used for deterministic ID
    # generation in Phase 5.
    pa.field("centroid_h3_r8",       pa.string()),
    pa.field("bbox_min_lat",         pa.float64()),
    pa.field("bbox_max_lat",         pa.float64()),
    pa.field("bbox_min_lon",         pa.float64()),
    pa.field("bbox_max_lon",         pa.float64()),
])


@dataclass
class Phase3Config:
    interim_dir: str
    min_cells_per_cluster: int = 1        # keep even single-cell harbours by default
    min_events_per_cluster: int = 5       # drop statistical noise with very few visits
    min_unique_mmsi_per_cluster: int = 5  # distinct vessels required per cluster
    connectivity_resolution: Optional[int] = 9  # parent res; None → legacy rings
    cluster_ring_size: int = 3            # legacy ring search radius (see above)
    s3_cfg: dict = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, cfg: dict) -> "Phase3Config":
        p3 = cfg.get("phase3", {})
        return cls(
            interim_dir=cfg.get("data", {}).get("interim_dir", "data/interim"),
            min_cells_per_cluster=p3.get(
                "min_cells_per_harbour", p3.get("min_cells_per_cluster", 1)
            ),
            min_events_per_cluster=p3.get("min_events_per_cluster", 5),
            min_unique_mmsi_per_cluster=p3.get("min_unique_mmsi_per_cluster", 5),
            connectivity_resolution=p3.get("connectivity_resolution", 9),
            cluster_ring_size=p3.get("cluster_ring_size", 3),
            s3_cfg=build_s3_config(cfg.get("s3", {})),
        )


# ---------------------------------------------------------------------------
# Step 1: build adjacency graph
# ---------------------------------------------------------------------------

def _build_adjacency(hot_cells: set[str], ring_size: int = 1) -> dict[str, set[str]]:
    """
    For every hot cell, find which other hot cells lie within ring_size H3
    rings. ring_size=1 connects only touching cells; larger values bridge
    gaps of cold cells, merging fragmented harbour complexes.
    Returns an adjacency dict: cell → set of reachable hot cells.
    """
    graph: dict[str, set[str]] = {cell: set() for cell in hot_cells}
    for cell in hot_cells:
        for neighbour in h3.grid_disk(cell, ring_size):
            if neighbour != cell and neighbour in hot_cells:
                graph[cell].add(neighbour)
    return graph


# ---------------------------------------------------------------------------
# Step 2: connected components via BFS
# ---------------------------------------------------------------------------

def _connected_components(graph: dict[str, set[str]]) -> list[list[str]]:
    """
    Standard BFS connected-component finder.
    Returns a list of components, each a list of cell strings.
    """
    visited: set[str] = set()
    components: list[list[str]] = []

    for start in graph:
        if start in visited:
            continue
        component: list[str] = []
        queue: deque[str] = deque([start])
        while queue:
            node = queue.popleft()
            if node in visited:
                continue
            visited.add(node)
            component.append(node)
            queue.extend(graph[node] - visited)
        components.append(component)

    return components


def _parent_components(hot_cells: set[str], connectivity_res: int) -> list[list[str]]:
    """
    Connect hot cells through their parent cells at connectivity_res: two hot
    cells are in the same cluster when their parents are identical or adjacent.

    A res-9 parent is ~350 m across, so this bridges the cold-cell gaps between
    the berths/terminals of one harbour complex while still separating harbours
    that are kilometres apart. It also subsumes fine-cell ring adjacency: any
    two touching fine cells always have identical or adjacent parents.
    """
    cell_res = h3.get_resolution(next(iter(hot_cells)))
    if connectivity_res >= cell_res:
        raise ValueError(
            f"connectivity_resolution ({connectivity_res}) must be coarser than "
            f"the cell resolution ({cell_res})"
        )

    cells_by_parent: dict[str, list[str]] = {}
    for cell in hot_cells:
        parent = h3.cell_to_parent(cell, connectivity_res)
        cells_by_parent.setdefault(parent, []).append(cell)

    parent_graph = _build_adjacency(set(cells_by_parent), ring_size=1)
    parent_comps = _connected_components(parent_graph)
    logger.info(
        "  %d hot cells → %d occupied parent cells (res %d) → %d components",
        len(hot_cells), len(cells_by_parent), connectivity_res, len(parent_comps),
    )

    return [
        [cell for parent in comp for cell in cells_by_parent[parent]]
        for comp in parent_comps
    ]


# ---------------------------------------------------------------------------
# Step 3: exact per-cluster vessel counts from stops
# ---------------------------------------------------------------------------

def _load_cell_mmsi_map(
    config: Phase3Config, hot_cells: set[str],
) -> Optional[dict[str, set]]:
    """
    Re-join stops.parquet to the hot cells: returns h3_cell → set of MMSIs,
    so cluster-level unique-vessel counts are exact (a vessel spanning several
    cells of one cluster counts once). Returns None when stops.parquet is
    missing, in which case the caller falls back to approximate counts.
    """
    stops_path = path_join(config.interim_dir, "stops.parquet")
    if is_s3_path(config.interim_dir):
        fs = get_s3_filesystem(config.s3_cfg)
        exists = fs.exists(stops_path)
    else:
        exists = Path(stops_path).exists()
    if not exists:
        logger.warning(
            "stops.parquet not found at %s — cluster vessel counts will be "
            "approximate (sum of per-cell uniques)", stops_path,
        )
        return None

    if is_s3_path(config.interim_dir):
        stops = pd.read_parquet(
            stops_path, columns=["mmsi", "lat", "lon"],
            storage_options=get_s3_storage_options(config.s3_cfg),
        )
    else:
        stops = pd.read_parquet(stops_path, columns=["mmsi", "lat", "lon"])

    resolution = h3.get_resolution(next(iter(hot_cells)))
    stops["h3_cell"] = [
        h3.latlng_to_cell(lat, lon, resolution)
        for lat, lon in zip(stops["lat"], stops["lon"])
    ]
    stops = stops[stops["h3_cell"].isin(hot_cells)]
    logger.info("  joined %d stops onto %d hot cells for exact vessel counts",
                len(stops), len(hot_cells))
    return stops.groupby("h3_cell")["mmsi"].agg(set).to_dict()


# ---------------------------------------------------------------------------
# Step 4: compute per-cluster statistics
# ---------------------------------------------------------------------------

def _cluster_stats(
    components: list[list[str]],
    cell_df: pd.DataFrame,
    cell_mmsi: Optional[dict[str, set]] = None,
) -> list[dict]:
    """
    For each component, aggregate the per-cell counts from h3_counts and
    compute a traffic-weighted centroid. When cell_mmsi is given, cluster
    vessel counts are exact; otherwise per-cell uniques are summed.
    """
    # Index h3_counts by cell string for fast lookup
    cell_index = cell_df.set_index("h3_cell")

    records = []
    for cluster_id, cells in enumerate(components):
        sub = cell_index.loc[cells]

        n_events      = int(sub["n_events"].sum())
        weights       = sub["n_events"].to_numpy(dtype=float)
        total_weight  = weights.sum() or 1.0

        centroid_lat = float((sub["cell_lat"] * weights).sum() / total_weight)
        centroid_lon = float((sub["cell_lon"] * weights).sum() / total_weight)

        if cell_mmsi is not None:
            n_unique = len(set().union(*(cell_mmsi.get(c, set()) for c in cells)))
        else:
            n_unique = int(sub["n_unique_mmsi"].sum())

        records.append({
            "cluster_id":           cluster_id,
            "h3_cells":             sorted(cells),
            "n_cells":              len(cells),
            "n_events":             n_events,
            "n_unique_mmsi":        n_unique,
            "n_draught_changes":    int(sub["n_draught_changes"].sum()),
            "centroid_lat":         centroid_lat,
            "centroid_lon":         centroid_lon,
            "centroid_h3_r8":       h3.latlng_to_cell(centroid_lat, centroid_lon, 8),
            "bbox_min_lat":         float(sub["cell_lat"].min()),
            "bbox_max_lat":         float(sub["cell_lat"].max()),
            "bbox_min_lon":         float(sub["cell_lon"].min()),
            "bbox_max_lon":         float(sub["cell_lon"].max()),
        })

    return records


# ---------------------------------------------------------------------------
# Step 4: filter noise
# ---------------------------------------------------------------------------

def _filter_clusters(df: pd.DataFrame, config: Phase3Config) -> pd.DataFrame:
    before = len(df)
    df = df[
        (df["n_cells"]        >= config.min_cells_per_cluster) &
        (df["n_events"]       >= config.min_events_per_cluster) &
        (df["n_unique_mmsi"]  >= config.min_unique_mmsi_per_cluster)
    ].reset_index(drop=True)
    # Re-assign sequential IDs after filtering
    df["cluster_id"] = np.arange(len(df), dtype="int32")
    logger.info(
        "Filtered clusters: %d → %d  (min_cells=%d, min_events=%d, min_unique_mmsi=%d)",
        before, len(df), config.min_cells_per_cluster,
        config.min_events_per_cluster, config.min_unique_mmsi_per_cluster,
    )
    return df


# ---------------------------------------------------------------------------
# Step 5: write output
# ---------------------------------------------------------------------------

def _write_clusters(df: pd.DataFrame, config: Phase3Config) -> str:
    out_path = path_join(config.interim_dir, "harbour_clusters.parquet")

    # PyArrow requires explicit list type for the h3_cells column
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
    if is_s3_path(config.interim_dir):
        fs = get_s3_filesystem(config.s3_cfg)
        with fs.open(out_path, "wb") as fh:
            pq.write_table(table, fh, compression="snappy")
    else:
        pq.write_table(table, out_path, compression="snappy")
    logger.info("Wrote %d harbour clusters → %s", len(df), out_path)
    return out_path


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_phase3(config: Phase3Config) -> str:
    counts_path = path_join(config.interim_dir, "h3_counts.parquet")
    if not is_s3_path(config.interim_dir) and not Path(counts_path).exists():
        raise FileNotFoundError(
            f"h3_counts.parquet not found at {counts_path} — run phase2 first"
        )

    logger.info("Phase 3: reading %s …", counts_path)
    if is_s3_path(config.interim_dir):
        cell_df = pd.read_parquet(
            counts_path, storage_options=get_s3_storage_options(config.s3_cfg)
        )
    else:
        cell_df = pd.read_parquet(counts_path)
    logger.info("  loaded %d hot H3 cells", len(cell_df))

    hot_cells = set(cell_df["h3_cell"])
    if not hot_cells:
        logger.warning("No hot cells in h3_counts — writing empty cluster output.")
        empty = pd.DataFrame({f.name: pd.Series(dtype=object) for f in CLUSTER_SCHEMA})
        return _write_clusters(empty, config)

    if config.connectivity_resolution is not None:
        logger.info("Clustering via parent cells (connectivity_resolution=%d) …",
                    config.connectivity_resolution)
        components = _parent_components(hot_cells, config.connectivity_resolution)
    else:
        logger.info("Building adjacency graph (ring_size=%d) …",
                    config.cluster_ring_size)
        graph = _build_adjacency(hot_cells, config.cluster_ring_size)
        components = _connected_components(graph)
    logger.info("  found %d raw components", len(components))

    logger.info("Joining stops for exact vessel counts …")
    cell_mmsi = _load_cell_mmsi_map(config, hot_cells)

    logger.info("Computing cluster statistics …")
    records = _cluster_stats(components, cell_df, cell_mmsi)
    df = pd.DataFrame(records)

    df = _filter_clusters(df, config)
    return _write_clusters(df, config)
