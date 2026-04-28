"""
Phase 3: Cluster Formation

Reads h3_counts.parquet, builds an adjacency graph of neighbouring hot H3
cells, finds connected components via BFS, and computes per-cluster statistics.

No external graph library is needed — BFS is implemented inline.

Output: data/interim/harbour_clusters.parquet
"""

import logging
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

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
    # Sum of per-cell unique MMSIs; vessels spanning multiple cells are counted
    # once per cell — use as a relative traffic indicator, not an exact count.
    pa.field("n_unique_mmsi_approx", pa.int32()),
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
    min_cells_per_cluster: int = 1       # keep even single-cell harbours by default
    min_events_per_cluster: int = 5      # drop statistical noise with very few visits
    cluster_ring_size: int = 3           # H3 rings to search for neighbours; >1 bridges gaps between hot cells
    s3_cfg: dict = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, cfg: dict) -> "Phase3Config":
        p3 = cfg.get("phase3", {})
        return cls(
            interim_dir=cfg.get("data", {}).get("interim_dir", "data/interim"),
            min_cells_per_cluster=p3.get("min_cells_per_cluster", 1),
            min_events_per_cluster=p3.get("min_events_per_cluster", 5),
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


# ---------------------------------------------------------------------------
# Step 3: compute per-cluster statistics
# ---------------------------------------------------------------------------

def _cluster_stats(
    components: list[list[str]],
    cell_df: pd.DataFrame,
) -> list[dict]:
    """
    For each component, aggregate the per-cell counts from h3_counts and
    compute a traffic-weighted centroid.
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

        records.append({
            "cluster_id":           cluster_id,
            "h3_cells":             sorted(cells),
            "n_cells":              len(cells),
            "n_events":             n_events,
            "n_unique_mmsi_approx": int(sub["n_unique_mmsi"].sum()),
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
        (df["n_cells"]  >= config.min_cells_per_cluster) &
        (df["n_events"] >= config.min_events_per_cluster)
    ].reset_index(drop=True)
    # Re-assign sequential IDs after filtering
    df["cluster_id"] = np.arange(len(df), dtype="int32")
    logger.info(
        "Filtered clusters: %d → %d  (min_cells=%d, min_events=%d)",
        before, len(df), config.min_cells_per_cluster, config.min_events_per_cluster,
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
        raise FileNotFoundError(f"h3_counts.parquet not found at {counts_path} — run phase2 first")

    logger.info("Phase 3: reading %s …", counts_path)
    if is_s3_path(config.interim_dir):
        cell_df = pd.read_parquet(counts_path, storage_options=get_s3_storage_options(config.s3_cfg))
    else:
        cell_df = pd.read_parquet(counts_path)
    logger.info("  loaded %d hot H3 cells", len(cell_df))

    hot_cells = set(cell_df["h3_cell"])

    logger.info("Building adjacency graph (ring_size=%d) …", config.cluster_ring_size)
    graph = _build_adjacency(hot_cells, config.cluster_ring_size)

    logger.info("Finding connected components …")
    components = _connected_components(graph)
    logger.info("  found %d raw components", len(components))

    logger.info("Computing cluster statistics …")
    records = _cluster_stats(components, cell_df)
    df = pd.DataFrame(records)

    df = _filter_clusters(df, config)
    return _write_clusters(df, config)
