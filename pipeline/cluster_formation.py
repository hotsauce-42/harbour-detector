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

from pipeline.h3_aggregation import VESSEL_COUNTS
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
    # Behavioural signature, carried up from h3_counts so Phase 4 can tell a
    # harbour from a place vessels merely pass through (see VESSEL_COUNTS).
    pa.field("mean_dwell_minutes",   pa.float64()),
    pa.field("max_visits_per_mmsi",  pa.int32()),
    pa.field("n_cargo",              pa.int32()),
    pa.field("n_tanker",             pa.int32()),
    pa.field("n_passenger",          pa.int32()),
    pa.field("n_fishing",            pa.int32()),
    pa.field("n_recreational",       pa.int32()),
    pa.field("n_tug_pilot",          pa.int32()),
    pa.field("centroid_lat",         pa.float64()),
    pa.field("centroid_lon",         pa.float64()),
    # H3 cell at resolution 8 of the centroid — used for deterministic ID
    # generation in Phase 5.
    pa.field("centroid_id_cell",       pa.string()),
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
    prune_detached_cells: bool = True     # drop stray specks off the harbour body
    detached_ring_size: int = 2           # rings that still count as "attached"
    max_detached_cells: int = 2           # a bigger detached group is a real berth
    max_detached_event_share: float = 0.05  # …and so is a busier one
    # Resolution of the cell the harbour_id is hashed from. Two harbours whose
    # centroids share this cell get the SAME id however well Phase 5 matches,
    # so it has to be finer than the closest harbours get: a cell's span is
    # 2x its edge, 1063 m at res 8 and 402 m at res 9. Changing it re-ids every
    # harbour that does not match an existing database — see
    # scripts/carry_manual_edits.py.
    centroid_id_resolution: int = 9
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
            prune_detached_cells=p3.get("prune_detached_cells", True),
            detached_ring_size=p3.get("detached_ring_size", 2),
            max_detached_cells=p3.get("max_detached_cells", 2),
            max_detached_event_share=p3.get("max_detached_event_share", 0.05),
            centroid_id_resolution=int(p3.get("centroid_id_resolution", 9)),
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
# Step 3: prune detached outlier cells
# ---------------------------------------------------------------------------

def _prune_detached_cells(
    components: list[list[str]],
    cell_df: pd.DataFrame,
    config: Phase3Config,
) -> list[list[str]]:
    """
    Drop stray cells that sit off the harbour body.

    Clusters are formed on res-9 *parent* cells, so two res-11 cells hundreds of
    metres apart join the same cluster whenever their parents happen to touch —
    and nothing afterwards reconsiders it. The result is a lone cell out in the
    water, exported as its own polygon part. Tunø By (DK-f1661c3f) carried two,
    205 m and 369 m out, 2 AIS events each of the harbour's 107.

    So connectivity is checked a second time on the fine cells, and a group that
    does not reach the main body within `detached_ring_size` rings is dropped —
    but only if it is both small and quiet. Either test alone is wrong: real
    harbours legitimately have detached berths (233 of 331 harbours are
    fragmented at single-ring adjacency, and a detached group can hold half the
    traffic), so size alone would eat a third of every cell in the output, and
    traffic share alone would eat whole outlying terminals.

    Whatever survives is what `_cluster_stats` then measures — counts, the
    event-weighted centroid, `centroid_id_cell` and the bbox all come out of these
    lists — and `_filter_clusters` re-applies the cluster minimums afterwards,
    so a cluster pruned below them is still dropped.
    """
    if not config.prune_detached_cells:
        return components

    events = cell_df.set_index("h3_cell")["n_events"].to_dict()
    pruned: list[list[str]] = []
    dropped_cells = 0
    touched = 0

    for cells in components:
        groups = _connected_components(
            _build_adjacency(set(cells), config.detached_ring_size)
        )
        if len(groups) == 1:
            pruned.append(cells)
            continue

        def group_events(group: list[str]) -> int:
            return sum(int(events.get(cell, 0)) for cell in group)

        # Most traffic wins, then most cells; the cell id only breaks a full tie,
        # so the choice does not depend on dict ordering.
        groups.sort(key=lambda g: (-group_events(g), -len(g), sorted(g)[0]))
        total = sum(group_events(g) for g in groups) or 1

        keep = list(groups[0])
        lost = 0
        for group in groups[1:]:
            detached = (
                len(group) <= config.max_detached_cells
                and group_events(group) / total < config.max_detached_event_share
            )
            if detached:
                lost += len(group)
            else:
                keep.extend(group)

        if lost:
            touched += 1
            dropped_cells += lost
        pruned.append(keep)

    if dropped_cells:
        logger.info(
            "  pruned %d detached cell(s) from %d cluster(s) "
            "(<=%d cells and <%.0f%% of events, beyond %d rings)",
            dropped_cells, touched, config.max_detached_cells,
            config.max_detached_event_share * 100, config.detached_ring_size,
        )
    return pruned


# ---------------------------------------------------------------------------
# Step 4: exact per-cluster vessel counts from stops
# ---------------------------------------------------------------------------

def _load_cell_mmsi_map(
    config: Phase3Config, hot_cells: set[str],
) -> Optional[dict[str, set]]:
    """
    Re-join stops.parquet to the hot cells: returns h3_cell → set of MMSIs,
    so cluster-level unique-vessel counts are exact (a vessel spanning several
    cells of one cluster counts once). Returns None when stops.parquet is
    missing or unreadable, in which case the caller falls back to approximate
    counts.

    Spark writes stops.parquet as a *directory*, so it can exist and still hold
    no part files — after a cleaned-up or interrupted Phase 1. pyarrow cannot
    infer a schema from that and raises on the column selection, which is the
    same situation as the file being absent and is handled the same way.
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

    try:
        if is_s3_path(config.interim_dir):
            stops = pd.read_parquet(
                stops_path, columns=["mmsi", "lat", "lon"],
                storage_options=get_s3_storage_options(config.s3_cfg),
            )
        else:
            stops = pd.read_parquet(stops_path, columns=["mmsi", "lat", "lon"])
    except Exception as exc:
        logger.warning(
            "Could not read %s (%s) — cluster vessel counts will be "
            "approximate (sum of per-cell uniques)", stops_path, exc,
        )
        return None

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
# Step 5: compute per-cluster statistics
# ---------------------------------------------------------------------------

def _cluster_stats(
    components: list[list[str]],
    cell_df: pd.DataFrame,
    cell_mmsi: Optional[dict[str, set]] = None,
    centroid_id_resolution: int = 9,
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

        # Event-weighted, like the centroid: a cell that saw one stop should
        # not pull the harbour's dwell as hard as one that saw fifty.
        dwell = float((sub["mean_duration_minutes"] * weights).sum() / total_weight)

        records.append({
            "cluster_id":           cluster_id,
            "h3_cells":             sorted(cells),
            "n_cells":              len(cells),
            "n_events":             n_events,
            "n_unique_mmsi":        n_unique,
            "n_draught_changes":    int(sub["n_draught_changes"].sum()),
            "mean_dwell_minutes":   dwell,
            "max_visits_per_mmsi":  int(sub["max_visits_per_mmsi"].max()),
            **{c: int(sub[c].sum()) for c in VESSEL_COUNTS},
            "centroid_lat":         centroid_lat,
            "centroid_lon":         centroid_lon,
            "centroid_id_cell":     h3.latlng_to_cell(centroid_lat, centroid_lon,
                                                      centroid_id_resolution),
            "bbox_min_lat":         float(sub["cell_lat"].min()),
            "bbox_max_lat":         float(sub["cell_lat"].max()),
            "bbox_min_lon":         float(sub["cell_lon"].min()),
            "bbox_max_lon":         float(sub["cell_lon"].max()),
        })

    return records


# ---------------------------------------------------------------------------
# Step 6: filter noise
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
# Step 7: write output
# ---------------------------------------------------------------------------

def _write_clusters(df: pd.DataFrame, config: Phase3Config) -> str:
    out_path = path_join(config.interim_dir, "harbour_clusters.parquet")

    # Built from CLUSTER_SCHEMA rather than a hand-written column list, so
    # adding a field to the schema cannot silently skip the writer. h3_cells is
    # the one column pyarrow needs told about its element type.
    table = pa.table(
        {
            field.name: (
                pa.array(df["h3_cells"].tolist(), type=field.type)
                if field.name == "h3_cells"
                else pa.array(df[field.name], type=field.type)
            )
            for field in CLUSTER_SCHEMA
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

    components = _prune_detached_cells(components, cell_df, config)

    logger.info("Joining stops for exact vessel counts …")
    cell_mmsi = _load_cell_mmsi_map(config, hot_cells)

    logger.info("Computing cluster statistics …")
    cell_res = h3.get_resolution(next(iter(hot_cells)))
    if config.centroid_id_resolution >= cell_res:
        raise ValueError(
            f"centroid_id_resolution ({config.centroid_id_resolution}) must be "
            f"coarser than the cell resolution ({cell_res})"
        )
    records = _cluster_stats(components, cell_df, cell_mmsi,
                             config.centroid_id_resolution)
    df = pd.DataFrame(records)

    df = _filter_clusters(df, config)
    return _write_clusters(df, config)
