"""Unit tests for Phase 3 cluster formation."""

from pathlib import Path

import h3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline.cluster_formation import (
    Phase3Config,
    _build_adjacency,
    _cluster_stats,
    _connected_components,
    _filter_clusters,
    _parent_components,
    run_phase3,
)
from pipeline.h3_aggregation import H3_COUNTS_SCHEMA

RES = 11
# A cell in Hamburg harbour
SEED_CELL = h3.latlng_to_cell(53.54, 9.97, RES)


def _neighbours(cell: str, n: int = 1) -> list[str]:
    """Return n rings of neighbours (excluding the centre cell)."""
    return [c for c in h3.grid_disk(cell, n) if c != cell]


def _make_counts(cells: list[str], n_unique_mmsi: int = 10,
                 n_events: int = 20) -> pd.DataFrame:
    latlons = [h3.cell_to_latlng(c) for c in cells]
    return pd.DataFrame({
        "h3_cell":                cells,
        "n_unique_mmsi":          [n_unique_mmsi] * len(cells),
        "n_events":               [n_events]      * len(cells),
        "total_duration_minutes": [120.0]         * len(cells),
        "mean_duration_minutes":  [60.0]          * len(cells),
        "cell_lat":               [ll[0] for ll in latlons],
        "cell_lon":               [ll[1] for ll in latlons],
        "n_draught_changes":      [2]             * len(cells),
    })


# ---------------------------------------------------------------------------

def test_adjacency_connects_neighbours():
    neighbours = _neighbours(SEED_CELL)[:3]
    cells = set([SEED_CELL] + neighbours)
    graph = _build_adjacency(cells)

    # seed cell should be connected to all three neighbours
    for n in neighbours:
        assert n in graph[SEED_CELL]


def test_adjacency_no_self_loops():
    cells = set([SEED_CELL] + _neighbours(SEED_CELL))
    graph = _build_adjacency(cells)
    for cell, nbrs in graph.items():
        assert cell not in nbrs


def test_isolated_cells_form_separate_components():
    # Two cells far apart cannot be neighbours
    cell_a = h3.latlng_to_cell(53.54,  9.97, RES)   # Hamburg
    cell_b = h3.latlng_to_cell(51.90,  4.47, RES)   # Rotterdam
    graph = _build_adjacency({cell_a, cell_b})
    components = _connected_components(graph)
    assert len(components) == 2


def test_connected_cluster_forms_one_component():
    # SEED_CELL + all its 6 neighbours = 7 mutually-adjacent cells → 1 component
    cells = set(h3.grid_disk(SEED_CELL, 1))
    graph = _build_adjacency(cells)
    components = _connected_components(graph)
    assert len(components) == 1
    assert len(components[0]) == len(cells)


def test_parent_components_bridge_terminal_gaps():
    """Hot cells in adjacent res-9 parents merge, even when they are too far
    apart for the legacy fine-cell ring search to connect them."""
    parent_a = h3.latlng_to_cell(53.54, 9.97, 9)
    parent_b = h3.grid_ring(parent_a, 1)[0]

    # One res-11 cell at the centre of each parent — several hundred metres apart
    cell_a = h3.latlng_to_cell(*h3.cell_to_latlng(parent_a), RES)
    cell_b = h3.latlng_to_cell(*h3.cell_to_latlng(parent_b), RES)
    hot = {cell_a, cell_b}

    # Legacy ring-3 search cannot bridge the gap …
    legacy = _connected_components(_build_adjacency(hot, ring_size=3))
    assert len(legacy) == 2

    # … but parent-based connectivity merges them into one harbour
    components = _parent_components(hot, connectivity_res=9)
    assert len(components) == 1
    assert sorted(components[0]) == sorted(hot)


def test_parent_components_separate_distant_harbours():
    cell_a = h3.latlng_to_cell(53.54, 9.97, RES)   # Hamburg
    cell_b = h3.latlng_to_cell(51.90, 4.47, RES)   # Rotterdam
    components = _parent_components({cell_a, cell_b}, connectivity_res=9)
    assert len(components) == 2


def test_parent_components_rejects_finer_resolution():
    cell = h3.latlng_to_cell(53.54, 9.97, RES)
    with pytest.raises(ValueError):
        _parent_components({cell}, connectivity_res=RES)


def test_cluster_stats_centroid_weighted():
    cell_a = SEED_CELL
    cell_b = _neighbours(SEED_CELL)[0]
    cell_df = pd.DataFrame({
        "h3_cell":            [cell_a, cell_b],
        "n_events":           [100, 10],          # cell_a gets 10× more weight
        "n_unique_mmsi":      [50, 5],
        "n_draught_changes":  [3, 1],
        "cell_lat":           [h3.cell_to_latlng(cell_a)[0], h3.cell_to_latlng(cell_b)[0]],
        "cell_lon":           [h3.cell_to_latlng(cell_a)[1], h3.cell_to_latlng(cell_b)[1]],
    })
    records = _cluster_stats([[cell_a, cell_b]], cell_df)
    r = records[0]

    # Centroid should be much closer to cell_a (higher weight)
    lat_a, lon_a = h3.cell_to_latlng(cell_a)
    lat_b, lon_b = h3.cell_to_latlng(cell_b)
    assert abs(r["centroid_lat"] - lat_a) < abs(r["centroid_lat"] - lat_b)


def test_cluster_stats_sums():
    cells = [SEED_CELL] + _neighbours(SEED_CELL)[:2]
    cell_df = _make_counts(cells, n_events=20, n_unique_mmsi=10)
    records = _cluster_stats([cells], cell_df)
    r = records[0]

    assert r["n_cells"]   == 3
    assert r["n_events"]  == 60   # 3 × 20
    assert r["n_draught_changes"] == 6  # 3 × 2


def test_cluster_stats_exact_mmsi_dedup():
    """A vessel stopping in several cells of one cluster is counted once."""
    cell_a = SEED_CELL
    cell_b = _neighbours(SEED_CELL)[0]
    cell_df = _make_counts([cell_a, cell_b], n_unique_mmsi=2, n_events=10)
    cell_mmsi = {
        cell_a: {111111111, 222222222},
        cell_b: {222222222, 333333333},   # vessel 222… spans both cells
    }
    records = _cluster_stats([[cell_a, cell_b]], cell_df, cell_mmsi)
    assert records[0]["n_unique_mmsi"] == 3   # not 4 (per-cell sum)


def test_cluster_stats_falls_back_to_cell_sums():
    cells = [SEED_CELL] + _neighbours(SEED_CELL)[:1]
    cell_df = _make_counts(cells, n_unique_mmsi=10)
    records = _cluster_stats([cells], cell_df, cell_mmsi=None)
    assert records[0]["n_unique_mmsi"] == 20


def test_filter_removes_low_vessel_clusters():
    """Cluster-level unique-vessel threshold drops clusters with few vessels
    even when they have plenty of events."""
    cell_a = h3.latlng_to_cell(53.54, 9.97, RES)
    df = pd.DataFrame([
        {"cluster_id": 0, "h3_cells": [cell_a], "n_cells": 1, "n_events": 100,
         "n_unique_mmsi": 2, "n_draught_changes": 0,
         "centroid_lat": 53.54, "centroid_lon": 9.97,
         "centroid_h3_r8": h3.latlng_to_cell(53.54, 9.97, 8),
         "bbox_min_lat": 53.54, "bbox_max_lat": 53.54,
         "bbox_min_lon": 9.97,  "bbox_max_lon": 9.97},
    ])
    config = Phase3Config(interim_dir="", min_events_per_cluster=1,
                          min_unique_mmsi_per_cluster=5)
    assert len(_filter_clusters(df, config)) == 0

    config = Phase3Config(interim_dir="", min_events_per_cluster=1,
                          min_unique_mmsi_per_cluster=2)
    assert len(_filter_clusters(df, config)) == 1


def test_filter_removes_small_clusters():
    cell_a = h3.latlng_to_cell(53.54, 9.97, RES)
    cell_b = h3.latlng_to_cell(51.90, 4.47, RES)

    df = pd.DataFrame([
        {"cluster_id": 0, "h3_cells": [cell_a], "n_cells": 1, "n_events": 100,
         "n_unique_mmsi": 50, "n_draught_changes": 2,
         "centroid_lat": 53.54, "centroid_lon": 9.97,
         "centroid_h3_r8": h3.latlng_to_cell(53.54, 9.97, 8),
         "bbox_min_lat": 53.54, "bbox_max_lat": 53.54,
         "bbox_min_lon": 9.97,  "bbox_max_lon": 9.97},
        {"cluster_id": 1, "h3_cells": [cell_b], "n_cells": 1, "n_events": 2,
         "n_unique_mmsi": 1, "n_draught_changes": 0,
         "centroid_lat": 51.90, "centroid_lon": 4.47,
         "centroid_h3_r8": h3.latlng_to_cell(51.90, 4.47, 8),
         "bbox_min_lat": 51.90, "bbox_max_lat": 51.90,
         "bbox_min_lon": 4.47,  "bbox_max_lon": 4.47},
    ])
    config = Phase3Config(interim_dir="", min_cells_per_cluster=1, min_events_per_cluster=5)
    result = _filter_clusters(df, config)

    assert len(result) == 1
    assert result.iloc[0]["n_events"] == 100


def test_filter_resets_cluster_ids():
    cell_a = h3.latlng_to_cell(53.54, 9.97, RES)
    df = pd.DataFrame([
        {"cluster_id": 99, "h3_cells": [cell_a], "n_cells": 1, "n_events": 50,
         "n_unique_mmsi": 10, "n_draught_changes": 0,
         "centroid_lat": 53.54, "centroid_lon": 9.97,
         "centroid_h3_r8": h3.latlng_to_cell(53.54, 9.97, 8),
         "bbox_min_lat": 53.54, "bbox_max_lat": 53.54,
         "bbox_min_lon": 9.97,  "bbox_max_lon": 9.97},
    ])
    config = Phase3Config(interim_dir="", min_events_per_cluster=1)
    result = _filter_clusters(df, config)
    assert result.iloc[0]["cluster_id"] == 0


def test_run_phase3_end_to_end(tmp_path):
    # Two separate harbour clusters + one isolated noise cell
    cluster_a = list(h3.grid_disk(h3.latlng_to_cell(53.54, 9.97, RES), 1))  # Hamburg, 7 cells
    cluster_b = [h3.latlng_to_cell(51.90, 4.47, RES)]                        # Rotterdam, 1 cell
    noise     = [h3.latlng_to_cell(20.00, 0.00, RES)]                        # 1 event — filtered

    all_cells  = cluster_a + cluster_b + noise
    n_events   = [20] * len(cluster_a) + [20] + [1]

    n = len(all_cells)
    latlons = [h3.cell_to_latlng(c) for c in all_cells]
    counts_df = pd.DataFrame({
        "h3_cell":                all_cells,
        "n_unique_mmsi":          [10]    * n,
        "n_events":               n_events,
        "total_duration_minutes": [120.0] * n,
        "mean_duration_minutes":  [60.0]  * n,
        "cell_lat":               [ll[0] for ll in latlons],
        "cell_lon":               [ll[1] for ll in latlons],
        "n_draught_changes":      [1]     * n,
        "max_visits_per_mmsi":    [2]     * n,
        "mean_visits_per_mmsi":   [1.5]   * n,
        "n_cargo":                [5]     * n,
        "n_tanker":               [2]     * n,
        "n_passenger":            [1]     * n,
        "n_fishing":              [0]     * n,
        "n_recreational":         [0]     * n,
        "n_tug_pilot":            [1]     * n,
        "top_destination_locode": [None]  * n,
    })
    pq.write_table(
        pa.Table.from_pandas(counts_df, schema=H3_COUNTS_SCHEMA, safe=False),
        tmp_path / "h3_counts.parquet",
    )

    config = Phase3Config(
        interim_dir=str(tmp_path),
        min_cells_per_cluster=1,
        min_events_per_cluster=5,
    )
    out = run_phase3(config)
    result = pd.read_parquet(out)

    assert len(result) == 2
    sizes = sorted(result["n_cells"].tolist())
    assert sizes == [1, 7]


def test_run_phase3_small_harbour_survives_with_exact_counts(tmp_path):
    """A small harbour whose vessels are scattered over cells (2 per cell)
    passes the cluster-level threshold via exact stop-based counting, and
    vessels spanning several cells are not double-counted."""
    seed = h3.latlng_to_cell(53.54, 9.97, RES)
    cells = sorted(h3.grid_disk(seed, 1))[:5]

    # 5 vessels; each stops in exactly two of the five cells
    stop_rows = []
    for i, mmsi in enumerate(range(100000001, 100000006)):
        for cell in (cells[i % 5], cells[(i + 1) % 5]):
            lat, lon = h3.cell_to_latlng(cell)
            stop_rows.append({"mmsi": mmsi, "lat": lat, "lon": lon})
    pd.DataFrame(stop_rows).to_parquet(tmp_path / "stops.parquet")

    latlons = [h3.cell_to_latlng(c) for c in cells]
    n = len(cells)
    counts_df = pd.DataFrame({
        "h3_cell":                cells,
        "n_unique_mmsi":          [2]     * n,   # no single cell reaches 5
        "n_events":               [2]     * n,
        "total_duration_minutes": [120.0] * n,
        "mean_duration_minutes":  [60.0]  * n,
        "cell_lat":               [ll[0] for ll in latlons],
        "cell_lon":               [ll[1] for ll in latlons],
        "n_draught_changes":      [0]     * n,
        "max_visits_per_mmsi":    [1]     * n,
        "mean_visits_per_mmsi":   [1.0]   * n,
        "n_cargo":                [0]     * n,
        "n_tanker":               [0]     * n,
        "n_passenger":            [0]     * n,
        "n_fishing":              [2]     * n,
        "n_recreational":         [0]     * n,
        "n_tug_pilot":            [0]     * n,
        "top_destination_locode": [None]  * n,
    })
    pq.write_table(
        pa.Table.from_pandas(counts_df, schema=H3_COUNTS_SCHEMA, safe=False),
        tmp_path / "h3_counts.parquet",
    )

    config = Phase3Config(interim_dir=str(tmp_path), min_unique_mmsi_per_cluster=5)
    result = pd.read_parquet(run_phase3(config))

    assert len(result) == 1
    assert result.iloc[0]["n_cells"] == 5
    # exactly 5 distinct vessels — not 10 (the per-cell sum)
    assert result.iloc[0]["n_unique_mmsi"] == 5
