"""
Phase 5: ID Matching + GeoJSON Export

For each enriched harbour cluster:
  1. Match against the existing harbour database (if supplied) using:
       a) H3 cell Jaccard overlap  (preferred, precise)
       b) Centroid distance        (fallback when existing DB has no h3_cells)
  2. Assign the matched harbour_id, or generate a new deterministic one
     (UUID5 of the centroid H3 cell at resolution 8 — stable across re-runs).
  3. Write data/output/harbours.parquet (both geometries), plus
     harbours.geojson (harbour outline) and harbours_cells.geojson (H3 cells).

Existing harbour database format (Parquet or GeoJSON):
  Required : harbour_id  (string)
  Preferred: h3_cells    (list of H3 cell strings at any resolution)
  Fallback : centroid_lat, centroid_lon  (float)
"""

import json
import logging
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import mapping
from shapely.wkt import dumps as to_wkt
from shapely.wkt import loads as from_wkt

from utils.geo import haversine_meters, merge_outlines
from pipeline.h3_aggregation import VESSEL_COUNTS
from utils.overrides import (
    DETECTED_TRANSIT_KEY,
    MANUAL_TRANSIT_KEY,
    DETECTED_OUTLINE_KEY,
    EDITABLE_FIELDS,
    MANUAL_OUTLINE_KEY,
    OVERRIDES_KEY,
    manual_outline,
    manual_transit,
    normalise_overrides,
    override_values,
)
from utils.s3 import (
    build_s3_config,
    ensure_dir,
    get_s3_filesystem,
    get_s3_storage_options,
    is_s3_path,
    path_join,
)

logger = logging.getLogger(__name__)

# Fixed namespace — keeps harbour_id stable across pipeline re-runs
_HARBOUR_NS = uuid.UUID("b8d7e3a2-5f1c-4e8b-9a6d-3c7f2e1b4a5d")

OUTPUT_SCHEMA = pa.schema([
    pa.field("harbour_id",             pa.string()),
    pa.field("cluster_id",             pa.int32()),
    pa.field("h3_cells",               pa.list_(pa.string())),
    pa.field("n_cells",                pa.int32()),
    pa.field("n_events",               pa.int32()),
    pa.field("n_unique_mmsi",          pa.int32()),
    pa.field("n_draught_changes",      pa.int32()),
    # Behavioural signature and vessel mix, from Phase 3 via Phase 4.
    pa.field("mean_dwell_minutes",     pa.float64()),
    pa.field("max_visits_per_mmsi",    pa.int32()),
    pa.field("n_cargo",                pa.int32()),
    pa.field("n_tanker",               pa.int32()),
    pa.field("n_passenger",            pa.int32()),
    pa.field("n_fishing",              pa.int32()),
    pa.field("n_recreational",         pa.int32()),
    pa.field("n_tug_pilot",            pa.int32()),
    # Effective verdict: an operator's, when they gave one, else Phase 4's.
    pa.field("transit_like",           pa.bool_()),
    pa.field("centroid_lat",           pa.float64()),
    pa.field("centroid_lon",           pa.float64()),
    pa.field("country_iso2",           pa.string()),
    pa.field("country_name",           pa.string()),
    pa.field("nearest_city",           pa.string()),
    pa.field("nearest_city_dist_km",   pa.float32()),
    pa.field("admin1",                 pa.string()),
    pa.field("geometry_wkt",           pa.string()),  # exact H3-cell union
    pa.field("outline_wkt",            pa.string()),  # effective outline (see below)
    pa.field("matched_existing",       pa.bool_()),  # True = reused existing harbour_id
    # Fields carried over from a GUI correction instead of being re-geocoded
    pa.field("manual_overrides",       pa.list_(pa.string())),
    # Outline drawn in the GUI, verbatim; null for a harbour nobody edited.
    pa.field("manual_outline_wkt",     pa.string()),
    # Phase 4's outline before the manual one was merged in. outline_wkt is the
    # union of the two, so this is what a GUI revert falls back to.
    pa.field("detected_outline_wkt",   pa.string()),
    # The operator's lock verdict, tri-state: null means they had no opinion
    # and transit_like is simply Phase 4's. Unlike the outline this replaces
    # rather than merges — a human overruling a heuristic is the whole point.
    pa.field("manual_transit_like",    pa.bool_()),
    # Phase 4's own verdict, so the GUI can show what was detected and revert.
    pa.field("detected_transit_like",  pa.bool_()),
])


@dataclass
class Phase5Config:
    interim_dir: str
    output_dir: str
    existing_db_path: Optional[str] = None
    h3_jaccard_threshold: float = 0.3
    centroid_match_distance_meters: float = 500.0
    # Mirrors phase4.outline_fill_holes so merging a manual outline treats voids
    # the same way the detected outline did — otherwise this phase would quietly
    # fill holes that Phase 4 was configured to keep.
    outline_fill_holes: bool = True
    s3_cfg: dict = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, cfg: dict) -> "Phase5Config":
        p5 = cfg.get("phase5", {})
        data = cfg.get("data", {})
        return cls(
            interim_dir=data.get("interim_dir", "data/interim"),
            output_dir=data.get("output_dir", "data/output"),
            existing_db_path=p5.get("existing_db_path"),
            h3_jaccard_threshold=p5.get("h3_jaccard_threshold", 0.3),
            centroid_match_distance_meters=p5.get(
                "centroid_match_distance_meters", 500.0
            ),
            outline_fill_holes=cfg.get("phase4", {}).get("outline_fill_holes", True),
            s3_cfg=build_s3_config(cfg.get("s3", {})),
        )


# ---------------------------------------------------------------------------
# Deterministic ID generation
# ---------------------------------------------------------------------------

def make_harbour_id(centroid_cell: str, country_iso2: str | None = None) -> str:
    """
    Deterministic harbour ID: '{CC}-{hex8}' where CC is the ISO 3166-1 alpha-2
    country code and hex8 is the first 8 hex chars of the UUID5 of the centroid
    H3 cell at resolution 8.  Falls back to 'ZZ' when country is unknown.

    Examples: 'DE-b8d7e3a2', 'NL-4c2e1af3', 'ZZ-9a6d3c7f'
    """
    prefix = (country_iso2 or "").strip().upper() or "ZZ"
    hex8   = uuid.uuid5(_HARBOUR_NS, centroid_cell).hex[:8]
    return f"{prefix}-{hex8}"


# ---------------------------------------------------------------------------
# Load existing harbour database
# ---------------------------------------------------------------------------

def _load_existing_db(path: str, s3_cfg: dict) -> pd.DataFrame:
    """
    Load an existing harbour database from Parquet or GeoJSON.
    Returns a DataFrame with at minimum: harbour_id, centroid_lat, centroid_lon.
    h3_cells column (list<str>) is used when present.
    Accepts both local paths and s3:// URIs.
    """
    suffix = Path(path).suffix.lower()

    if is_s3_path(path):
        fs = get_s3_filesystem(s3_cfg)
        if not fs.exists(path):
            raise FileNotFoundError(f"Existing harbour DB not found: {path}")
        if suffix == ".parquet":
            df = pd.read_parquet(path, storage_options=get_s3_storage_options(s3_cfg))
        elif suffix in (".geojson", ".json"):
            with fs.open(path, "r") as f:
                fc = json.load(f)
            df = _geojson_to_df(fc)
        else:
            raise ValueError(
                f"Unsupported existing DB format: {suffix}. Use .parquet or .geojson"
            )
    else:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Existing harbour DB not found: {path}")
        if suffix == ".parquet":
            df = pd.read_parquet(p)
        elif suffix in (".geojson", ".json"):
            with open(p) as f:
                fc = json.load(f)
            df = _geojson_to_df(fc)
        else:
            raise ValueError(
                f"Unsupported existing DB format: {suffix}. Use .parquet or .geojson"
            )

    required = {"harbour_id"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Existing DB is missing required columns: {missing}")

    logger.info("Loaded existing harbour DB: %d harbours from %s", len(df), path)
    return df


def _geojson_to_df(fc: dict) -> pd.DataFrame:
    rows = []
    for feat in fc.get("features", []):
        props = feat.get("properties", {})
        geom  = feat.get("geometry")
        row = dict(props)
        if "centroid_lat" not in row and geom:
            from shapely.geometry import shape as _shape
            try:
                c = _shape(geom).centroid
                row.setdefault("centroid_lat", c.y)
                row.setdefault("centroid_lon", c.x)
            except Exception:
                pass
        rows.append(row)
    return pd.DataFrame(rows)


def cell_list(value) -> list[str]:
    """
    Coerce a stored `h3_cells` value into a plain list of cell strings.

    A Parquet round-trip hands back a **numpy array**, not a list, and
    `isinstance(ndarray, (list, tuple))` is False. Testing for list/tuple
    therefore treated every Parquet-loaded harbour as having no cells at all,
    which silently (a) fed `_find_match` an empty set so the H3 Jaccard
    strategy could never fire, and (b) wrote `"h3_cells": []` into both
    GeoJSON outputs — so a database built from one carried no cells either,
    and matching everywhere fell back to centroid distance alone.
    """
    if value is None:
        return []
    if isinstance(value, float):        # NaN, the null a Parquet column leaves
        return []
    try:
        return [str(cell) for cell in value]
    except TypeError:
        return []


# ---------------------------------------------------------------------------
# Build lookup indexes from existing DB
# ---------------------------------------------------------------------------

def _build_indexes(
    existing: pd.DataFrame,
) -> tuple[
    dict[str, str], list[dict], dict[str, dict], dict[str, str], dict[str, bool],
]:
    """
    Returns:
      cell_index      : h3_cell → harbour_id  (for Jaccard matching)
      centroid_list   : list of {harbour_id, centroid_lat, centroid_lon, h3_cells}
      overrides_by_id : harbour_id → {field: manually corrected value}
      outlines_by_id  : harbour_id → manually drawn outline WKT
      transit_by_id   : harbour_id → the operator's lock verdict (True or False)
    """
    cell_index: dict[str, str] = {}
    centroid_list: list[dict] = []
    overrides_by_id: dict[str, dict] = {}
    outlines_by_id: dict[str, str] = {}
    transit_by_id: dict[str, bool] = {}

    has_cells = "h3_cells" in existing.columns

    for _, row in existing.iterrows():
        hid = row["harbour_id"]
        cells: list[str] = []

        corrected = override_values(row)
        if corrected:
            overrides_by_id[hid] = corrected

        drawn = manual_outline(row)
        if drawn:
            outlines_by_id[hid] = drawn

        # Tri-state: only an actual verdict is stored, so False ("checked, not
        # a lock") is kept and "nobody looked" is not.
        verdict = manual_transit(row)
        if verdict is not None:
            transit_by_id[hid] = verdict

        if has_cells:
            cells = cell_list(row["h3_cells"])
            for cell in cells:
                cell_index[cell] = hid

        has_lat = "centroid_lat" in row and pd.notna(row.get("centroid_lat"))
        has_lon = "centroid_lon" in row and pd.notna(row.get("centroid_lon"))
        clat = float(row["centroid_lat"]) if has_lat else None
        clon = float(row["centroid_lon"]) if has_lon else None

        centroid_list.append({
            "harbour_id":   hid,
            "centroid_lat": clat,
            "centroid_lon": clon,
            "h3_cells":     set(cells),
        })

    if overrides_by_id:
        logger.info("Existing DB carries manual overrides for %d harbours",
                    len(overrides_by_id))
    if outlines_by_id:
        logger.info("Existing DB carries manually drawn outlines for %d harbours",
                    len(outlines_by_id))
    if transit_by_id:
        logger.info("Existing DB carries manual lock verdicts for %d harbours",
                    len(transit_by_id))

    return (cell_index, centroid_list, overrides_by_id, outlines_by_id,
            transit_by_id)


# ---------------------------------------------------------------------------
# Matching logic
# ---------------------------------------------------------------------------

def _jaccard(set_a: set, set_b: set) -> float:
    union = set_a | set_b
    return len(set_a & set_b) / len(union) if union else 0.0


def _rank_match(
    new_cells: set[str],
    new_lat: float,
    new_lon: float,
    cell_index: dict[str, str],
    centroid_list: list[dict],
    config: Phase5Config,
) -> Optional[tuple[str, tuple]]:
    """
    The best existing harbour for this cluster, with a comparable quality.

    Returns `(harbour_id, quality)` where a *smaller* quality is a better
    claim, so competing clusters can be ranked against each other:

        (0, -jaccard)  an H3 overlap match — stronger evidence, so it outranks
        (1, distance)  any centroid-only match

    `_assign_ids` needs the quality because an existing harbour can only be
    claimed once; on its own, use `_find_match`.

    Strategy A — H3 Jaccard:
      Use the cell_index to collect candidate existing harbour_ids (any cell overlap),
      compute Jaccard for the best candidate, accept if >= threshold.

    Strategy B — centroid distance:
      The *nearest* existing centroid within the distance threshold. Nearest,
      not first: two existing harbours can both fall inside the radius, and
      taking whichever the database happened to list earlier picked a 495.8 m
      neighbour over a 0.0 m exact match on a real run, orphaning one harbour's
      id and handing another's to two clusters at once.
    """
    # --- Strategy A ---
    if cell_index:
        votes: dict[str, int] = {}
        for cell in new_cells:
            if cell in cell_index:
                eid = cell_index[cell]
                votes[eid] = votes.get(eid, 0) + 1

        if votes:
            best_id = max(votes, key=votes.__getitem__)
            # Find full record for best candidate
            best = next((e for e in centroid_list if e["harbour_id"] == best_id), None)
            if best and best["h3_cells"]:
                # set(): centroid_list stores cells as a list, and _jaccard
                # needs two sets — `set | list` raises. Latent until the
                # h3_cells coercion fix let Strategy A fire for the first time.
                score = _jaccard(new_cells, set(best["h3_cells"]))
                if score >= config.h3_jaccard_threshold:
                    return best_id, (0, -score)

    # --- Strategy B ---
    nearest_id, nearest_dist = None, None
    for entry in centroid_list:
        if entry["centroid_lat"] is None or entry["centroid_lon"] is None:
            continue
        dist = haversine_meters(new_lat, new_lon,
                                entry["centroid_lat"], entry["centroid_lon"])
        if dist > config.centroid_match_distance_meters:
            continue
        if nearest_dist is None or dist < nearest_dist:
            nearest_id, nearest_dist = entry["harbour_id"], dist

    return None if nearest_id is None else (nearest_id, (1, nearest_dist))


def _find_match(
    new_cells: set[str],
    new_lat: float,
    new_lon: float,
    cell_index: dict[str, str],
    centroid_list: list[dict],
    config: Phase5Config,
) -> Optional[str]:
    """The matched existing harbour_id, ignoring how good the match was."""
    found = _rank_match(new_cells, new_lat, new_lon,
                        cell_index, centroid_list, config)
    return None if found is None else found[0]


# ---------------------------------------------------------------------------
# Assign harbour_ids to all clusters
# ---------------------------------------------------------------------------

def _assign_ids(
    enriched: pd.DataFrame,
    cell_index: dict[str, str],
    centroid_list: list[dict],
    config: Phase5Config,
    overrides_by_id: Optional[dict[str, dict]] = None,
    outlines_by_id: Optional[dict[str, str]] = None,
    transit_by_id: Optional[dict[str, bool]] = None,
) -> pd.DataFrame:
    """
    Assign a harbour_id to every cluster and re-apply manual corrections.

    When a cluster matches an existing harbour whose record marks fields as
    manually overridden, those fields replace the values Phase 4 geocoded. Every
    other field — and every unmatched harbour — keeps the fresh Phase 4 value.

    A manually drawn outline is attached here but merged later, in
    `_apply_manual_outlines`: matching itself always runs against the freshly
    detected geometry.

    A stored lock verdict is attached the same way and applied in
    `_apply_manual_transit`, where it *replaces* Phase 4's rather than merging
    with it.
    """
    overrides_by_id = overrides_by_id or {}
    outlines_by_id  = outlines_by_id or {}
    transit_by_id   = transit_by_id or {}

    harbour_ids     = []
    matched_flags   = []
    override_lists  = []
    manual_outlines: list[Optional[str]] = []
    manual_transits: list[Optional[bool]] = []
    # column → {row position → corrected value}, applied after the loop so the
    # matching itself always runs against the freshly geocoded data.
    patches: dict[str, dict[int, object]] = {}

    n_matched    = 0
    n_new        = 0
    n_overridden = 0

    # Pass 1 — what each cluster would like to claim, and how good the claim is.
    wanted: dict[int, tuple[str, tuple]] = {}
    for pos, (_, row) in enumerate(enriched.iterrows()):
        found = _rank_match(
            set(cell_list(row["h3_cells"])),
            float(row["centroid_lat"]), float(row["centroid_lon"]),
            cell_index, centroid_list, config,
        )
        if found is not None:
            wanted[pos] = found

    # Pass 2 — an existing harbour can be claimed once. Two clusters landing on
    # the same one is not rare: `centroid_match_distance_meters` is 500 m and
    # real harbours get closer than that (two Hellerup basins are 442 m apart),
    # so without this the pair share an id and the database grows a duplicate
    # that re-matches both clusters on every later run.
    best_claim: dict[str, tuple[tuple, int]] = {}
    for pos, (existing_id, quality) in wanted.items():
        if existing_id not in best_claim or quality < best_claim[existing_id][0]:
            best_claim[existing_id] = (quality, pos)
    winner_at = {pos: eid for eid, (_, pos) in best_claim.items()}

    n_contested = len(wanted) - len(winner_at)
    if n_contested:
        logger.info(
            "  %d cluster(s) lost a contested existing harbour to a closer "
            "match and were given fresh ids", n_contested,
        )

    for pos, (_, row) in enumerate(enriched.iterrows()):
        existing_id = winner_at.get(pos)

        if existing_id:
            harbour_ids.append(existing_id)
            matched_flags.append(True)
            manual_outlines.append(outlines_by_id.get(existing_id))
            manual_transits.append(transit_by_id.get(existing_id))
            n_matched += 1

            corrected = overrides_by_id.get(existing_id, {})
            for column, value in corrected.items():
                patches.setdefault(column, {})[pos] = value
            fields = [f for f in corrected if f in EDITABLE_FIELDS]
            override_lists.append(fields)
            if fields:
                n_overridden += 1
        else:
            harbour_ids.append(
                make_harbour_id(row["centroid_id_cell"], row.get("country_iso2"))
            )
            matched_flags.append(False)
            override_lists.append([])
            manual_outlines.append(None)
            manual_transits.append(None)
            n_new += 1

    logger.info(
        "ID assignment: %d matched existing, %d new harbour_ids generated",
        n_matched, n_new,
    )
    if n_overridden:
        logger.info("  kept manual city/region/country corrections on %d harbours",
                    n_overridden)

    enriched = enriched.copy().reset_index(drop=True)
    for column, by_position in patches.items():
        if column not in enriched.columns:
            continue
        # object dtype so a corrected string can land in any column without
        # tripping over the original pandas dtype.
        series = enriched[column].astype(object)
        for pos, value in by_position.items():
            series.iat[pos] = value
        enriched[column] = series

    enriched["harbour_id"]        = harbour_ids
    enriched["matched_existing"]  = matched_flags
    enriched[OVERRIDES_KEY]       = override_lists
    enriched[MANUAL_OUTLINE_KEY]  = manual_outlines
    enriched[MANUAL_TRANSIT_KEY]  = manual_transits
    return enriched


def _apply_manual_transit(df: pd.DataFrame) -> pd.DataFrame:
    """
    Let an operator's lock verdict overrule Phase 4's.

    Unlike the outline, which is merged, this replaces: the whole point of the
    control is that a person can say "this is a lock" about something the
    heuristic missed, or "this is not" about something it flagged. Phase 4's
    verdict is preserved in `detected_transit_like` so the GUI can show both
    and offer a revert.

    A null verdict means nobody expressed one, and the detected value stands.
    """
    df = df.copy()
    if "transit_like" not in df.columns:
        df["transit_like"] = False
    df[DETECTED_TRANSIT_KEY] = df["transit_like"].fillna(False).astype(bool)

    if MANUAL_TRANSIT_KEY not in df.columns:
        df[MANUAL_TRANSIT_KEY] = None
        return df

    verdicts = [manual_transit({MANUAL_TRANSIT_KEY: v})
                for v in df[MANUAL_TRANSIT_KEY]]
    df[MANUAL_TRANSIT_KEY] = verdicts
    df["transit_like"] = [
        detected if verdict is None else verdict
        for detected, verdict in zip(df[DETECTED_TRANSIT_KEY], verdicts)
    ]

    n_applied = sum(1 for v in verdicts if v is not None)
    if n_applied:
        logger.info("  applied %d manual lock verdict(s) over the detected ones",
                    n_applied)
    return df


def _apply_manual_outlines(
    df: pd.DataFrame,
    fill_holes: bool = True,
) -> pd.DataFrame:
    """
    Union each harbour's manually drawn outline into its detected one.

    The drawn outline is a floor, not a replacement: `outline_wkt` becomes
    `detected ∪ manual`, so new stop events can still push the boundary
    outwards, while a re-run can never pull it back inside what the operator
    drew. `detected_outline_wkt` keeps Phase 4's shape so the GUI can show what
    was actually detected and revert an edit.

    The stored `manual_outline_wkt` is never rewritten — it stays the frozen
    baseline the operator drew, however far the harbour grows around it.
    Unparseable geometry is logged and skipped rather than failing the phase:
    one bad polygon in the existing DB must not cost a whole run.
    """
    df = df.copy()
    df[DETECTED_OUTLINE_KEY] = df["outline_wkt"]

    if MANUAL_OUTLINE_KEY not in df.columns:
        return df

    merged_wkts: list[Optional[str]] = []
    n_applied = 0
    n_skipped = 0

    for _, row in df.iterrows():
        detected_wkt = row["outline_wkt"] if pd.notna(row["outline_wkt"]) else None
        drawn_wkt    = manual_outline(row)

        if not drawn_wkt:
            merged_wkts.append(detected_wkt)
            continue

        try:
            drawn    = from_wkt(drawn_wkt)
            detected = from_wkt(detected_wkt) if detected_wkt else None
            merged   = merge_outlines(detected, drawn, fill_holes=fill_holes)
        except Exception as exc:
            logger.warning("Harbour %s: could not merge its manual outline (%s) — "
                           "keeping the detected one", row["harbour_id"], exc)
            merged_wkts.append(detected_wkt)
            n_skipped += 1
            continue

        if merged is None:
            logger.warning("Harbour %s: manual outline encloses no area — "
                           "keeping the detected one", row["harbour_id"])
            merged_wkts.append(detected_wkt)
            n_skipped += 1
            continue

        merged_wkts.append(to_wkt(merged))
        n_applied += 1

    df["outline_wkt"] = merged_wkts

    if n_applied:
        logger.info("Merged manually drawn outlines into %d harbours", n_applied)
    if n_skipped:
        logger.warning("  %d manual outlines were unusable and ignored", n_skipped)
    return df


# ---------------------------------------------------------------------------
# Write outputs
# ---------------------------------------------------------------------------

def _write_parquet(df: pd.DataFrame, out_dir: str, s3_cfg: dict) -> str:
    out_path = path_join(out_dir, "harbours.parquet")
    h3_cells_array = pa.array(df["h3_cells"].tolist(), type=pa.list_(pa.string()))
    dist_km_array = pa.array(
        df["nearest_city_dist_km"].astype("float32"), type=pa.float32()
    )
    stored_overrides = (
        df[OVERRIDES_KEY] if OVERRIDES_KEY in df.columns else [None] * len(df)
    )
    overrides_array = pa.array(
        [normalise_overrides(v) for v in stored_overrides],
        type=pa.list_(pa.string()),
    )
    # Nullable: only harbours edited in the GUI carry a drawn outline. Written
    # through manual_outline() so a blank string lands as null, not as "".
    manual_outline_array = pa.array(
        [manual_outline(row) for _, row in df.iterrows()], type=pa.string()
    )
    detected_outline_array = pa.array(
        df[DETECTED_OUTLINE_KEY] if DETECTED_OUTLINE_KEY in df.columns
        else df["outline_wkt"],
        type=pa.string(),
    )
    # Nullable and tri-state: null is "no operator opinion", which is not the
    # same as False. Written through manual_transit() so every round-trip shape
    # of "missing" lands as null.
    manual_transit_array = pa.array(
        [manual_transit(row) for _, row in df.iterrows()], type=pa.bool_()
    )

    # Special-cased columns, then the rest straight off OUTPUT_SCHEMA so a new
    # field cannot be added to the schema and silently dropped by the writer.
    special = {
        "h3_cells":             h3_cells_array,
        "nearest_city_dist_km": dist_km_array,
        OVERRIDES_KEY:          overrides_array,
        MANUAL_OUTLINE_KEY:     manual_outline_array,
        DETECTED_OUTLINE_KEY:   detected_outline_array,
        MANUAL_TRANSIT_KEY:     manual_transit_array,
    }
    table = pa.table(
        {
            field.name: special.get(
                field.name,
                pa.array(
                    df[field.name] if field.name in df.columns
                    else [None] * len(df),
                    type=field.type,
                ),
            )
            for field in OUTPUT_SCHEMA
        },
        schema=OUTPUT_SCHEMA,
    )
    if is_s3_path(out_dir):
        fs = get_s3_filesystem(s3_cfg)
        with fs.open(out_path, "wb") as fh:
            pq.write_table(table, fh, compression="snappy")
    else:
        pq.write_table(table, out_path, compression="snappy")
    logger.info("Wrote harbours.parquet → %s", out_path)
    return out_path


def _optional_wkt(value) -> Optional[str]:
    """A WKT string for JSON, or None — never a NaN, which is not valid JSON."""
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _write_geojson(
    df: pd.DataFrame,
    out_dir: str,
    s3_cfg: dict,
    geometry_col: str = "outline_wkt",
    filename: str = "harbours.geojson",
) -> str:
    """
    Write one GeoJSON FeatureCollection using `geometry_col` as the feature
    geometry. GeoJSON allows a single geometry per feature, so the outline and
    the H3-cell union are written to separate files; the `geometry_kind`
    property says which one a file holds.
    """
    out_path = path_join(out_dir, filename)
    kind = "outline" if geometry_col == "outline_wkt" else "cells"

    features = []
    for _, row in df.iterrows():
        # Geometry
        geom = None
        if pd.notna(row.get(geometry_col)):
            try:
                geom = mapping(from_wkt(row[geometry_col]))
            except Exception as exc:
                logger.warning("Could not parse WKT for harbour %s: %s",
                               row["harbour_id"], exc)

        cells = cell_list(row["h3_cells"])

        feature = {
            "type": "Feature",
            "geometry": geom,
            "properties": {
                "harbour_id":             row["harbour_id"],
                "geometry_kind":          kind,
                "h3_cells":               cells,
                "n_cells":                int(row["n_cells"]),
                "n_events":               int(row["n_events"]),
                "n_unique_mmsi":          int(row["n_unique_mmsi"]),
                "n_draught_changes":      int(row["n_draught_changes"]),
                "mean_dwell_minutes":     round(float(row["mean_dwell_minutes"]), 1),
                "max_visits_per_mmsi":    int(row["max_visits_per_mmsi"]),
                **{c: int(row[c]) for c in VESSEL_COUNTS},
                # Effective verdict, then the two halves it came from, so the
                # GUI can show what was detected and what a person decided.
                "transit_like":           bool(row["transit_like"]),
                "detected_transit_like":  bool(row.get(DETECTED_TRANSIT_KEY,
                                                       row["transit_like"])),
                "manual_transit_like":    manual_transit(row),
                "centroid_lat":           float(row["centroid_lat"]),
                "centroid_lon":           float(row["centroid_lon"]),
                "country_iso2":           row["country_iso2"] or "",
                "country_name":           row["country_name"] or "",
                "nearest_city":           row["nearest_city"] or "",
                "nearest_city_dist_km":   round(float(row["nearest_city_dist_km"]), 3),
                "admin1":                 row["admin1"] or "",
                "matched_existing":       bool(row["matched_existing"]),
                "manual_overrides":       normalise_overrides(row.get(OVERRIDES_KEY)),
                # Both outline files carry these, so either one works as the
                # existing DB the next run matches against.
                MANUAL_OUTLINE_KEY:       manual_outline(row),
                DETECTED_OUTLINE_KEY:     _optional_wkt(row.get(DETECTED_OUTLINE_KEY)),
            },
        }
        features.append(feature)

    geojson = {"type": "FeatureCollection", "features": features}

    if is_s3_path(out_dir):
        fs = get_s3_filesystem(s3_cfg)
        with fs.open(out_path, "wb") as fh:
            fh.write(json.dumps(geojson, ensure_ascii=False, indent=2).encode("utf-8"))
    else:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(geojson, f, ensure_ascii=False, indent=2)

    logger.info("Wrote %s (%d features, %s geometry) → %s",
                filename, len(features), kind, out_path)
    return out_path


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_phase5(config: Phase5Config) -> tuple[str, str, str]:
    """
    Returns (parquet_path, geojson_path, cells_geojson_path).

    harbours.geojson       carries the closed harbour outline
    harbours_cells.geojson carries the exact H3-cell union
    harbours.parquet       carries both, as outline_wkt and geometry_wkt
    """
    enriched_path = path_join(config.interim_dir, "harbours_enriched.parquet")
    if not is_s3_path(config.interim_dir) and not Path(enriched_path).exists():
        raise FileNotFoundError(
            f"harbours_enriched.parquet not found at {enriched_path} — run phase4 first"
        )

    logger.info("Phase 5: reading %s …", enriched_path)
    if is_s3_path(config.interim_dir):
        enriched = pd.read_parquet(
            enriched_path, storage_options=get_s3_storage_options(config.s3_cfg)
        )
    else:
        enriched = pd.read_parquet(enriched_path)
    logger.info("  loaded %d enriched clusters", len(enriched))

    # Enriched files written before outlines existed still carry only the cell
    # union — fall back to it so Phase 5 stays runnable against older interims.
    if "outline_wkt" not in enriched.columns:
        logger.warning(
            "harbours_enriched.parquet has no outline_wkt column — falling back to "
            "the H3-cell union. Re-run phase4 to generate real outlines."
        )
        enriched["outline_wkt"] = enriched["geometry_wkt"]

    # Load existing harbour DB (optional)
    cell_index:      dict[str, str]  = {}
    centroid_list:   list[dict]      = []
    overrides_by_id: dict[str, dict] = {}
    outlines_by_id:  dict[str, str]  = {}
    transit_by_id:   dict[str, bool] = {}

    if config.existing_db_path:
        existing = _load_existing_db(config.existing_db_path, config.s3_cfg)
        (cell_index, centroid_list, overrides_by_id, outlines_by_id,
         transit_by_id) = _build_indexes(existing)
    else:
        logger.info(
            "No existing harbour DB supplied — all IDs will be newly generated."
        )

    # Assign IDs, then merge in any outline an operator drew for a matched harbour
    result = _assign_ids(enriched, cell_index, centroid_list, config,
                         overrides_by_id, outlines_by_id, transit_by_id)
    result = _apply_manual_outlines(result, fill_holes=config.outline_fill_holes)
    result = _apply_manual_transit(result)

    # Write outputs
    ensure_dir(config.output_dir)

    parquet_path = _write_parquet(result, config.output_dir, config.s3_cfg)
    geojson_path = _write_geojson(
        result, config.output_dir, config.s3_cfg,
        geometry_col="outline_wkt", filename="harbours.geojson",
    )
    cells_path = _write_geojson(
        result, config.output_dir, config.s3_cfg,
        geometry_col="geometry_wkt", filename="harbours_cells.geojson",
    )

    return parquet_path, geojson_path, cells_path
