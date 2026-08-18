"""
Phase 1: Stop Extraction — per-vessel logic.

Two complementary strategies:
  A) Nav-status filter  — ships broadcasting nav_status=moored/anchored
  B) Sustained low-speed — ships that stop without broadcasting correct status

Both produce the same stop-event schema. Type-5 draught data is joined to each
stop for arrival/departure draught delta.

This module holds only the pure-pandas logic that runs per MMSI; the reading,
distribution and writing live in extract_stops_spark.py, which runs these
functions inside an applyInPandas UDF.
"""

import re
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa

from utils.geo import positional_variance_meters
from utils.s3 import build_s3_config

# Output schema for stops.parquet — the contract Phase 2 reads back
STOP_SCHEMA = pa.schema([
    pa.field("mmsi", pa.int64()),
    pa.field("lat", pa.float64()),
    pa.field("lon", pa.float64()),
    pa.field("timestamp_start", pa.timestamp("ms", tz="UTC")),
    pa.field("timestamp_end", pa.timestamp("ms", tz="UTC")),
    pa.field("duration_minutes", pa.float32()),
    pa.field("n_messages", pa.int32()),
    pa.field("pos_variance_meters", pa.float32()),
    pa.field("nav_status", pa.int16()),
    pa.field("draught_arrival", pa.float32()),
    pa.field("draught_departure", pa.float32()),
    pa.field("draught_delta", pa.float32()),
    pa.field("detection_method", pa.string()),
    pa.field("ship_type", pa.int16()),
    pa.field("destination_raw", pa.string()),
    pa.field("destination_locode", pa.string()),
])


@dataclass
class Phase1Config:
    raw_glob: str
    interim_dir: str
    s3_cfg: dict = field(default_factory=dict)

    # column name mapping
    col_mmsi: str = "mmsi"
    col_timestamp: str = "timestamp"
    col_lat: str = "lat"
    col_lon: str = "lon"
    col_sog: str = "sog"
    col_nav_status: str = "nav_status"
    col_msg_type: str = "msg_type"
    col_draught: str = "draught"
    col_destination: str = "destination"
    col_ship_type: str = "ship_type"

    moored_nav_statuses: list = field(default_factory=lambda: [1, 5])
    sog_threshold_knots: float = 0.5
    max_gap_minutes: float = 15.0
    min_stop_duration_minutes: float = 30.0
    min_messages_per_stop: int = 3
    positional_variance_max_meters: float = 300.0
    mmsi_min: int = 100_000_000
    mmsi_max: int = 999_999_999
    draught_lookup_hours: int = 6

    @classmethod
    def from_yaml(cls, cfg: dict) -> "Phase1Config":
        cols = cfg.get("columns", {})
        p1 = cfg.get("phase1", {})
        data = cfg.get("data", {})
        return cls(
            raw_glob=data.get("raw_glob", "data/raw/**/*.parquet"),
            interim_dir=data.get("interim_dir", "data/interim"),
            s3_cfg=build_s3_config(cfg.get("s3", {})),
            col_mmsi=cols.get("mmsi", "mmsi"),
            col_timestamp=cols.get("timestamp", "timestamp"),
            col_lat=cols.get("lat", "lat"),
            col_lon=cols.get("lon", "lon"),
            col_sog=cols.get("sog", "sog"),
            col_nav_status=cols.get("nav_status", "nav_status"),
            col_msg_type=cols.get("msg_type", "msg_type"),
            col_draught=cols.get("draught", "draught"),
            col_destination=cols.get("destination", "destination"),
            col_ship_type=cols.get("ship_type", "ship_type"),
            moored_nav_statuses=p1.get("moored_nav_statuses", [1, 5]),
            sog_threshold_knots=p1.get("sog_threshold_knots", 0.5),
            max_gap_minutes=p1.get("max_gap_minutes", 15.0),
            min_stop_duration_minutes=p1.get("min_stop_duration_minutes", 30.0),
            min_messages_per_stop=p1.get("min_messages_per_stop", 3),
            positional_variance_max_meters=p1.get(
                "positional_variance_max_meters", 300.0
            ),
            mmsi_min=p1.get("mmsi_min", 100_000_000),
            mmsi_max=p1.get("mmsi_max", 999_999_999),
            draught_lookup_hours=p1.get("draught_lookup_hours", 6),
        )


# ---------------------------------------------------------------------------
# Destination parsing
# ---------------------------------------------------------------------------

# UN/LOCODE: 2-letter ISO country code + 3 alphanumeric chars (digits 2-9, A-Z)
_LOCODE_RE = re.compile(r'\b([A-Z]{2}[A-Z2-9]{3})\b')


def _parse_locode(destination: Optional[str]) -> Optional[str]:
    """
    Extract the most likely UN/LOCODE from a free-text destination string.

    Takes the LAST match in the string because destination LOCODEs tend to
    appear at the end ("EN ROUTE DEHAM", "VIA NLRTM DEHAM"), while common
    English words like ROUTE appear in the middle.
    """
    if not destination:
        return None
    dest = destination.strip().upper()
    # Exact 5-char match — fastest path for the common case
    if re.fullmatch(r'[A-Z]{2}[A-Z2-9]{3}', dest):
        return dest
    # Spaced variant like "DE HAM"
    no_space = dest.replace(" ", "")
    if re.fullmatch(r'[A-Z]{2}[A-Z2-9]{3}', no_space):
        return no_space
    # Find ALL matches and return the last one
    matches = _LOCODE_RE.findall(dest)
    return matches[-1] if matches else None


# ---------------------------------------------------------------------------
# Group rows into stop segments (per MMSI)
# ---------------------------------------------------------------------------

def _group_into_segments(vessel_df: pd.DataFrame, config: Phase1Config) -> list[dict]:
    """
    For a single vessel's candidate rows (already sorted by timestamp),
    group consecutive messages separated by < max_gap into segments,
    then filter by duration and positional variance.
    """
    vessel_df = vessel_df.sort_values("timestamp").reset_index(drop=True)

    # Mark segment boundaries where time gap exceeds max_gap
    gaps = vessel_df["timestamp"].diff().dt.total_seconds().fillna(0) / 60
    vessel_df["_seg"] = (gaps > config.max_gap_minutes).cumsum()

    segments = []
    for _, seg in vessel_df.groupby("_seg", sort=False):
        if len(seg) < config.min_messages_per_stop:
            continue

        t_start = seg["timestamp"].min()
        t_end = seg["timestamp"].max()
        duration = (t_end - t_start).total_seconds() / 60

        if duration < config.min_stop_duration_minutes:
            continue

        lats = seg["lat"].to_numpy()
        lons = seg["lon"].to_numpy()
        variance = positional_variance_meters(lats, lons)

        if variance > config.positional_variance_max_meters:
            continue

        nav_vals = seg["nav_status"].dropna()
        nav_mode = int(nav_vals.mode().iloc[0]) if len(nav_vals) > 0 else None

        segments.append({
            "mmsi": int(seg["mmsi"].iloc[0]),
            "lat": float(lats.mean()),
            "lon": float(lons.mean()),
            "timestamp_start": t_start,
            "timestamp_end": t_end,
            "duration_minutes": float(duration),
            "n_messages": int(len(seg)),
            "pos_variance_meters": float(variance),
            "nav_status": nav_mode,
        })

    return segments


# ---------------------------------------------------------------------------
# Label detection method
# ---------------------------------------------------------------------------

def _label_detection_method(stops: pd.DataFrame, config: Phase1Config) -> pd.DataFrame:
    """
    Tag each stop with how it was detected.

    - nav_status   : nav_status is moored/anchored for the majority of messages
    - sustained_speed : slow but status not moored/anchored
    - both         : slow AND moored/anchored
    """
    moored = set(config.moored_nav_statuses)

    def _method(row) -> str:
        has_nav = pd.notna(row["nav_status"]) and int(row["nav_status"]) in moored
        # SOG was already < threshold (that's how the row was selected)
        # If nav_status is moored AND speed is low → 'both'
        # If nav_status is moored only → 'nav_status'
        # Otherwise → 'sustained_speed'
        # (We cannot distinguish "speed only" vs "both" perfectly at this stage
        #  because we merged the filter, so we label conservatively.)
        return "nav_status" if has_nav else "sustained_speed"

    stops["detection_method"] = stops.apply(_method, axis=1)
    return stops


# ---------------------------------------------------------------------------
# Join type-5 data (draught + destination + ship_type)
# ---------------------------------------------------------------------------

def _join_type5_data(stops: pd.DataFrame, type5: pd.DataFrame,
                     config: Phase1Config) -> pd.DataFrame:
    """
    For each stop event, join three things from type-5 messages:

    draught_arrival   — most recent type-5 with draught > 0 before t_start
    draught_departure — first type-5 with draught > 0 after t_end
    destination_raw   — most recent type-5 destination string before t_start
    destination_locode— UN/LOCODE extracted from destination_raw
    ship_type         — mode of ship_type across ALL type-5 for this MMSI
                        (ship type is a vessel characteristic, rarely changes)
    """
    if type5.empty:
        stops["draught_arrival"] = np.nan
        stops["draught_departure"] = np.nan
        stops["draught_delta"] = np.nan
        stops["ship_type"] = np.nan
        stops["destination_raw"] = None
        stops["destination_locode"] = None
        return stops

    lookup_td = pd.Timedelta(hours=config.draught_lookup_hours)

    # Pre-compute per-MMSI ship_type mode (stable vessel attribute)
    ship_type_mode: dict[int, Optional[int]] = {}
    for mmsi, grp in type5.groupby("mmsi"):
        valid = grp["ship_type"].dropna()
        valid = valid[valid > 0]
        ship_type_mode[int(mmsi)] = (
            int(valid.mode().iloc[0]) if not valid.empty else None
        )

    # Index type5 by mmsi for per-stop lookups
    t5_by_mmsi = {int(mmsi): grp.reset_index(drop=True)
                  for mmsi, grp in type5.groupby("mmsi")}

    arrivals = []
    departures = []
    dest_raws: list[Optional[str]] = []
    dest_locodes: list[Optional[str]] = []
    ship_types = []

    for _, stop in stops.iterrows():
        mmsi = int(stop["mmsi"])
        t_start = stop["timestamp_start"]
        t_end = stop["timestamp_end"]
        t5 = t5_by_mmsi.get(mmsi)

        ship_types.append(ship_type_mode.get(mmsi))

        if t5 is None:
            arrivals.append(np.nan)
            departures.append(np.nan)
            dest_raws.append(None)
            dest_locodes.append(None)
            continue

        # Most recent type-5 before t_start (within lookup window)
        before = t5[(t5["timestamp"] <= t_start) &
                    (t5["timestamp"] >= t_start - lookup_td)]

        if not before.empty:
            last_before = before.iloc[-1]
            has_draught = (pd.notna(last_before["draught"])
                           and last_before["draught"] > 0)
            arr = float(last_before["draught"]) if has_draught else np.nan
            has_dest = pd.notna(last_before.get("destination"))
            raw_dest = str(last_before["destination"]) if has_dest else None
        else:
            arr = np.nan
            raw_dest = None

        # First type-5 after t_end (within lookup window)
        after = t5[(t5["timestamp"] >= t_end) &
                   (t5["timestamp"] <= t_end + lookup_td)]
        dep = float(after.iloc[0]["draught"]) if (
                not after.empty
                and pd.notna(after.iloc[0]["draught"])
                and after.iloc[0]["draught"] > 0
        ) else np.nan

        arrivals.append(arr)
        departures.append(dep)
        dest_raws.append(raw_dest)
        dest_locodes.append(_parse_locode(raw_dest))

    stops = stops.copy()
    stops["draught_arrival"] = arrivals
    stops["draught_departure"] = departures
    stops["draught_delta"] = (
            stops["draught_departure"] - stops["draught_arrival"]
    ).where(
        pd.to_numeric(stops["draught_arrival"], errors="coerce").notna() &
        pd.to_numeric(stops["draught_departure"], errors="coerce").notna()
    )
    stops["ship_type"] = ship_types
    stops["destination_raw"] = dest_raws
    stops["destination_locode"] = dest_locodes

    return stops
