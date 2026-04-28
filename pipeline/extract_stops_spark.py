"""
Phase 1: Stop Extraction — Spark implementation.

Reads raw AIS parquet files from S3, distributes stop-detection per MMSI
across Spark executors, and writes stops.parquet back to S3.

Core per-vessel logic (_group_into_segments, _label_detection_method,
_join_type5_data) is imported unchanged from extract_stops.py.
"""

import logging

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipeline.extract_stops import (
    Phase1Config,
    _group_into_segments,
    _join_type5_data,
    _label_detection_method,
)
from utils.s3 import is_s3_path, path_join

logger = logging.getLogger(__name__)

SPARK_STOP_SCHEMA = StructType([
    StructField("mmsi",                LongType(),      nullable=False),
    StructField("lat",                 DoubleType(),    nullable=True),
    StructField("lon",                 DoubleType(),    nullable=True),
    StructField("timestamp_start",     TimestampType(), nullable=True),
    StructField("timestamp_end",       TimestampType(), nullable=True),
    StructField("duration_minutes",    FloatType(),     nullable=True),
    StructField("n_messages",          IntegerType(),   nullable=True),
    StructField("pos_variance_meters", FloatType(),     nullable=True),
    StructField("nav_status",          ShortType(),     nullable=True),
    StructField("draught_arrival",     FloatType(),     nullable=True),
    StructField("draught_departure",   FloatType(),     nullable=True),
    StructField("draught_delta",       FloatType(),     nullable=True),
    StructField("detection_method",    StringType(),    nullable=True),
    StructField("ship_type",           ShortType(),     nullable=True),
    StructField("destination_raw",     StringType(),    nullable=True),
    StructField("destination_locode",  StringType(),    nullable=True),
])

_OUTPUT_COLS = [f.name for f in SPARK_STOP_SCHEMA.fields]


def _make_vessel_processor(config: Phase1Config):
    """Return an applyInPandas function closed over config (serialised to executors)."""

    def process_vessel(vessel_df: pd.DataFrame) -> pd.DataFrame:
        pos = vessel_df[vessel_df["_row_type"] == "pos"].copy()
        t5  = vessel_df[vessel_df["_row_type"] == "t5"].copy()

        if pos.empty:
            return pd.DataFrame(columns=_OUTPUT_COLS)

        pos["timestamp"] = pd.to_datetime(pos["timestamp"], unit="s", utc=True)

        segs = _group_into_segments(pos, config)
        if not segs:
            return pd.DataFrame(columns=_OUTPUT_COLS)

        stops = pd.DataFrame(segs)
        stops = _label_detection_method(stops, config)

        if not t5.empty:
            t5["timestamp"] = pd.to_datetime(t5["timestamp"], unit="s", utc=True)
            for col in ("draught", "destination", "ship_type"):
                if col not in t5.columns:
                    t5[col] = None
            t5_clean = (
                t5[["mmsi", "timestamp", "draught", "destination", "ship_type"]]
                .sort_values(["mmsi", "timestamp"])
                .reset_index(drop=True)
            )
        else:
            t5_clean = pd.DataFrame(
                columns=["mmsi", "timestamp", "draught", "destination", "ship_type"]
            )

        stops = _join_type5_data(stops, t5_clean, config)

        # Normalise dtypes to match SPARK_STOP_SCHEMA
        stops["n_messages"] = stops["n_messages"].astype("int32")
        for col in ("nav_status", "ship_type"):
            stops[col] = pd.to_numeric(stops[col], errors="coerce").astype("Int16")
        for col in ("draught_arrival", "draught_departure", "draught_delta",
                    "duration_minutes", "pos_variance_meters"):
            stops[col] = pd.to_numeric(stops[col], errors="coerce").astype("float32")

        return stops[_OUTPUT_COLS]

    return process_vessel


def _to_s3a(path: str) -> str:
    """Rewrite s3:// URIs to s3a:// for Spark's Hadoop S3A connector."""
    return "s3a://" + path[5:] if path.startswith("s3://") else path


def _base_dir(glob_path: str) -> str:
    """
    Extract the non-glob prefix of a glob pattern so Spark can use
    recursiveFileLookup=true, which is more reliable than Hadoop's **-glob.

    E.g. "s3a://bucket/ais/2024/**/*.parquet" → "s3a://bucket/ais/2024/"
    """
    for i, ch in enumerate(glob_path):
        if ch in ("*", "?", "[", "{"):
            return glob_path[: glob_path.rfind("/", 0, i) + 1]
    return glob_path


def run_phase1(config: Phase1Config, spark: SparkSession) -> str:
    """
    Execute Phase 1 end-to-end using Spark.
    Returns the output path (s3:// or local) to stops.parquet.
    """
    c = config
    spark_path = _base_dir(_to_s3a(c.raw_glob))

    logger.info("Spark: reading parquet from %s (recursive)", spark_path)
    raw = (
        spark.read
        .option("mergeSchema", "true")
        .option("recursiveFileLookup", "true")
        .parquet(spark_path)
    )

    raw_cols = set(raw.columns)
    statuses = c.moored_nav_statuses

    # ── Candidate positional rows (slow / moored) ────────────────────────────
    candidates = (
        raw
        .filter(
            (F.col(c.col_sog) < c.sog_threshold_knots)
            | F.col(c.col_nav_status).isin(statuses)
        )
        .filter(F.col(c.col_lat).between(-90, 90))
        .filter(F.col(c.col_lon).between(-180, 180))
        .filter(F.col(c.col_mmsi).between(c.mmsi_min, c.mmsi_max))
        .select(
            F.col(c.col_mmsi).cast(LongType()).alias("mmsi"),
            F.col(c.col_timestamp).alias("timestamp"),
            F.col(c.col_lat).cast(DoubleType()).alias("lat"),
            F.col(c.col_lon).cast(DoubleType()).alias("lon"),
            F.col(c.col_sog).alias("sog"),
            F.col(c.col_nav_status).alias("nav_status"),
        )
        .withColumn("_row_type", F.lit("pos"))
    )

    # ── Type-5 rows (draught / destination / ship_type) ──────────────────────
    t5_select = [
        F.col(c.col_mmsi).cast(LongType()).alias("mmsi"),
        F.col(c.col_timestamp).alias("timestamp"),
    ]
    for src, dst in [
        (c.col_draught,     "draught"),
        (c.col_destination, "destination"),
        (c.col_ship_type,   "ship_type"),
    ]:
        t5_select.append(
            F.col(src).alias(dst) if src in raw_cols else F.lit(None).alias(dst)
        )

    type5 = (
        raw
        .filter(F.col(c.col_msg_type) == 5)
        .filter(F.col(c.col_mmsi).between(c.mmsi_min, c.mmsi_max))
        .select(*t5_select)
        .withColumn("_row_type", F.lit("t5"))
    )

    # ── Union → per-MMSI pandas UDF ──────────────────────────────────────────
    combined = candidates.unionByName(type5, allowMissingColumns=True)

    process_vessel = _make_vessel_processor(config)
    stops_df = combined.groupBy("mmsi").applyInPandas(
        process_vessel, schema=SPARK_STOP_SCHEMA
    )

    # ── Write ────────────────────────────────────────────────────────────────
    out_path  = path_join(config.interim_dir, "stops.parquet")
    spark_out = _to_s3a(out_path)
    logger.info("Writing stops → %s", spark_out)
    stops_df.write.mode("overwrite").parquet(spark_out)

    count = spark.read.parquet(spark_out).count()
    logger.info("Wrote %d stop events → %s", count, out_path)
    return out_path
