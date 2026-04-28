"""
SparkSession factory with S3A / MinIO configuration.

In K8s cluster mode (Spark Operator), the operator injects master URL and
sparkConf before the driver starts — getOrCreate() picks them up automatically.
Outside K8s (local dev), this function sets local[*] and applies S3A settings
from s3_cfg so the session can reach MinIO/S3.
"""

import logging
import os

logger = logging.getLogger(__name__)


def create_spark_session(s3_cfg: dict, app_name: str = "harbour-detector"):
    """Return a configured SparkSession for the current environment."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)

    # KUBERNETES_SERVICE_HOST is always set inside a K8s pod.
    # Outside K8s we run in local mode and configure S3A ourselves.
    if not os.environ.get("KUBERNETES_SERVICE_HOST"):
        builder = builder.master("local[*]")
        builder = _apply_s3a_conf(builder, s3_cfg)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession ready  master=%s", spark.sparkContext.master)
    return spark


def _apply_s3a_conf(builder, s3_cfg: dict):
    """Apply Hadoop S3A settings for local-mode development."""
    builder = builder.config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )
    if s3_cfg.get("key"):
        builder = builder.config("spark.hadoop.fs.s3a.access.key", s3_cfg["key"])
    if s3_cfg.get("secret"):
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", s3_cfg["secret"])
    if s3_cfg.get("endpoint_url"):
        ep = s3_cfg["endpoint_url"].rstrip("/")
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", ep)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                "true" if ep.startswith("https") else "false",
            )
        )
    if s3_cfg.get("region"):
        builder = builder.config("spark.hadoop.fs.s3a.endpoint.region", s3_cfg["region"])
    return builder
