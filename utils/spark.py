"""
SparkSession factory with S3A / MinIO configuration.

In K8s cluster mode (Spark Operator), the operator injects master URL and
sparkConf before the driver starts — getOrCreate() picks them up automatically.
Outside K8s (local dev), this function sizes the session for a workstation and
applies S3A settings from s3_cfg so it can reach MinIO/S3. Those local defaults
(DEFAULT_LOCAL_CONF, overridable from the `spark:` config section) exist because
Spark's own defaults assume a cluster: one thread per core each running a Python
worker, against a 1 GB driver heap that in local mode is the whole JVM.
"""

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


DEFAULT_LOCAL_CONF = {
    # One Python worker per thread, each holding a pandas frame and its Arrow
    # buffers, so `local[*]` on an 8-core laptop means 8 copies at once.
    "local_threads":         2,
    "local_driver_memory":   "3g",
    "local_shuffle_partitions": 64,
    "local_arrow_batch_size":   5000,
    "local_max_partition_bytes": "32m",
}


def create_spark_session(s3_cfg: dict, app_name: str = "harbour-detector",
                         spark_cfg: Optional[dict] = None):
    """Return a configured SparkSession for the current environment."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)

    # KUBERNETES_SERVICE_HOST is always set inside a K8s pod.
    # Outside K8s we run in local mode and configure S3A ourselves.
    if not os.environ.get("KUBERNETES_SERVICE_HOST"):
        builder = _apply_local_conf(builder, spark_cfg or {})
        builder = _apply_s3a_conf(builder, s3_cfg)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession ready  master=%s", spark.sparkContext.master)
    return spark


def set_driver_memory(memory: str) -> None:
    """
    Raise the local driver's JVM heap, which is Spark's default 1 GB otherwise.

    This cannot go through `builder.config("spark.driver.memory", …)`: in local
    and client mode the driver JVM is already the process PySpark launched, and
    it is launched from PYSPARK_SUBMIT_ARGS (`pyspark/java_gateway.py`). Setting
    the property on the builder is accepted and silently ignored, leaving the
    heap at 1 GB — which in local mode is also the executor heap.

    An existing PYSPARK_SUBMIT_ARGS is left alone: whoever set it knows more
    about the environment than this default does.
    """
    if os.environ.get("PYSPARK_SUBMIT_ARGS"):
        logger.info("PYSPARK_SUBMIT_ARGS already set — leaving driver memory alone")
        return
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-memory {memory} pyspark-shell"


def _apply_local_conf(builder, spark_cfg: dict):
    """
    Size a local-mode session for a workstation.

    Only reached outside Kubernetes; under the Spark Operator these come from
    `deploy/spark_job.yaml` sparkConf and this branch never runs.
    """
    conf = {**DEFAULT_LOCAL_CONF, **{k: v for k, v in spark_cfg.items()
                                     if k in DEFAULT_LOCAL_CONF}}
    set_driver_memory(str(conf["local_driver_memory"]))

    logger.info("Local Spark: %s thread(s), %s driver heap",
                conf["local_threads"], conf["local_driver_memory"])
    return (
        builder
        .master(f"local[{conf['local_threads']}]")
        # groupBy(mmsi) shuffles; fewer, larger partitions cost memory, and 200
        # is Spark's cluster-sized default.
        .config("spark.sql.shuffle.partitions", str(conf["local_shuffle_partitions"]))
        # Smaller read splits → smaller task working sets.
        .config("spark.sql.files.maxPartitionBytes",
                str(conf["local_max_partition_bytes"]))
        # Rows per Arrow batch handed to a Python worker.
        .config("spark.sql.execution.arrow.maxRecordsPerBatch",
                str(conf["local_arrow_batch_size"]))
    )


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
        builder = builder.config("spark.hadoop.fs.s3a.endpoint.region",
                                 s3_cfg["region"])
    return builder
