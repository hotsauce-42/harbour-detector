"""
Local-mode SparkSession sizing.

None of this needs a JVM: the point is what gets configured *before* one is
launched, which is exactly the part that is easy to get silently wrong.
"""

import os

import pytest

from utils.spark import (
    DEFAULT_LOCAL_CONF,
    _apply_local_conf,
    set_driver_memory,
)


class FakeBuilder:
    """Records what create_spark_session would have asked Spark for."""

    def __init__(self):
        self.master_url = None
        self.conf = {}

    def master(self, url):
        self.master_url = url
        return self

    def config(self, key, value):
        self.conf[key] = value
        return self


@pytest.fixture(autouse=True)
def clean_submit_args(monkeypatch):
    monkeypatch.delenv("PYSPARK_SUBMIT_ARGS", raising=False)


# ── Driver memory ──────────────────────────────────────────────────────────

def test_driver_memory_goes_through_the_submit_args_env_var():
    """
    The one that bites. In local mode the driver JVM is the process PySpark
    launched, and it is launched from PYSPARK_SUBMIT_ARGS — so a
    `builder.config("spark.driver.memory", ...)` is accepted and ignored, and
    the heap stays at Spark's 1 GB default.
    """
    set_driver_memory("3g")
    assert os.environ["PYSPARK_SUBMIT_ARGS"] == "--driver-memory 3g pyspark-shell"


def test_an_existing_submit_args_is_left_alone(monkeypatch):
    """Whoever set it knows more about the environment than this default."""
    monkeypatch.setenv("PYSPARK_SUBMIT_ARGS", "--driver-memory 16g pyspark-shell")
    set_driver_memory("3g")
    assert "16g" in os.environ["PYSPARK_SUBMIT_ARGS"]


# ── Local sizing ───────────────────────────────────────────────────────────

def test_local_mode_caps_the_thread_count():
    """
    `local[*]` gives one task slot per core, and every slot runs a Python
    worker holding its own pandas frame — 8 copies at once on an 8-core laptop.
    """
    builder = _apply_local_conf(FakeBuilder(), {})
    assert builder.master_url == f"local[{DEFAULT_LOCAL_CONF['local_threads']}]"
    assert "*" not in builder.master_url


def test_config_section_overrides_the_defaults():
    builder = _apply_local_conf(FakeBuilder(), {
        "local_threads": 6,
        "local_driver_memory": "10g",
        "local_shuffle_partitions": 200,
    })
    assert builder.master_url == "local[6]"
    assert builder.conf["spark.sql.shuffle.partitions"] == "200"
    assert "10g" in os.environ["PYSPARK_SUBMIT_ARGS"]


def test_unrelated_config_keys_are_ignored():
    """`spark:` also holds app_name, which is not a session property."""
    builder = _apply_local_conf(FakeBuilder(), {"app_name": "harbour-detector"})
    assert builder.master_url == f"local[{DEFAULT_LOCAL_CONF['local_threads']}]"


def test_the_memory_relevant_properties_are_all_set():
    builder = _apply_local_conf(FakeBuilder(), {})
    for key in ("spark.sql.shuffle.partitions",
                "spark.sql.files.maxPartitionBytes",
                "spark.sql.execution.arrow.maxRecordsPerBatch"):
        assert key in builder.conf, key


def test_every_default_is_a_string_spark_will_accept():
    """Spark config values must be strings; ints from YAML have to be coerced."""
    builder = _apply_local_conf(FakeBuilder(), {"local_shuffle_partitions": 64})
    assert all(isinstance(v, str) for v in builder.conf.values())
