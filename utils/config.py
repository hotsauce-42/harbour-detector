"""
Shared configuration loader.

Priority (highest wins):
  1. Environment variables
  2. .env file  (loaded with override=False so real env vars always beat it)
  3. config/settings.yaml  (baked-in defaults)

All YAML keys are overridable via the SECTION__KEY env var convention:
  SECTION__KEY=value  →  cfg[section][key] = value  (types are inferred from YAML)

Examples:
  DATA__RAW_GLOB=s3://bucket/ais/**/*.parquet
  DATA__INTERIM_DIR=s3://bucket/interim
  PHASE1__SOG_THRESHOLD_KNOTS=0.3
  PHASE1__MOORED_NAV_STATUSES=1,5
  PHASE2__MIN_UNIQUE_MMSI=3
  PHASE3__CLUSTER_RING_SIZE=5
  S3__ENDPOINT_URL=http://minio.minio.svc.cluster.local:9000
  SPARK__APP_NAME=harbour-detector-prod

Legacy flat env vars are also still honoured for backwards compatibility:
  RAW_GLOB, INTERIM_DIR, OUTPUT_DIR, EXISTING_DB
"""

import logging
import os
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

_DOTENV_LOADED = False

# Flat legacy overrides kept for backwards compatibility.
_LEGACY: dict[str, tuple[str, str]] = {
    "RAW_GLOB":    ("data",   "raw_glob"),
    "INTERIM_DIR": ("data",   "interim_dir"),
    "OUTPUT_DIR":  ("data",   "output_dir"),
    "EXISTING_DB": ("phase5", "existing_db_path"),
}


def load_config(config_path: str | Path) -> dict:
    """Load settings.yaml and apply env var overrides."""
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    _ensure_dotenv()
    _apply_section_keys(cfg)
    _apply_legacy(cfg)
    return cfg


def _ensure_dotenv() -> None:
    """Load .env once. override=False means real env vars always beat .env values."""
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    try:
        from dotenv import load_dotenv
        load_dotenv(override=False)
    except ImportError:
        pass
    _DOTENV_LOADED = True


def _apply_section_keys(cfg: dict) -> None:
    """Apply SECTION__KEY env vars to the nested config dict."""
    for raw_key, raw_val in os.environ.items():
        if "__" not in raw_key or not raw_val.strip():
            continue
        section, _, key = raw_key.lower().partition("__")
        if section not in cfg or not isinstance(cfg[section], dict):
            continue
        coerced = _coerce(raw_val.strip(), cfg[section].get(key))
        if coerced == cfg[section].get(key):
            continue
        cfg[section][key] = coerced
        logger.info("Config override  %s.%s = %r  (from %s)", section, key, coerced, raw_key)


def _apply_legacy(cfg: dict) -> None:
    """Apply the flat legacy env vars (RAW_GLOB, INTERIM_DIR, …)."""
    for env_key, (section, key) in _LEGACY.items():
        value = os.environ.get(env_key, "").strip()
        if not value:
            continue
        cfg.setdefault(section, {})[key] = value
        logger.info("Config override  %s.%s = %r  (from %s)", section, key, value, env_key)


def _coerce(value: str, existing):
    """
    Cast the string env var value to match the type of the existing YAML entry.
    bool must be checked before int because bool is a subclass of int in Python.
    """
    if existing is None:
        return value
    if isinstance(existing, bool):
        return value.lower() in ("true", "1", "yes")
    if isinstance(existing, int):
        try:
            return int(value)
        except ValueError:
            return value
    if isinstance(existing, float):
        try:
            return float(value)
        except ValueError:
            return value
    if isinstance(existing, list):
        item_hint = existing[0] if existing else None
        return [_coerce(v.strip(), item_hint) for v in value.split(",")]
    return value
