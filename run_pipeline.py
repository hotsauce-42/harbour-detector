"""
Docker / Kubernetes entrypoint.

Loads config/settings.yaml (baked into the image), applies environment
variable overrides, then runs all five pipeline phases sequentially.

Environment variable overrides
-------------------------------
RAW_GLOB     → data.raw_glob          (e.g. s3://bucket/ais/**/*.parquet)
INTERIM_DIR  → data.interim_dir       (e.g. s3://bucket/harbour-detector/interim)
OUTPUT_DIR   → data.output_dir        (e.g. s3://bucket/harbour-detector/output)
EXISTING_DB  → phase5.existing_db_path (optional, local or s3:// path)

S3 credentials (picked up automatically by utils/s3.py)
---------------------------------------------------------
AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, S3_ENDPOINT_URL
"""

import logging
import os
import sys
from pathlib import Path

import yaml

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger("pipeline")

_CONFIG_PATH = Path(__file__).parent / "config" / "settings.yaml"

# Maps env var name → (yaml_section, yaml_key)
_ENV_OVERRIDES: dict[str, tuple[str, str]] = {
    "RAW_GLOB":    ("data",   "raw_glob"),
    "INTERIM_DIR": ("data",   "interim_dir"),
    "OUTPUT_DIR":  ("data",   "output_dir"),
    "EXISTING_DB": ("phase5", "existing_db_path"),
}


def _load_config() -> dict:
    with open(_CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)

    for env_key, (section, key) in _ENV_OVERRIDES.items():
        value = os.environ.get(env_key, "").strip()
        if value:
            cfg.setdefault(section, {})[key] = value
            logger.info("Config override  %s.%s = %s", section, key, value)

    return cfg


def main() -> None:
    from pipeline.extract_stops   import Phase1Config, run_phase1
    from pipeline.h3_aggregation  import Phase2Config, run_phase2
    from pipeline.cluster_formation import Phase3Config, run_phase3
    from pipeline.enrichment       import Phase4Config, run_phase4
    from pipeline.id_matching      import Phase5Config, run_phase5

    cfg = _load_config()

    logger.info("━━━ Phase 1: Stop extraction ━━━")
    run_phase1(Phase1Config.from_yaml(cfg))

    logger.info("━━━ Phase 2: H3 aggregation ━━━")
    run_phase2(Phase2Config.from_yaml(cfg))

    logger.info("━━━ Phase 3: Cluster formation ━━━")
    run_phase3(Phase3Config.from_yaml(cfg))

    logger.info("━━━ Phase 4: Enrichment ━━━")
    run_phase4(Phase4Config.from_yaml(cfg))

    logger.info("━━━ Phase 5: ID matching + export ━━━")
    parquet_path, geojson_path = run_phase5(Phase5Config.from_yaml(cfg))

    logger.info("Pipeline complete.")
    logger.info("  Parquet : %s", parquet_path)
    logger.info("  GeoJSON : %s", geojson_path)


if __name__ == "__main__":
    main()
