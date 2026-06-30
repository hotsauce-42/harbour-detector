"""
Spark entrypoint — Phase 1 only (stop extraction).

Runs the distributed stop-extraction phase on Spark, writing its output to
INTERIM_DIR. Phases 2-5 run separately in a plain pod (see run_enrich.py),
sequenced by an external orchestrator. All phases communicate through S3, so
this script needs no knowledge of the downstream phases.

Config resolution and env var overrides are identical to run_pipeline.py.
"""

import logging
import sys
from pathlib import Path

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    stream=sys.stdout,
)
logger = logging.getLogger("pipeline")

_CONFIG_PATH = Path(__file__).parent / "config" / "settings.yaml"


def main() -> None:
    from pipeline.extract_stops       import Phase1Config
    from pipeline.extract_stops_spark import run_phase1
    from utils.s3    import build_s3_config
    from utils.spark import create_spark_session

    from utils.config import load_config
    cfg    = load_config(_CONFIG_PATH)
    s3_cfg = build_s3_config(cfg.get("s3", {}))

    logger.info("━━━ Phase 1: Stop extraction (Spark) ━━━")
    spark = create_spark_session(s3_cfg, app_name=cfg.get("spark", {}).get("app_name", "harbour-detector"))
    try:
        out = run_phase1(Phase1Config.from_yaml(cfg), spark)
    finally:
        spark.stop()

    logger.info("Phase 1 complete. Output: %s", out)


if __name__ == "__main__":
    main()
