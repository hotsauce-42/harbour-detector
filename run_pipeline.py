"""
Docker / Kubernetes entrypoint.

Loads config/settings.yaml (baked into the image), then applies overrides.

Priority (highest wins): env vars → .env file → settings.yaml

Any YAML key can be overridden via SECTION__KEY env vars, e.g.:
  DATA__RAW_GLOB, DATA__INTERIM_DIR, DATA__OUTPUT_DIR
  PHASE1__SOG_THRESHOLD_KNOTS, PHASE3__CLUSTER_RING_SIZE, …
  S3__ENDPOINT_URL

Legacy flat vars also still work: RAW_GLOB, INTERIM_DIR, OUTPUT_DIR, EXISTING_DB
S3 credentials: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, S3_ENDPOINT_URL
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
    from pipeline.extract_stops        import Phase1Config
    from pipeline.extract_stops_spark  import run_phase1
    from pipeline.h3_aggregation       import Phase2Config, run_phase2
    from pipeline.cluster_formation    import Phase3Config, run_phase3
    from pipeline.enrichment           import Phase4Config, run_phase4
    from pipeline.id_matching          import Phase5Config, run_phase5
    from utils.s3    import build_s3_config
    from utils.spark import create_spark_session

    from utils.config import load_config
    cfg    = load_config(_CONFIG_PATH)
    s3_cfg = build_s3_config(cfg.get("s3", {}))

    logger.info("━━━ Phase 1: Stop extraction (Spark) ━━━")
    spark = create_spark_session(s3_cfg, app_name=cfg.get("spark", {}).get("app_name", "harbour-detector"))
    try:
        run_phase1(Phase1Config.from_yaml(cfg), spark)
    finally:
        spark.stop()

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
