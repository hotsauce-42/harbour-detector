"""
Enrichment entrypoint — Phases 2-5 (no Spark).

Runs H3 aggregation, cluster formation, enrichment, and ID matching/export.
These phases are plain pandas / shapely / DuckDB code and need no Spark
runtime, so they run in a small standalone pod rather than on the Spark driver.

Input is read from INTERIM_DIR (written by Phase 1, run_phase1.py); the final
outputs are written to OUTPUT_DIR. Run this only after Phase 1 has completed
successfully — an external orchestrator is responsible for that ordering.

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
    from pipeline.h3_aggregation    import Phase2Config, run_phase2
    from pipeline.cluster_formation import Phase3Config, run_phase3
    from pipeline.enrichment        import Phase4Config, run_phase4
    from pipeline.id_matching       import Phase5Config, run_phase5

    from utils.config import load_config
    cfg = load_config(_CONFIG_PATH)

    logger.info("━━━ Phase 2: H3 aggregation ━━━")
    run_phase2(Phase2Config.from_yaml(cfg))

    logger.info("━━━ Phase 3: Cluster formation ━━━")
    run_phase3(Phase3Config.from_yaml(cfg))

    logger.info("━━━ Phase 4: Enrichment ━━━")
    run_phase4(Phase4Config.from_yaml(cfg))

    logger.info("━━━ Phase 5: ID matching + export ━━━")
    parquet_path, geojson_path, cells_path = run_phase5(Phase5Config.from_yaml(cfg))

    logger.info("Enrichment complete.")
    logger.info("  Parquet : %s", parquet_path)
    logger.info("  Outline : %s", geojson_path)
    logger.info("  Cells   : %s", cells_path)


if __name__ == "__main__":
    main()
