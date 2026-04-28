"""
harbour-detector — CLI entry point

Usage:
  python run.py phase1
  python run.py phase1 --config config/settings.yaml
  python run.py phase1 --raw-glob "data/raw/**/*.parquet"
"""

import argparse
import logging
import sys


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=level,
        stream=sys.stdout,
    )


def cmd_phase1(args, cfg: dict) -> None:
    from pipeline.extract_stops        import Phase1Config
    from pipeline.extract_stops_spark  import run_phase1
    from utils.s3    import build_s3_config
    from utils.spark import create_spark_session

    config = Phase1Config.from_yaml(cfg)
    if args.raw_glob:
        config.raw_glob = args.raw_glob

    s3_cfg = build_s3_config(cfg.get("s3", {}))
    spark  = create_spark_session(s3_cfg, app_name=cfg.get("spark", {}).get("app_name", "harbour-detector"))
    try:
        out = run_phase1(config, spark)
    finally:
        spark.stop()
    print(f"\nPhase 1 complete. Output: {out}")


def cmd_phase2(args, cfg: dict) -> None:
    from pipeline.h3_aggregation import Phase2Config, run_phase2

    config = Phase2Config.from_yaml(cfg)
    out = run_phase2(config)
    print(f"\nPhase 2 complete. Output: {out}")


def cmd_phase3(args, cfg: dict) -> None:
    from pipeline.cluster_formation import Phase3Config, run_phase3

    config = Phase3Config.from_yaml(cfg)
    out = run_phase3(config)
    print(f"\nPhase 3 complete. Output: {out}")


def cmd_phase4(args, cfg: dict) -> None:
    from pipeline.enrichment import Phase4Config, run_phase4

    config = Phase4Config.from_yaml(cfg)
    out = run_phase4(config)
    print(f"\nPhase 4 complete. Output: {out}")


def cmd_phase5(args, cfg: dict) -> None:
    from pipeline.id_matching import Phase5Config, run_phase5

    config = Phase5Config.from_yaml(cfg)
    if args.existing_db:
        config.existing_db_path = args.existing_db

    parquet_path, geojson_path = run_phase5(config)
    print(f"\nPhase 5 complete.")
    print(f"  Parquet : {parquet_path}")
    print(f"  GeoJSON : {geojson_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="harbour-detector",
        description="Detect harbours from historical AIS data",
    )
    parser.add_argument("--config", default="config/settings.yaml",
                        help="Path to settings YAML (default: config/settings.yaml)")
    parser.add_argument("--verbose", "-v", action="store_true")

    subparsers = parser.add_subparsers(dest="command", required=True)

    p1 = subparsers.add_parser("phase1", help="Extract stop events from raw Parquet files")
    p1.add_argument("--raw-glob", help="Override raw_glob from settings")

    subparsers.add_parser("phase2", help="Aggregate stops into H3 cells")
    subparsers.add_parser("phase3", help="Cluster hot H3 cells into harbour candidates")
    subparsers.add_parser("phase4", help="Enrich clusters with polygon, country, and city")

    p5 = subparsers.add_parser("phase5", help="Assign harbour IDs and write GeoJSON output")
    p5.add_argument("--existing-db",
                    help="Path to existing harbour database (.geojson or .parquet) for ID matching")

    args = parser.parse_args()
    _setup_logging(args.verbose)

    from utils.config import load_config
    cfg = load_config(args.config)

    if args.command == "phase1":
        cmd_phase1(args, cfg)
    elif args.command == "phase2":
        cmd_phase2(args, cfg)
    elif args.command == "phase3":
        cmd_phase3(args, cfg)
    elif args.command == "phase4":
        cmd_phase4(args, cfg)
    elif args.command == "phase5":
        cmd_phase5(args, cfg)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
