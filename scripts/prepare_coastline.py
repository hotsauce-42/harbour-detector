#!/usr/bin/env python3
"""
Build the land-polygon file Phase 4's offshore flag uses, from OSM data.

Phase 4 flags harbours that lie too far from land — waiting areas and
anchorages, which look like harbours to Phases 1-3. It needs land *polygons*,
not a coastline: a line cannot tell a river port from the open sea, a polygon
can (see utils/coastline.py).

Download `land-polygons-split-4326.zip` from
https://osmdata.openstreetmap.de/data/land-polygons.html — the *split* variant,
whose small tiles each carry a tight bounding box. Unsplit, Eurasia is a single
polygon whose box covers half the planet, so no region filter could skip it.

The zip is ~900 MB holding a 1.3 GB shapefile — 873k polygons, 79M vertices
for the world. This streams it once, keeps the polygons that overlap `--bbox`,
and writes them grouped by area so Phase 4 can read only the parts near its
harbours. Memory stays bounded (a spill budget plus one 10° cell) whatever the
region, so a world build works on an ordinary machine. Nothing is extracted to
disk and no GDAL is needed.

Feeds `phase4.coastline_path`; without the file the offshore flag is skipped.

Usage:
    python3 scripts/prepare_coastline.py
    python3 scripts/prepare_coastline.py --bbox -5 50 32 72
    python3 scripts/prepare_coastline.py --world \\
        --out data/reference/coastline/land_world.parquet
    python3 scripts/prepare_coastline.py --source ~/land-polygons-split-4326.zip \\
        --out data/reference/coastline/land.parquet
"""

import argparse
import sys
import time
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.coastline import read_shapefile_polygons, write_land_parquet  # noqa: E402

DEFAULT_SOURCE = Path("data/reference/coastline/land-polygons-split-4326.zip")
DEFAULT_OUT = Path("data/reference/coastline/land.parquet")
# North Sea, Baltic, Skagerrak/Kattegat and the Norwegian coast, plus margin.
DEFAULT_BBOX = (-5.0, 50.0, 32.0, 72.0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE,
                        help="land-polygons-split-4326.zip, or an extracted .shp")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--bbox", type=float, nargs=4, default=DEFAULT_BBOX,
                        metavar=("MIN_LON", "MIN_LAT", "MAX_LON", "MAX_LAT"),
                        help="region to keep (default: %(default)s)")
    parser.add_argument("--world", action="store_true",
                        help="keep everything (same as --bbox -180 -90 180 90)")
    args = parser.parse_args()

    if not args.source.exists():
        print(f"error: {args.source} not found — download "
              "land-polygons-split-4326.zip from osmdata.openstreetmap.de",
              file=sys.stderr)
        return 1

    bbox = (-180.0, -90.0, 180.0, 90.0) if args.world else tuple(args.bbox)
    start = time.time()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    if args.source.suffix.lower() == ".zip":
        with zipfile.ZipFile(args.source) as zf:
            shp = next((n for n in zf.namelist() if n.lower().endswith(".shp")),
                       None)
            if shp is None:
                print(f"error: no .shp inside {args.source}", file=sys.stderr)
                return 1
            with zf.open(shp) as stream:
                n_polygons, n_vertices = write_land_parquet(
                    read_shapefile_polygons(stream, bbox), args.out, coverage=bbox)
    else:
        with open(args.source, "rb") as stream:
            n_polygons, n_vertices = write_land_parquet(
                read_shapefile_polygons(stream, bbox), args.out, coverage=bbox)

    if not n_polygons:
        args.out.unlink(missing_ok=True)
        print(f"error: no land polygon overlaps bbox {bbox}", file=sys.stderr)
        return 1

    size_mb = args.out.stat().st_size / 1e6
    print(f"{n_polygons:,} polygons, {n_vertices:,} vertices in {bbox} "
          f"→ {args.out} ({size_mb:.1f} MB, {time.time() - start:.0f} s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
