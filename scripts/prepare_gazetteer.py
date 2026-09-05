#!/usr/bin/env python3
"""
Build the nearest-place gazetteer Phase 4 uses, from a GeoNames dump.

`reverse_geocoder` bundles GeoNames cities1000 — population > 1000 only — so
the village a small harbour is actually named after is not in it and the lookup
returns a town tens of kilometres away. This turns a GeoNames dump into a
compact Parquet file of populated places, keeping only the columns Phase 4
needs and sorting by latitude so a regional query reads a few row groups
instead of the whole file.

Feeds `phase4.gazetteer_path`; without it Phase 4 falls back to the bundled
cities1000 dataset.

Usage:
    python3 scripts/prepare_gazetteer.py                       # global, downloads
    python3 scripts/prepare_gazetteer.py --countries DK SE DE NO PL
    python3 scripts/prepare_gazetteer.py --source ~/allCountries.zip   # offline
    python3 scripts/prepare_gazetteer.py --out data/reference/geonames/places.parquet
"""

import argparse
import csv
import io
import sys
import time
import zipfile
from pathlib import Path
from urllib.request import urlopen

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.gazetteer import DEFUNCT_CODES, GAZETTEER_SCHEMA  # noqa: E402

GN_URL = "https://download.geonames.org/export/dump/"
DEFAULT_OUT = Path("data/reference/geonames/places.parquet")

# GeoNames "main" table columns we care about (tab-separated, no header).
COL_NAME, COL_LAT, COL_LON = 1, 4, 5
COL_FEATURE_CLASS, COL_FEATURE_CODE, COL_CC = 6, 7, 8
COL_ADMIN1_CODE, COL_POPULATION = 10, 14
COL_ADMIN2_CODE, COL_ADMIN3_CODE, COL_ADMIN4_CODE = 11, 12, 13

# Feature class P is "city, village, ..." — every populated place, which is the
# whole point. A, H, T, ... are regions, water bodies and landforms.
#
# Only the defunct codes are dropped here. Unpopulated city districts are *kept*
# even though `Gazetteer` will not name a harbour after one: they carry the
# administrative codes that say which municipality a stretch of water belongs
# to, and they are often the only row that does. The harbour at Kiel-Holtenau
# reaches Kiel through the district "Holtenau"; the nearest place that survives
# filtering, Knoop, is in another Kreis entirely. Gazetteer applies the rest of
# the filtering on load, where the districts have already served their purpose.
FEATURE_CLASS = "P"

BATCH_ROWS = 250_000
ROW_GROUP_ROWS = 100_000


def _download(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"  downloading {url}")
    with urlopen(url, timeout=300) as response:  # noqa: S310 — fixed GeoNames URL
        dest.write_bytes(response.read())
    print(f"  {dest.name}: {dest.stat().st_size / 1e6:.0f} MB")
    return dest


def _admin1_names(search_dirs: list[Path], download: bool) -> dict[str, str]:
    """
    'DK.17' → 'Region Sjaelland'.

    The admin1 code in the main table is opaque; Phase 4 stores a name, so it
    is resolved here rather than at query time. Fetched separately from the main
    dump — including when that dump came from `--source`, since it is a few
    hundred kB and leaving it out silently empties the column.
    """
    path = next((d / "admin1CodesASCII.txt" for d in search_dirs
                 if (d / "admin1CodesASCII.txt").is_file()), None)
    if path is None:
        if not download:
            print("  no admin1CodesASCII.txt and downloads are off — "
                  "admin1 will be left empty")
            return {}
        path = search_dirs[-1] / "admin1CodesASCII.txt"
        try:
            _download(GN_URL + "admin1CodesASCII.txt", path)
        except OSError as exc:
            print(f"  could not fetch admin1CodesASCII.txt ({exc}) — "
                  "admin1 will be left empty")
            return {}

    names = {}
    with open(path, encoding="utf-8") as fh:
        for row in csv.reader(fh, delimiter="\t"):
            if len(row) >= 2:
                names[row[0]] = row[1]
    return names


def _open_dump(source: Path):
    """Line iterator over a GeoNames dump, given either the .zip or the .txt."""
    if source.suffix == ".zip":
        archive = zipfile.ZipFile(source)
        member = next(n for n in archive.namelist() if n.endswith(".txt"))
        return io.TextIOWrapper(archive.open(member), encoding="utf-8")
    return open(source, encoding="utf-8")


def _rows(source: Path, countries: set[str], admin1: dict[str, str]):
    """Yield one record dict per populated place in the dump."""
    with _open_dump(source) as fh:
        for line in fh:
            row = line.rstrip("\n").split("\t")
            if len(row) <= COL_POPULATION or row[COL_FEATURE_CLASS] != FEATURE_CLASS:
                continue
            cc = row[COL_CC]
            if countries and cc not in countries:
                continue
            try:
                lat, lon = float(row[COL_LAT]), float(row[COL_LON])
                population = int(row[COL_POPULATION] or 0)
            except ValueError:
                continue
            if row[COL_FEATURE_CODE] in DEFUNCT_CODES:
                continue
            yield {
                "name":         row[COL_NAME],
                "lat":          lat,
                "lon":          lon,
                "population":   population,
                "feature_code": row[COL_FEATURE_CODE],
                "cc":           cc,
                "admin1":       admin1.get(f"{cc}.{row[COL_ADMIN1_CODE]}", ""),
                # Raw codes, not names: they are only ever compared with each
                # other, to ask whether two places share a municipality.
                "admin2":       row[COL_ADMIN2_CODE],
                "admin3":       row[COL_ADMIN3_CODE],
                "admin4":       row[COL_ADMIN4_CODE],
            }


def _to_table(batch: list[dict]) -> pa.Table:
    columns = {
        field.name: pa.array([r[field.name] for r in batch], type=field.type)
        for field in GAZETTEER_SCHEMA
    }
    return pa.table(columns, schema=GAZETTEER_SCHEMA)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path,
                        help="local allCountries.zip / .txt (default: download)")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT,
                        help=f"output Parquet file (default: {DEFAULT_OUT})")
    parser.add_argument("--countries", nargs="*", default=[], metavar="CC",
                        help="ISO2 codes to keep (default: every country)")
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_OUT.parent,
                        help="where downloads are cached")
    parser.add_argument("--no-download", action="store_true",
                        help="never reach the network; --source is then required")
    args = parser.parse_args()

    started = time.time()
    source = args.source
    if source is None:
        if args.no_download:
            print("--no-download needs --source pointing at a GeoNames dump.")
            return 1
        source = _download(GN_URL + "allCountries.zip",
                           args.work_dir / "allCountries.zip")
    if not source.is_file():
        print(f"Source not found: {source}")
        return 1

    countries = {c.upper() for c in args.countries}
    admin1 = _admin1_names([source.parent, args.work_dir],
                           download=not args.no_download)

    print(f"Reading {source.name}"
          + (f" (countries: {', '.join(sorted(countries))})" if countries else ""))

    tables, batch, total = [], [], 0
    for record in _rows(source, countries, admin1):
        batch.append(record)
        if len(batch) >= BATCH_ROWS:
            tables.append(_to_table(batch))
            total += len(batch)
            batch = []
            print(f"  {total:,} places …")
    if batch:
        tables.append(_to_table(batch))
        total += len(batch)

    if not total:
        print("No populated places matched — nothing written.")
        return 1

    # Sorted by latitude so Phase 4's regional filter can skip whole row groups
    # on their statistics instead of reading the file end to end.
    table = pa.concat_tables(tables).sort_by("lat")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, args.out, compression="zstd",
                   row_group_size=ROW_GROUP_ROWS)

    size_mb = args.out.stat().st_size / 1e6
    print(f"\nWrote {total:,} places to {args.out} "
          f"({size_mb:.0f} MB) in {time.time() - started:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
