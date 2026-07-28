"""
Convert a Danish Maritime Authority (DMA) AIS CSV dump into the Parquet layout
this pipeline expects, split into fixed-length time slices.

The DMA files (https://web.ais.dk/aisdata/, "aisdk-YYYY-MM-DD.zip") are a single
flat CSV where every enum is a human-readable string and every row carries both
position and voyage fields. Phase 1 wants integer AIS codes, epoch-second
timestamps and a `msg_type` column, so this script translates:

  # Timestamp            → timestamp   (int64, Unix epoch SECONDS — see CLAUDE.md)
  MMSI                   → mmsi        (int64)
  Latitude / Longitude   → lat / lon   (float64)
  SOG                    → sog         (float32)
  Navigational status    → nav_status  (int16, text → AIS code, 15 = undefined)
  Ship type              → ship_type   (int16, text → AIS code, null if undefined)
  Draught                → draught     (float32)
  Destination            → destination (string, "Unknown" → null)
  (synthesised)          → msg_type    (int16, 5 for rows carrying voyage data)

`msg_type` does not exist in the DMA export: it merges static/voyage reports into
the position stream. A row is emitted as type 5 when it actually carries voyage
data (draught, destination or a defined ship type) — that is exactly the set
Phase 1's type-5 scan needs. Those rows still qualify as position candidates,
since the candidate filter never looks at `msg_type`.

Usage:
    python scripts/convert_aisdk_csv.py /mnt/c/Users/vze/Downloads/aisdk-2026-07-21.zip
    python scripts/convert_aisdk_csv.py <zip> --out-dir data/raw --slice-minutes 10
    python scripts/convert_aisdk_csv.py <zip> --limit-rows 2000000   # quick smoke test
"""

import argparse
import gzip
import sys
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Source CSV columns we actually read — everything else is skipped by the parser.
SRC_COLUMNS = [
    "# Timestamp",
    "Type of mobile",
    "MMSI",
    "Latitude",
    "Longitude",
    "Navigational status",
    "SOG",
    "Ship type",
    "Draught",
    "Destination",
]

TIMESTAMP_FORMAT = "%d/%m/%Y %H:%M:%S"
EPOCH = pd.Timestamp("1970-01-01", tz="UTC")

# Output schema — column names match `columns:` in config/settings.yaml.
OUT_SCHEMA = pa.schema([
    pa.field("mmsi",        pa.int64()),
    pa.field("timestamp",   pa.int64()),    # Unix epoch seconds, UTC
    pa.field("lat",         pa.float64()),
    pa.field("lon",         pa.float64()),
    pa.field("sog",         pa.float32()),
    pa.field("nav_status",  pa.int16()),
    pa.field("msg_type",    pa.int16()),
    pa.field("draught",     pa.float32()),
    pa.field("destination", pa.string()),
    pa.field("ship_type",   pa.int16()),
])

# DMA "Navigational status" text → ITU-R M.1371 code. 15 = undefined.
NAV_STATUS_CODES = {
    "Under way using engine":                                0,
    "At anchor":                                             1,
    "Not under command":                                     2,
    "Restricted maneuverability":                            3,
    "Constrained by her draught":                            4,
    "Moored":                                                5,
    "Aground":                                               6,
    "Engaged in fishing":                                    7,
    "Under way sailing":                                     8,
    "Reserved for future amendment [HSC]":                   9,
    "Reserved for future amendment [WIG]":                  10,
    "Power-driven vessel towing astern":                    11,
    "Power-driven vessel pushing ahead or towing alongside": 12,
    "Reserved for future use":                              13,
    "AIS-SART":                                             14,
    "Unknown value":                                        15,
}
NAV_STATUS_UNDEFINED = 15

# DMA "Ship type" text → AIS ship-type code. The DMA export drops the cargo
# sub-digit (71-79, 81-89 …), so the base code of each range is used; that is
# enough for _classify_ship_type() in pipeline/h3_aggregation.py.
# "Undefined"/"Reserved" deliberately have no entry → null, which Phase 1 skips.
SHIP_TYPE_CODES = {
    "WIG":                   20,
    "Fishing":               30,
    "Towing":                31,
    "Towing long/wide":      32,
    "Dredging":              33,
    "Diving":                34,
    "Military":              35,
    "Sailing":               36,
    "Pleasure":              37,
    "HSC":                   40,
    "Pilot":                 50,
    "SAR":                   51,
    "Tug":                   52,
    "Port tender":           53,
    "Anti-pollution":        54,
    "Law enforcement":       55,
    "Spare 1":               56,
    "Spare 2":               57,
    "Medical":               58,
    "Not party to conflict": 59,
    "Passenger":             60,
    "Cargo":                 70,
    "Tanker":                80,
    "Other":                 90,
}

# Only these carry vessel movements. Base stations and AtoNs are stationary
# transmitters and would otherwise show up as permanently "moored" harbours.
DEFAULT_MOBILE_TYPES = ("Class A", "Class B")

# Placeholder destinations that carry no information.
NULL_DESTINATIONS = {"", "Unknown", "UNKNOWN", "unknown"}

# Fallback msg_type per mobile class for rows without voyage data.
POSITION_MSG_TYPE = {"Class A": 1, "Class B": 18}


class SliceWriter:
    """
    Fans chunks out to one Parquet file per time slice.

    Keeps a bounded number of Parquet writers open at once. DMA dumps are
    chronologically sorted, so evicting the lowest slice key is effectively free;
    should a slice reappear after eviction it is reopened as a new `_pN` part
    file rather than overwriting the earlier one.
    """

    def __init__(self, out_dir: Path, max_open: int = 64):
        self.out_dir = out_dir
        self.max_open = max_open
        self._writers: dict[str, pq.ParquetWriter] = {}
        self._parts: dict[str, int] = {}
        self.rows_per_slice: dict[str, int] = {}
        self.files: list[Path] = []

    def _path_for(self, slice_key: str) -> Path:
        day = slice_key[:10]                       # YYYY-MM-DD
        part = self._parts.get(slice_key, 0)
        suffix = f"_p{part}" if part else ""
        return self.out_dir / day / f"ais_{slice_key}{suffix}.parquet"

    def _writer_for(self, slice_key: str) -> pq.ParquetWriter:
        writer = self._writers.get(slice_key)
        if writer is not None:
            return writer

        if len(self._writers) >= self.max_open:
            oldest = min(self._writers)
            self._writers.pop(oldest).close()

        path = self._path_for(slice_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        writer = pq.ParquetWriter(path, OUT_SCHEMA, compression="snappy")
        self._writers[slice_key] = writer
        self.files.append(path)
        # Next time this slice is opened it must not clobber the file just made.
        self._parts[slice_key] = self._parts.get(slice_key, 0) + 1
        return writer

    def write(self, slice_key: str, df: pd.DataFrame) -> None:
        table = pa.Table.from_pandas(df, schema=OUT_SCHEMA, preserve_index=False)
        self._writer_for(slice_key).write_table(table)
        self.rows_per_slice[slice_key] = self.rows_per_slice.get(slice_key, 0) + len(df)

    def close(self) -> None:
        for writer in self._writers.values():
            writer.close()
        self._writers.clear()


def open_csv_stream(path: Path):
    """Open the AIS CSV for reading, transparently handling .zip / .gz / plain."""
    if path.suffix == ".zip":
        archive = zipfile.ZipFile(path)
        members = [n for n in archive.namelist() if n.lower().endswith(".csv")]
        if not members:
            raise SystemExit(f"No .csv entry found inside {path}")
        if len(members) > 1:
            print(f"  archive holds {len(members)} CSVs, using {members[0]}")
        return archive.open(members[0])
    if path.suffix == ".gz":
        return gzip.open(path, "rb")
    return path.open("rb")


def transform(
    chunk: pd.DataFrame,
    mobile_types: tuple[str, ...] | None,
    slice_minutes: int,
) -> pd.DataFrame:
    """
    Translate one raw CSV chunk into the pipeline's Parquet schema.

    Returns the output columns plus a `_slice` key naming the time slice each
    row belongs to. `mobile_types` of None keeps every mobile class.
    """
    empty = pd.DataFrame(columns=[*OUT_SCHEMA.names, "_slice"])

    if mobile_types is not None:
        chunk = chunk[chunk["Type of mobile"].isin(mobile_types)]
    if chunk.empty:
        return empty

    ts = pd.to_datetime(
        chunk["# Timestamp"], format=TIMESTAMP_FORMAT, utc=True, errors="coerce"
    )
    mmsi = pd.to_numeric(chunk["MMSI"], errors="coerce")
    lat = pd.to_numeric(chunk["Latitude"], errors="coerce")
    lon = pd.to_numeric(chunk["Longitude"], errors="coerce")

    # DMA encodes "position unavailable" as lat 91 / lon 181.
    keep = (
        ts.notna() & mmsi.notna()
        & lat.between(-90, 90) & lon.between(-180, 180)
    )
    if not keep.any():
        return empty

    chunk, ts = chunk[keep], ts[keep]

    draught = pd.to_numeric(chunk["Draught"], errors="coerce")
    ship_type = chunk["Ship type"].map(SHIP_TYPE_CODES)
    destination = chunk["Destination"].where(
        ~chunk["Destination"].isin(NULL_DESTINATIONS)
    )

    # Voyage-bearing rows stand in for AIS message type 5 (see module docstring).
    is_static = draught.notna() | ship_type.notna() | destination.notna()
    msg_type = chunk["Type of mobile"].map(POSITION_MSG_TYPE).fillna(1)

    out = pd.DataFrame({
        "mmsi": mmsi[keep].astype("int64"),
        # Epoch SECONDS — Phase 1 reads this with pd.to_datetime(unit='s').
        # Divide through Timedelta rather than assuming a datetime64 resolution:
        # pandas 2 parses to nanoseconds, pandas 3 to microseconds.
        "timestamp": ((ts - EPOCH) // pd.Timedelta(1, "s")).astype("int64"),
        "lat": lat[keep].astype("float64"),
        "lon": lon[keep].astype("float64"),
        "sog": pd.to_numeric(chunk["SOG"], errors="coerce").astype("float32"),
        "nav_status": chunk["Navigational status"]
            .map(NAV_STATUS_CODES).fillna(NAV_STATUS_UNDEFINED).astype("int16"),
        "msg_type": msg_type.where(~is_static, 5).astype("int16"),
        "draught": draught.astype("float32"),
        "destination": destination.astype("string"),
        "ship_type": ship_type.astype("Float64").astype("Int16"),
    })
    out["_slice"] = (
        ts.dt.floor(f"{slice_minutes}min").dt.strftime("%Y-%m-%dT%H%M")
    )
    return out


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("source", type=Path,
                   help="DMA AIS dump (.zip, .csv or .csv.gz)")
    p.add_argument("--out-dir", type=Path, default=Path("data/raw"),
                   help="output root; files land in <out-dir>/<YYYY-MM-DD>/ "
                        "(default: data/raw)")
    p.add_argument("--slice-minutes", type=int, default=10,
                   help="length of each time slice in minutes (default: 10)")
    p.add_argument("--chunk-rows", type=int, default=2_000_000,
                   help="CSV rows parsed per batch (default: 2000000)")
    p.add_argument("--limit-rows", type=int, default=None,
                   help="stop after this many source rows — for quick trials")
    p.add_argument("--mobile-types", default=",".join(DEFAULT_MOBILE_TYPES),
                   help="comma-separated 'Type of mobile' values to keep "
                        f"(default: {','.join(DEFAULT_MOBILE_TYPES)}); "
                        "pass 'all' to disable the filter")
    p.add_argument("--max-open-files", type=int, default=64,
                   help="Parquet writers held open at once (default: 64)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])

    if not args.source.exists():
        raise SystemExit(f"Source not found: {args.source}")
    if args.slice_minutes < 1:
        raise SystemExit("--slice-minutes must be >= 1")

    mobile_types = (
        tuple(t.strip() for t in args.mobile_types.split(",") if t.strip())
        if args.mobile_types.lower() != "all"
        else None
    )

    print(f"Reading  {args.source}")
    print(f"Writing  {args.out_dir}/<date>/  ({args.slice_minutes}-minute slices)")

    writer = SliceWriter(args.out_dir, max_open=args.max_open_files)
    read_rows = written_rows = 0

    try:
        with open_csv_stream(args.source) as stream:
            reader = pd.read_csv(
                stream,
                usecols=SRC_COLUMNS,
                dtype=str,
                chunksize=args.chunk_rows,
                encoding="utf-8",
                encoding_errors="replace",
                on_bad_lines="warn",
            )
            for chunk in reader:
                read_rows += len(chunk)

                out = transform(chunk, mobile_types, args.slice_minutes)
                if not out.empty:
                    for slice_key, group in out.groupby("_slice", sort=False):
                        writer.write(slice_key, group.drop(columns="_slice"))
                    written_rows += len(out)

                print(f"  {read_rows:>12,} read → {written_rows:>12,} kept "
                      f"({len(writer.rows_per_slice)} slices)", flush=True)

                if args.limit_rows and read_rows >= args.limit_rows:
                    print("  --limit-rows reached, stopping early")
                    break
    finally:
        writer.close()

    if not writer.rows_per_slice:
        print("No rows written — check --mobile-types and the source file.")
        return 1

    slices = sorted(writer.rows_per_slice)
    counts = writer.rows_per_slice
    print()
    print(f"Rows read    : {read_rows:,}")
    print(f"Rows written : {written_rows:,}")
    print(f"Slices       : {len(slices)}  ({slices[0]} … {slices[-1]})")
    print(f"Files        : {len(writer.files)}")
    print(f"Rows/slice   : min {min(counts.values()):,}  "
          f"max {max(counts.values()):,}  "
          f"avg {written_rows // len(slices):,}")
    print()
    print("Point the pipeline at the result with:")
    print(f'  python run.py phase1 --raw-glob "{args.out_dir}/**/*.parquet"')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
