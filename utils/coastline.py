"""
Distance from a harbour to the nearest land, for Phase 4's offshore flag.

Waiting areas and anchorages produce the same signature as a harbour — vessels
stop and stay — but they lie out on the water, while a harbour sits at a quay.
The test is therefore "how far is this site from land": a quay touches the land
polygon (0 km), and so does a river or canal port, which lies *inside* it.

Land, not a coastline. A coastline is only a line, so a distance to it cannot
say which side of it a site is on, and a river port 80 km up the Elbe would look
like the open sea. With land polygons, being on land and being next to it both
read as 0 km.

The data is OSM's `land-polygons-split-4326` (osmdata.openstreetmap.de), which
`scripts/prepare_coastline.py` clips to a region and writes as Parquet — one row
per polygon, WKB plus its bounding box, in small row groups that each cover one
10°×10° cell. The world is 873k polygons and 79M vertices, so Phase 4 never
loads the whole file: it reads the four bbox columns, picks the rows near some
harbour, and decodes geometry only from the row groups holding them. Memory
scales with the number of harbours, not with the size of the file.

Natural Earth was measured and rejected: at 1:10M it has no small islands, so
Christiansø came out 17.6 km "offshore" from Bornholm and every island harbour
looked like an anchorage.

Distances are computed in a local equirectangular projection around each
harbour, never in degrees: at 56°N a degree of longitude is 0.56× a degree of
latitude, and `STRtree.query_nearest` would minimise the wrong thing (see the
`reverse_geocoder` gotcha in CLAUDE.md). The tree is used for envelope
candidates only, which is metric-free and therefore exact.
"""

import logging
import math
import struct
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from shapely.geometry import LinearRing, Polygon, box

from utils.s3 import get_s3_filesystem, is_s3_path

logger = logging.getLogger(__name__)

EARTH_RADIUS_KM = 6371.0088
KM_PER_DEG = math.pi / 180.0 * EARTH_RADIUS_KM

LAND_SCHEMA = pa.schema([
    pa.field("wkb", pa.binary()),
    pa.field("min_lon", pa.float64()),
    pa.field("min_lat", pa.float64()),
    pa.field("max_lon", pa.float64()),
    pa.field("max_lat", pa.float64()),
])

# The region the file was clipped to, as "min_lon,min_lat,max_lon,max_lat" in
# the Parquet key-value metadata. Without it, "no land within the search
# radius" could mean either the open sea or a harbour the file does not cover.
COVERAGE_KEY = b"coverage_bbox"

# The search grows from the first radius by doubling until land turns up. The
# cap is wider than any distance to land in the North Sea or the Baltic (the
# middle of the North Sea is ~150 km from shore). Past ~100 km the local
# projection is off by a couple of percent, which no threshold here cares about.
FIRST_SEARCH_KM = 5.0
MAX_SEARCH_KM = 400.0

# Write-side layout. Rows are bucketed into cells of BUCKET_DEG and each row
# group holds rows of one cell only, so the groups a harbour needs are few and
# small. The shapefile's own record order is globally scattered — every
# 5,000-record run spans ~350° of longitude — so without the bucketing every
# row group would hold something near every harbour.
BUCKET_DEG = 10.0
ROW_GROUP_ROWS = 2_000
# WKB held in memory before it is spilled to the temporary file.
SPILL_BYTES = 256 * 1024 * 1024
_BUCKET_COLS = int(360 // BUCKET_DEG)

SHP_MAGIC = 9994
SHP_POLYGON = 5


# ---------------------------------------------------------------------------
# Shapefile reading (prepare time only)
# ---------------------------------------------------------------------------

def _rings_to_polygons(rings: list[np.ndarray]) -> list[Polygon]:
    """
    Assemble a shapefile record's rings into polygons.

    The shapefile rule: an outer ring runs clockwise, a hole counter-clockwise.
    Each hole goes to the outer ring that contains it; a hole no outer ring
    claims is dropped rather than guessed at.
    """
    outers: list[np.ndarray] = []
    holes: list[np.ndarray] = []
    for ring in rings:
        if len(ring) < 4:
            continue
        (holes if LinearRing(ring).is_ccw else outers).append(ring)

    shells = [Polygon(o) for o in outers]
    assigned: list[list[np.ndarray]] = [[] for _ in outers]
    for hole in holes:
        probe = shapely.Point(hole[0])
        for i, shell in enumerate(shells):
            if shell.contains(probe):
                assigned[i].append(hole)
                break
    return [Polygon(o, h) for o, h in zip(outers, assigned)]


def read_shapefile_polygons(
    stream: BinaryIO,
    bbox: tuple[float, float, float, float] | None = None,
) -> Iterator[Polygon]:
    """
    Stream the polygons out of a `.shp` file, skipping those outside `bbox`.

    `bbox` is (min_lon, min_lat, max_lon, max_lat). Every record header carries
    its own bounding box, so a record outside the region is skipped without
    decoding a coordinate — which is what makes a 1.3 GB global file cheap to
    clip. Only the geometry is read; the `.dbf` holds nothing Phase 4 needs.

    A plain reader rather than a dependency: the format is three structs, and
    the project deliberately carries no GDAL (see CLAUDE.md).
    """
    header = stream.read(100)
    if len(header) < 100 or struct.unpack(">i", header[:4])[0] != SHP_MAGIC:
        raise ValueError("not a shapefile (.shp)")
    shape_type = struct.unpack("<i", header[32:36])[0]
    if shape_type != SHP_POLYGON:
        raise ValueError(f"expected a Polygon shapefile (type 5), got {shape_type}")

    while True:
        record_header = stream.read(8)
        if len(record_header) < 8:
            return
        _, length_words = struct.unpack(">ii", record_header)
        body = stream.read(length_words * 2)
        rec_type = struct.unpack("<i", body[:4])[0]
        if rec_type != SHP_POLYGON:       # 0 is a null shape
            continue
        xmin, ymin, xmax, ymax, n_parts, n_points = struct.unpack("<4d2i",
                                                                  body[4:44])
        if bbox is not None and (xmax < bbox[0] or xmin > bbox[2]
                                 or ymax < bbox[1] or ymin > bbox[3]):
            continue
        parts = np.frombuffer(body, "<i4", n_parts, 44)
        points = np.frombuffer(body, "<f8", n_points * 2,
                               44 + 4 * n_parts).reshape(-1, 2)
        ends = [*parts[1:], n_points]
        yield from _rings_to_polygons(
            [points[a:b] for a, b in zip(parts, ends)]
        )


def _bucket(min_lon: float, min_lat: float) -> int:
    """The BUCKET_DEG cell a polygon belongs to, row-major from the south-west."""
    row = min(int((min_lat + 90.0) // BUCKET_DEG), int(180 // BUCKET_DEG) - 1)
    col = min(int((min_lon + 180.0) // BUCKET_DEG), _BUCKET_COLS - 1)
    return max(row, 0) * _BUCKET_COLS + max(col, 0)


_SPILL_SCHEMA = LAND_SCHEMA.append(pa.field("bucket", pa.int32()))


def write_land_parquet(
    polygons: Iterable[Polygon],
    path: str | Path,
    coverage: tuple[float, float, float, float] | None = None,
    spill_bytes: int = SPILL_BYTES,
) -> tuple[int, int]:
    """
    Write polygons in LAND_SCHEMA, grouped by area. Returns (polygons, vertices).

    Streams in two passes so a world-sized input never sits in memory at once:
    polygons are bucketed by cell and spilled to a temporary Parquet file
    whenever `spill_bytes` of WKB has built up, one row group per bucket; then
    each bucket is read back in turn, sorted by latitude and written out in
    ROW_GROUP_ROWS-row groups. Peak memory is the spill budget plus the
    largest bucket.
    """
    path = Path(path)
    metadata = None
    if coverage is not None:
        metadata = {COVERAGE_KEY: ",".join(f"{v:.6f}" for v in coverage).encode()}

    n_polygons = n_vertices = 0
    with tempfile.TemporaryDirectory(dir=path.parent) as tmp:
        spill_path = Path(tmp) / "spill.parquet"
        buffers: dict[int, list] = defaultdict(list)
        buffered = 0
        spill = pq.ParquetWriter(str(spill_path), _SPILL_SCHEMA)

        def flush() -> None:
            nonlocal buffered
            for bucket, rows in buffers.items():
                wkb, x0, y0, x1, y1 = zip(*rows)
                spill.write_table(pa.table({
                    "wkb": pa.array(wkb, type=pa.binary()),
                    "min_lon": x0, "min_lat": y0, "max_lon": x1, "max_lat": y1,
                    "bucket": pa.array([bucket] * len(rows), type=pa.int32()),
                }, schema=_SPILL_SCHEMA), row_group_size=len(rows))
            buffers.clear()
            buffered = 0

        try:
            for polygon in polygons:
                wkb = shapely.to_wkb(polygon)
                x0, y0, x1, y1 = polygon.bounds
                buffers[_bucket(x0, y0)].append((wkb, x0, y0, x1, y1))
                buffered += len(wkb)
                n_polygons += 1
                n_vertices += shapely.get_num_coordinates(polygon)
                if buffered >= spill_bytes:
                    flush()
            flush()
        finally:
            spill.close()

        # Which spill row groups hold which bucket — one bucket per group by
        # construction, so the group's statistics name it.
        spilled = pq.ParquetFile(str(spill_path))
        bucket_col = _SPILL_SCHEMA.get_field_index("bucket")
        groups: dict[int, list[int]] = defaultdict(list)
        for i in range(spilled.metadata.num_row_groups):
            stats = spilled.metadata.row_group(i).column(bucket_col).statistics
            groups[int(stats.min)].append(i)

        schema = LAND_SCHEMA.with_metadata(metadata) if metadata else LAND_SCHEMA
        with pq.ParquetWriter(str(path), schema, compression="zstd") as out:
            for bucket in sorted(groups):
                table = spilled.read_row_groups(groups[bucket],
                                                columns=LAND_SCHEMA.names)
                table = table.sort_by("min_lat").cast(schema)
                out.write_table(table, row_group_size=ROW_GROUP_ROWS)
    return n_polygons, n_vertices


def search_windows(geoms, margin_km: float) -> np.ndarray:
    """
    One (min_lon, min_lat, max_lon, max_lat) box per geometry, grown by
    `margin_km` on every side — the land each harbour can possibly need.

    Clamped at the poles and at ±180°: a harbour within `margin_km` of the
    antimeridian (Fiji, Chukotka) does not see land on the far side of it.
    """
    bounds = shapely.bounds(np.asarray(geoms, dtype=object)).reshape(-1, 4)
    widest = np.maximum(np.abs(bounds[:, 1]), np.abs(bounds[:, 3]))
    dlat = margin_km / KM_PER_DEG
    dlon = margin_km / (KM_PER_DEG * np.maximum(np.cos(np.radians(widest)), 1e-3))
    return np.column_stack([
        np.maximum(bounds[:, 0] - dlon, -180.0),
        np.maximum(bounds[:, 1] - dlat, -90.0),
        np.minimum(bounds[:, 2] + dlon, 180.0),
        np.minimum(bounds[:, 3] + dlat, 90.0),
    ])


# ---------------------------------------------------------------------------
# Runtime lookup
# ---------------------------------------------------------------------------

def _to_local_km(geom, lat0: float, lon0: float):
    """Project lon/lat degrees to km on a plane tangent at (lat0, lon0)."""
    k = math.cos(math.radians(lat0))
    return shapely.transform(
        geom,
        lambda xy: np.column_stack(((xy[:, 0] - lon0) * k * KM_PER_DEG,
                                    (xy[:, 1] - lat0) * KM_PER_DEG)),
    )


def _exists(path: str, s3_cfg: dict | None) -> bool:
    if is_s3_path(path):
        try:
            return bool(get_s3_filesystem(s3_cfg or {}).exists(path))
        except Exception as exc:
            logger.warning("Could not reach land polygons %s (%s)", path, exc)
            return False
    return Path(path).exists()


class Land:
    """Land polygons of a region, indexed for distance-to-land queries."""

    def __init__(
        self,
        polygons: list,
        coverage: tuple[float, float, float, float] | None = None,
    ) -> None:
        self.polygons = np.asarray(polygons, dtype=object)
        self.tree = shapely.STRtree(self.polygons)
        # (min_lon, min_lat, max_lon, max_lat); None = unknown, trust the file.
        self.coverage = coverage

    def __len__(self) -> int:
        return len(self.polygons)

    @classmethod
    def from_parquet(
        cls,
        path: str,
        windows: np.ndarray | None = None,
        s3_cfg: dict | None = None,
    ) -> "Land":
        """
        Load a file written by `scripts/prepare_coastline.py`.

        `windows` is an (n, 4) array of (min_lon, min_lat, max_lon, max_lat)
        boxes — see `search_windows` — and only polygons overlapping one of
        them are decoded. None loads everything.

        The selection runs on the four bbox columns alone (32 bytes a row, 28 MB
        for the world), and geometry is then read only from the row groups
        holding a selected row. A file written before the area grouping still
        loads correctly, just less selectively.
        """
        if is_s3_path(path):
            source = get_s3_filesystem(s3_cfg or {}).open(path, "rb")
        else:
            source = path
        pf = pq.ParquetFile(source)
        try:
            meta = pf.schema_arrow.metadata or {}
            coverage = None
            if COVERAGE_KEY in meta:
                coverage = tuple(
                    float(v) for v in meta[COVERAGE_KEY].decode().split(",")
                )

            n_groups = pf.metadata.num_row_groups
            if windows is None:
                wanted = {g: None for g in range(n_groups)}
            else:
                wanted = cls._rows_in_windows(pf, np.asarray(windows, dtype=float))

            polygons: list = []
            for group, rows in wanted.items():
                column = pf.read_row_group(group, columns=["wkb"]).column("wkb")
                if rows is not None:
                    column = column.take(pa.array(rows))
                polygons.extend(
                    shapely.from_wkb(column.to_numpy(zero_copy_only=False))
                )
        finally:
            if source is not path:
                source.close()
        return cls(polygons, coverage=coverage)

    @staticmethod
    def _rows_in_windows(pf: pq.ParquetFile, windows: np.ndarray) -> dict:
        """{row group: row offsets within it} for every polygon in a window."""
        if not len(windows):
            return {}
        b = pf.read(columns=["min_lon", "min_lat", "max_lon", "max_lat"])
        tiles = shapely.box(*(b.column(c).to_numpy()
                              for c in ("min_lon", "min_lat", "max_lon", "max_lat")))
        window_tree = shapely.STRtree(shapely.box(*windows.T))
        hits = np.unique(window_tree.query(tiles, predicate="intersects")[0])

        sizes = [pf.metadata.row_group(g).num_rows
                 for g in range(pf.metadata.num_row_groups)]
        starts = np.concatenate([[0], np.cumsum(sizes)])
        group_of = np.searchsorted(starts, hits, side="right") - 1
        return {
            int(g): (hits[group_of == g] - starts[g]).tolist()
            for g in np.unique(group_of)
        }

    @classmethod
    def open(
        cls,
        path: str | None,
        windows: np.ndarray | None = None,
        s3_cfg: dict | None = None,
    ) -> "Land | None":
        """Load `path`, or None when it is unset or missing — the step is off."""
        if not path:
            return None
        if not _exists(str(path), s3_cfg):
            logger.warning(
                "Land polygons %s not found — the offshore flag is skipped. Run "
                "scripts/prepare_coastline.py to build them.", path,
            )
            return None
        land = cls.from_parquet(str(path), windows=windows, s3_cfg=s3_cfg)
        logger.info("Land polygons: %s (%d near the run's harbours)",
                    path, len(land))
        return land

    def covers(self, lat: float, lon: float) -> bool:
        """Is this point inside the region the file was clipped to?"""
        if self.coverage is None:
            return True
        min_lon, min_lat, max_lon, max_lat = self.coverage
        return min_lon <= lon <= max_lon and min_lat <= lat <= max_lat

    def distance_km(
        self,
        geom,
        lat0: float,
        lon0: float,
        max_km: float = MAX_SEARCH_KM,
    ) -> float | None:
        """
        Distance in km from `geom` (lon/lat degrees) to the nearest land.

        0 when the geometry touches or lies on land. The search box starts at
        FIRST_SEARCH_KM and doubles up to `max_km` until land is found; a
        result is only accepted when it lies inside the box's inscribed
        radius, because land just outside the box could be nearer than a
        corner hit inside it. Returns None when there is no land within
        `max_km` — which is exact only if the land loaded covers that far
        (Phase 4 loads exactly `max_km` around each harbour).
        """
        if geom is None or geom.is_empty or not len(self.polygons):
            return None
        # Fast path: most harbours touch land at their quay, and an
        # intersection test needs neither clipping nor projection.
        if len(self.tree.query(geom, predicate="intersects")):
            return 0.0
        local_geom = _to_local_km(geom, lat0, lon0)
        minx, miny, maxx, maxy = geom.bounds

        radius = min(FIRST_SEARCH_KM, max_km)
        while True:
            dlat = radius / KM_PER_DEG
            # Degrees of longitude per km at the window's poleward edge, so the
            # box contains the whole radius rather than falling short of it.
            widest = min(max(abs(miny - dlat), abs(maxy + dlat)), 89.9)
            dlon = radius / (KM_PER_DEG * math.cos(math.radians(widest)))
            window = box(minx - dlon, miny - dlat, maxx + dlon, maxy + dlat)
            hits = self.tree.query(window)
            if len(hits):
                # Clipped to the window first: a land tile can carry tens of
                # thousands of vertices, and only the part near here matters.
                # Clipping keeps every bit of land inside the window, so the
                # nearest point within `radius` is unaffected.
                parts = shapely.clip_by_rect(self.polygons[hits], *window.bounds)
                parts = parts[~shapely.is_empty(parts)]
                if len(parts):
                    local = _to_local_km(parts, lat0, lon0)
                    dist = float(shapely.distance(local, local_geom).min())
                    if dist <= radius:
                        return dist
            if radius >= max_km:
                return None
            radius = min(radius * 2, max_km)
