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
per polygon, WKB plus its bounding box. Natural Earth was measured and rejected:
at 1:10M it has no small islands, so Christiansø came out 17.6 km "offshore"
from Bornholm and every island harbour looked like an anchorage.

Distances are computed in a local equirectangular projection around each
harbour, never in degrees: at 56°N a degree of longitude is 0.56× a degree of
latitude, and `STRtree.query_nearest` would minimise the wrong thing (see the
`reverse_geocoder` gotcha in CLAUDE.md). The tree is used for envelope
candidates only, which is metric-free and therefore exact.
"""

import logging
import math
import struct
from pathlib import Path
from typing import BinaryIO, Iterator

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


def write_land_parquet(
    polygons: list[Polygon],
    path: str | Path,
    coverage: tuple[float, float, float, float] | None = None,
) -> None:
    """Write polygons in LAND_SCHEMA, sorted by latitude for row-group pruning."""
    bounds = shapely.bounds(np.asarray(polygons, dtype=object)).reshape(-1, 4)
    order = np.argsort(bounds[:, 1], kind="stable")
    table = pa.table({
        "wkb": pa.array(shapely.to_wkb(np.asarray(polygons, dtype=object)[order]),
                        type=pa.binary()),
        "min_lon": bounds[order, 0],
        "min_lat": bounds[order, 1],
        "max_lon": bounds[order, 2],
        "max_lat": bounds[order, 3],
    }, schema=LAND_SCHEMA)
    if coverage is not None:
        table = table.replace_schema_metadata(
            {COVERAGE_KEY: ",".join(f"{v:.6f}" for v in coverage).encode()}
        )
    pq.write_table(table, str(path), compression="zstd", row_group_size=5_000)


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
        bbox: tuple[float, float, float, float] | None = None,
        s3_cfg: dict | None = None,
    ) -> "Land":
        """
        Load a file written by `scripts/prepare_coastline.py`.

        `bbox` is (min_lat, min_lon, max_lat, max_lon) — the order
        `utils.gazetteer.bbox_around` returns — and keeps only polygons whose
        own box overlaps it.
        """
        filters = None
        if bbox is not None:
            min_lat, min_lon, max_lat, max_lon = bbox
            filters = [
                ("max_lat", ">=", min_lat), ("min_lat", "<=", max_lat),
                ("max_lon", ">=", min_lon), ("min_lon", "<=", max_lon),
            ]
        filesystem = get_s3_filesystem(s3_cfg or {}) if is_s3_path(path) else None
        table = pq.read_table(path, columns=["wkb"], filters=filters,
                              filesystem=filesystem)
        meta = pq.read_schema(path, filesystem=filesystem).metadata or {}
        coverage = None
        if COVERAGE_KEY in meta:
            coverage = tuple(float(v) for v in meta[COVERAGE_KEY].decode().split(","))
        polygons = shapely.from_wkb(table.column("wkb").to_numpy(zero_copy_only=False))
        return cls(list(polygons), coverage=coverage)

    @classmethod
    def open(
        cls,
        path: str | None,
        bbox: tuple[float, float, float, float] | None = None,
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
        land = cls.from_parquet(str(path), bbox=bbox, s3_cfg=s3_cfg)
        logger.info("Land polygons: %s (%d in range)", path, len(land))
        return land

    def covers(self, lat: float, lon: float) -> bool:
        """Is this point inside the region the file was clipped to?"""
        if self.coverage is None:
            return True
        min_lon, min_lat, max_lon, max_lat = self.coverage
        return min_lon <= lon <= max_lon and min_lat <= lat <= max_lat

    def distance_km(self, geom, lat0: float, lon0: float) -> float | None:
        """
        Distance in km from `geom` (lon/lat degrees) to the nearest land.

        0 when the geometry touches or lies on land. The search box starts at
        FIRST_SEARCH_KM and doubles until land is found; a result is only
        accepted when it lies inside the box's inscribed radius, because land
        just outside the box could be nearer than a corner hit inside it.
        Returns None when nothing is found within MAX_SEARCH_KM.
        """
        if geom is None or geom.is_empty or not len(self.polygons):
            return None
        local_geom = _to_local_km(geom, lat0, lon0)
        k = max(math.cos(math.radians(lat0)), 1e-6)
        minx, miny, maxx, maxy = geom.bounds

        radius = FIRST_SEARCH_KM
        while radius <= MAX_SEARCH_KM:
            dlat = radius / KM_PER_DEG
            dlon = radius / (KM_PER_DEG * k)
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
            radius *= 2
        return None
