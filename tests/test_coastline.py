"""Unit tests for utils.coastline — land polygons and distance to land."""

import io
import math
import struct

import numpy as np
import pyarrow.parquet as pq
import pytest
from shapely.geometry import Point, Polygon, box

from utils.coastline import (
    BUCKET_DEG,
    Land,
    read_shapefile_polygons,
    search_windows,
    write_land_parquet,
)
from utils.geo import haversine_meters

LAT, LON = 56.0, 10.0
# A 0.1° square of land, south-west corner at (LAT, LON).
ISLAND = box(LON, LAT, LON + 0.1, LAT + 0.1)


# ---------------------------------------------------------------------------
# Shapefile reading
# ---------------------------------------------------------------------------

def _shp_record(number: int, rings: list[list[tuple[float, float]]]) -> bytes:
    points = [pt for ring in rings for pt in ring]
    xs, ys = [p[0] for p in points], [p[1] for p in points]
    parts, offset = [], 0
    for ring in rings:
        parts.append(offset)
        offset += len(ring)
    content = struct.pack("<i4d2i", 5, min(xs), min(ys), max(xs), max(ys),
                          len(rings), len(points))
    content += struct.pack(f"<{len(parts)}i", *parts)
    content += b"".join(struct.pack("<2d", *p) for p in points)
    return struct.pack(">ii", number, len(content) // 2) + content


def _shapefile(records: list[bytes], shape_type: int = 5) -> io.BytesIO:
    body = b"".join(records)
    header = struct.pack(">i5ii", 9994, 0, 0, 0, 0, 0, (100 + len(body)) // 2)
    header += struct.pack("<ii", 1000, shape_type) + struct.pack("<8d", *[0.0] * 8)
    return io.BytesIO(header + body)


def _cw(minx, miny, maxx, maxy):
    """A clockwise ring — the shapefile's outer-ring orientation."""
    return [(minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny), (minx, miny)]


def _ccw(minx, miny, maxx, maxy):
    """Counter-clockwise — the shapefile's hole orientation."""
    return list(reversed(_cw(minx, miny, maxx, maxy)))


def test_the_reader_returns_the_polygons_in_the_file():
    stream = _shapefile([_shp_record(1, [_cw(10, 56, 11, 57)])])
    polygons = list(read_shapefile_polygons(stream))

    assert len(polygons) == 1
    assert polygons[0].equals(box(10, 56, 11, 57))


def test_a_counter_clockwise_ring_is_a_hole_in_its_outer_ring():
    stream = _shapefile([_shp_record(1, [_cw(10, 56, 11, 57),
                                         _ccw(10.4, 56.4, 10.6, 56.6)])])
    (polygon,) = read_shapefile_polygons(stream)

    assert len(polygon.interiors) == 1
    assert not polygon.contains(Point(10.5, 56.5))
    assert polygon.contains(Point(10.1, 56.1))


def test_two_outer_rings_in_one_record_are_two_polygons():
    stream = _shapefile([_shp_record(1, [_cw(10, 56, 11, 57),
                                         _cw(12, 56, 13, 57)])])
    assert len(list(read_shapefile_polygons(stream))) == 2


def test_records_outside_the_bbox_are_skipped():
    """What makes clipping a 1.3 GB global file cheap."""
    stream = _shapefile([
        _shp_record(1, [_cw(10, 56, 11, 57)]),
        _shp_record(2, [_cw(100, 1, 101, 2)]),       # Singapore-ish
    ])
    polygons = list(read_shapefile_polygons(stream, bbox=(-5, 50, 32, 72)))

    assert len(polygons) == 1
    assert polygons[0].bounds == (10, 56, 11, 57)


def test_a_line_shapefile_is_rejected():
    """The coastline (lines) file cannot answer 'is this on land'."""
    with pytest.raises(ValueError, match="Polygon"):
        list(read_shapefile_polygons(_shapefile([], shape_type=3)))


def test_land_round_trips_through_parquet_with_its_coverage(tmp_path):
    path = tmp_path / "land.parquet"
    far = box(100, 1, 101, 2)
    write_land_parquet([ISLAND, far], path, coverage=(-5, 50, 32, 72))

    windows = search_windows([Point(10.05, 56.05)], margin_km=50)
    land = Land.from_parquet(str(path), windows=windows)

    assert len(land) == 1                  # the window filter dropped `far`
    assert land.coverage == (-5, 50, 32, 72)
    assert land.covers(56, 10)
    assert not land.covers(1.5, 100.5)


def test_a_missing_file_turns_the_step_off(tmp_path):
    assert Land.open(str(tmp_path / "nope.parquet")) is None
    assert Land.open("") is None


# ---------------------------------------------------------------------------
# Distance to land
# ---------------------------------------------------------------------------

def test_the_distance_is_the_real_ground_distance():
    land = Land([ISLAND])
    lat = LAT + 0.05
    lon = LON + 0.1 + 0.05                 # 0.05° east of the east shore
    expected = haversine_meters(lat, LON + 0.1, lat, lon) / 1000

    got = land.distance_km(Point(lon, lat), lat, lon)

    assert got == pytest.approx(expected, rel=0.01)
    # At 56°N that is ~3.1 km, not the 5.6 km a degree of latitude would be.
    assert got == pytest.approx(0.05 * 111.2 * math.cos(math.radians(lat)),
                                rel=0.01)


def test_the_nearest_land_is_found_in_kilometres_not_degrees():
    """
    The reverse_geocoder trap: land 0.05° north (5.6 km) is nearer in degrees
    than land 0.07° east (4.4 km), but not on the ground.
    """
    harbour = Point(LON, LAT)
    north = box(LON - 0.01, LAT + 0.05, LON + 0.01, LAT + 0.06)
    east = box(LON + 0.07, LAT - 0.01, LON + 0.08, LAT + 0.01)

    got = Land([north, east]).distance_km(harbour, LAT, LON)

    assert got == pytest.approx(0.07 * 111.2 * math.cos(math.radians(LAT)),
                                rel=0.01)


def test_a_harbour_touching_land_is_zero():
    quay = box(LON + 0.1 - 0.001, LAT + 0.05, LON + 0.1 + 0.002, LAT + 0.052)
    assert Land([ISLAND]).distance_km(quay, LAT + 0.05, LON + 0.1) == 0.0


def test_a_river_port_inside_the_land_polygon_is_zero():
    """Why the data is land polygons and not a coastline line."""
    inland = Point(LON + 0.05, LAT + 0.05)
    assert Land([ISLAND]).distance_km(inland, LAT + 0.05, LON + 0.05) == 0.0


def test_a_distant_shore_is_found_by_widening_the_search():
    """30 km out — well past the first search radius."""
    lat, lon = LAT + 0.05, LON + 0.1 + 30 / (111.2 * math.cos(math.radians(LAT)))
    got = Land([ISLAND]).distance_km(Point(lon, lat), lat, lon)

    assert got == pytest.approx(30, rel=0.02)


def test_the_search_stops_at_max_km():
    """Phase 4 loads land only that far out, so it must not search further."""
    lat, lon = LAT + 0.05, LON + 0.1 + 30 / (111.2 * math.cos(math.radians(LAT)))
    land = Land([ISLAND])

    assert land.distance_km(Point(lon, lat), lat, lon, max_km=10) is None
    assert land.distance_km(Point(lon, lat), lat, lon, max_km=40) == \
        pytest.approx(30, rel=0.02)


def test_nothing_in_reach_is_none():
    lat, lon = 1.0, 100.0
    assert Land([ISLAND]).distance_km(Point(lon, lat), lat, lon) is None


def test_the_outline_is_measured_not_the_centroid():
    """
    A long harbour whose far end touches land: its centroid is 2 km out, but
    the harbour is at a quay.
    """
    k = 111.2 * math.cos(math.radians(LAT + 0.05))
    shore = LON + 0.1
    harbour = Polygon([(shore, LAT + 0.05), (shore + 4 / k, LAT + 0.05),
                       (shore + 4 / k, LAT + 0.051), (shore, LAT + 0.051)])
    c = harbour.centroid

    assert Land([ISLAND]).distance_km(harbour, c.y, c.x) == 0.0
    assert Land([ISLAND]).distance_km(c, c.y, c.x) == pytest.approx(2, rel=0.02)


# ---------------------------------------------------------------------------
# World-scale layout: bounded-memory writing, selective reading
# ---------------------------------------------------------------------------

def _scattered_world(n: int = 600) -> list[Polygon]:
    """Small squares all over the globe, in a deliberately shuffled order —
    like the OSM shapefile, whose record order is globally scattered."""
    rng = np.random.default_rng(7)
    lons = rng.uniform(-179, 178, n)
    lats = rng.uniform(-80, 79, n)
    return [box(x, y, x + 0.5, y + 0.5) for x, y in zip(lons, lats)]


def test_spilling_in_small_batches_writes_the_same_polygons(tmp_path):
    polygons = _scattered_world()
    one_go, spilled = tmp_path / "a.parquet", tmp_path / "b.parquet"

    write_land_parquet(polygons, one_go)
    counts = write_land_parquet(iter(polygons), spilled, spill_bytes=2_000)

    assert counts == (len(polygons), 5 * len(polygons))
    a = {p.wkb for p in Land.from_parquet(str(one_go)).polygons}
    b = {p.wkb for p in Land.from_parquet(str(spilled)).polygons}
    assert a == b == {p.wkb for p in polygons}


def test_every_row_group_covers_one_cell(tmp_path):
    """What lets a harbour's land come from a few row groups, not all."""
    path = tmp_path / "world.parquet"
    write_land_parquet(_scattered_world(), path, spill_bytes=2_000)

    meta = pq.ParquetFile(str(path)).metadata
    lon_i = meta.schema.to_arrow_schema().get_field_index("min_lon")
    lat_i = meta.schema.to_arrow_schema().get_field_index("min_lat")
    assert meta.num_row_groups > 1
    for g in range(meta.num_row_groups):
        lon = meta.row_group(g).column(lon_i).statistics
        lat = meta.row_group(g).column(lat_i).statistics
        assert (lon.min + 180) // BUCKET_DEG == (lon.max + 180) // BUCKET_DEG
        assert (lat.min + 90) // BUCKET_DEG == (lat.max + 90) // BUCKET_DEG


def test_only_the_land_near_a_harbour_is_loaded(tmp_path):
    near = box(10.0, 56.0, 10.1, 56.1)
    path = tmp_path / "world.parquet"
    write_land_parquet([*_scattered_world(), near], path)

    harbour = Point(10.2, 56.05)
    land = Land.from_parquet(str(path),
                             windows=search_windows([harbour], margin_km=20))

    assert near.wkb in {p.wkb for p in land.polygons}
    assert len(land) < 5
    assert land.distance_km(harbour, 56.05, 10.2) == pytest.approx(
        0.1 * 111.2 * math.cos(math.radians(56.05)), rel=0.01)


def test_windows_from_several_harbours_are_all_honoured(tmp_path):
    a, b = box(10.0, 56.0, 10.1, 56.1), box(-70.0, -33.0, -69.9, -32.9)
    path = tmp_path / "world.parquet"
    write_land_parquet([*_scattered_world(), a, b], path)

    land = Land.from_parquet(str(path), windows=search_windows(
        [Point(10.2, 56.05), Point(-69.8, -32.95)], margin_km=20))

    loaded = {p.wkb for p in land.polygons}
    assert a.wkb in loaded and b.wkb in loaded


def test_no_harbours_loads_nothing(tmp_path):
    path = tmp_path / "world.parquet"
    write_land_parquet(_scattered_world(), path)

    assert len(Land.from_parquet(str(path), windows=np.empty((0, 4)))) == 0


def test_a_window_grows_more_in_longitude_at_high_latitude():
    (w,) = search_windows([Point(10.0, 60.0)], margin_km=111.2)

    assert w[3] - w[1] == pytest.approx(2.0, rel=0.01)       # ±1° of latitude
    assert w[2] - w[0] == pytest.approx(4.0, rel=0.02)       # ±2° at 60°N
