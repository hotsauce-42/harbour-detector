"""Unit tests for utils.coastline — land polygons and distance to land."""

import io
import math
import struct

import pytest
from shapely.geometry import Point, Polygon, box

from utils.coastline import (
    Land,
    read_shapefile_polygons,
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

    land = Land.from_parquet(str(path), bbox=(55, 9, 57, 11))

    assert len(land) == 1                  # the bbox filter dropped `far`
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
