"""
Unit tests for the shared geometry helpers in utils.geo.

Focus is on merge_outlines, which backs the manually adjusted harbour outline:
a drawn outline is a floor the pipeline may grow but never shrink below.
"""

import h3
from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiPolygon,
    Point,
    Polygon,
    box,
    shape,
)

from utils.geo import clean_polygon, merge_outlines, outline_polygon

RES = 11
HAMBURG_LAT, HAMBURG_LON = 53.54, 9.97

# A bow-tie: the classic self-intersection a dragged vertex produces.
BOWTIE = Polygon([(0, 0), (1, 1), (1, 0), (0, 1)])


def _assert_covers(merged, original) -> None:
    """
    Assert `merged` gave up none of `original`'s area.

    Deliberately not `merged.covers(original)`: GEOS perturbs boundary
    coordinates by a few ULPs when it unions, so the topological predicate
    reports False for a harbour-sized polygon at real lon/lat magnitudes even
    though the difference is a zero-area sliver. Lost area is the invariant
    that actually matters here.
    """
    lost = original.difference(merged).area
    assert lost <= original.area * 1e-9, f"union lost {lost} of {original.area}"


def _square_with_hole() -> Polygon:
    return Polygon(
        [(0, 0), (10, 0), (10, 10), (0, 10)],
        holes=[[(4, 4), (6, 4), (6, 6), (4, 6)]],
    )


# ---------------------------------------------------------------------------
# clean_polygon — everything the GUI can hand back has to survive this
# ---------------------------------------------------------------------------

def test_clean_polygon_passes_through_valid_polygon():
    square = box(0, 0, 1, 1)
    assert clean_polygon(square).equals(square)


def test_clean_polygon_repairs_self_intersection():
    cleaned = clean_polygon(BOWTIE)
    assert cleaned is not None
    assert cleaned.is_valid
    # The two triangles survive; their combined area is the bow-tie's.
    assert cleaned.area == 0.5


def test_clean_polygon_drops_non_areal_geometries():
    assert clean_polygon(LineString([(0, 0), (1, 1)])) is None
    assert clean_polygon(Point(0, 0)) is None


def test_clean_polygon_keeps_only_the_areal_part_of_a_collection():
    square = box(0, 0, 1, 1)
    collection = GeometryCollection([square, LineString([(5, 5), (6, 6)])])
    assert clean_polygon(collection).equals(square)


def test_clean_polygon_handles_missing_and_empty_input():
    assert clean_polygon(None) is None
    assert clean_polygon(Polygon()) is None


# ---------------------------------------------------------------------------
# merge_outlines — the "never shrinks" guarantee
# ---------------------------------------------------------------------------

def test_merge_contains_both_inputs():
    detected = box(0, 0, 10, 10)
    manual   = box(8, 8, 15, 15)

    merged = merge_outlines(detected, manual)

    _assert_covers(merged, detected)
    _assert_covers(merged, manual)
    assert merged.area > detected.area


def test_manual_outline_inside_detected_cannot_shrink_it():
    """Dragging the boundary inwards is undone by the union, by design."""
    detected = box(0, 0, 10, 10)
    manual   = box(2, 2, 5, 5)

    assert merge_outlines(detected, manual).equals(detected)


def test_disjoint_manual_part_is_kept_as_a_separate_part():
    detected = box(0, 0, 10, 10)
    manual   = box(50, 50, 60, 60)

    merged = merge_outlines(detected, manual)

    assert isinstance(merged, MultiPolygon)
    assert len(merged.geoms) == 2
    _assert_covers(merged, detected)
    _assert_covers(merged, manual)


def test_invalid_manual_outline_is_repaired_not_dropped():
    detected = box(10, 10, 11, 11)

    merged = merge_outlines(detected, BOWTIE)

    assert merged.is_valid
    _assert_covers(merged, detected)
    # Both lobes of the bow-tie survive: make_valid keeps 0.5, where the older
    # buffer(0) trick would have silently discarded one triangle and kept 0.25.
    assert merged.area == detected.area + 0.5


def test_holes_are_filled_by_default():
    merged = merge_outlines(_square_with_hole(), box(9, 9, 12, 12))

    for part in getattr(merged, "geoms", [merged]):
        assert not part.interiors


def test_holes_can_be_kept():
    merged = merge_outlines(_square_with_hole(), box(9, 9, 12, 12),
                            fill_holes=False)

    assert any(part.interiors for part in getattr(merged, "geoms", [merged]))


def test_merge_with_one_side_missing_returns_the_other():
    detected = box(0, 0, 1, 1)

    assert merge_outlines(detected, None).equals(detected)
    assert merge_outlines(None, detected).equals(detected)


def test_merge_with_nothing_to_merge_returns_none():
    assert merge_outlines(None, None) is None
    assert merge_outlines(LineString([(0, 0), (1, 1)]), None) is None


# ---------------------------------------------------------------------------
# Against a real harbour outline
# ---------------------------------------------------------------------------

def test_merge_extends_a_real_outline_without_losing_any_cell():
    """An operator drags one edge outwards; every detected cell stays covered."""
    seed      = h3.latlng_to_cell(HAMBURG_LAT, HAMBURG_LON, RES)
    cell_geom = shape(h3.cells_to_h3shape(sorted(h3.grid_disk(seed, 1))))
    detected  = outline_polygon(cell_geom, buffer_meters=75.0)

    # ~200 m of quay added to the east, overlapping the detected outline.
    _, miny, maxx, maxy = detected.bounds
    manual = box(maxx - 0.0005, miny, maxx + 0.003, maxy)

    merged = merge_outlines(detected, manual)

    _assert_covers(merged, detected)
    _assert_covers(merged, cell_geom)
    assert merged.area > detected.area
    for part in getattr(merged, "geoms", [merged]):
        assert not part.interiors
