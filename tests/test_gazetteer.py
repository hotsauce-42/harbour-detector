"""
Unit tests for the nearest-place gazetteer behind Phase 4's `nearest_city`.

Self-contained: every gazetteer here is built in memory from a handful of
places, so nothing downloads and nothing depends on a prepared GeoNames file.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from utils.gazetteer import (
    DEFUNCT_CODES,
    _chord_km,
    _great_circle_km,
    DISTRICT_ISOLATION_KM,
    GAZETTEER_SCHEMA,
    Gazetteer,
    bbox_around,
)
from utils.geo import haversine_meters

# 56°N — the latitude of the Danish/Swedish harbours this pipeline was built
# for, where a degree of longitude is 0.56× a degree of latitude.
NORTH = 56.0


def _gazetteer(places: list[tuple], has_population: bool = True) -> Gazetteer:
    """
    places: (name, lat, lon, population, feature_code, cc, admin1) and
    optionally (admin2, admin3, admin4). Short rows are padded, so a test only
    spells out the division codes when it is actually about municipalities.
    """
    width = len(GAZETTEER_SCHEMA)
    places = [tuple(p) + ("",) * (width - len(p)) for p in places]
    columns = list(zip(*places)) if places else [()] * width
    table = pa.table({
        field.name: pa.array(list(values), type=field.type)
        for field, values in zip(GAZETTEER_SCHEMA, columns)
    }, schema=GAZETTEER_SCHEMA)
    return Gazetteer(table, has_population=has_population)


# ── The distance metric ────────────────────────────────────────────────────

def test_nearest_is_nearest_on_the_ground_not_in_degrees():
    """
    The whole reason this module exists. reverse_geocoder minimises Euclidean
    distance over raw degrees, so at 56°N it over-penalises anything east or
    west by 1/cos(56°) ≈ 1.8×. Here the eastern village is 12 km away and the
    northern one 17 km, but in degree-space the northern one looks closer
    (0.15° vs 0.20°) — which is exactly the wrong answer.
    """
    gz = _gazetteer([
        ("East",  NORTH,        10.2, 500, "PPL", "DK", "Region"),
        ("North", NORTH + 0.15, 10.0, 500, "PPL", "DK", "Region"),
    ])
    place = gz.nearest(NORTH, 10.0)

    assert place.name == "East"
    # …and it really is the closer of the two.
    east = haversine_meters(NORTH, 10.0, NORTH, 10.2) / 1000
    north = haversine_meters(NORTH, 10.0, NORTH + 0.15, 10.0) / 1000
    assert east < north


def test_distance_matches_a_haversine_calculation():
    gz = _gazetteer([("Town", 55.5, 9.5, 2000, "PPL", "DK", "Region")])
    place = gz.nearest(55.0, 10.0)
    expected = haversine_meters(55.0, 10.0, 55.5, 9.5) / 1000
    assert place.distance_km == pytest.approx(expected, rel=1e-3)


def test_the_antimeridian_does_not_split_neighbours():
    """Cartesian coordinates have no seam; a lon-based metric would show 359°."""
    gz = _gazetteer([
        ("WestSide", 66.0, -179.9, 500, "PPL", "RU", ""),
        ("FarAway",  66.0,  170.0, 500, "PPL", "RU", ""),
    ])
    assert gz.nearest(66.0, 179.9).name == "WestSide"


# ── Population floors ──────────────────────────────────────────────────────

def test_no_floor_takes_the_nearest_place_however_small():
    """A small harbour should be named after the village beside it."""
    gz = _gazetteer([
        ("Hamlet", 55.01, 10.0,     0, "PPL", "DK", "Region"),
        ("City",   55.30, 10.0, 50000, "PPL", "DK", "Region"),
    ])
    assert gz.nearest(55.0, 10.0, 0).name == "Hamlet"


def test_a_floor_skips_the_village_for_the_real_town():
    """And a large port should not be named after the hamlet on its edge."""
    gz = _gazetteer([
        ("Hamlet", 55.01, 10.0,     0, "PPL", "DK", "Region"),
        ("City",   55.30, 10.0, 50000, "PPL", "DK", "Region"),
    ])
    assert gz.nearest(55.0, 10.0, 1000).name == "City"


def test_unknown_population_is_treated_as_too_small():
    """GeoNames writes 0 for 'not recorded', which is the small-place case."""
    gz = _gazetteer([("Unrecorded", 55.0, 10.0, 0, "PPL", "DK", "")])
    assert gz.nearest(55.0, 10.0, 0).name == "Unrecorded"
    assert gz.nearest(55.0, 10.0, 1000) is None


def test_an_administrative_seat_clears_any_floor():
    """
    A county town is significant regardless of headcount, and GeoNames often
    records a low population for one. Without the exemption a large harbour
    would skip its own seat for a bigger place further away.
    """
    gz = _gazetteer([
        ("Seat",     55.05, 10.0,   800, "PPLA2", "DK", "Region"),
        ("Populous", 55.40, 10.0, 90000, "PPL",   "DK", "Region"),
    ])
    assert gz.nearest(55.0, 10.0, 15000).name == "Seat"


def test_a_source_without_population_ignores_floors():
    """
    The bundled cities1000 dataset has no population column. Applying a floor
    to it would exclude everything and leave the city empty.
    """
    gz = _gazetteer([("Only", 55.0, 10.0, 0, "", "DK", "")], has_population=False)
    assert gz.has_population is False
    assert gz.nearest(55.0, 10.0, 15000).name == "Only"


def test_an_empty_gazetteer_returns_nothing_rather_than_raising():
    assert _gazetteer([]).nearest(55.0, 10.0) is None


# ── Feature codes that are not settlements ─────────────────────────────────

def test_an_unpopulated_district_never_wins_over_the_town_it_belongs_to():
    """
    The Heiligenhafen case. GeoNames records the districts of a town as PPLX
    entries at their own coordinates, and a harbour lies closer to the
    waterfront district than to the town centre — so nearest-place alone names
    the harbour "Altstadt" ("old town"), which is true of a hundred German
    towns and identifies none of them. This one has no population, which is
    what separates it from Warnemünde below.
    """
    gz = _gazetteer([
        ("Altstadt",      54.3809, 10.9787,    0, "PPLX",  "DE", "SH"),
        ("Heiligenhafen", 54.3719, 10.9808, 9308, "PPLA4", "DE", "SH"),
    ])
    assert gz.nearest(54.3765, 10.9830).name == "Heiligenhafen"


def test_a_district_geonames_counts_people_in_is_kept():
    """
    The other half of the rule. Warnemünde is a PPLX — a district of Rostock —
    but a named port in its own right, and GeoNames gives it 8 441 people.
    Dropping every PPLX would rename that harbour after a village inland.
    """
    gz = _gazetteer([
        ("Warnemünde",  54.1770, 12.0840, 8441, "PPLX", "DE", "MV"),
        ("Petersdorf",  54.2000, 12.1300,    0, "PPL",  "DE", "MV"),
    ])
    assert gz.nearest(54.1790, 12.0900).name == "Warnemünde"


def test_a_district_inside_its_city_loses_to_the_city():
    """
    The other side of the same coin. Christiania is a populated PPLX too, but
    850 people 2 km from the centre of Copenhagen — a neighbourhood, not a
    town. Population alone would let it name five Copenhagen harbours; the
    bigger place next door is what rules it out.
    """
    gz = _gazetteer([
        ("Christiania", 55.6750, 12.5980,     850, "PPLX", "DK", "Hovedstaden"),
        ("Copenhagen",  55.6761, 12.5683, 1153615, "PPLC", "DK", "Hovedstaden"),
    ])
    assert gz.nearest(55.6740, 12.6000).name == "Copenhagen"


def test_a_district_is_judged_against_its_neighbours_not_its_own_size():
    """
    Size is not the signal — isolation is. The same 850-person district keeps
    its harbour once the city is far enough away, and Wandsbek (411,422) is
    dropped in real data because Hamburg is next to it.
    """
    far = 2 * DISTRICT_ISOLATION_KM / 111.32       # degrees of latitude
    gz = _gazetteer([
        ("Christiania", 55.6750,       12.598,     850, "PPLX", "DK", "H"),
        ("Copenhagen",  55.6750 - far, 12.598, 1153615, "PPLC", "DK", "H"),
    ])
    assert gz.nearest(55.6740, 12.6000).name == "Christiania"


@pytest.mark.parametrize("code", sorted(DEFUNCT_CODES))
def test_places_that_no_longer_exist_are_dropped_whatever_their_population(code):
    """PPLQ/PPLW/PPLH/PPLCH: abandoned, destroyed, historical."""
    assert len(_gazetteer([("Gone", 55.0, 10.0, 9000, code, "DK", "")])) == 0


def test_ordinary_small_places_survive_the_filter():
    """The filter must not undo the reason the dense gazetteer exists."""
    kept = [("Village", 55.0, 10.0, 0, c, "DK", "")
            for c in ("PPL", "PPLL", "PPLF", "PPLA4", "PPLC", "")]
    assert len(_gazetteer(kept)) == len(kept)


# ── Loading ────────────────────────────────────────────────────────────────

def _write_places(path, places) -> None:
    width = len(GAZETTEER_SCHEMA)
    places = [tuple(p) + ("",) * (width - len(p)) for p in places]
    columns = list(zip(*places))
    pq.write_table(pa.table({
        field.name: pa.array(list(values), type=field.type)
        for field, values in zip(GAZETTEER_SCHEMA, columns)
    }, schema=GAZETTEER_SCHEMA), path)


def test_parquet_round_trip_with_a_bounding_box(tmp_path):
    path = tmp_path / "places.parquet"
    _write_places(path, [
        ("Near", 55.0,  10.0, 100, "PPL", "DK", "Region"),
        ("Far",  -33.9, 18.4, 100, "PPL", "ZA", "Western Cape"),
    ])
    gz = Gazetteer.from_parquet(path, bbox=(54.0, 9.0, 56.0, 11.0))

    assert len(gz) == 1
    assert gz.nearest(55.0, 10.0).name == "Near"


def test_a_missing_gazetteer_falls_back_to_the_bundled_dataset(tmp_path):
    """
    Phase 4 must still run on a machine where nobody prepared the file — with
    the old, coarse dataset, but running.
    """
    gz = Gazetteer.open(tmp_path / "absent.parquet")
    assert len(gz) > 100_000          # cities1000 is ~145k places
    assert gz.has_population is False


def test_no_configured_gazetteer_falls_back_without_a_warning_path(tmp_path):
    assert len(Gazetteer.open("")) > 100_000


def test_an_out_of_range_gazetteer_falls_back(tmp_path):
    """A prepared file that covers the wrong continent is as useless as none."""
    path = tmp_path / "places.parquet"
    _write_places(path, [("Cape Town", -33.9, 18.4, 100, "PPL", "ZA", "WC")])
    gz = Gazetteer.open(path, bbox=(54.0, 9.0, 56.0, 11.0))
    assert gz.has_population is False   # i.e. it is the bundled fallback


# ── Bounding box ───────────────────────────────────────────────────────────

def test_bbox_grows_the_extent_by_the_margin():
    box = bbox_around([55.0, 55.5], [10.0, 10.5], margin_km=111.32)
    min_lat, min_lon, max_lat, max_lon = box

    assert min_lat == pytest.approx(54.0, abs=0.01)
    assert max_lat == pytest.approx(56.5, abs=0.01)
    # A degree of longitude is shorter this far north, so the same margin in km
    # has to reach across more degrees of longitude than of latitude.
    assert (max_lon - 10.5) > (max_lat - 55.5)


def test_bbox_of_nothing_is_no_filter():
    assert bbox_around([], []) is None


# ── Nearest administrative seat ────────────────────────────────────────────

def test_nearest_seat_ignores_a_bigger_place_that_is_not_a_seat():
    """Population alone is not enough — GeoNames records some odd ones."""
    gz = _gazetteer([
        ("Big Village", 55.05, 10.0, 90000, "PPL",   "DK", "R"),
        ("County Town", 55.10, 10.0, 60000, "PPLA2", "DK", "R"),
    ])
    assert gz.nearest_seat(55.0, 10.0, 50000, 20.0).name == "County Town"


def test_nearest_seat_respects_the_radius():
    gz = _gazetteer([("Town", 55.20, 10.0, 60000, "PPLA2", "DK", "R")])
    assert gz.nearest_seat(55.0, 10.0, 50000, 30.0) is not None
    assert gz.nearest_seat(55.0, 10.0, 50000, 5.0) is None


def test_nearest_seat_respects_the_population_floor():
    gz = _gazetteer([("Small Seat", 55.01, 10.0, 900, "PPLA2", "DK", "R")])
    assert gz.nearest_seat(55.0, 10.0, 50000, 20.0) is None
    assert gz.nearest_seat(55.0, 10.0, 500, 20.0).name == "Small Seat"


def test_nearest_seat_skips_past_a_closer_foreign_city():
    """
    The Øresund. A k=1 query would hit Helsingborg and give up; the radius
    search has to look behind it for the Danish seat.
    """
    gz = _gazetteer([
        ("Helsingborg", 56.045, 12.694, 140000, "PPLA",  "SE", "Skåne"),
        ("Helsingør",   56.036, 12.613,  47000, "PPLA2", "DK", "H"),
    ])
    found = gz.nearest_seat(56.038, 12.650, 40000, 10.0, "DK")

    assert found.name == "Helsingør"
    assert found.cc == "DK"


def test_nearest_seat_returns_nothing_when_the_radius_is_zero():
    gz = _gazetteer([("Town", 55.0, 10.0, 60000, "PPLA2", "DK", "R")])
    assert gz.nearest_seat(55.0, 10.0, 50000, 0.0) is None


def test_a_chord_converts_back_to_the_distance_it_came_from():
    """nearest_seat searches the ECEF tree, whose metric is the chord."""
    for km in (0.5, 8.0, 120.0):
        assert _great_circle_km(_chord_km(km)) == pytest.approx(km, rel=1e-9)


# ── Municipalities ─────────────────────────────────────────────────────────

# (name, lat, lon, population, feature_code, cc, admin1, admin2, admin3, admin4)
KIEL_MUNI = ("DE", "10", "00", "01002", "01002000")
KIEL = ("Kiel", 54.3233, 10.1394, 252668, "PPLA", *KIEL_MUNI)
HOLTENAU = ("Holtenau", 54.3730, 10.1400, 0, "PPLX", *KIEL_MUNI)
KNOOP = ("Knoop", 54.3830, 10.1130, 0, "PPL",
         "DE", "10", "00", "01058", "01058005")
HOLTENAU_QUAY = (54.3730, 10.1400)


def test_a_municipality_is_resolved_through_a_dropped_district():
    """
    Kiel-Holtenau. The nearest place that survives filtering is Knoop, which is
    in a different Kreis; only the unpopulated district "Holtenau" carries the
    code that says the harbour is in Kiel. So the municipality index must be
    built before the districts are dropped — and prepare_gazetteer.py must
    write them to the file in the first place.
    """
    gz = _gazetteer([KIEL, HOLTENAU, KNOOP])
    kiel = gz.nearest_seat(54.3659, 10.1418, 50000, 8.0, "DE")

    assert kiel.name == "Kiel"
    assert gz.shares_municipality(54.3659, 10.1418, kiel) is True
    # …and Holtenau itself is not selectable as a city.
    assert gz.nearest(54.3659, 10.1418).name != "Holtenau"


def test_a_nearby_city_in_another_municipality_is_rejected():
    """
    Hellerup is 4.6 km from Copenhagen but in Gentofte kommune, so a harbour
    there is not Copenhagen's. Distance alone cannot see that.
    """
    gz = _gazetteer([
        ("Hellerup",   55.7300, 12.5800,       0, "PPL",  "DK", "17", "157"),
        ("Copenhagen", 55.6761, 12.5683, 1153615, "PPLC", "DK", "17", "101"),
    ])
    city = gz.nearest_seat(55.7300, 12.5800, 50000, 8.0, "DK")

    assert city.name == "Copenhagen"
    assert gz.shares_municipality(55.7300, 12.5800, city) is False


def test_a_gazetteer_without_division_codes_does_not_veto():
    """
    An older prepared file, or the bundled cities1000 fallback. The guard has
    to stand down rather than reject everything.
    """
    gz = _gazetteer([
        ("Hamlet", 55.7300, 12.5800,       0, "PPL",  "DK", "17"),
        ("City",   55.6761, 12.5683, 1153615, "PPLC", "DK", "17"),
    ])
    assert gz.has_municipalities is False
    assert gz.shares_municipality(55.73, 12.58, gz.nearest(55.6761, 12.5683)) is True


def test_the_finest_available_division_is_the_municipality():
    """
    Germany fills admin4, Denmark stops at admin2. Comparing whatever is
    finest keeps both usable without special-casing a country.
    """
    gz = _gazetteer([KIEL, HOLTENAU])
    kiel = gz.nearest_seat(*HOLTENAU_QUAY, 50000, 8.0, "DE")

    assert gz.has_municipalities is True
    assert gz.shares_municipality(*HOLTENAU_QUAY, kiel) is True
