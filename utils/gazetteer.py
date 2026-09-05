"""
Nearest populated place, for Phase 4's `nearest_city`.

Phase 4 used to take the city straight from `reverse_geocoder`. Two things
about that library made the answer wrong often enough to matter:

  * Its k-d tree is built on raw (lat, lon) **degree** pairs, so it minimises
    Euclidean distance in degrees. A degree of longitude is only cos(lat) as
    long as a degree of latitude — 0.56× at 56°N — which penalises places to
    the east and west. On a Danish run, 38 of 331 harbours were assigned a city
    that was not the nearest one even within its own dataset. (The library does
    ship an ECEF conversion, but nothing calls it, and it feeds degrees to a
    function expecting radians.)
  * It bundles GeoNames cities1000: population > 1000 only. The village a small
    harbour is named after is not in the file at all, so the lookup returns a
    town tens of kilometres away — 52 of those 331 harbours sat more than 10 km
    from their assigned city, the worst at 51 km.

Distances here are great-circle. The k-d tree is built in earth-centred
cartesian coordinates, where the straight-line chord between two points is
monotonic in their great-circle distance — so the tree's nearest neighbour is
genuinely the nearest neighbour on the sphere.
"""

import csv
import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from scipy.spatial import cKDTree

from utils.s3 import get_s3_filesystem, is_s3_path

logger = logging.getLogger(__name__)

EARTH_RADIUS_KM = 6371.0088

GAZETTEER_SCHEMA = pa.schema([
    pa.field("name", pa.string()),
    pa.field("lat", pa.float64()),
    pa.field("lon", pa.float64()),
    pa.field("population", pa.int32()),
    pa.field("feature_code", pa.string()),
    pa.field("cc", pa.string()),
    pa.field("admin1", pa.string()),
    # Administrative division codes, finest first when present. Used only to
    # answer "is this place in that city's municipality?" — see
    # municipality_key(). GeoNames fills them unevenly: Germany goes down to
    # admin4 (Gemeinde), Denmark stops at admin2 (kommune), and plenty of rows
    # have none at all.
    pa.field("admin2", pa.string()),
    pa.field("admin3", pa.string()),
    pa.field("admin4", pa.string()),
])

# Finest administrative division first: the smallest one a place has is the
# municipality it belongs to.
ADMIN_CODE_FIELDS = ("admin4", "admin3", "admin2")

# GeoNames codes for capitals and administrative seats. A regional seat is a
# significant place whatever its headcount, so it clears any population floor —
# that is what keeps a small county town selectable for a large harbour.
SEAT_CODES = frozenset({
    "PPLC", "PPLG", "PPLA", "PPLA2", "PPLA3", "PPLA4", "PPLA5",
})

# GeoNames feature class P is "populated place", but two groups of its codes
# are not places a harbour can be named after.
#
# Defunct — the settlement is gone, so nothing can be beside it:
#   PPLQ  abandoned          PPLW   destroyed
#   PPLH  no longer exists   PPLCH  historical capital
DEFUNCT_CODES = frozenset({"PPLQ", "PPLW", "PPLH", "PPLCH"})

# PPLX is a *section* of a place — a city district — recorded at the district,
# which for a waterfront district is nearer the quay than its own town centre.
# Most are bare labels: 157,020 of the 170,619 PPLX rows carry no population,
# and those are what turn Heiligenhafen into "Altstadt" and Copenhagen into
# "Holmen" — names shared by a hundred towns and identifying none of them.
#
# A district GeoNames actually counts people in is a different thing.
# Warnemünde (8,441) is what a harbour there should be called; the next village
# inland is not. So a populated district is kept — but only if it is not
# standing in the shadow of the city it belongs to.
SECTION_CODE = "PPLX"

# That shadow test, since the dump carries no district → parent link: a district
# is a town in its own right when the nearest *more populous* place is this far
# away. Warnemünde's is Rostock at 12 km, so it survives; Christiania's is
# Copenhagen at 2 km, so the harbour is called Copenhagen instead — which is the
# answer wanted, Christiania being 850 people inside a city of 1.1 million.
#
# On a Danish/German/Polish extract this keeps 18 districts of 159, and they are
# all the same kind of thing: Travemünde, Warnemünde, Vegesack, Harburg,
# Bergedorf, Dąbie — outlying towns a city grew around.
DISTRICT_ISOLATION_KM = 5.0

# Unknown population in a GeoNames dump is 0, which reads as "too small to be
# recorded" — exactly how a floor should treat it.
UNKNOWN_POPULATION = 0


@dataclass(frozen=True)
class Place:
    """One populated place, with its distance from the point that found it."""
    name: str
    lat: float
    lon: float
    population: int
    feature_code: str
    cc: str
    admin1: str
    distance_km: float


def _to_ecef(lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
    """(lat, lon) in degrees → earth-centred cartesian, in kilometres."""
    lat_r, lon_r = np.radians(lat), np.radians(lon)
    cos_lat = np.cos(lat_r)
    return np.column_stack([
        EARTH_RADIUS_KM * cos_lat * np.cos(lon_r),
        EARTH_RADIUS_KM * cos_lat * np.sin(lon_r),
        EARTH_RADIUS_KM * np.sin(lat_r),
    ])


def _exists(path: str | Path, s3_cfg: dict | None) -> bool:
    """
    Is the gazetteer file there?

    `pathlib.Path` collapses 's3://bucket' to 's3:/bucket', so an S3 URI has to
    go through s3fs instead. A credentials or network failure counts as "not
    there": Phase 4 then falls back to the bundled dataset with a warning
    rather than dying on an optional input.
    """
    if is_s3_path(str(path)):
        try:
            return bool(get_s3_filesystem(s3_cfg or {}).exists(str(path)))
        except Exception as exc:
            logger.warning("Could not reach gazetteer %s (%s)", path, exc)
            return False
    return Path(path).exists()


def _great_circle_km(chord_km: float) -> float:
    """Chord length between two points on the sphere → distance along it."""
    ratio = min(1.0, chord_km / (2.0 * EARTH_RADIUS_KM))
    return 2.0 * EARTH_RADIUS_KM * float(np.arcsin(ratio))


def _chord_km(great_circle_km: float) -> float:
    """The inverse: a distance along the sphere → the straight-line chord.

    Radius queries run against the ECEF tree, whose metric is the chord.
    """
    return 2.0 * EARTH_RADIUS_KM * float(
        np.sin(great_circle_km / (2.0 * EARTH_RADIUS_KM))
    )


class _AdminIndex:
    """
    Which municipality is a point in, and which municipality is a place in?

    Kept over *every* row of the gazetteer, districts included, because the
    dropped ones are often the only thing carrying the answer: the harbour at
    Kiel-Holtenau reaches Kiel through the PPLX "Holtenau", while the nearest
    surviving place, Knoop, sits in a different Kreis.

    Inactive — every lookup returns None — when the file has no division codes,
    which is the case for the bundled cities1000 dataset and for any gazetteer
    prepared before those columns existed.
    """

    def __init__(self, table: pa.Table) -> None:
        columns = {f: (table.column(f).to_pylist()
                       if f in table.schema.names else None)
                   for f in ADMIN_CODE_FIELDS}
        self.active = any(
            any(v for v in values) for values in columns.values() if values
        )
        if not self.active:
            self._keys: list[tuple | None] = []
            self._tree = None
            return

        ccs = table.column("cc").to_pylist()
        admin1 = table.column("admin1").to_pylist()
        self._keys = [
            self._key(ccs[i], admin1[i], {f: (columns[f][i] if columns[f] else "")
                                          for f in ADMIN_CODE_FIELDS})
            for i in range(table.num_rows)
        ]
        points = _to_ecef(table.column("lat").to_numpy(),
                          table.column("lon").to_numpy())
        self._tree = cKDTree(points) if len(points) else None

    @staticmethod
    def _key(cc: str, admin1: str, codes: dict[str, str]) -> tuple | None:
        """The finest division a place belongs to, or None if it has none."""
        for field in ADMIN_CODE_FIELDS:
            if codes.get(field):
                return (cc, admin1, field, codes[field])
        return None

    def key_at(self, lat: float, lon: float) -> tuple | None:
        """The municipality of the place nearest to a point."""
        if self._tree is None:
            return None
        _, position = self._tree.query(_to_ecef(np.array([lat]), np.array([lon])), k=1)
        return self._keys[int(position[0])]

    def key_of(self, place: "Place") -> tuple | None:
        """The municipality of a place already found, matched on its position."""
        return self.key_at(place.lat, place.lon)


class Gazetteer:
    """
    An in-memory index of populated places, queried by nearest great-circle.

    One k-d tree is built per population floor actually asked for, and cached —
    Phase 4 uses a handful of floors across thousands of harbours.
    """

    def __init__(self, table: pa.Table, has_population: bool = True) -> None:
        # Municipality lookups run against every row, *including* the districts
        # dropped below. Kiel is only reachable through Holtenau — a PPLX with
        # no population — because the nearest surviving place, Knoop, is in a
        # different Kreis altogether.
        self._admin = _AdminIndex(table)
        table = drop_overshadowed_districts(drop_non_settlements(table))
        self._names = table.column("name").to_pylist()
        self._codes = table.column("feature_code").to_pylist()
        self._ccs = table.column("cc").to_pylist()
        self._admin1 = table.column("admin1").to_pylist()
        self._lat = table.column("lat").to_numpy()
        self._lon = table.column("lon").to_numpy()
        self._population = table.column("population").to_numpy()
        self._has_population = has_population

        self._points = _to_ecef(self._lat, self._lon)
        self._trees: dict[int, tuple[cKDTree, np.ndarray] | None] = {}
        self._seat_trees: dict[int, tuple[cKDTree, np.ndarray] | None] = {}

    def __len__(self) -> int:
        return len(self._names)

    @property
    def has_population(self) -> bool:
        """False for a source with no population column, where floors mean nothing."""
        return self._has_population

    # -- construction -------------------------------------------------------

    @classmethod
    def from_parquet(
        cls,
        path: str | Path,
        bbox: tuple[float, float, float, float] | None = None,
        s3_cfg: dict | None = None,
    ) -> "Gazetteer":
        """
        Load a gazetteer written by `scripts/prepare_gazetteer.py`.

        `bbox` is (min_lat, min_lon, max_lat, max_lon). The file is sorted by
        latitude, so the latitude half of the filter skips whole row groups on
        their statistics — a regional run never materialises the global file.
        """
        filters = None
        if bbox is not None:
            min_lat, min_lon, max_lat, max_lon = bbox
            filters = [
                ("lat", ">=", min_lat), ("lat", "<=", max_lat),
                ("lon", ">=", min_lon), ("lon", "<=", max_lon),
            ]
        filesystem = (get_s3_filesystem(s3_cfg or {})
                      if is_s3_path(str(path)) else None)
        # Only the columns the file actually has: admin2/3/4 were added later,
        # and a gazetteer prepared before that must keep working — it simply
        # cannot answer municipality questions. Rebuilding means downloading
        # 421 MB again, which is not a thing to require for an upgrade.
        present = set(pq.read_schema(str(path), filesystem=filesystem).names)
        table = pq.read_table(
            str(path), columns=[n for n in GAZETTEER_SCHEMA.names if n in present],
            filters=filters, filesystem=filesystem,
        )
        return cls(_with_missing_columns(table))

    @classmethod
    def bundled(cls) -> "Gazetteer":
        """
        The cities1000 dataset that ships inside `reverse_geocoder`.

        The fallback when no gazetteer is configured. It carries no population
        column — every entry is over 1000 by construction — so floors do not
        apply to it, but the distances are at least computed correctly.
        """
        import reverse_geocoder as rg

        path = Path(rg.__file__).parent / "rg_cities1000.csv"
        names, lats, lons, ccs, admin1 = [], [], [], [], []
        with open(path, encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                try:
                    lats.append(float(row["lat"]))
                    lons.append(float(row["lon"]))
                except (KeyError, ValueError):
                    continue
                names.append(row.get("name", ""))
                ccs.append(row.get("cc", ""))
                admin1.append(row.get("admin1", ""))

        table = pa.table({
            "name": pa.array(names, type=pa.string()),
            "lat": pa.array(lats, type=pa.float64()),
            "lon": pa.array(lons, type=pa.float64()),
            "population": pa.array([UNKNOWN_POPULATION] * len(names), type=pa.int32()),
            "feature_code": pa.array([""] * len(names), type=pa.string()),
            "cc": pa.array(ccs, type=pa.string()),
            "admin1": pa.array(admin1, type=pa.string()),
            # cities1000 carries no division codes, so the municipality veto is
            # simply inactive on the fallback dataset.
            "admin2": pa.array([""] * len(names), type=pa.string()),
            "admin3": pa.array([""] * len(names), type=pa.string()),
            "admin4": pa.array([""] * len(names), type=pa.string()),
        }, schema=GAZETTEER_SCHEMA)
        return cls(table, has_population=False)

    @classmethod
    def open(
        cls,
        path: str | Path | None,
        bbox: tuple[float, float, float, float] | None = None,
        s3_cfg: dict | None = None,
    ) -> "Gazetteer":
        """Load `path` if it is there, else fall back to the bundled dataset."""
        if path and _exists(path, s3_cfg):
            gz = cls.from_parquet(path, bbox=bbox, s3_cfg=s3_cfg)
            if len(gz):
                logger.info("Gazetteer: %s (%d places in range)", path, len(gz))
                return gz
            logger.warning(
                "Gazetteer %s holds no place in range — falling back to the "
                "bundled cities1000 dataset", path,
            )
        elif path:
            logger.warning(
                "Gazetteer %s not found — falling back to the bundled cities1000 "
                "dataset. Run scripts/prepare_gazetteer.py to build it.", path,
            )
        gz = cls.bundled()
        logger.info("Gazetteer: bundled cities1000 (%d places)", len(gz))
        return gz

    # -- queries ------------------------------------------------------------

    def _tree_for(self, min_population: int):
        """(tree, index map) over the places clearing a floor, or None if none do."""
        if min_population not in self._trees:
            if min_population <= 0 or not self._has_population:
                indices = np.arange(len(self._names))
            else:
                seat = np.array([c in SEAT_CODES for c in self._codes], dtype=bool)
                indices = np.flatnonzero(
                    (self._population >= min_population) | seat
                )
            self._trees[min_population] = (
                (cKDTree(self._points[indices]), indices) if len(indices) else None
            )
        return self._trees[min_population]

    def nearest(self, lat: float, lon: float, min_population: int = 0) -> Place | None:
        """
        The nearest place clearing `min_population`, or None when none does.

        Administrative seats clear any floor (see SEAT_CODES), and a source
        without population data ignores floors entirely.
        """
        found = self._tree_for(min_population)
        if found is None or not len(self._names):
            return None

        tree, indices = found
        point = _to_ecef(np.array([lat]), np.array([lon]))
        chord, position = tree.query(point, k=1)
        i = int(indices[int(position[0])])

        return self._place(i, float(chord[0]))

    def _place(self, i: int, chord_km: float) -> Place:
        return Place(
            name=self._names[i],
            lat=float(self._lat[i]),
            lon=float(self._lon[i]),
            population=int(self._population[i]),
            feature_code=self._codes[i],
            cc=self._ccs[i],
            admin1=self._admin1[i],
            distance_km=_great_circle_km(chord_km),
        )

    @property
    def has_municipalities(self) -> bool:
        """False when the source carries no administrative division codes."""
        return self._admin.active

    def shares_municipality(self, lat: float, lon: float, place: Place) -> bool:
        """
        Is the point in the same municipality as `place`?

        The guard on `nearest_seat`: a big city close to a harbour is not
        necessarily the city the harbour belongs to. Hellerup lies 4.6 km from
        Copenhagen but in Gentofte kommune, Sandwig 7.9 km from Flensburg but
        in Glücksburg — while Petersdorf really is inside Rostock and Holtenau
        inside Kiel. Measured over 28 candidate overrides, this confirmed 22
        and rejected 6, every rejection a correction.

        Returns True when the gazetteer has no division codes, so an older file
        keeps the un-guarded behaviour rather than losing the feature.
        """
        if not self._admin.active:
            return True
        here = self._admin.key_at(lat, lon)
        return here is not None and here == self._admin.key_of(place)

    def _seat_tree_for(self, min_population: int):
        """(tree, index map) over administrative seats of at least a size."""
        if min_population not in self._seat_trees:
            indices = np.flatnonzero(
                np.array([c in SEAT_CODES for c in self._codes], dtype=bool)
                & (self._population >= min_population)
            )
            self._seat_trees[min_population] = (
                (cKDTree(self._points[indices]), indices) if len(indices) else None
            )
        return self._seat_trees[min_population]

    def nearest_seat(
        self,
        lat: float,
        lon: float,
        min_population: int,
        max_km: float,
        cc: str | None = None,
    ) -> Place | None:
        """
        The nearest administrative seat of at least `min_population` within
        `max_km`, optionally restricted to one country.

        This is how a port gets named after the city it belongs to rather than
        the nameless hamlet nearest the quay: Rostock's Überseehafen is 0.6 km
        from Petersdorf (population 0) and 7.5 km from Rostock.

        A radius search rather than a nearest-neighbour one, because the country
        filter has to be applied *before* choosing: at Helsingør the closest
        large seat is Helsingborg, across the Øresund in Sweden, and a `k=1`
        query would return it and then have to give up rather than looking for
        the Danish one behind it.
        """
        found = self._seat_tree_for(min_population)
        if found is None or max_km <= 0:
            return None

        tree, indices = found
        point = _to_ecef(np.array([lat]), np.array([lon]))[0]
        within = tree.query_ball_point(point, _chord_km(max_km))
        if not within:
            return None

        best, best_chord = None, None
        for position in within:
            i = int(indices[position])
            if cc and self._ccs[i] != cc:
                continue
            chord = float(np.linalg.norm(self._points[i] - point))
            if best_chord is None or chord < best_chord:
                best, best_chord = i, chord

        return None if best is None else self._place(best, best_chord)


def _with_missing_columns(table: pa.Table) -> pa.Table:
    """Pad a table up to GAZETTEER_SCHEMA, filling absent columns with ''.

    Lets a gazetteer built before a column existed load unchanged; the features
    that need it simply stay inactive.
    """
    for field in GAZETTEER_SCHEMA:
        if field.name not in table.schema.names:
            table = table.append_column(
                field, pa.array([""] * table.num_rows, type=field.type)
            )
    return table.select(GAZETTEER_SCHEMA.names)


def is_settlement(feature_code: str, population: int) -> bool:
    """
    Is this class-P row somewhere a harbour can be named after?

    The row-at-a-time form, for `scripts/prepare_gazetteer.py` streaming the
    dump. `drop_non_settlements` is the same rule over a whole table.
    """
    if feature_code in DEFUNCT_CODES:
        return False
    if feature_code == SECTION_CODE:
        return population > 0
    return True


def drop_overshadowed_districts(
    table: pa.Table,
    radius_km: float = DISTRICT_ISOLATION_KM,
) -> pa.Table:
    """
    Drop districts that sit inside the city they belong to.

    A PPLX with a bigger place within `radius_km` is a neighbourhood, and the
    harbour beside it belongs to the city; one with no bigger place that close
    is an absorbed town that kept its own name, and the harbour is genuinely
    called after it. See DISTRICT_ISOLATION_KM.

    Row-at-a-time filtering cannot answer this, so unlike `is_settlement` it
    runs on load rather than in `scripts/prepare_gazetteer.py`. Callers pass a
    region, not the globe, and `bbox_around` pads by 100 km — twenty times this
    radius — so a district never loses its city to the edge of the box.
    """
    codes = table.column("feature_code").to_numpy(zero_copy_only=False)
    districts = np.flatnonzero(codes == SECTION_CODE)
    if not len(districts):
        return table

    population = table.column("population").to_numpy()
    points = _to_ecef(table.column("lat").to_numpy(), table.column("lon").to_numpy())
    tree = cKDTree(points)

    keep = np.ones(len(codes), dtype=bool)
    for i in districts:
        neighbours = tree.query_ball_point(points[i], radius_km)
        keep[i] = not np.any(population[neighbours] > population[i])
    return table.filter(pa.array(keep))


def drop_non_settlements(table: pa.Table) -> pa.Table:
    """
    Apply `is_settlement` to a table.

    Applied on load rather than only at prepare time so that a file built
    before this filter existed is corrected in place — a rebuild means
    downloading 421 MB again.
    """
    codes, population = table.column("feature_code"), table.column("population")
    keep = pc.and_(
        pc.invert(pc.is_in(codes, value_set=pa.array(sorted(DEFUNCT_CODES)))),
        pc.or_(pc.not_equal(codes, SECTION_CODE), pc.greater(population, 0)),
    )
    return table.filter(pc.fill_null(keep, True))


def bbox_around(
    lats,
    lons,
    margin_km: float = 100.0,
) -> tuple[float, float, float, float] | None:
    """
    A (min_lat, min_lon, max_lat, max_lon) box covering points plus a margin.

    The margin has to be generous: it only needs to reach the nearest inhabited
    place, and an offshore harbour can be a long way from one. Returns None for
    an empty input, meaning "no filter".
    """
    lats = [float(v) for v in lats if v is not None]
    lons = [float(v) for v in lons if v is not None]
    if not lats or not lons:
        return None

    lat_margin = margin_km / 111.32
    # A degree of longitude shrinks with latitude; use the widest latitude in
    # the set so the box is generous at its narrowest point rather than short.
    widest = max(abs(min(lats)), abs(max(lats)))
    lon_margin = margin_km / max(1.0, 111.32 * float(np.cos(np.radians(widest))))

    return (
        max(-90.0, min(lats) - lat_margin),
        max(-180.0, min(lons) - lon_margin),
        min(90.0, max(lats) + lat_margin),
        min(180.0, max(lons) + lon_margin),
    )
