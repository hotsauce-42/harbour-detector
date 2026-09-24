"""
Compare two harbour databases as peers.

The pipeline has no external check on whether the harbours it finds are real.
This module is the measuring apparatus for pointing it at a database built by
some other program: which harbours the two agree on, how closely their shapes
agree, and what each side holds alone.

Neither side is treated as truth. The vocabulary is `only_in_old` /
`only_in_new`, never "miss" or "false positive", because the other program has
its own error rate and nothing here can tell which side is wrong.

Two things make this harder than matching two of our own runs:

**No shared keys.** A foreign database has no `harbour_id` and no H3 cells, so
`pipeline.id_matching`'s cell-overlap matching cannot be used. Correspondence
has to come from the polygons themselves.

**No shared convention.** Our polygon is the trafficked water — a 75 m closing
of the cells where ships actually stopped. Another database may draw the whole
administrative port area, land included. When it does, a *perfect* detection
scores an IoU around 0.15, so linking on IoU would report a perfect result as a
disagreement. Nothing here gates on IoU. Pairs link on overlap *area*, on
one-sided containment, or on the gap between them — each of which survives a
scale mismatch — and `calibrate()` measures the convention gap and names it so
the reader knows which geometry number is worth reading.

Correspondence is not forced one-to-one. Links form a bipartite graph and the
answer is its connected components, so a port one side splits into three basins
reads as a single 1:3 group rather than one disappearance and three inventions.
"""

from __future__ import annotations

import json
import math
import re
import unicodedata
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, Sequence

import numpy as np
import pycountry
import shapely
from shapely.geometry import box, shape
from shapely.ops import transform

from utils.geo import (
    METERS_PER_DEGREE,
    _local_metric_frame,
    clean_polygon,
    haversine_meters,
)
from utils.overrides import resolve_country_iso2

# Property keys a foreign harbour database might carry, most specific first.
# Ties in fill rate are broken by this order, so put the unambiguous spellings
# ahead of the generic ones.
ID_FIELDS = ("harbour_id", "harbor_id", "port_id", "locode", "unlocode",
             "un_locode", "id", "ID", "fid", "FID", "objectid", "OBJECTID")
NAME_FIELDS = ("port_name", "PORT_NAME", "harbour_name", "harbor_name",
               "portname", "name", "NAME", "Name", "title", "label",
               "nearest_city")   # last: lets one of our own runs be the 'old'
COUNTRY_FIELDS = ("country_iso2", "iso2", "ISO2", "iso_a2", "ISO_A2",
                  "country_code", "COUNTRY", "country", "Country", "cntry",
                  "iso3", "ISO3", "iso_a3")

# Words that describe *that* something is a port rather than *which* port it
# is. Removed only as whole tokens, never as a suffix: Danish and Norwegian
# city names end in these (København, Frederikshavn, Bergen's Sandviken), and
# stripping the ending would turn a city into a stump that matches nothing.
PORT_WORDS = frozenset({
    "port", "ports", "harbour", "harbor", "haven",      # en / nl
    "havn", "hamn", "hafen",                            # da-no / sv / de
    "marina", "lystbadehavn", "of", "de", "du", "da",
})

# Letters NFKD does not decompose, because they are letters in their own right
# rather than an accented a/o/d. Without this København normalises to
# "k benhavn" — the ø survives the decomposition, then the non-ASCII filter
# below splits the word in two, and the city never matches itself.
TRANSLITERATE = str.maketrans({
    "ø": "o", "æ": "ae", "å": "aa", "ß": "ss",
    "ð": "d", "þ": "th", "ł": "l", "đ": "d", "ħ": "h", "ı": "i",
})


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Record:
    """One harbour from one side, normalised to what a comparison needs."""

    side: str                      # "old" | "new"
    rec_id: str
    geom: Any                      # valid Polygon/MultiPolygon, WGS84 degrees
    lat: float
    lon: float
    name: Optional[str] = None
    country: Optional[str] = None  # ISO 3166-1 alpha-2
    props: dict = field(default_factory=dict, compare=False, repr=False)


@dataclass(frozen=True)
class FieldChoice:
    """Which properties a foreign database was read through, and how full."""

    id_field: Optional[str]
    name_field: Optional[str]
    country_field: Optional[str]
    fill: dict = field(default_factory=dict, compare=False)


@dataclass(frozen=True)
class Link:
    """A candidate correspondence between one old and one new harbour."""

    old_id: str
    new_id: str
    area_old_m2: float
    area_new_m2: float
    inter_m2: float
    union_m2: float
    iou: float
    coverage_old: float            # intersection / old area
    coverage_new: float            # intersection / new area
    gap_m: float                   # min surface distance, 0 when overlapping
    centroid_m: float              # reported for interpretability, never gated


@dataclass(frozen=True)
class Component:
    """A connected group of records: the unit correspondence is decided on."""

    comp_id: int
    old_ids: tuple[str, ...]
    new_ids: tuple[str, ...]
    cardinality: str               # "1:1" "1:0" "0:1" "1:N" "N:1" "N:M"
    tangled: bool = False


@dataclass(frozen=True)
class Calibration:
    """Whether the two databases mean the same thing by 'harbour polygon'."""

    regime: str                    # comparable | old_is_superset |
                                   # new_is_superset | incomparable
    median_area_ratio: float       # new / old
    median_coverage_old: float
    median_coverage_new: float
    median_iou: float
    n_pairs: int

    @property
    def headline(self) -> str:
        """The geometry statistic that is honest under this regime."""
        return {
            "comparable": "iou",
            "old_is_superset": "coverage_new",
            "new_is_superset": "coverage_old",
        }.get(self.regime, "gap_m")

    @property
    def note(self) -> str:
        if self.n_pairs == 0:
            return ("No unambiguous 1:1 pair to calibrate on — the geometry "
                    "statistics below are unverified.")
        if self.regime == "comparable":
            return ("Both databases draw harbours at the same scale "
                    f"(median area ratio {self.median_area_ratio:.2f}). "
                    "IoU is a fair quality number.")
        if self.regime == "old_is_superset":
            return ("The old database draws larger areas than the pipeline "
                    f"(median area ratio {self.median_area_ratio:.2f}, "
                    f"{self.median_coverage_new:.0%} of each new shape falls "
                    "inside its old one). IoU is structurally capped and is "
                    "shown for reference only — read coverage_new instead.")
        if self.regime == "new_is_superset":
            return ("The pipeline draws larger areas than the old database "
                    f"(median area ratio {self.median_area_ratio:.2f}). "
                    "Read coverage_old rather than IoU.")
        return ("The two databases do not agree on what a harbour polygon is, "
                "so the geometry statistics are not a quality measure. The "
                "agreement rates, which depend only on whether a "
                "correspondence exists, remain valid.")


@dataclass(frozen=True)
class Diagnosis:
    """Why one side holds a harbour the other does not."""

    rec_id: str
    bucket: str
    n_stops: int
    n_cells: int
    n_clusters: int
    nearest_id: Optional[str] = None
    nearest_gap_m: Optional[float] = None


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def _clean(geom_json: Any):
    """A repaired areal geometry, or None when the feature encloses no area."""
    if not geom_json:
        return None
    try:
        return clean_polygon(shape(geom_json))
    except Exception:
        return None


def _read_features(path: Path) -> list[dict]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, dict) and data.get("type") == "FeatureCollection":
        return list(data.get("features") or [])
    if isinstance(data, dict) and data.get("type") == "Feature":
        return [data]
    if isinstance(data, list):
        return list(data)
    raise ValueError(f"{path} is not a GeoJSON FeatureCollection")


def _is_filled(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, float) and math.isnan(value):
        return False
    return str(value).strip() != ""


def detect_field(
    features: Sequence[dict], candidates: Sequence[str],
) -> tuple[Optional[str], float]:
    """
    Pick the property that actually carries the data, and say how full it is.

    Chosen by highest non-null fill rate rather than by first key present: a
    database that carries both an empty `name` and a populated `PORT_NAME`
    would otherwise be read through the empty one, and the comparison would
    silently lose every name without anything looking wrong.
    """
    best, best_fill, best_rank = None, 0.0, len(candidates)
    total = len(features) or 1
    for rank, key in enumerate(candidates):
        filled = sum(
            1 for f in features
            if _is_filled((f.get("properties") or {}).get(key))
        )
        fill = filled / total
        if fill > best_fill or (fill == best_fill and fill > 0
                                and rank < best_rank):
            best, best_fill, best_rank = key, fill, rank
    return best, best_fill


def _unique_ids(raw: list[Optional[str]], prefix: str) -> list[str]:
    """
    Stable, unique ids. A synthesised one is positional, so re-running against
    the same file reproduces it; a duplicate real id is suffixed rather than
    silently collapsing two harbours into one.
    """
    width = max(4, len(str(len(raw))))
    seen: dict[str, int] = {}
    out: list[str] = []
    for i, value in enumerate(raw):
        rec_id = str(value).strip() if _is_filled(value) else \
            f"{prefix}-{i + 1:0{width}d}"
        if rec_id in seen:
            seen[rec_id] += 1
            rec_id = f"{rec_id}#{seen[rec_id]}"
        else:
            seen[rec_id] = 0
        out.append(rec_id)
    return out


def load_new(path: Path) -> tuple[list[Record], list[str]]:
    """
    Load the pipeline's own `harbours.geojson`.

    The effective geometry is `feature.geometry`, not any `*_wkt` property:
    `outline_wkt` is a Parquet-only column, and the GeoJSON carries the merged
    detected-plus-manual outline in the geometry itself.

    The centroid comes from `centroid_lat`/`centroid_lon`, which is the
    event-weighted one Phase 3 computed. Recomputing it from the polygon would
    move it into the geometric middle and silently change every distance this
    module reports.
    """
    features = _read_features(path)
    ids = _unique_ids(
        [(f.get("properties") or {}).get("harbour_id") for f in features],
        "NEW",
    )
    records, invalid = [], []
    for rec_id, feat in zip(ids, features):
        props = feat.get("properties") or {}
        geom = _clean(feat.get("geometry"))
        if geom is None:
            invalid.append(rec_id)
            continue
        lat, lon = props.get("centroid_lat"), props.get("centroid_lon")
        if not _is_filled(lat) or not _is_filled(lon):
            lat, lon = geom.centroid.y, geom.centroid.x
        records.append(Record(
            side="new", rec_id=rec_id, geom=geom,
            lat=float(lat), lon=float(lon),
            name=props.get("nearest_city") or None,
            country=normalise_country(props.get("country_iso2")),
            props=props,
        ))
    return records, invalid


def load_old(
    path: Path, *,
    id_field: Optional[str] = None,
    name_field: Optional[str] = None,
    country_field: Optional[str] = None,
) -> tuple[list[Record], FieldChoice, list[str]]:
    """
    Load a foreign harbour database, detecting its property names.

    Returns the records, the fields it read them through (so the caller can
    print the choice — a wrong guess here quietly wrecks every attribute
    comparison), and the ids of features that enclosed no usable area.
    """
    features = _read_features(path)
    fill: dict[str, float] = {}

    if id_field is None:
        id_field, fill["id"] = detect_field(features, ID_FIELDS)
    if name_field is None:
        name_field, fill["name"] = detect_field(features, NAME_FIELDS)
    if country_field is None:
        country_field, fill["country"] = detect_field(features, COUNTRY_FIELDS)

    ids = _unique_ids(
        [(f.get("properties") or {}).get(id_field) if id_field else None
         for f in features],
        "OLD",
    )
    records, invalid = [], []
    for rec_id, feat in zip(ids, features):
        props = feat.get("properties") or {}
        geom = _clean(feat.get("geometry"))
        if geom is None:
            invalid.append(rec_id)
            continue
        records.append(Record(
            side="old", rec_id=rec_id, geom=geom,
            lat=geom.centroid.y, lon=geom.centroid.x,
            name=(props.get(name_field) if name_field else None) or None,
            country=normalise_country(
                props.get(country_field) if country_field else None),
            props=props,
        ))
    choice = FieldChoice(id_field, name_field, country_field, fill)
    return records, choice, invalid


def oversized(records: Sequence[Record], max_span_km: float) -> list[str]:
    """
    Records whose bounding box is too big to be one harbour.

    A continent-sized polygon is a data error, and left in it would make the
    candidate search return every record on the other side.
    """
    out = []
    for r in records:
        minx, miny, maxx, maxy = r.geom.bounds
        span = haversine_meters(miny, minx, maxy, maxx) / 1000.0
        if span > max_span_km:
            out.append(r.rec_id)
    return out


# ---------------------------------------------------------------------------
# Attribute normalisation
# ---------------------------------------------------------------------------

@lru_cache(maxsize=4096)
def normalise_country(value: Optional[str]) -> Optional[str]:
    """
    Any spelling of a country to ISO 3166-1 alpha-2, or None.

    Memoised because the fallback reaches `pycountry.search_fuzzy`, which is
    slow enough to matter across thousands of records and occasionally
    surprising enough that it should be called once per distinct spelling.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in ("nan", "none"):
        return None
    if len(text) == 2 and text.isalpha():
        return text.upper()
    if len(text) == 3 and text.isalpha():
        found = pycountry.countries.get(alpha_3=text.upper())
        if found is not None:
            return found.alpha_2
    if text.isdigit():
        found = pycountry.countries.get(numeric=text.zfill(3))
        return found.alpha_2 if found is not None else None
    return resolve_country_iso2(text)


def normalise_name(value: Optional[str]) -> str:
    """Casefolded, unaccented, port-words removed — for comparing names."""
    if not _is_filled(value):
        return ""
    text = str(value).casefold().translate(TRANSLITERATE)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    tokens = [t for t in re.split(r"[^0-9a-z]+", text) if t]
    kept = [t for t in tokens if t not in PORT_WORDS]
    return " ".join(kept or tokens)


def compare_names(old_name: Optional[str], new_name: Optional[str]) -> str:
    """
    `exact` / `contains` / `differs` / `unknown`.

    Deliberately not called name accuracy: the pipeline has no port name, only
    `nearest_city` from the gazetteer. This scores Phase 4's city pick against
    whatever the other database calls the place, which is a weaker question
    than whether either name is right.
    """
    a, b = normalise_name(old_name), normalise_name(new_name)
    if not a or not b:
        return "unknown"
    if a == b:
        return "exact"
    ta, tb = set(a.split()), set(b.split())
    return "contains" if ta & tb else "differs"


# ---------------------------------------------------------------------------
# Geometry
# ---------------------------------------------------------------------------

def metric_frame(lat0: float, lon0: float):
    """
    Transforms between WGS84 degrees and metres around (lat0, lon0).

    Named here so this module's dependency on a local flat-earth frame is
    explicit. There is no pyproj in this project by design, and areas must be
    in m²: a degree of longitude is 0.56 × a degree of latitude at 56°N, so an
    area computed in degrees² would be wrong by that factor and would vary with
    latitude across the run.
    """
    return _local_metric_frame(lat0, lon0)


def link_metrics(old: Record, new: Record) -> Link:
    """Every geometric statistic for one pair, measured in metres."""
    to_m, _ = metric_frame((old.lat + new.lat) / 2, (old.lon + new.lon) / 2)
    g_old, g_new = transform(to_m, old.geom), transform(to_m, new.geom)

    area_old, area_new = g_old.area, g_new.area
    inter = g_old.intersection(g_new).area
    union = area_old + area_new - inter
    return Link(
        old_id=old.rec_id, new_id=new.rec_id,
        area_old_m2=area_old, area_new_m2=area_new,
        inter_m2=inter, union_m2=union,
        iou=inter / union if union > 0 else 0.0,
        coverage_old=inter / area_old if area_old > 0 else 0.0,
        coverage_new=inter / area_new if area_new > 0 else 0.0,
        gap_m=0.0 if inter > 0 else g_old.distance(g_new),
        centroid_m=haversine_meters(old.lat, old.lon, new.lat, new.lon),
    )


def candidate_pairs(
    old: Sequence[Record], new: Sequence[Record], *, max_gap_m: float,
) -> list[tuple[int, int]]:
    """
    Index pairs close enough to be worth measuring exactly.

    An STRtree envelope query only — never `query_nearest`, which minimises
    distance in degree space and would penalise east-west separation by ~1.8×
    at these latitudes. Envelope containment is a metric-free test, so this
    prefilter introduces no projection error of its own; the real distances are
    computed afterwards in `link_metrics`.
    """
    if not old or not new:
        return []
    tree = shapely.STRtree([r.geom for r in old])
    pairs: list[tuple[int, int]] = []
    for j, rec in enumerate(new):
        minx, miny, maxx, maxy = rec.geom.bounds
        dlat = max_gap_m / METERS_PER_DEGREE
        cos_lat = max(abs(math.cos(math.radians(rec.lat))), 1e-6)
        dlon = max_gap_m / (METERS_PER_DEGREE * cos_lat)
        envelope = box(minx - dlon, miny - dlat, maxx + dlon, maxy + dlat)
        for i in tree.query(envelope):
            pairs.append((int(i), j))
    return pairs


def links(
    old: Sequence[Record], new: Sequence[Record], *,
    min_coverage: float, max_gap_m: float, min_overlap_m2: float,
) -> list[Link]:
    """
    Every pair that plausibly describes the same harbour.

    A pair links on *any* of three independent grounds, and deliberately not on
    IoU:

    1. a real shared area — enough overlap that no convention gap explains it;
    2. one-sided containment — one shape mostly inside the other, which is what
       a whole-port polygon does to a trafficked-water one;
    3. a small gap — for the reverse case, a tight quay-line polygon that never
       quite reaches the water where ships moor.

    Linking on IoU would bake one database's idea of a harbour polygon into the
    result: under a whole-port convention a perfect detection caps around 0.15,
    and every harbour would be reported as a disagreement.
    """
    out = []
    for i, j in candidate_pairs(old, new, max_gap_m=max_gap_m):
        lk = link_metrics(old[i], new[j])
        if (lk.inter_m2 >= min_overlap_m2
                or max(lk.coverage_old, lk.coverage_new) >= min_coverage
                or lk.gap_m <= max_gap_m):
            out.append(lk)
    return out


# ---------------------------------------------------------------------------
# Components
# ---------------------------------------------------------------------------

def _cardinality(n_old: int, n_new: int) -> str:
    if n_old == 1 and n_new == 1:
        return "1:1"
    if n_new == 0:
        return "1:0"
    if n_old == 0:
        return "0:1"
    if n_old == 1:
        return "1:N"
    if n_new == 1:
        return "N:1"
    return "N:M"


def components(
    old: Sequence[Record], new: Sequence[Record], link_list: Sequence[Link], *,
    find_components: Callable[[dict], list[list[str]]],
    max_component: int = 8,
) -> list[Component]:
    """
    Group both sides into connected correspondence groups.

    The graph is seeded with every record, linked or not, so an unlinked one
    falls out as its own singleton component and needs no separate bookkeeping
    to be found again.

    `find_components` is injected rather than imported: this module lives in
    `utils/`, which must not import from `pipeline/`. The caller passes
    `pipeline.cluster_formation._connected_components`, so the whole project
    uses one BFS.

    A group larger than `max_component` is marked `tangled`. Big polygons chain
    — old A meets new X, new X meets old B — and one chain can swallow a port
    city. It is still reported, but kept out of the statistics so it cannot
    move a median on its own.
    """
    graph: dict[str, set[str]] = {}
    for r in old:
        graph[f"old:{r.rec_id}"] = set()
    for r in new:
        graph[f"new:{r.rec_id}"] = set()
    for lk in link_list:
        a, b = f"old:{lk.old_id}", f"new:{lk.new_id}"
        graph[a].add(b)
        graph[b].add(a)

    out = []
    for nodes in find_components(graph):
        olds = tuple(sorted(n[4:] for n in nodes if n.startswith("old:")))
        news = tuple(sorted(n[4:] for n in nodes if n.startswith("new:")))
        out.append(Component(
            comp_id=0, old_ids=olds, new_ids=news,
            cardinality=_cardinality(len(olds), len(news)),
            tangled=len(olds) + len(news) > max_component,
        ))
    # Deterministic order, so two runs over the same inputs number alike.
    out.sort(key=lambda c: (c.old_ids[:1] or ("~",), c.new_ids[:1] or ("~",)))
    return [
        Component(i, c.old_ids, c.new_ids, c.cardinality, c.tangled)
        for i, c in enumerate(out)
    ]


def primary_links(link_list: Sequence[Link]) -> dict[tuple[str, str], Link]:
    """
    Each record's single best counterpart, keyed by (side, rec_id).

    Computed per record rather than per component: a 2:3 group has no one
    primary pair, but every record in it still has a best partner. Sorted by
    greatest shared area, then smallest gap, then nearest centroid, then the
    counterpart id — the last is a deterministic tie-break, so the choice never
    depends on dict ordering.
    """
    best: dict[tuple[str, str], Link] = {}
    for lk in sorted(link_list,
                     key=lambda k: (-k.inter_m2, k.gap_m, k.centroid_m,
                                    k.new_id, k.old_id)):
        best.setdefault(("old", lk.old_id), lk)
        best.setdefault(("new", lk.new_id), lk)
    return best


# ---------------------------------------------------------------------------
# Calibration and scoring
# ---------------------------------------------------------------------------

def _median(values: Iterable[float]) -> float:
    arr = np.asarray(list(values), dtype=float)
    return float(np.median(arr)) if arr.size else 0.0


def calibrate(
    link_list: Sequence[Link], comps: Sequence[Component],
) -> Calibration:
    """
    Decide whether the two databases mean the same thing by a harbour polygon.

    Measured only on 1:1 components, which are the pairs whose correspondence
    is unambiguous, and only to *label* the result: it never moves the link
    thresholds. Tuning the gates from the data would make the outcome depend on
    the data it is scoring, and would paper over the disagreement rather than
    name it.
    """
    by_pair = {(lk.old_id, lk.new_id): lk for lk in link_list}
    pairs = [
        by_pair[(c.old_ids[0], c.new_ids[0])]
        for c in comps
        if c.cardinality == "1:1" and not c.tangled
        and (c.old_ids[0], c.new_ids[0]) in by_pair
    ]
    if not pairs:
        return Calibration("incomparable", 0.0, 0.0, 0.0, 0.0, 0)

    ratio = _median(
        lk.area_new_m2 / lk.area_old_m2 for lk in pairs if lk.area_old_m2 > 0
    )
    cov_old = _median(lk.coverage_old for lk in pairs)
    cov_new = _median(lk.coverage_new for lk in pairs)
    iou = _median(lk.iou for lk in pairs)

    if cov_new >= 0.8 and ratio < 0.5:
        regime = "old_is_superset"
    elif cov_old >= 0.8 and ratio > 2.0:
        regime = "new_is_superset"
    elif cov_old >= 0.5 and cov_new >= 0.5 and 0.5 <= ratio <= 2.0:
        regime = "comparable"
    else:
        regime = "incomparable"
    return Calibration(regime, ratio, cov_old, cov_new, iou, len(pairs))


def score(
    old: Sequence[Record], new: Sequence[Record], comps: Sequence[Component],
) -> dict:
    """
    Agreement from each side's point of view.

    Two separate rates, never combined. An F1 would be a ground-truth score,
    and there is no ground truth here — folding the two into one number would
    assert that one side's omissions and the other's are the same kind of
    error.
    """
    matched_old = {i for c in comps if c.new_ids for i in c.old_ids}
    matched_new = {i for c in comps if c.old_ids for i in c.new_ids}
    n_old, n_new = len(old), len(new)
    cardinality: dict[str, int] = {}
    for c in comps:
        cardinality[c.cardinality] = cardinality.get(c.cardinality, 0) + 1
    return {
        "n_old": n_old,
        "n_new": n_new,
        "matched_old": len(matched_old),
        "matched_new": len(matched_new),
        "only_in_old": n_old - len(matched_old),
        "only_in_new": n_new - len(matched_new),
        "agreement_old": len(matched_old) / n_old if n_old else 0.0,
        "agreement_new": len(matched_new) / n_new if n_new else 0.0,
        "cardinality": dict(sorted(cardinality.items())),
        "n_tangled": sum(1 for c in comps if c.tangled),
    }


def stratify(
    records: Sequence[Record], comps: Sequence[Component],
    key_fn: Callable[[Record], str], *, side: str = "old",
) -> dict[str, dict]:
    """Agreement broken down by some property of the records."""
    matched = {
        i for c in comps
        if (c.new_ids if side == "old" else c.old_ids)
        for i in (c.old_ids if side == "old" else c.new_ids)
    }
    buckets: dict[str, dict] = {}
    for r in records:
        b = buckets.setdefault(key_fn(r), {"n": 0, "matched": 0})
        b["n"] += 1
        b["matched"] += 1 if r.rec_id in matched else 0
    for b in buckets.values():
        b["agreement"] = b["matched"] / b["n"] if b["n"] else 0.0
    return dict(sorted(buckets.items()))


def area_quartiles(records: Sequence[Record]) -> dict[str, tuple[float, float]]:
    """Area-quartile edges in m², for stratifying by harbour size."""
    areas = []
    for r in records:
        to_m, _ = metric_frame(r.lat, r.lon)
        areas.append(transform(to_m, r.geom).area)
    if not areas:
        return {}
    edges = np.percentile(np.asarray(areas), [0, 25, 50, 75, 100])
    return {
        f"Q{i + 1}": (float(edges[i]), float(edges[i + 1]))
        for i in range(4)
    }


def area_m2(rec: Record) -> float:
    """One record's area in m², in a frame centred on itself."""
    to_m, _ = metric_frame(rec.lat, rec.lon)
    return float(transform(to_m, rec.geom).area)


# ---------------------------------------------------------------------------
# Coverage — built but off by default (see the script's --coverage-gate)
# ---------------------------------------------------------------------------

def coverage_cells(
    lats: Sequence[float], lons: Sequence[float], *,
    resolution: int = 5, dilate: int = 1,
) -> set[str]:
    """
    The region the AIS input actually covered, as coarse H3 cells.

    Built from where vessels really stopped, never from a bounding box: the
    box around a Danish run reaches from the North Sea to inland Poland, so it
    would admit as "covered" a great deal of water no vessel was ever seen in.

    Resolution 5 is ~8 km across; one ring of dilation puts a ~25 km tolerance
    around each mooring, which is loose enough not to punish a harbour sitting
    just outside a stop cluster.
    """
    import h3

    cells: set[str] = set()
    for lat, lon in zip(lats, lons):
        if lat is None or lon is None:
            continue
        cells.add(h3.latlng_to_cell(float(lat), float(lon), resolution))
    if dilate <= 0:
        return cells
    grown: set[str] = set()
    for cell in cells:
        grown.update(h3.grid_disk(cell, dilate))
    return grown


def in_coverage(
    records: Sequence[Record], cells: set[str], *, resolution: int = 5,
) -> tuple[list[Record], list[Record]]:
    """Split records into (inside, outside) the covered region."""
    import h3

    if not cells:
        return list(records), []
    inside, outside = [], []
    for r in records:
        minx, miny, maxx, maxy = r.geom.bounds
        probes = [(r.lat, r.lon), (miny, minx), (miny, maxx),
                  (maxy, minx), (maxy, maxx)]
        hit = any(
            h3.latlng_to_cell(lat, lon, resolution) in cells
            for lat, lon in probes
        )
        (inside if hit else outside).append(r)
    return inside, outside


# ---------------------------------------------------------------------------
# Diagnosis
# ---------------------------------------------------------------------------

def load_interim_layers(interim_dir: Path) -> dict[str, tuple]:
    """
    The pipeline's intermediate evidence, as (lats, lons) point layers.

    Each layer answers a different "how far did this harbour get":
    `stops` is every extracted stop, `cells` the H3 cells that survived the
    Phase-2 noise floor, `clusters` the clusters that survived Phase 3.

    Missing layers are simply absent from the result — the caller degrades to
    an undiagnosed verdict rather than failing. `stops.parquet` is a Spark
    *directory* that can exist and hold no part files, which pyarrow raises on;
    that is the same situation as the file being absent and is handled so.
    """
    import pandas as pd

    spec = {
        "stops": ("stops.parquet", "lat", "lon"),
        "cells": ("h3_counts.parquet", "cell_lat", "cell_lon"),
        "clusters": ("harbour_clusters.parquet", "centroid_lat",
                     "centroid_lon"),
    }
    layers: dict[str, tuple] = {}
    for key, (name, lat_col, lon_col) in spec.items():
        path = Path(interim_dir) / name
        if not path.exists():
            continue
        try:
            df = pd.read_parquet(path, columns=[lat_col, lon_col])
        except Exception:
            continue
        layers[key] = (df[lat_col].to_numpy(), df[lon_col].to_numpy())
    return layers


def _buffered(records: Sequence[Record], buffer_m: float) -> list:
    """Each polygon grown by buffer_m, in a frame centred on itself."""
    out = []
    for r in records:
        to_m, to_deg = metric_frame(r.lat, r.lon)
        out.append(transform(to_deg,
                             transform(to_m, r.geom).buffer(buffer_m)))
    return out


def _counts_inside(geoms: list, layer: Optional[tuple]) -> np.ndarray:
    """How many points of a layer fall in each geometry — one bulk query."""
    if not geoms:
        return np.zeros(0, dtype=int)
    if layer is None:
        return np.full(len(geoms), -1, dtype=int)
    lats, lons = layer
    if len(lats) == 0:
        return np.zeros(len(geoms), dtype=int)
    tree = shapely.STRtree(geoms)
    hits = tree.query(shapely.points(np.asarray(lons, dtype=float),
                                     np.asarray(lats, dtype=float)),
                      predicate="intersects")
    # query() over an array returns [input_index, tree_index]; we want the
    # tally per tree geometry, which is the second row.
    return np.bincount(hits[1], minlength=len(geoms)).astype(int)


def diagnose_only_in_old(
    records: Sequence[Record], other: Sequence[Record],
    layers: dict[str, tuple], *,
    buffer_m: float = 200.0, nearby_m: float = 1000.0,
) -> dict[str, Diagnosis]:
    """
    How far each unmatched old harbour got through the pipeline.

    Buckets, in order — the first that fits wins:

    `unpaired_nearby`      a new harbour sits within `nearby_m` but the link
                           rule did not fire; the two disagree about extent,
                           not about the harbour existing.
    `no_stops`             no extracted stop falls inside it. Note this cannot
                           separate "no AIS here at all" from "one vessel,
                           below the Phase-2 `min_unique_mmsi` floor", because
                           the stop file is written before that floor but the
                           cell file after it.
    `stops_below_cell_floor`  stops, but no surviving H3 cell.
    `below_cluster_floor`  cells, but no surviving cluster. Reached **by
                           elimination**: `harbour_clusters.parquet` holds only
                           the clusters that passed `_filter_clusters`, so a
                           cluster that was formed and then dropped leaves no
                           trace. Inferred, not observed.
    `detected_not_linked`  a cluster is there and nothing above explains it.
    `unknown`              an interim layer was missing.
    """
    geoms = _buffered(records, buffer_m)
    stops = _counts_inside(geoms, layers.get("stops"))
    cells = _counts_inside(geoms, layers.get("cells"))
    clusters = _counts_inside(geoms, layers.get("clusters"))

    out: dict[str, Diagnosis] = {}
    for i, rec in enumerate(records):
        nearest_id, nearest_gap = None, None
        for o in other:
            gap = haversine_meters(rec.lat, rec.lon, o.lat, o.lon)
            if nearest_gap is None or gap < nearest_gap:
                nearest_id, nearest_gap = o.rec_id, gap

        n_stops, n_cells, n_clusters = (int(stops[i]), int(cells[i]),
                                        int(clusters[i]))
        if nearest_gap is not None and nearest_gap <= nearby_m:
            bucket = "unpaired_nearby"
        elif -1 in (n_stops, n_cells, n_clusters):
            bucket = "unknown"
        elif n_stops == 0:
            bucket = "no_stops"
        elif n_cells == 0:
            bucket = "stops_below_cell_floor"
        elif n_clusters == 0:
            bucket = "below_cluster_floor"
        else:
            bucket = "detected_not_linked"

        out[rec.rec_id] = Diagnosis(
            rec_id=rec.rec_id, bucket=bucket,
            n_stops=n_stops, n_cells=n_cells, n_clusters=n_clusters,
            nearest_id=nearest_id, nearest_gap_m=nearest_gap,
        )
    return out


def nearest_other(
    rec: Record, other: Sequence[Record],
) -> tuple[Optional[str], Optional[float]]:
    """The closest record on the other side, by centroid distance in metres."""
    best_id, best = None, None
    for o in other:
        d = haversine_meters(rec.lat, rec.lon, o.lat, o.lon)
        if best is None or d < best:
            best_id, best = o.rec_id, d
    return best_id, best
