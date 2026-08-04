import numpy as np
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import transform, unary_union

# Meters per degree of latitude; also per degree of longitude at the equator.
METERS_PER_DEGREE = 111_320.0


def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters between two points."""
    R = 6_371_000
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(a))


def positional_variance_meters(lats: np.ndarray, lons: np.ndarray) -> float:
    """RMS deviation from centroid in meters."""
    if len(lats) < 2:
        return 0.0
    mean_lat = lats.mean()
    lat_std_m = lats.std() * 111_320
    lon_std_m = lons.std() * 111_320 * abs(np.cos(np.radians(mean_lat)))
    return float(np.sqrt(lat_std_m**2 + lon_std_m**2))


def _local_metric_frame(lat0: float, lon0: float):
    """
    Build a pair of transforms between WGS84 degrees and a local metric frame
    centred on (lat0, lon0), where one unit is one meter.

    A local flat-earth approximation is accurate to well under a meter across
    the few-kilometre span of a harbour, and avoids a projection dependency.
    """
    lon_scale = METERS_PER_DEGREE * abs(np.cos(np.radians(lat0))) or 1e-9

    def to_meters(x, y):
        return ((np.asarray(x) - lon0) * lon_scale,
                (np.asarray(y) - lat0) * METERS_PER_DEGREE)

    def to_degrees(x, y):
        return (np.asarray(x) / lon_scale + lon0,
                np.asarray(y) / METERS_PER_DEGREE + lat0)

    return to_meters, to_degrees


def _drop_interior_rings(geom):
    """Replace every polygon by its exterior ring, discarding holes."""
    if isinstance(geom, Polygon):
        return Polygon(geom.exterior)
    if isinstance(geom, MultiPolygon):
        return unary_union([Polygon(p.exterior) for p in geom.geoms])
    return geom


def outline_polygon(
    geom,
    buffer_meters: float = 75.0,
    simplify_meters: float = 0.0,
    fill_holes: bool = True,
):
    """
    Morphological closing of a lat/lon geometry: dilate by `buffer_meters`,
    then erode by the same amount.

    Applied to the union of a harbour's H3 cells this closes the gaps between
    berths and terminals and — with `fill_holes` — removes the interior voids
    left by cells that saw no traffic, yielding the harbour's outline rather
    than its exact cell footprint.

    Closing only fills concavities narrower than 2 × `buffer_meters`; the outer
    boundary is preserved, so the result never extends beyond roughly
    `buffer_meters` past the outermost cell. Parts further apart than that stay
    separate, so a MultiPolygon result is possible.

    The result always contains the input. `simplify_meters` is the one exception
    and is off by default: it thins vertices but may shave the boundary inwards
    by up to its own tolerance, which at res 11 is enough to drop a whole cell.

    Returns the closed geometry in WGS84 degrees, or `geom` unchanged when it
    is empty or not polygonal.
    """
    if geom is None or geom.is_empty:
        return geom
    if buffer_meters <= 0 and not fill_holes:
        return geom

    centroid = geom.centroid
    to_meters, to_degrees = _local_metric_frame(centroid.y, centroid.x)

    metric = transform(to_meters, geom)
    if buffer_meters > 0:
        # join_style=1 (round) keeps quay corners from being spiked outwards.
        closed = metric.buffer(buffer_meters, join_style=1).buffer(
            -buffer_meters, join_style=1
        )
        # Closing is extensive in theory, but Shapely approximates the buffer
        # arcs with line segments, which can dip just inside the original at
        # convex corners. Union restores the guarantee that the outline covers
        # every cell that actually saw traffic.
        metric = unary_union([metric, closed])
    if fill_holes:
        metric = _drop_interior_rings(metric)
    if simplify_meters > 0:
        metric = metric.simplify(simplify_meters, preserve_topology=True)

    if metric.is_empty:
        return geom
    return transform(to_degrees, metric)
