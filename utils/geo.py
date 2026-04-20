import numpy as np


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
    mean_lon = lons.mean()
    lat_std_m = lats.std() * 111_320
    lon_std_m = lons.std() * 111_320 * abs(np.cos(np.radians(mean_lat)))
    return float(np.sqrt(lat_std_m**2 + lon_std_m**2))
