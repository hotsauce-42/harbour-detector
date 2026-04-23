"""
Generate a dummy harbours.geojson for GUI testing.
Uses real H3 cells and polygons at resolution 11.
"""

import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import h3
from shapely.geometry import mapping, shape

OUT_PATH = Path("data/output/harbours.geojson")
RES = 11
_NS = uuid.UUID("b8d7e3a2-5f1c-4e8b-9a6d-3c7f2e1b4a5d")

# (name, nearest_city, admin1, country_iso2, country_name, lat, lon, rings,
#  n_events, n_vessels, n_cargo, n_tanker, n_passenger, n_fishing, n_recreational, n_draught_changes)
HARBOURS = [
    ("Hamburg",         "Hamburg",        "Hamburg",           "DE", "Germany",              53.5396,   9.9700, 3, 48_200, 2_140, 320, 180,  12,  4,  2, 890),
    ("Rotterdam",       "Rotterdam",      "South Holland",     "NL", "Netherlands",          51.9000,   4.4900, 3, 61_500, 3_820, 410, 290,   8,  2,  1, 1_200),
    ("Singapore",       "Singapore",      "Singapore",         "SG", "Singapore",             1.2640, 103.8220, 3, 92_300, 5_600, 520, 410,  15,  0,  0, 1_800),
    ("Antwerp",         "Antwerp",        "Flemish Region",    "BE", "Belgium",              51.2500,   4.4050, 3, 39_800, 1_950, 280, 140,   6,  1,  0, 750),
    ("Los Angeles",     "Los Angeles",    "California",        "US", "United States",        33.7298, -118.264, 3, 44_100, 2_310, 370, 210,  20,  0,  3, 920),
    ("Shanghai",        "Shanghai",       "Shanghai",          "CN", "China",                31.2304, 121.4737, 3, 114_000, 7_200, 680, 510,  22,  0,  0, 2_300),
    ("Jebel Ali",       "Dubai",          "Dubai",             "AE", "United Arab Emirates", 25.0073,  55.0655, 3, 38_600, 2_050, 290, 220,   5,  0,  0, 810),
    ("Felixstowe",      "Felixstowe",     "England",           "GB", "United Kingdom",       51.9527,   1.3393, 2, 21_400, 1_180, 180,  90,   2,  1,  0, 430),
    ("Santos",          "Santos",         "São Paulo",         "BR", "Brazil",              -23.9618, -46.3322, 2, 18_900, 1_020, 150,  95,   4,  2,  0, 380),
    ("Sydney",          "Sydney",         "New South Wales",   "AU", "Australia",           -33.8560, 151.2080, 2, 14_200,   840, 100,  60,  45,  0, 12, 290),
    ("Gothenburg",      "Gothenburg",     "Västra Götaland",   "SE", "Sweden",               57.6880,  11.8990, 2, 16_300,   920, 130,  80,  18,  3,  4, 320),
    ("Marseille",       "Marseille",      "Provence",          "FR", "France",               43.3050,   5.3700, 2, 19_800, 1_050, 140, 110,  28,  5,  8, 410),
    ("Piraeus",         "Piraeus",        "Attica",            "GR", "Greece",               37.9420,  23.6380, 2, 24_100, 1_340, 160, 120,  55,  2,  5, 490),
    ("Busan",           "Busan",          "Busan",             "KR", "South Korea",          35.1796, 129.0756, 2, 31_500, 1_720, 230, 160,  10,  0,  0, 630),
    ("Lagos",           "Lagos",          "Lagos State",       "NG", "Nigeria",               6.4530,   3.3960, 2,  9_800,   580,  80,  60,   4,  2,  0, 195),
    ("Bremerhaven",     "Bremerhaven",    "Bremen",            "DE", "Germany",              53.5480,   8.5780, 1,  8_400,   490,  70,  35,   2,  1,  0, 160),
    ("Limassol",        "Limassol",       "Limassol",          "CY", "Cyprus",               34.6780,  33.0490, 1,  5_200,   310,  40,  30,  10,  1,  4, 105),
    ("Tallinn",         "Tallinn",        "Harju County",      "EE", "Estonia",              59.4370,  24.7450, 1,  4_100,   260,  25,  15,  35,  0,  6,  82),
    ("Maputo",          "Maputo",         "Maputo",            "MZ", "Mozambique",          -25.9692,  32.5732, 1,  2_800,   180,  22,  18,   2,  3,  0,  56),
    ("Noumea",          "Noumea",         "South Province",    "NC", "New Caledonia",       -22.2708, 166.4380, 1,  1_200,    90,  10,   8,  12,  0,  8,  24),
]


def make_feature(row: tuple) -> dict:
    (label, city, admin1, iso2, country,
     lat, lon, rings,
     n_events, n_vessels, n_cargo, n_tanker, n_passenger,
     n_fishing, n_recreational, n_draught) = row

    seed  = h3.latlng_to_cell(lat, lon, RES)
    cells = sorted(h3.grid_disk(seed, rings))
    r8    = h3.latlng_to_cell(lat, lon, 8)
    hid   = str(uuid.uuid5(_NS, r8))

    geo_dict = h3.cells_to_geo(cells)
    geom     = mapping(shape(geo_dict))   # normalise via Shapely

    cell_centres = [h3.cell_to_latlng(c) for c in cells]
    clat = sum(c[0] for c in cell_centres) / len(cell_centres)
    clon = sum(c[1] for c in cell_centres) / len(cell_centres)

    return {
        "type": "Feature",
        "geometry": geom,
        "properties": {
            "harbour_id":             hid,
            "h3_cells":               cells,
            "n_cells":                len(cells),
            "n_events":               n_events,
            "n_unique_mmsi_approx":   n_vessels,
            "n_draught_changes":      n_draught,
            "centroid_lat":           round(clat, 6),
            "centroid_lon":           round(clon, 6),
            "country_iso2":           iso2,
            "country_name":           country,
            "nearest_city":           city,
            "nearest_city_dist_km":   round(0.8 + rings * 0.3, 1),
            "admin1":                 admin1,
            "matched_existing":       False,
            "n_cargo":                n_cargo,
            "n_tanker":               n_tanker,
            "n_passenger":            n_passenger,
            "n_fishing":              n_fishing,
            "n_recreational":         n_recreational,
            "top_destination_locode": iso2 + "".join(c for c in city.upper()[:3] if c.isalpha()),
        },
    }


def main() -> None:
    features = [make_feature(row) for row in HARBOURS]
    fc = {"type": "FeatureCollection", "features": features}

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=2)

    print(f"Written {len(features)} dummy harbours → {OUT_PATH}")


if __name__ == "__main__":
    main()
