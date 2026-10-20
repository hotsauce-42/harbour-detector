#!/usr/bin/env python3
"""
Draw Phase 6's port areas over the detected harbours, as a standalone HTML map.

For looking at what the grouping did: each area is drawn as its exact H3-cell
union (what a tracker tests against), the harbours as outlines on top. Hover
an area for its id, name, member count, cell count and how many separate
pieces it consists of.

`--compare` adds a second port_areas.geojson as a layer that starts hidden —
e.g. a run with phase6.cut_land on against one with it off, or two gaps.

Local files only. Copy from S3 first — `pathlib.Path` collapses `s3://bucket`
to `s3:/bucket`, so an S3 URI cannot be passed here.

Usage:
    python3 scripts/map_port_areas.py

    # Two runs side by side (copy the first port_areas.geojson away first).
    python3 scripts/map_port_areas.py --compare /tmp/port_areas_cut.geojson \\
        --compare-label "with land cut"

    # Map that works without internet (run scripts/vendor_map_assets.py first).
    python3 scripts/map_port_areas.py --offline --tiles-url \\
        "https://tiles.internal/{z}/{x}/{y}.png" --tiles-attr "© internal"
"""

import argparse
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import folium  # noqa: E402
from shapely.geometry import shape  # noqa: E402

from utils.map_assets import VENDOR_DIR, is_vendored, use_local_assets  # noqa: E402

AREA_TOOLTIP = ["port_area_id", "name", "n_harbours", "n_cells", "pieces"]
STYLES = {
    "areas":    {"color": "#d6336c", "weight": 1, "fillOpacity": 0.3},
    "compare":  {"color": "#2f9e44", "weight": 1, "fillOpacity": 0.3},
    "harbours": {"color": "#1c7ed6", "weight": 2, "fillOpacity": 0.0},
}


def _load(path: Path) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _area_layer(path: Path) -> dict:
    """Port areas without their cell lists (they would double the page size)
    and with the number of separate pieces each one consists of."""
    fc = _load(path)
    for feature in fc["features"]:
        props = feature["properties"]
        props.pop("h3_cells", None)
        geom = shape(feature["geometry"]) if feature["geometry"] else None
        props["pieces"] = len(getattr(geom, "geoms", [geom])) if geom else 0
    return fc


def _harbour_layer(path: Path) -> dict:
    """Harbour outlines with only the properties the tooltip shows."""
    fc = _load(path)
    keep = ("harbour_id", "nearest_city")
    for feature in fc["features"]:
        feature["properties"] = {k: feature["properties"].get(k) for k in keep}
    return fc


def build_map(areas: Path, harbours: Path | None, out: Path, *,
              compare: Path | None = None, compare_label: str = "comparison",
              tiles: str = "OpenStreetMap", tiles_attr: str | None = None,
              offline: bool = False) -> Path:
    if offline:
        if not is_vendored():
            print("--offline: static/vendor/ is not filled — run "
                  "scripts/vendor_map_assets.py first. Falling back to CDNs.")
            offline = False
        else:
            # Relative hrefs, so the page works from wherever it is opened.
            shutil.copytree(VENDOR_DIR, out.parent / "vendor", dirs_exist_ok=True)
            use_local_assets("vendor")

    area_fc = _area_layer(areas)
    if not area_fc["features"]:
        sys.exit(f"{areas} holds no port areas")
    bounds = [shape(f["geometry"]).bounds for f in area_fc["features"]
              if f["geometry"]]
    south, west = min(b[1] for b in bounds), min(b[0] for b in bounds)
    north, east = max(b[3] for b in bounds), max(b[2] for b in bounds)

    # The public OpenStreetMap name is a CDN too: offline it only works with a
    # tile URL of your own.
    use_tiles = tiles if not (offline and tiles == "OpenStreetMap") else None
    fmap = folium.Map(location=[(south + north) / 2, (west + east) / 2],
                      zoom_start=8, tiles=use_tiles,
                      attr=tiles_attr if use_tiles != "OpenStreetMap" else None)

    # Tooltips only — a Popup would need jquery (see utils/map_assets.py).
    folium.GeoJson(
        area_fc, name=f"Port areas ({areas.name})",
        style_function=lambda f: STYLES["areas"],
        tooltip=folium.GeoJsonTooltip(AREA_TOOLTIP),
    ).add_to(fmap)
    if compare:
        folium.GeoJson(
            _area_layer(compare), name=f"Port areas — {compare_label}",
            show=False, style_function=lambda f: STYLES["compare"],
            tooltip=folium.GeoJsonTooltip(AREA_TOOLTIP),
        ).add_to(fmap)
    if harbours:
        folium.GeoJson(
            _harbour_layer(harbours), name="Detected harbours",
            style_function=lambda f: STYLES["harbours"],
            tooltip=folium.GeoJsonTooltip(["harbour_id", "nearest_city"]),
        ).add_to(fmap)

    folium.LayerControl(collapsed=False).add_to(fmap)
    fmap.fit_bounds([[south, west], [north, east]])
    out.parent.mkdir(parents=True, exist_ok=True)
    fmap.save(str(out))
    return out


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--areas", type=Path,
                   default=Path("data/output/port_areas.geojson"),
                   help="Phase 6 output (default: %(default)s)")
    p.add_argument("--harbours", type=Path,
                   default=Path("data/output/harbours.geojson"),
                   help="Phase 5 outlines drawn on top (default: %(default)s)")
    p.add_argument("--no-harbours", action="store_true",
                   help="draw the port areas only")
    p.add_argument("--compare", type=Path,
                   help="a second port_areas.geojson, as a hidden layer")
    p.add_argument("--compare-label", default="comparison",
                   help="layer name for --compare (default: %(default)s)")
    p.add_argument("--out", type=Path,
                   default=Path("data/output/port_areas_map.html"),
                   help="HTML file to write (default: %(default)s)")
    p.add_argument("--offline", action="store_true",
                   help="serve Leaflet from a vendor/ folder next to the page; "
                        "the basemap still needs a reachable --tiles-url, "
                        "otherwise the background is blank")
    p.add_argument("--tiles-url", default="OpenStreetMap",
                   help="basemap tile layer (default: %(default)s)")
    p.add_argument("--tiles-attr", default=None,
                   help="tile attribution, required with a custom --tiles-url")
    return p


def main() -> None:
    args = _parser().parse_args()
    harbours = None if args.no_harbours or not args.harbours.exists() else args.harbours
    out = build_map(args.areas, harbours, args.out,
                    compare=args.compare, compare_label=args.compare_label,
                    tiles=args.tiles_url, tiles_attr=args.tiles_attr,
                    offline=args.offline)
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
