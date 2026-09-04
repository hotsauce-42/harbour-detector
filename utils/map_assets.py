"""
Serve the map's Leaflet assets from the app itself, for offline deployments.

folium renders a page that pulls six libraries from four public CDNs. In a
sealed network none of them load and the map is a blank box — including
Leaflet.Draw, which is the whole outline editor. `scripts/vendor_map_assets.py`
downloads the two libraries that matter into `static/vendor/`, and
`use_local_assets()` repoints folium at those copies.

Tile servers are a separate concern: they are plain config (`gui.map_tiles`).
"""

from pathlib import Path

import folium
from folium.plugins import Draw

# folium's base template loads jquery, bootstrap, glyphicons, fontawesome and
# awesome-markers on every map. Nothing in this GUI uses them — in the rendered
# page they appear only as their own <script>/<link> tags — so they are dropped
# rather than vendored, and an offline browser makes no dead requests at all.
# Adding a folium plugin that needs them means adding its key here.
KEEP = ("leaflet", "leaflet_css", "leaflet_draw_js", "leaflet_draw_css")

# Files the stylesheets pull in by relative URL, plus the marker icons that
# leaflet.js resolves at runtime. Paths are relative to the stylesheet's URL,
# and both `images/` sets share one directory (no name collides).
SUB_ASSETS = {
    "leaflet_css": (
        "images/layers.png",
        "images/layers-2x.png",
        "images/marker-icon.png",
        "images/marker-icon-2x.png",   # these two are loaded by L.Icon.Default,
        "images/marker-shadow.png",    # not by the CSS
    ),
    "leaflet_draw_css": (
        "images/spritesheet.png",
        "images/spritesheet-2x.png",
        "images/spritesheet.svg",
    ),
}

# Where Streamlit's static server exposes static/ (see .streamlit/config.toml).
# Absolute on purpose: the map renders inside a srcdoc iframe, where a relative
# path would resolve against the component's URL, not the app's.
VENDOR_URL = "/app/static/vendor"
VENDOR_DIR = Path(__file__).resolve().parents[1] / "static" / "vendor"

# The only folium classes that contribute assets to this app's map.
_HOLDERS = (folium.Map, Draw)
_ATTRS = ("default_js", "default_css")


def asset_urls() -> dict[str, str]:
    """{key: URL} for the assets we keep, at the versions folium pins today."""
    return {
        key: url
        for cls in _HOLDERS
        for attr in _ATTRS
        for key, url in getattr(cls, attr)
        if key in KEEP
    }


def use_local_assets(base_url: str = VENDOR_URL) -> None:
    """
    Point folium at the vendored copies and drop the libraries we don't use.

    Mutates folium's class attributes, so every map built afterwards is
    affected. Idempotent — Streamlit re-runs the whole script on every
    interaction, and the filenames are already the local ones by then.
    """
    for cls in _HOLDERS:
        for attr in _ATTRS:
            setattr(cls, attr, [
                (key, f"{base_url}/{url.rsplit('/', 1)[-1]}")
                for key, url in getattr(cls, attr)
                if key in KEEP
            ])


def is_vendored(out_dir: Path = VENDOR_DIR) -> bool:
    """True when every kept library is actually on disk to be served."""
    return all(
        (out_dir / url.rsplit("/", 1)[-1]).is_file()
        for url in asset_urls().values()
    )
