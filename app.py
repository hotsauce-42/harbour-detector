"""
Harbour Detector — Streamlit GUI

Layout:
  Sidebar  — tile-server selector + search/filter
  Left col — sortable harbour table (click row to select)
  Right col — Folium map with the selected harbour's polygon + metadata

Run:
  ~/harbour-venv/bin/streamlit run app.py
  ~/harbour-venv/bin/streamlit run app.py -- --config config/settings.yaml
"""

import json
from pathlib import Path

import folium
import pandas as pd
import streamlit as st
import yaml
from shapely.geometry import shape
from streamlit_folium import st_folium

# ---------------------------------------------------------------------------
# Config & data loading
# ---------------------------------------------------------------------------

CONFIG_PATH = Path("config/settings.yaml")


@st.cache_data(show_spinner=False)
def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


@st.cache_data(show_spinner="Loading harbour data …")
def load_features(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        fc = json.load(f)
    return fc.get("features", [])


def _build_display_df(features: list[dict]) -> pd.DataFrame:
    rows = []
    for i, feat in enumerate(features):
        p = feat.get("properties", {})
        rows.append({
            "_idx":    i,
            "City":    p.get("nearest_city", ""),
            "Region":  p.get("admin1", ""),
            "Country": p.get("country_name", ""),
            "Events":  int(p.get("n_events", 0)),
            "Vessels": int(p.get("n_unique_mmsi_approx", 0)),
            "Cells":   int(p.get("n_cells", 0)),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Map builder
# ---------------------------------------------------------------------------

def _build_map(
    feat: dict,
    tile_url: str,
    tile_attr: str,
    tile_name: str,
) -> folium.Map:
    props = feat.get("properties", {})
    geom  = feat.get("geometry")

    clat = props.get("centroid_lat", 0.0)
    clon = props.get("centroid_lon", 0.0)

    m = folium.Map(location=[clat, clon], zoom_start=13, tiles=None)
    folium.TileLayer(tiles=tile_url, attr=tile_attr, name=tile_name).add_to(m)

    if geom:
        city    = props.get("nearest_city", "Harbour")
        hid     = props.get("harbour_id", "")[:8]
        country = props.get("country_name", "")

        popup_html = f"""
        <b>{city}, {country}</b><br/>
        ID: {hid}…<br/>
        Events: {props.get('n_events', 0):,}<br/>
        Vessels: {props.get('n_unique_mmsi_approx', 0):,}<br/>
        H3 cells: {props.get('n_cells', 0)}<br/>
        Draught changes: {props.get('n_draught_changes', 0)}
        """

        folium.GeoJson(
            geom,
            style_function=lambda _: {
                "fillColor":   "#1E88E5",
                "color":       "#0D47A1",
                "weight":      2,
                "fillOpacity": 0.35,
            },
            tooltip=folium.Tooltip(f"{city} ({hid}…)"),
            popup=folium.Popup(popup_html, max_width=260),
        ).add_to(m)

        # Fit view to polygon bounds
        try:
            bounds = shape(geom).bounds   # (minlon, minlat, maxlon, maxlat)
            m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])
        except Exception:
            pass

    return m


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------

def main() -> None:
    st.set_page_config(
        page_title="Harbour Detector",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    cfg     = load_config()
    gui_cfg = cfg.get("gui", {})

    output_file = gui_cfg.get("output_file", "data/output/harbours.geojson")
    tile_layers = gui_cfg.get("map_tiles", [
        {
            "name":        "OpenStreetMap",
            "url":         "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
            "attribution": "© OpenStreetMap contributors",
        }
    ])
    default_tile = gui_cfg.get("default_tile", tile_layers[0]["name"])

    # ── Sidebar ────────────────────────────────────────────────────────────
    with st.sidebar:
        st.title("Harbour Detector")
        st.caption("AIS-based harbour detection pipeline")
        st.divider()

        tile_names = [t["name"] for t in tile_layers]
        default_idx = tile_names.index(default_tile) if default_tile in tile_names else 0
        selected_tile_name = st.selectbox("Map tiles", tile_names, index=default_idx)
        selected_tile = next(t for t in tile_layers if t["name"] == selected_tile_name)

        st.divider()
        search = st.text_input("Search city / country", placeholder="e.g. Hamburg")

        st.divider()
        sort_col = st.selectbox("Sort list by", ["Events", "Vessels", "Cells", "City", "Country"])
        sort_asc = st.checkbox("Ascending", value=False)

    # ── Load data ──────────────────────────────────────────────────────────
    if not Path(output_file).exists():
        st.error(
            f"**Output file not found:** `{output_file}`\n\n"
            "Run the pipeline first:\n"
            "```\npython run.py phase1 && python run.py phase2 && "
            "python run.py phase3 && python run.py phase4 && python run.py phase5\n```"
        )
        return

    features = load_features(output_file)
    if not features:
        st.warning("The output file contains no harbour features.")
        return

    df = _build_display_df(features)

    # ── Filter & sort ──────────────────────────────────────────────────────
    if search:
        mask = (
            df["City"].str.contains(search, case=False, na=False) |
            df["Country"].str.contains(search, case=False, na=False) |
            df["Region"].str.contains(search, case=False, na=False)
        )
        df = df[mask]

    df = df.sort_values(sort_col, ascending=sort_asc).reset_index(drop=True)

    # ── Harbour list (top) ────────────────────────────────────────────────
    st.subheader(f"Harbours ({len(df):,})")

    selection = st.dataframe(
        df.drop(columns=["_idx"]),
        use_container_width=True,
        height=280,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
    )

    st.divider()

    # Resolve which harbour is selected
    selected_rows = getattr(getattr(selection, "selection", None), "rows", [])
    row_pos       = int(selected_rows[0]) if selected_rows else 0
    row_pos       = min(row_pos, len(df) - 1)
    global_idx    = int(df.iloc[row_pos]["_idx"]) if len(df) > 0 else 0

    feat  = features[global_idx]
    props = feat.get("properties", {})

    # ── Selected harbour (bottom) ─────────────────────────────────────────
    city    = props.get("nearest_city", "Unknown")
    country = props.get("country_name", "")
    region  = props.get("admin1", "")
    loc_str = ", ".join(filter(None, [city, region, country]))
    st.subheader(loc_str)

    # ── Metrics row ────────────────────────────────────────────────────────
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Events",          f"{props.get('n_events', 0):,}")
    m2.metric("Vessels",         f"{props.get('n_unique_mmsi_approx', 0):,}")
    m3.metric("H3 cells",        props.get("n_cells", 0))
    m4.metric("Draught changes", props.get("n_draught_changes", 0))
    m5.metric("Country",         props.get("country_iso2", ""))

    # ── Map ────────────────────────────────────────────────────────────────
    fmap = _build_map(
        feat,
        tile_url=selected_tile["url"],
        tile_attr=selected_tile["attribution"],
        tile_name=selected_tile["name"],
    )
    st_folium(
        fmap,
        use_container_width=True,
        height=500,
        returned_objects=[],
    )

    # ── Details expander ───────────────────────────────────────────────────
    with st.expander("Full properties"):
        display_props = {k: v for k, v in props.items() if k != "h3_cells"}
        st.json(display_props)
        h3_cells = props.get("h3_cells", [])
        if h3_cells:
            st.caption(
                f"{len(h3_cells)} H3 cells (res 11) — "
                f"first: `{h3_cells[0]}` … last: `{h3_cells[-1]}`"
            )


if __name__ == "__main__":
    main()
