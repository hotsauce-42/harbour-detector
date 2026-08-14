"""
Harbour Detector — Streamlit GUI

Layout:
  Sidebar  — tile-server selector + geometry toggle + search/filter
  Left col — sortable harbour table (click row to select)
  Right col — Folium map with the selected harbour's polygon + metadata

The map can draw either the harbour outline (harbours.geojson), the H3 cells it
was built from (harbours_cells.geojson), or both stacked.

Run:
  ~/harbour-venv/bin/streamlit run app.py
  ~/harbour-venv/bin/streamlit run app.py -- --config config/settings.yaml
"""

import json
import os
import tempfile
from pathlib import Path

import folium
import pandas as pd
import streamlit as st
import yaml
from shapely.geometry import shape
from streamlit_folium import st_folium

from utils.overrides import (
    EDITABLE_FIELDS,
    OVERRIDES_KEY,
    normalise_overrides,
    resolve_country_iso2,
)

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


@st.cache_data(show_spinner=False)
def load_geometry_by_id(path: str) -> dict[str, dict]:
    """
    Map harbour_id → geometry for a companion GeoJSON.

    Keyed by ID rather than position so the two files stay in step even if one
    of them was written by an older pipeline run.
    """
    if not path or not Path(path).exists():
        return {}
    with open(path, encoding="utf-8") as f:
        fc = json.load(f)
    return {
        feat.get("properties", {}).get("harbour_id"): feat.get("geometry")
        for feat in fc.get("features", [])
        if feat.get("properties", {}).get("harbour_id") and feat.get("geometry")
    }


def _cells_path(output_file: str, gui_cfg: dict) -> str:
    """
    Locate the H3-cell companion file. Defaults to the outline file's name with
    a '_cells' suffix, i.e. harbours.geojson → harbours_cells.geojson.
    """
    explicit = gui_cfg.get("cells_file")
    if explicit:
        return explicit
    p = Path(output_file)
    return str(p.with_name(f"{p.stem}_cells{p.suffix}"))


# ---------------------------------------------------------------------------
# Manual property edits
# ---------------------------------------------------------------------------

def _write_feature_collection(path: str, fc: dict) -> None:
    """
    Rewrite a GeoJSON file atomically.

    The file is the pipeline's output and may be several MB; a partial write
    would leave it unparseable, so the new content lands in a sibling temp file
    and is moved into place in one step.
    """
    target = Path(path)
    handle = tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=target.parent,
        prefix=f".{target.name}.", suffix=".tmp", delete=False,
    )
    try:
        with handle as fh:
            json.dump(fc, fh, ensure_ascii=False, indent=2)
        os.replace(handle.name, target)
    except BaseException:
        Path(handle.name).unlink(missing_ok=True)
        raise


def plan_edits(
    props: dict,
    new_values: dict[str, str],
) -> tuple[dict, list[str], str | None]:
    """
    Work out what a submitted edit form actually changes.

    Returns (updates, overrides, note):
      updates   — properties to write, including country_iso2 when the country
                  name resolved to an ISO code
      overrides — the field names to record in `manual_overrides`, i.e. the
                  previously overridden ones plus whatever just changed
      note      — a message about ISO resolution, or None
    """
    updates: dict = {}
    overrides = normalise_overrides(props.get(OVERRIDES_KEY))
    note = None

    for field in EDITABLE_FIELDS:
        value = (new_values.get(field) or "").strip()
        if value == str(props.get(field) or "").strip():
            continue

        updates[field] = value
        if field not in overrides:
            overrides.append(field)

        if field == "country_name":
            iso2 = resolve_country_iso2(value)
            if iso2:
                updates["country_iso2"] = iso2
                note = f"Country resolved to ISO code **{iso2}**."
            else:
                current = props.get("country_iso2") or "—"
                note = (f"No ISO code matched “{value}” — `country_iso2` left "
                        f"as **{current}**.")

    # Keep the marker in a stable order regardless of edit sequence.
    overrides = [f for f in EDITABLE_FIELDS if f in overrides]
    return updates, overrides, note


def save_harbour_edits(
    paths: list[str],
    harbour_id: str,
    updates: dict,
    overrides: list[str],
) -> list[str]:
    """
    Persist edited properties for one harbour into every GeoJSON that holds it.

    The outline and H3-cell files carry the same properties with different
    geometry, so both are updated — otherwise whichever one is later used as the
    existing harbour database would hand back stale values.

    Returns the paths actually rewritten.
    """
    written = []
    for path in paths:
        if not path or not Path(path).exists():
            continue
        with open(path, encoding="utf-8") as f:
            fc = json.load(f)

        touched = False
        for feat in fc.get("features", []):
            props = feat.setdefault("properties", {})
            if props.get("harbour_id") != harbour_id:
                continue
            props.update(updates)
            if overrides:
                props[OVERRIDES_KEY] = overrides
            else:
                props.pop(OVERRIDES_KEY, None)
            touched = True

        if touched:
            _write_feature_collection(path, fc)
            written.append(path)
    return written


def _row_for_harbour(
    df: pd.DataFrame,
    features: list[dict],
    harbour_id: str | None,
) -> int:
    """Row position of a harbour in the filtered table, or 0 when not present."""
    if not harbour_id or df.empty:
        return 0
    for pos in range(len(df)):
        idx = int(df.iloc[pos]["_idx"])
        if features[idx].get("properties", {}).get("harbour_id") == harbour_id:
            return pos
    return 0


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
            "Vessels": int(p.get("n_unique_mmsi", p.get("n_unique_mmsi_approx", 0))),
            "Cells":   int(p.get("n_cells", 0)),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Map builder
# ---------------------------------------------------------------------------

OUTLINE_STYLE = {
    "fillColor":   "#1E88E5",
    "color":       "#0D47A1",
    "weight":      2.5,
    "fillOpacity": 0.30,
}
CELLS_STYLE = {
    "fillColor":   "#FB8C00",
    "color":       "#E65100",
    "weight":      1,
    "fillOpacity": 0.45,
}

SHOW_OUTLINE = "Outline"
SHOW_CELLS   = "H3 cells"
SHOW_BOTH    = "Both"


def _build_map(
    feat: dict,
    tile_url: str,
    tile_attr: str,
    tile_name: str,
    cells_geom: dict | None = None,
    show: str = SHOW_OUTLINE,
) -> folium.Map:
    props        = feat.get("properties", {})
    outline_geom = feat.get("geometry")

    clat = props.get("centroid_lat", 0.0)
    clon = props.get("centroid_lon", 0.0)

    m = folium.Map(location=[clat, clon], zoom_start=13, tiles=None)
    folium.TileLayer(tiles=tile_url, attr=tile_attr, name=tile_name).add_to(m)

    city    = props.get("nearest_city", "Harbour")
    hid     = props.get("harbour_id", "")[:8]
    country = props.get("country_name", "")
    vessels = props.get("n_unique_mmsi", props.get("n_unique_mmsi_approx", 0))

    popup_html = f"""
    <b>{city}, {country}</b><br/>
    ID: {hid}…<br/>
    Events: {props.get('n_events', 0):,}<br/>
    Vessels: {vessels:,}<br/>
    H3 cells: {props.get('n_cells', 0)}<br/>
    Draught changes: {props.get('n_draught_changes', 0)}
    """

    # Outline first so the finer cells stay legible on top of it.
    layers = []
    if show in (SHOW_OUTLINE, SHOW_BOTH) and outline_geom:
        layers.append((outline_geom, OUTLINE_STYLE, f"{city} — outline"))
    if show in (SHOW_CELLS, SHOW_BOTH) and cells_geom:
        layers.append((cells_geom, CELLS_STYLE, f"{city} — H3 cells"))

    for geom, style, label in layers:
        gj = folium.GeoJson(
            geom,
            style_function=lambda _, s=style: s,
            tooltip=folium.Tooltip(f"{label} ({hid}…)"),
        )
        # Attached as a child rather than via popup=: GeoJson types that kwarg as
        # GeoJsonPopup (per-feature fields), but we want one static HTML popup.
        folium.Popup(popup_html, max_width=260).add_to(gj)
        gj.add_to(m)

    # Fit the view to everything drawn (the outline already covers the cells).
    if layers:
        try:
            bounds = shape(layers[0][0]).bounds  # (minlon, minlat, maxlon, maxlat)
            m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])
        except Exception:
            pass

    return m


def _edit_panel(feat: dict, paths: list[str], existing_db: str = "") -> None:
    """Form for correcting a harbour's city / region / country."""
    props = feat.get("properties", {})
    hid   = props.get("harbour_id", "")
    marked = normalise_overrides(props.get(OVERRIDES_KEY))

    title = "Edit location details"
    if marked:
        labels = ", ".join(EDITABLE_FIELDS[f] for f in marked)
        title += f"  •  manually set: {labels}"

    with st.expander(title):
        st.caption(
            "Corrections are written straight into the GeoJSON and recorded "
            "under `manual_overrides`, so Phase 5 reapplies them whenever this "
            "harbour is matched again. The harbour ID is not editable — it is "
            "what the match is keyed on."
        )
        # Phase 5 reads its existing database, not this output file. When they
        # are different files, edits only survive a re-run once they are copied
        # across — say so rather than implying it happens by itself.
        if existing_db and Path(existing_db) != Path(paths[0]):
            st.caption(
                f"⚠️ Edits are saved to `{paths[0]}`, but Phase 5 matches "
                f"against `{existing_db}`. Copy the file across before the next "
                "run, or point `phase5.existing_db_path` at the output."
            )

        with st.form(f"edit_{hid}"):
            cols = st.columns(len(EDITABLE_FIELDS))
            new_values = {}
            for col, (field, label) in zip(cols, EDITABLE_FIELDS.items()):
                new_values[field] = col.text_input(
                    f"{label} ●" if field in marked else label,
                    value=str(props.get(field) or ""),
                    key=f"edit_{hid}_{field}",
                )
            save_col, clear_col = st.columns([1, 1])
            submitted = save_col.form_submit_button("Save changes",
                                                    type="primary")
            cleared = clear_col.form_submit_button(
                "Clear manual flags", disabled=not marked,
                help="Keeps the current values but lets the next pipeline run "
                     "re-derive them from the geocoder.",
            )

        if submitted:
            updates, overrides, note = plan_edits(props, new_values)
            if not updates:
                st.info("No changes to save.")
                return
            written = save_harbour_edits(paths, hid, updates, overrides)
            if not written:
                st.error(f"Could not find harbour `{hid}` in any output file.")
                return
            changed = [EDITABLE_FIELDS[f] for f in EDITABLE_FIELDS if f in updates]
            _after_save(hid, note,
                        f"Saved {', '.join(changed)} to "
                        f"{', '.join(Path(p).name for p in written)}.")

        if cleared:
            written = save_harbour_edits(paths, hid, {}, [])
            _after_save(hid, None,
                        "Cleared manual flags — the next pipeline run will "
                        f"re-derive these fields ({len(written)} file(s) updated).")


def _after_save(harbour_id: str, note: str | None, message: str) -> None:
    """Drop the data caches and rerun so the edit is visible immediately."""
    load_features.clear()
    load_geometry_by_id.clear()
    st.session_state["selected_harbour_id"] = harbour_id
    st.session_state["save_message"] = message
    st.session_state["save_note"] = note
    st.rerun()


def _map_legend(show: str) -> None:
    """Colour key matching the layers currently drawn."""
    swatch = (
        '<span style="display:inline-block;width:11px;height:11px;'
        'background:{fill};border:1.5px solid {line};margin-right:5px;'
        'vertical-align:middle;"></span>'
    )
    entries = []
    if show in (SHOW_OUTLINE, SHOW_BOTH):
        entries.append(
            swatch.format(fill=OUTLINE_STYLE["fillColor"], line=OUTLINE_STYLE["color"])
            + "Harbour outline"
        )
    if show in (SHOW_CELLS, SHOW_BOTH):
        entries.append(
            swatch.format(fill=CELLS_STYLE["fillColor"], line=CELLS_STYLE["color"])
            + "H3 cells with stop events"
        )
    st.markdown(
        '<div style="font-size:0.85em;opacity:0.85;">'
        + "&nbsp;&nbsp;&nbsp;".join(entries)
        + "</div>",
        unsafe_allow_html=True,
    )


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
    cells_file  = _cells_path(output_file, gui_cfg)
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
        default_idx = (tile_names.index(default_tile)
                       if default_tile in tile_names else 0)
        selected_tile_name = st.selectbox("Map tiles", tile_names, index=default_idx)
        selected_tile = next(t for t in tile_layers if t["name"] == selected_tile_name)

        st.divider()

        # Geometry toggle — only offer the cell layers when the file is there.
        cells_by_id = load_geometry_by_id(cells_file)
        if cells_by_id:
            show_geom = st.segmented_control(
                "Show on map",
                [SHOW_OUTLINE, SHOW_CELLS, SHOW_BOTH],
                default=SHOW_OUTLINE,
                help="Outline is the closed harbour boundary; H3 cells are the "
                     "individual cells that had stop events.",
            ) or SHOW_OUTLINE
        else:
            show_geom = SHOW_OUTLINE
            st.caption(
                f"Showing outlines only — `{Path(cells_file).name}` not found. "
                "Re-run phase 5 to generate the H3-cell layer."
            )

        st.divider()
        search = st.text_input("Search city / country", placeholder="e.g. Hamburg")

        st.divider()
        sort_col = st.selectbox(
            "Sort list by", ["Events", "Vessels", "Cells", "City", "Country"]
        )
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

    # Resolve which harbour is selected. Saving an edit triggers a rerun, which
    # clears the table selection — fall back to the harbour we were just on
    # rather than snapping back to the first row.
    selected_rows = getattr(getattr(selection, "selection", None), "rows", [])
    if selected_rows:
        row_pos = int(selected_rows[0])
    else:
        row_pos = _row_for_harbour(
            df, features, st.session_state.get("selected_harbour_id")
        )
    row_pos       = min(row_pos, len(df) - 1)
    global_idx    = int(df.iloc[row_pos]["_idx"]) if len(df) > 0 else 0

    feat  = features[global_idx]
    props = feat.get("properties", {})
    st.session_state["selected_harbour_id"] = props.get("harbour_id")

    # Feedback from the save that caused this rerun.
    message = st.session_state.pop("save_message", None)
    note    = st.session_state.pop("save_note", None)
    if message:
        st.success(message)
    if note:
        st.info(note)

    # ── Selected harbour (bottom) ─────────────────────────────────────────
    city    = props.get("nearest_city", "Unknown")
    country = props.get("country_name", "")
    region  = props.get("admin1", "")
    loc_str = ", ".join(filter(None, [city, region, country]))
    st.subheader(loc_str)

    # ── Metrics row ────────────────────────────────────────────────────────
    vessels = props.get("n_unique_mmsi", props.get("n_unique_mmsi_approx", 0))
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Events",          f"{props.get('n_events', 0):,}")
    m2.metric("Vessels",         f"{vessels:,}")
    m3.metric("H3 cells",        props.get("n_cells", 0))
    m4.metric("Draught changes", props.get("n_draught_changes", 0))
    m5.metric("Country",         props.get("country_iso2", ""))

    # ── Manual property edits ──────────────────────────────────────────────
    _edit_panel(feat, [output_file, cells_file],
                existing_db=cfg.get("phase5", {}).get("existing_db_path", ""))

    # ── Map ────────────────────────────────────────────────────────────────
    fmap = _build_map(
        feat,
        tile_url=selected_tile["url"],
        tile_attr=selected_tile["attribution"],
        tile_name=selected_tile["name"],
        cells_geom=cells_by_id.get(props.get("harbour_id")),
        show=show_geom,
    )
    st_folium(
        fmap,
        use_container_width=True,
        height=500,
        returned_objects=[],
    )

    _map_legend(show_geom)

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
