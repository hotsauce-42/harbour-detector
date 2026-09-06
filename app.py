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
from folium.plugins import Draw
from shapely.geometry import MultiPolygon, Point, mapping, shape
from shapely.ops import unary_union
from shapely.wkt import dumps as to_wkt
from shapely.wkt import loads as from_wkt
from streamlit_folium import st_folium

from utils.geo import clean_polygon, merge_outlines
from utils.map_assets import VENDOR_URL, is_vendored, use_local_assets
from utils.overrides import (
    DETECTED_TRANSIT_KEY,
    MANUAL_TRANSIT_KEY,
    DETECTED_OUTLINE_KEY,
    EDITABLE_FIELDS,
    MANUAL_OUTLINE_KEY,
    OVERRIDES_KEY,
    manual_outline,
    manual_transit,
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


def _apply_map_assets(gui_cfg: dict) -> None:
    """
    Switch Leaflet from the public CDNs to a local copy, when configured.

    `gui.local_map_assets` (or GUI__LOCAL_MAP_ASSETS) turns it on;
    `gui.map_assets_url` points at somewhere other than this app's own
    static/ directory — e.g. the same web server that serves the tiles.
    """
    env = os.getenv("GUI__LOCAL_MAP_ASSETS")
    enabled = (env.strip().lower() in ("true", "1", "yes") if env is not None
               else bool(gui_cfg.get("local_map_assets", False)))
    if not enabled:
        return

    base_url = gui_cfg.get("map_assets_url", VENDOR_URL)
    use_local_assets(base_url)
    # Only static/vendor/ is ours to check; another host we cannot see from here.
    if base_url == VENDOR_URL and not is_vendored():
        st.sidebar.warning(
            "Local map assets are enabled but `static/vendor/` is incomplete — "
            "the map will not render. Run `python3 scripts/vendor_map_assets.py`."
        )


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


def _edit_harbour_in_file(path: str, harbour_id: str, mutate) -> bool:
    """
    Apply `mutate(feature)` to every feature for one harbour and rewrite the file.

    Returns True when the file held the harbour and was rewritten, False when
    the path is missing or the harbour is not in it.
    """
    if not path or not Path(path).exists():
        return False

    with open(path, encoding="utf-8") as f:
        fc = json.load(f)

    touched = False
    for feat in fc.get("features", []):
        props = feat.setdefault("properties", {})
        if props.get("harbour_id") != harbour_id:
            continue
        mutate(feat)
        touched = True

    if touched:
        _write_feature_collection(path, fc)
    return touched


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
    def mutate(feat: dict) -> None:
        props = feat["properties"]
        props.update(updates)
        if overrides:
            props[OVERRIDES_KEY] = overrides
        else:
            props.pop(OVERRIDES_KEY, None)

    return [p for p in paths if _edit_harbour_in_file(p, harbour_id, mutate)]


def save_harbour_transit(
    paths: list[str],
    harbour_id: str,
    verdict: bool | None,
) -> list[str]:
    """
    Persist an operator's lock verdict for one harbour into every GeoJSON.

    Tri-state, and the third state is what the property's *absence* means:
    `verdict=None` removes it, which is "no opinion — use whatever the pipeline
    detected", and is different from storing False ("looked at it; not a lock").
    Phase 5 reads the stored value back and lets it replace its own verdict.
    """
    def mutate(feat: dict) -> None:
        props = feat["properties"]
        if verdict is None:
            props.pop(MANUAL_TRANSIT_KEY, None)
        else:
            props[MANUAL_TRANSIT_KEY] = bool(verdict)
        # Keep the effective flag in step so the map and table react at once,
        # without waiting for the next pipeline run.
        detected = bool(props.get(DETECTED_TRANSIT_KEY,
                                  props.get("transit_like", False)))
        props["transit_like"] = detected if verdict is None else bool(verdict)

    return [p for p in paths if _edit_harbour_in_file(p, harbour_id, mutate)]


def is_transit(props: dict) -> bool:
    """The effective lock verdict for a harbour: the operator's, else the pipeline's."""
    verdict = manual_transit(props)
    if verdict is not None:
        return verdict
    return bool(props.get("transit_like", False))


def transit_harbours(features: list[dict]) -> list[int]:
    """Indices of every harbour currently considered a transit site."""
    return [i for i, f in enumerate(features) if is_transit(f.get("properties", {}))]


# ---------------------------------------------------------------------------
# Manual outline edits
# ---------------------------------------------------------------------------

def detected_geometry(feat: dict):
    """
    The outline as the pipeline detected it, before any manual edit.

    Phase 5 stores it in `detected_outline_wkt`. Output written before this
    feature existed has no such property — there the feature geometry is still
    the detected outline, because nothing had been drawn over it yet.

    Returns a shapely geometry, or None when the feature has no geometry at all.
    """
    props = feat.get("properties", {})
    stored = props.get(DETECTED_OUTLINE_KEY)
    if isinstance(stored, str) and stored.strip():
        try:
            return from_wkt(stored)
        except Exception:
            pass

    geom = feat.get("geometry")
    if not geom:
        return None
    try:
        return shape(geom)
    except Exception:
        return None


def drawn_geometry(feat: dict):
    """The operator's stored outline for this harbour, or None."""
    wkt = manual_outline(feat.get("properties", {}))
    if not wkt:
        return None
    try:
        return from_wkt(wkt)
    except Exception:
        return None


def outline_from_drawings(drawings) -> str | None:
    """
    Fold everything currently on the draw layer into one outline, as WKT.

    The layer holds whatever the operator left there — the seeded outline parts
    with their vertices dragged around, a freshly drawn replacement, or both —
    so the union of all of it is the new outline. Non-areal leftovers and the
    self-intersections a dragged vertex produces are cleaned up.

    Returns None when nothing areal is left, which is how "I deleted everything"
    arrives here.
    """
    geoms = []
    for drawing in drawings or []:
        geom = drawing.get("geometry") if isinstance(drawing, dict) else None
        if not geom:
            continue
        try:
            geoms.append(shape(geom))
        except Exception:
            continue

    if not geoms:
        return None

    merged = clean_polygon(unary_union(geoms))
    return to_wkt(merged) if merged is not None else None


def save_harbour_outline(
    paths: list[str],
    outline_path: str,
    harbour_id: str,
    drawn_wkt: str | None,
    detected_wkt: str | None,
    effective_geometry: dict | None,
) -> list[str]:
    """
    Persist a manually drawn outline for one harbour.

    Every file gets the properties — `manual_outline_wkt` is what Phase 5 reads
    back off the existing database, and it has to be there whichever file is
    pointed at. Only `outline_path` gets new geometry: the H3-cell file holds
    the cell union, which a manual outline does not touch.

    Passing `drawn_wkt=None` clears the edit and restores the detected outline.
    """
    def mutate(feat: dict) -> None:
        props = feat["properties"]
        if drawn_wkt:
            props[MANUAL_OUTLINE_KEY] = drawn_wkt
        else:
            props.pop(MANUAL_OUTLINE_KEY, None)
        if detected_wkt:
            props[DETECTED_OUTLINE_KEY] = detected_wkt

    written = []
    for path in paths:
        def mutate_file(feat: dict, path=path) -> None:
            mutate(feat)
            if path == outline_path and effective_geometry is not None:
                feat["geometry"] = effective_geometry

        if _edit_harbour_in_file(path, harbour_id, mutate_file):
            written.append(path)
    return written


def _index_for_harbour(features: list[dict], harbour_id: str | None) -> int | None:
    """
    Position of a harbour in the feature list, or None when it is not there.

    Resolved against the features rather than the filtered table, so a harbour
    picked on the map stays selected even when the search box would have hidden
    its row.
    """
    if not harbour_id:
        return None
    for i, feat in enumerate(features):
        if feat.get("properties", {}).get("harbour_id") == harbour_id:
            return i
    return None


def _city_key(props: dict) -> tuple[str, str] | None:
    """
    What counts as "the same city", or None for a harbour that has no city.

    Country is part of the key: the same city name in two countries is two
    different places, and drawing both on one map would zoom out to nothing.
    An empty `nearest_city` is not a place either — those harbours are a group
    of one rather than one big "Unknown" pile.
    """
    city = (props.get("nearest_city") or "").strip().casefold()
    if not city:
        return None
    country = (props.get("country_iso2")
               or props.get("country_name") or "").strip().casefold()
    return city, country


def city_harbours(features: list[dict], index: int) -> list[int]:
    """Indices of the *other* harbours the pipeline placed in the same city."""
    key = _city_key(features[index].get("properties", {}))
    if key is None:
        return []
    return [
        i for i, feat in enumerate(features)
        if i != index and _city_key(feat.get("properties", {})) == key
    ]


# A click that caught the outline's stroke from just outside it still counts.
# 2e-4° is roughly 20 m — under half a res-11 cell, so it cannot reach past a
# harbour into its neighbour.
CLICK_TOLERANCE_DEG = 2e-4


def harbour_at_click(
    candidates: list[tuple[str, dict]],
    click: dict | None,
    tolerance: float = CLICK_TOLERANCE_DEG,
) -> str | None:
    """
    Which harbour a map click landed on, or None for a click on open water.

    streamlit-folium reports `last_object_clicked` for a click on *any* layer,
    the basemap tiles included, so the point has to be tested against the
    harbours themselves — the event alone means nothing. The nearest harbour
    within the tolerance wins, which settles a click on a shared boundary.
    """
    if not isinstance(click, dict):
        return None
    lat, lon = click.get("lat"), click.get("lng")
    if lat is None or lon is None:
        return None

    point = Point(lon, lat)
    best, best_distance = None, None
    for harbour_id, geom in candidates:
        if not harbour_id or not geom:
            continue
        try:
            distance = shape(geom).distance(point)
        except Exception:
            continue
        if distance > tolerance:
            continue
        if best_distance is None or distance < best_distance:
            best, best_distance = harbour_id, distance
    return best


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
            # Sortable, so every flagged site can be pulled to the top of the
            # table without hunting for it on the map.
            "Lock?":   "⚓" if is_transit(p) else "",
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
# The city's other harbours, drawn as context: grey and thin, so the selected
# harbour stays the obvious subject of the map.
# A flagged site is drawn in a warning amber so it reads as "look at this",
# not as an error.
TRANSIT_OUTLINE_STYLE = {"fillColor": "#FB8C00", "color": "#E65100",
                         "weight": 2, "fillOpacity": 0.30}
TRANSIT_BADGE = "⚓ flagged as a possible lock"

SIBLING_OUTLINE_STYLE = {
    "fillColor":   "#78909C",
    "color":       "#37474F",
    "weight":      1.5,
    "fillOpacity": 0.18,
}
SIBLING_CELLS_STYLE = {
    "fillColor":   "#B0BEC5",
    "color":       "#607D8B",
    "weight":      0.8,
    "fillOpacity": 0.25,
}

SHOW_OUTLINE = "Outline"
SHOW_CELLS   = "H3 cells"
SHOW_BOTH    = "Both"


def _editable_polygons(geom, style: dict) -> list[folium.Polygon]:
    """
    Rebuild an outline as plain Leaflet polygons, ready for the draw control.

    Leaflet.Draw edits `L.Polygon` layers; `folium.GeoJson` renders an
    `L.GeoJSON` group, whose parts the edit toolbar will not touch. One Polygon
    per part — holes included — keeps a multi-part outline editable as well.
    """
    if geom is None or geom.is_empty:
        return []

    parts = list(geom.geoms) if isinstance(geom, MultiPolygon) else [geom]
    polygons = []
    for part in parts:
        # folium takes (lat, lon); GeoJSON and shapely store (lon, lat).
        rings = [[(lat, lon) for lon, lat in ring.coords]
                 for ring in [part.exterior, *part.interiors]]
        polygons.append(folium.Polygon(
            locations=rings,
            color=style["color"],
            weight=style["weight"],
            fill=True,
            fill_color=style["fillColor"],
            fill_opacity=style["fillOpacity"],
        ))
    return polygons


def _add_draw_control(m: folium.Map, geom, label: str) -> None:
    """Put the outline on an editable layer and wire the draw toolbar to it."""
    group = folium.FeatureGroup(name=label)
    for polygon in _editable_polygons(geom, OUTLINE_STYLE):
        polygon.add_to(group)
    group.add_to(m)

    # feature_group= makes this group the draw control's own layer, so the
    # outline is editable in place and comes back in st_folium's all_drawings.
    Draw(
        feature_group=group,
        export=False,
        position="topleft",
        show_geometry_on_click=False,
        draw_options={
            "polyline": False, "circle": False, "circlemarker": False,
            "marker": False, "rectangle": False,
            "polygon": {"allowIntersection": False, "showArea": True},
        },
        edit_options={"edit": {}, "remove": True},
    ).add_to(m)


def _popup_html(props: dict) -> str:
    """The metadata card shown when a harbour on the map is clicked."""
    vessels = props.get("n_unique_mmsi", props.get("n_unique_mmsi_approx", 0))
    dwell = props.get("mean_dwell_minutes")
    lines = [
        f"<b>{props.get('nearest_city', 'Harbour')}, "
        f"{props.get('country_name', '')}</b>",
        f"ID: {props.get('harbour_id', '')[:8]}…",
        f"Events: {props.get('n_events', 0):,}",
        f"Vessels: {vessels:,}",
        f"H3 cells: {props.get('n_cells', 0)}",
        f"Draught changes: {props.get('n_draught_changes', 0)}",
    ]
    if dwell is not None:
        lines.append(f"Mean dwell: {float(dwell):.0f} min")
    if is_transit(props):
        lines.append(f"<b>{TRANSIT_BADGE}</b>")
    return "<br/>\n    ".join(lines)


def _add_harbour_layers(
    m: folium.Map,
    feat: dict,
    cells_geom: dict | None,
    show: str,
    outline_style: dict,
    cells_style: dict,
    skip_outline: bool = False,
) -> list[dict]:
    """
    Draw one harbour on the map. Returns the geometries it added, for the fit.

    Outline first so the finer cells stay legible on top of it. `skip_outline`
    is for the harbour being edited, whose outline the draw control owns.
    """
    props = feat.get("properties", {})
    city  = props.get("nearest_city", "Harbour")
    hid   = props.get("harbour_id", "")[:8]

    layers = []
    if show in (SHOW_OUTLINE, SHOW_BOTH) and feat.get("geometry") and not skip_outline:
        layers.append((feat["geometry"], outline_style, f"{city} — outline"))
    if show in (SHOW_CELLS, SHOW_BOTH) and cells_geom:
        layers.append((cells_geom, cells_style, f"{city} — H3 cells"))

    popup_html = _popup_html(props)
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

    return [geom for geom, _, _ in layers]


def _build_map(
    feat: dict,
    tile_url: str,
    tile_attr: str,
    tile_name: str,
    cells_geom: dict | None = None,
    show: str = SHOW_OUTLINE,
    editable: bool = False,
    siblings: list[dict] | None = None,
    sibling_cells: dict[str, dict] | None = None,
    sibling_outline_style: dict | None = None,
    sibling_cells_style: dict | None = None,
) -> folium.Map:
    props        = feat.get("properties", {})
    outline_geom = feat.get("geometry")

    clat = props.get("centroid_lat", 0.0)
    clon = props.get("centroid_lon", 0.0)

    m = folium.Map(location=[clat, clon], zoom_start=13, tiles=None)
    folium.TileLayer(tiles=tile_url, attr=tile_attr, name=tile_name).add_to(m)

    # The context harbours go down first — the city's others, or every flagged
    # site — so the selected one is on top wherever two outlines overlap.
    drawn = []
    for other in siblings or []:
        other_id = other.get("properties", {}).get("harbour_id")
        drawn += _add_harbour_layers(
            m, other, (sibling_cells or {}).get(other_id), show,
            sibling_outline_style or SIBLING_OUTLINE_STYLE,
            sibling_cells_style or SIBLING_CELLS_STYLE,
        )

    drawn += _add_harbour_layers(
        m, feat, cells_geom, show, OUTLINE_STYLE, CELLS_STYLE,
        skip_outline=editable,
    )

    if editable and outline_geom:
        city = props.get("nearest_city", "Harbour")
        _add_draw_control(m, shape(outline_geom), f"{city} — outline (editing)")
        drawn.append(outline_geom)

    # Fit the view to everything drawn — for one harbour that is its outline
    # (which already covers its cells), for a city view the whole group.
    if drawn:
        try:
            bounds = unary_union([shape(g) for g in drawn]).bounds
            m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])
        except Exception:
            pass

    return m


AUTO_LABEL, LOCK_LABEL, HARBOUR_LABEL = "Auto", "Lock", "Not a lock"


def _transit_panel(feat: dict, paths: list[str]) -> None:
    """
    Let an operator confirm or overrule the lock flag.

    Three states, not two. "Auto" is the absence of a verdict — the pipeline's
    own answer stands and keeps refreshing on each run — while "Not a lock" is
    a decision that outranks the detector for good. Collapsing those two into a
    checkbox would make un-flagging indistinguishable from never having looked,
    and the next run would simply flag it again.
    """
    props = feat.get("properties", {})
    hid = props.get("harbour_id", "")
    detected = bool(props.get(DETECTED_TRANSIT_KEY,
                              props.get("transit_like", False)))
    stored = manual_transit(props)

    options = [AUTO_LABEL, LOCK_LABEL, HARBOUR_LABEL]
    current = {None: AUTO_LABEL, True: LOCK_LABEL, False: HARBOUR_LABEL}[stored]
    detected_text = "a possible lock" if detected else "a harbour"

    with st.expander(
        "Site type" + (f"  •  {TRANSIT_BADGE}" if is_transit(props) else ""),
        expanded=False,
    ):
        dwell = props.get("mean_dwell_minutes")
        st.caption(
            f"The pipeline detected **{detected_text}**"
            + (f" — mean dwell {float(dwell):.0f} min," if dwell is not None else "")
            + f" {props.get('n_cargo', 0)} cargo and {props.get('n_tanker', 0)} "
              f"tanker stops, max {props.get('max_visits_per_mmsi', 0)} visit(s) "
              "by any one vessel."
        )
        choice = st.radio(
            "Is this a harbour or a ship lock?",
            options, index=options.index(current), horizontal=True,
            key=f"transit_choice_{hid}",
            help="Auto follows the detector and keeps updating. The other two "
                 "are your decision and survive every future run.",
        )
        if st.button("Save site type", key=f"save_transit_{hid}"):
            verdict = {AUTO_LABEL: None, LOCK_LABEL: True,
                       HARBOUR_LABEL: False}[choice]
            # Guard inside the handler: AppTest runs a click even when the
            # button is disabled, and a stray one must not rewrite a verdict.
            if verdict == stored:
                st.info("No change to save.")
            else:
                written = save_harbour_transit(paths, hid, verdict)
                if written:
                    props[MANUAL_TRANSIT_KEY] = verdict
                    props["transit_like"] = detected if verdict is None else verdict
                    st.success(
                        "Set to Auto — the detector decides again."
                        if verdict is None
                        else f"Saved: {choice}."
                    )
                    st.cache_data.clear()
                else:
                    st.error("Could not write the verdict to any output file.")


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


def outline_edit_note(detected, drawn) -> str | None:
    """
    Flag an outline edit that will not do what it looks like.

    Drawing *inside* the detected shape is not one of those cases: the stored
    outline is the union of both, by design, so there is nothing to warn about.
    Only an outline large enough to have caught a neighbour is worth a word.
    """
    if detected is None or detected.is_empty or drawn is None:
        return None

    if drawn.area > detected.area * 10:
        return (
            "The drawn outline is more than 10× the detected area — check that "
            "it does not swallow a neighbouring harbour."
        )
    return None


def _outline_panel(
    feat: dict,
    paths: list[str],
    outline_path: str,
    map_state: dict | None,
) -> None:
    """Save / revert controls for the outline being edited on the map above."""
    props    = feat.get("properties", {})
    hid      = props.get("harbour_id", "")
    detected = detected_geometry(feat)
    stored   = manual_outline(props)

    st.caption(
        "Use the ✏️ tool to drag the outline's vertices (its midpoint handles "
        "add new ones), or draw a replacement polygon with the ▱ tool and "
        "delete the old one. Everything left on the map is saved as one "
        "outline — it is stored as a floor, so later runs can grow the harbour "
        "but never shrink it back inside what you drew."
    )

    drawings  = (map_state or {}).get("all_drawings")
    drawn_wkt = outline_from_drawings(drawings)

    save_col, revert_col = st.columns([1, 1])
    save = save_col.button(
        "Save outline", type="primary", key=f"save_outline_{hid}",
        disabled=not drawn_wkt,
        help=None if drawn_wkt else "Draw or edit the outline on the map first.",
    )
    reverted = revert_col.button(
        "Revert to detected", key=f"revert_outline_{hid}", disabled=not stored,
        help="Drops the manual outline; the harbour falls back to the shape "
             "the pipeline detected.",
    )

    if save and not drawn_wkt:
        # The button is disabled in the browser, but never let a stray click
        # through: saving nothing would quietly clear the stored outline, which
        # is what "Revert to detected" is for.
        st.info("Nothing drawn — the outline is unchanged.")
        return

    if save:
        drawn  = from_wkt(drawn_wkt)
        merged = merge_outlines(detected, drawn)
        written = save_harbour_outline(
            paths, outline_path, hid, drawn_wkt,
            to_wkt(detected) if detected is not None else None,
            mapping(merged) if merged is not None else None,
        )
        if not written:
            st.error(f"Could not find harbour `{hid}` in any output file.")
            return
        _after_save(
            hid, outline_edit_note(detected, drawn),
            f"Saved outline to {', '.join(Path(p).name for p in written)}.",
        )

    if reverted:
        written = save_harbour_outline(
            paths, outline_path, hid, None,
            to_wkt(detected) if detected is not None else None,
            mapping(detected) if detected is not None else None,
        )
        _after_save(hid, None,
                    "Reverted to the detected outline "
                    f"({len(written)} file(s) updated).")


def _after_save(harbour_id: str, note: str | None, message: str) -> None:
    """Drop the data caches and rerun so the edit is visible immediately."""
    load_features.clear()
    load_geometry_by_id.clear()
    st.session_state["selected_harbour_id"] = harbour_id
    st.session_state["save_message"] = message
    st.session_state["save_note"] = note
    st.rerun()


def _map_legend(show: str, siblings: bool = False, locks: bool = False) -> None:
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
    if siblings:
        entries.append(
            swatch.format(fill=SIBLING_OUTLINE_STYLE["fillColor"],
                          line=SIBLING_OUTLINE_STYLE["color"])
            + "Other harbours in this city"
        )
    if locks:
        entries.append(
            swatch.format(fill=TRANSIT_OUTLINE_STYLE["fillColor"],
                          line=TRANSIT_OUTLINE_STYLE["color"])
            + "Flagged as a possible lock"
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
    _apply_map_assets(gui_cfg)

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

    # Resolve which harbour is selected. The session holds the answer; a table
    # row only overrides it on the rerun that follows its own click, because
    # the table keeps reporting that row afterwards — including on the reruns a
    # map click or a save triggers, which would otherwise undo them.
    selected_rows = list(getattr(getattr(selection, "selection", None), "rows", []))
    if selected_rows and selected_rows != st.session_state.get("table_rows"):
        clicked_idx = int(df.iloc[min(selected_rows[0], len(df) - 1)]["_idx"])
        st.session_state["selected_harbour_id"] = (
            features[clicked_idx].get("properties", {}).get("harbour_id")
        )
    st.session_state["table_rows"] = selected_rows

    global_idx = _index_for_harbour(
        features, st.session_state.get("selected_harbour_id")
    )
    if global_idx is None:
        global_idx = int(df.iloc[0]["_idx"]) if len(df) > 0 else 0

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
    dwell = props.get("mean_dwell_minutes")
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Events",          f"{props.get('n_events', 0):,}")
    m2.metric("Vessels",         f"{vessels:,}")
    m3.metric("H3 cells",        props.get("n_cells", 0))
    m4.metric("Mean dwell",      "—" if dwell is None else f"{float(dwell):.0f} min")
    m5.metric("Draught changes", props.get("n_draught_changes", 0))
    m6.metric("Country",         props.get("country_iso2", ""))

    if is_transit(props):
        st.warning(
            f"{TRANSIT_BADGE} — vessels pass through rather than stay. "
            "Open **Site type** below to confirm or overrule."
        )

    # ── Site type and manual property edits ────────────────────────────────
    _transit_panel(feat, [output_file, cells_file])
    _edit_panel(feat, [output_file, cells_file],
                existing_db=cfg.get("phase5", {}).get("existing_db_path", ""))

    # ── Map ────────────────────────────────────────────────────────────────
    hid = props.get("harbour_id", "")
    edit_label = "Edit outline"
    if manual_outline(props):
        edit_label += "  •  manually adjusted"

    edit_col, city_col, lock_col = st.columns(3)
    editing = edit_col.toggle(
        edit_label, key=f"outline_mode_{hid}",
        help="Turn on to drag the outline's vertices. The map then reports "
             "every edit back to the app, so it redraws on each change.",
    )

    sibling_idx = city_harbours(features, global_idx)
    city_view = False
    if sibling_idx:
        # Keyed globally, not per harbour: clicking a sibling changes the
        # selection, and a per-harbour key would switch the view straight back
        # off under the click that used it.
        city_view = city_col.toggle(
            f"Show all {len(sibling_idx) + 1} harbours in {city}",
            key="city_view",
            help="Draws the city's other harbours in grey. Click one to select "
                 "it — the metrics, the edit form and the outline editor all "
                 "follow the selection.",
        )

    # Every flagged site, wherever it is — the review view for the lock
    # detector. Keyed globally for the same reason as the city toggle.
    flagged_idx = [i for i in transit_harbours(features) if i != global_idx]
    lock_view = False
    if flagged_idx or is_transit(props):
        lock_view = lock_col.toggle(
            f"Show all {len(flagged_idx) + (1 if is_transit(props) else 0)} "
            "flagged as locks",
            key="lock_view",
            help="Draws every site flagged as a possible lock in amber, "
                 "wherever it is. Click one to select it.",
        )

    # The two context views are mutually exclusive: both hand the map a set of
    # other harbours to draw, and overlaying them would make a click ambiguous.
    if lock_view:
        context_idx = flagged_idx
        context_style = TRANSIT_OUTLINE_STYLE
    elif city_view:
        context_idx = sibling_idx
        context_style = SIBLING_OUTLINE_STYLE
    else:
        context_idx = []
        context_style = SIBLING_OUTLINE_STYLE

    fmap = _build_map(
        feat,
        tile_url=selected_tile["url"],
        tile_attr=selected_tile["attribution"],
        tile_name=selected_tile["name"],
        cells_geom=cells_by_id.get(props.get("harbour_id")),
        show=show_geom,
        editable=editing,
        siblings=[features[i] for i in context_idx] if context_idx else None,
        sibling_cells=cells_by_id if context_idx else None,
        sibling_outline_style=context_style,
    )

    # Each returned object costs a rerun per interaction, so ask only for the
    # one the current mode acts on: the drawings while editing, the click that
    # selects a harbour in the city view, nothing at all otherwise.
    if editing:
        mode, returned = "edit", ["all_drawings"]
    elif context_idx:
        mode, returned = "browse", ["last_object_clicked"]
    else:
        mode, returned = "view", []

    map_state = st_folium(
        fmap,
        use_container_width=True,
        height=500,
        returned_objects=returned,
        key=f"map_{hid}_{mode}",
    )

    if mode == "browse":
        candidates = [
            (features[i].get("properties", {}).get("harbour_id"),
             features[i].get("geometry"))
            for i in [global_idx, *context_idx]
        ]
        clicked = harbour_at_click(candidates,
                                   (map_state or {}).get("last_object_clicked"))
        if clicked and clicked != hid:
            st.session_state["selected_harbour_id"] = clicked
            # Clicking a harbour is a request to look at it — open the full
            # properties on the way through rather than making it a second step.
            st.session_state["expand_props"] = True
            st.rerun()

    if editing:
        if city_view:
            st.caption(
                "Click-to-select is paused while editing, so a stray click "
                "cannot swap harbours out from under an unsaved outline."
            )
        _outline_panel(feat, [output_file, cells_file], output_file, map_state)

    _map_legend(show_geom, siblings=city_view and not lock_view,
                locks=lock_view)

    # ── Details expander ───────────────────────────────────────────────────
    with st.expander("Full properties",
                     expanded=st.session_state.pop("expand_props", False)):
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
