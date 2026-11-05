"""
Harbour quiz — label random detected sites, one click each.

    streamlit run quiz.py

Each round shows `quiz_round_size` random harbours (default 20) with the same
statistics as app.py and the detector's own category, and asks for a verdict:
Harbour, Lock, Offshore waiting area or Not sure. Every answer is appended to
`<gui.quiz_dir>/quiz_<timestamp>.csv` the moment it is clicked, so closing the
browser loses nothing. Sites already answered in an earlier quiz file, or
carrying a manual lock/offshore verdict, are drawn last.

The answers feed back in two ways:
    python3 scripts/apply_quiz_verdicts.py data/output/quiz/*.csv
        → manual_transit_like / manual_offshore_like in the output GeoJSONs
    python3 scripts/calibrate_offshore.py data/output/harbours.geojson \\
        --verdicts data/output/quiz/*.csv

Offline behaviour is app.py's: tiles come from `gui.map_tiles`, and
`gui.local_map_assets` (or GUI__LOCAL_MAP_ASSETS) serves Leaflet from
static/vendor/. Run it from the repo root so .streamlit/config.toml applies.
"""

import csv
import html
import random
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

import app
from pipeline.h3_aggregation import VESSEL_COUNTS
from utils.overrides import manual_offshore, manual_transit

DEFAULT_QUIZ_DIR = "data/output/quiz"
DEFAULT_ROUND_SIZE = 20

# The answers, in button order. The key is what the CSV stores.
ANSWERS = {
    "harbour":  "🚢 Harbour",
    "lock":     "⚓ Lock",          # the badge app.py uses for a lock
    "offshore": "🌊 Offshore waiting area",
    "unsure":   "❓ Not sure",
}
# Every answer button is the same two-line block: the icon, then the words on
# one line, its size easing down on a narrow screen. A window too narrow even
# for that gets an ellipsis (and the hover tooltip) rather than a taller
# button. Scoped to the row's container (`st-key-<key>`).
ANSWER_ROW_KEY = "quiz_answers"
ANSWER_BUTTON_CSS = f"""
<style>
.st-key-{ANSWER_ROW_KEY} button {{
    height: 4.6rem;
    padding: 0.3rem 0.5rem;
}}
.st-key-{ANSWER_ROW_KEY} button p {{
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    font-size: clamp(0.75rem, 1.1vw, 1rem);
    line-height: 1.35;
    text-align: center;
}}
</style>
"""
CATEGORY_LABELS = {
    "harbour":       "Harbour",
    "lock":          "⚓ Lock",
    "offshore":      "🌊 Offshore waiting area",
    "lock+offshore": "⚓🌊 Lock and offshore",
}
# Phase 4's COMMERCIAL_COUNTS (pipeline/enrichment.py). Not imported: that
# module imports reverse_geocoder, which wants to download its dataset — not
# something a GUI that must run offline should risk for a two-item tuple.
COMMERCIAL_COUNTS = ("n_cargo", "n_tanker")

CSV_FIELDS = [
    "answered_at", "harbour_id", "nearest_city", "country_iso2",
    "centroid_lat", "centroid_lon", "coast_dist_km", "mean_dwell_minutes",
    "detector_category", "answer",
]


# ---------------------------------------------------------------------------
# Pure helpers (no Streamlit) — shared with the scripts and the tests
# ---------------------------------------------------------------------------

def detector_category(props: dict) -> str:
    """The site's effective category, as app.py shows it."""
    lock, offshore = app.is_transit(props), app.is_offshore(props)
    if lock and offshore:
        return "lock+offshore"
    if lock:
        return "lock"
    if offshore:
        return "offshore"
    return "harbour"


def agrees(category: str, answer: str) -> bool | None:
    """Whether an answer confirms the detector; None for "not sure"."""
    if answer == "unsure":
        return None
    return answer in category.split("+")


def read_answers(paths) -> dict[str, dict]:
    """
    harbour_id → its latest answer row across all quiz files.

    "Latest" is by `answered_at` (ISO-8601 UTC, so it sorts as text), which
    lets a later round correct an earlier one regardless of file order.
    """
    latest: dict[str, dict] = {}
    for path in paths:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                hid = row.get("harbour_id")
                if not hid or row.get("answer") not in ANSWERS:
                    continue
                prev = latest.get(hid)
                if prev is None or row["answered_at"] >= prev["answered_at"]:
                    latest[hid] = row
    return latest


def quiz_files(quiz_dir: str) -> list[Path]:
    d = Path(quiz_dir)
    return sorted(d.glob("quiz_*.csv")) if d.is_dir() else []


def reviewed_ids(quiz_dir: str, features: list[dict]) -> set[str]:
    """Sites a person has already judged: in a quiz file or by a manual verdict."""
    done = set(read_answers(quiz_files(quiz_dir)))
    for feat in features:
        props = feat.get("properties", {})
        if manual_transit(props) is not None or manual_offshore(props) is not None:
            done.add(props.get("harbour_id"))
    return done


def pick_round(features: list[dict], n: int, reviewed: set[str],
               rng: random.Random) -> list[str]:
    """
    `n` random harbour ids, unreviewed ones first.

    Only when there are fewer than `n` unreviewed sites is the round topped up
    with reviewed ones, so a nearly-finished database still gives full rounds.
    """
    ids = [f.get("properties", {}).get("harbour_id") for f in features]
    ids = list(dict.fromkeys(i for i in ids if i))   # harbour_id is not unique
    fresh = [i for i in ids if i not in reviewed]
    seen = [i for i in ids if i in reviewed]
    picked = rng.sample(fresh, min(n, len(fresh)))
    if len(picked) < n:
        picked += rng.sample(seen, min(n - len(picked), len(seen)))
    return picked


def answer_row(props: dict, answer: str, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    return {
        "answered_at":        now.isoformat(timespec="seconds"),
        "harbour_id":         props.get("harbour_id", ""),
        "nearest_city":       props.get("nearest_city", ""),
        "country_iso2":       props.get("country_iso2", ""),
        "centroid_lat":       props.get("centroid_lat", ""),
        "centroid_lon":       props.get("centroid_lon", ""),
        "coast_dist_km":      _blank_none(props.get("coast_dist_km")),
        "mean_dwell_minutes": _blank_none(props.get("mean_dwell_minutes")),
        "detector_category":  detector_category(props),
        "answer":             answer,
    }


def append_answer(path: Path, row: dict) -> None:
    """Append one answer, writing the header when the file is new."""
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists() or path.stat().st_size == 0
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if new:
            writer.writeheader()
        writer.writerow(row)


def commercial_share(props: dict) -> float | None:
    classified = sum(int(props.get(c) or 0) for c in VESSEL_COUNTS)
    if not classified:
        return None
    return sum(int(props.get(c) or 0) for c in COMMERCIAL_COUNTS) / classified


def _blank_none(value):
    return "" if value is None else value


def _fmt(value, pattern: str = "{:,.0f}", none: str = "—") -> str:
    try:
        return none if value is None else pattern.format(float(value))
    except (TypeError, ValueError):
        return none


# Stat tiles instead of st.metric: st.metric cuts a long value off at a fixed
# size. These wrap into fewer columns on a narrow screen, and every value
# carries its full text as a hover tooltip for whatever still does not fit.
STAT_TILES_CSS = """
<style>
.quiz-stats { display: grid; gap: 0.6rem 1rem;
              grid-template-columns: repeat(auto-fill, minmax(6.5rem, 1fr)); }
.quiz-stat-label { font-size: 0.8rem; opacity: 0.7; }
.quiz-stat-value { font-size: 1.3rem; line-height: 1.3; white-space: nowrap;
                   overflow: hidden; text-overflow: ellipsis; }
</style>
"""


def stat_tiles_html(stats: list[tuple[str, str]]) -> str:
    """A grid of label/value tiles; the full value is in each tile's title."""
    tiles = []
    for label, value in stats:
        label, value = html.escape(label), html.escape(value)
        tiles.append(
            f'<div class="quiz-stat" title="{label}: {value}">'
            f'<div class="quiz-stat-label">{label}</div>'
            f'<div class="quiz-stat-value">{value}</div></div>')
    return STAT_TILES_CSS + '<div class="quiz-stats">' + "".join(tiles) + "</div>"


# ---------------------------------------------------------------------------
# Streamlit
# ---------------------------------------------------------------------------

def _new_round(features: list[dict], n: int, quiz_dir: str) -> None:
    reviewed = reviewed_ids(quiz_dir, features)
    ids = pick_round(features, n, reviewed, random.Random())
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    st.session_state.update({
        "quiz_ids":     ids,
        "quiz_answers": {},
        "quiz_pos":     0,
        "quiz_file":    str(Path(quiz_dir) / f"quiz_{stamp}.csv"),
        "quiz_topped_up": sum(i in reviewed for i in ids),
        "quiz_revisit": False,
    })


def _record(props: dict, answer: str) -> None:
    """Button callback: persist first, then advance."""
    append_answer(Path(st.session_state["quiz_file"]), answer_row(props, answer))
    st.session_state["quiz_answers"][props["harbour_id"]] = answer
    if st.session_state.get("quiz_revisit"):
        # A changed mind from the overview: the new row is appended (the
        # latest answer wins everywhere), then straight back to the overview.
        _back_to_overview()
    else:
        st.session_state["quiz_pos"] += 1
    category = detector_category(props)
    st.toast(f"{props.get('nearest_city', 'Site')}: you said "
             f"{ANSWERS[answer]}, the detector said {CATEGORY_LABELS[category]}")


def _back_to_overview() -> None:
    st.session_state["quiz_pos"] = len(st.session_state["quiz_ids"])
    st.session_state["quiz_revisit"] = False
    # A fresh table key, so the row clicked last time is not selected again.
    st.session_state["quiz_table_gen"] = st.session_state.get("quiz_table_gen", 0) + 1


def _open_from_overview(table_key: str) -> None:
    """Overview row click: show that harbour again to change the answer."""
    rows = st.session_state[table_key].selection.rows
    if rows:
        st.session_state["quiz_pos"] = int(rows[0])
        st.session_state["quiz_revisit"] = True


def _stats(props: dict, hide_verdict: bool) -> None:
    vessels = props.get("n_unique_mmsi", props.get("n_unique_mmsi_approx"))
    share = commercial_share(props)
    city = props.get("nearest_city") or "—"
    city_km = props.get("nearest_city_dist_km")
    if city_km is not None:
        city += f" ({_fmt(city_km, '{:.1f}')} km)"

    # The city as a heading: it wraps, so the whole name is always readable.
    st.markdown(f"#### {city}")
    st.markdown(f"**{props.get('harbour_id', '')}** · "
                f"{props.get('country_name', '')} · {props.get('admin1', '')}")
    if hide_verdict:
        st.info("Detector: *hidden until you answer*")
    else:
        category = detector_category(props)
        badge = st.success if category == "harbour" else st.warning
        badge(f"Detector: **{CATEGORY_LABELS[category]}**")

    st.markdown(stat_tiles_html([
        ("Distance to land", app.coast_km_text(props)),
        ("Mean dwell", _fmt(props.get("mean_dwell_minutes"), "{:,.0f} min")),
        ("Events", _fmt(props.get("n_events"))),
        ("Vessels", _fmt(vessels)),
        ("Max visits / vessel", _fmt(props.get("max_visits_per_mmsi"))),
        ("H3 cells", _fmt(props.get("n_cells"))),
        ("Commercial share", _fmt(share, "{:.0%}")),
        ("Draught changes", _fmt(props.get("n_draught_changes"))),
    ]), unsafe_allow_html=True)

    mix = {c.removeprefix("n_").replace("_", "/"): int(props.get(c) or 0)
           for c in VESSEL_COUNTS}
    if any(mix.values()):
        st.caption("Vessel mix: " + " · ".join(
            f"{k} {v}" for k, v in mix.items() if v))


def _overview(features_by_id: dict[str, dict]) -> None:
    answers = st.session_state["quiz_answers"]
    rows = []
    for hid in st.session_state["quiz_ids"]:
        props = features_by_id.get(hid, {}).get("properties", {})
        category = detector_category(props)
        answer = answers.get(hid)
        ok = agrees(category, answer) if answer else None
        rows.append({
            "ID":       hid,
            "City":     props.get("nearest_city", ""),
            "Country":  props.get("country_iso2", ""),
            "Detector": CATEGORY_LABELS[category],
            "You":      ANSWERS.get(answer, "—") if answer else "—",
            "Agree":    {True: "✅", False: "❌", None: "–"}[ok],
        })
    df = pd.DataFrame(rows)

    judged = df[df["Agree"] != "–"]
    st.subheader("Round complete")
    c1, c2, c3 = st.columns(3)
    c1.metric("Answered", f"{len(answers)} / {len(rows)}")
    n_agree = int((judged["Agree"] == "✅").sum())
    c2.metric("Agree with detector", f"{n_agree} / {len(judged)}")
    c3.metric("Not sure", int((df["You"] == ANSWERS["unsure"]).sum()))
    st.caption("Click a row to see that harbour again and change your answer.")
    table_key = f"quiz_table_{st.session_state.get('quiz_table_gen', 0)}"
    st.dataframe(df, hide_index=True, use_container_width=True, key=table_key,
                 on_select=lambda: _open_from_overview(table_key),
                 selection_mode="single-row")

    path = Path(st.session_state["quiz_file"])
    st.caption(f"Saved to `{path}`")
    if path.exists():
        st.download_button("Download CSV", path.read_bytes(), file_name=path.name,
                           mime="text/csv")
    st.markdown("Apply the verdicts to the harbour database, then re-run Phase 5:")
    st.code(f"python3 scripts/apply_quiz_verdicts.py {path}", language="bash")
    st.markdown("Use them to calibrate the offshore threshold:")
    st.code("python3 scripts/calibrate_offshore.py data/output/harbours.geojson "
            f"--verdicts {path}", language="bash")


def main() -> None:
    st.set_page_config(page_title="Harbour quiz", layout="wide")

    cfg = app.load_config()
    gui_cfg = cfg.get("gui", {})
    # Before any map is built: in an offline deployment this is what moves
    # Leaflet off the public CDNs, exactly as in app.py.
    app._apply_map_assets(gui_cfg)

    output_file = gui_cfg.get("output_file", "data/output/harbours.geojson")
    cells_file = app._cells_path(output_file, gui_cfg)
    quiz_dir = gui_cfg.get("quiz_dir", DEFAULT_QUIZ_DIR)
    tile_layers = gui_cfg.get("map_tiles", [{
        "name": "OpenStreetMap",
        "url": "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
        "attribution": "© OpenStreetMap contributors",
    }])
    default_tile = gui_cfg.get("default_tile", tile_layers[0]["name"])

    if not Path(output_file).exists():
        st.error(f"**Output file not found:** `{output_file}` — run the pipeline "
                 "first.")
        return
    features = app.load_features(output_file)
    if not features:
        st.warning("The output file contains no harbour features.")
        return
    features_by_id: dict[str, dict] = {}
    for feat in features:
        features_by_id.setdefault(feat.get("properties", {}).get("harbour_id"), feat)
    cells_by_id = app.load_geometry_by_id(cells_file)

    with st.sidebar:
        st.title("Harbour quiz")
        st.caption("What kind of site is this?")
        names = [t["name"] for t in tile_layers]
        tile_name = st.selectbox(
            "Map tiles", names,
            index=names.index(default_tile) if default_tile in names else 0)
        tile = next(t for t in tile_layers if t["name"] == tile_name)
        n = st.number_input("Harbours per round", 1, 200,
                            int(gui_cfg.get("quiz_round_size", DEFAULT_ROUND_SIZE)))
        show = st.toggle("Show the detector's verdict", value=False,
                         help="Off: answer without being nudged. The overview "
                              "at the end shows both either way.")
        if st.button("New round", use_container_width=True):
            _new_round(features, int(n), quiz_dir)
        st.divider()
        st.caption(f"Answers are written to `{quiz_dir}/`.")

    if "quiz_ids" not in st.session_state:
        _new_round(features, int(n), quiz_dir)

    ids = st.session_state["quiz_ids"]
    pos = st.session_state["quiz_pos"]
    if pos >= len(ids):
        _overview(features_by_id)
        return

    hid = ids[pos]
    feat = features_by_id[hid]
    props = feat.get("properties", {})

    if st.session_state.get("quiz_revisit"):
        before = st.session_state["quiz_answers"].get(hid)
        st.info(f"Changing your answer for harbour {pos + 1} / {len(ids)} — "
                f"you said **{ANSWERS.get(before, '—')}**.")
        st.button("← Back to the overview", on_click=_back_to_overview)
    else:
        st.progress(pos / len(ids), text=f"Harbour {pos + 1} / {len(ids)}")
    if (pos == 0 and st.session_state.get("quiz_topped_up")
            and not st.session_state.get("quiz_revisit")):
        st.caption(f"Only {len(ids) - st.session_state['quiz_topped_up']} "
                   "unreviewed sites were left; the rest of this round has "
                   "been judged before.")

    left, right = st.columns([3, 2])
    with left:
        m = app._build_map(
            feat, tile["url"], tile["attribution"], tile["name"],
            cells_geom=cells_by_id.get(hid),
            show=app.SHOW_BOTH if cells_by_id else app.SHOW_OUTLINE,
        )
        st_folium(m, height=520, use_container_width=True,
                  returned_objects=[], key=f"quiz_map_{pos}_{hid}")
    with right:
        _stats(props, hide_verdict=not show)

    # Full width, under the map: in the map's column alone a button is too
    # narrow for "Offshore waiting area" to fit on one line.
    st.markdown(ANSWER_BUTTON_CSS, unsafe_allow_html=True)
    with st.container(key=ANSWER_ROW_KEY):
        columns = st.columns(len(ANSWERS))
        for col, (key, label) in zip(columns, ANSWERS.items()):
            icon, _, words = label.partition(" ")
            # Two Markdown lines: the icon, then the words.
            col.button(f"{icon}  \n{words}", key=f"answer_{key}",
                       use_container_width=True, help=words,
                       on_click=_record, args=(props, key))


if __name__ == "__main__":
    main()
