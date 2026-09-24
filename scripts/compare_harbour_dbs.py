#!/usr/bin/env python3
"""
Compare the pipeline's harbours against a harbour database built elsewhere.

The detector has no external check on whether the harbours it finds are real.
This points it at somebody else's database and reports where the two agree,
how closely their outlines agree, and what each side holds alone — then draws
the whole thing on a map so the disagreements can be looked at rather than
guessed about.

Neither side is treated as the truth. The other program has its own error rate,
so a harbour only one side holds is reported as `only_in_old` / `only_in_new`,
never as a miss or a false positive, and the two agreement rates are kept
separate rather than folded into an F1 that would imply a verdict.

The hard part is that two harbour databases rarely agree on what a harbour
*polygon* is. Ours is the trafficked water — a 75 m closing of the cells where
ships actually stopped. Another database may draw the whole administrative port
area, land included, in which case a perfect detection scores an IoU around
0.15. So nothing here links on IoU: pairs link on shared area, on one-sided
containment, or on the gap between them. Before any number is printed, the run
calibrates the two conventions against each other and says which geometry
statistic is worth reading.

Correspondence is not forced one-to-one. A port the pipeline split into three
basins is reported as one 1:3 group, not as one disappearance and three
inventions.

Local files only. Copy from S3 first — `pathlib.Path` collapses `s3://bucket`
to `s3:/bucket`, so an S3 URI cannot be passed here.

Usage:
    python3 scripts/compare_harbour_dbs.py --old their_harbours.geojson

    python3 scripts/compare_harbour_dbs.py --old their_harbours.geojson \\
        --new data/output/harbours.geojson --out-dir /tmp/harbour-compare

    # Self-check against a known-good pair: two of our own runs.
    python3 scripts/compare_harbour_dbs.py \\
        --old data/existing_db/harbours.res8-backup.geojson

    # Their polygons cover more ground than the AIS run did.
    python3 scripts/compare_harbour_dbs.py --old theirs.geojson --coverage-gate

    # Map that works without internet (run scripts/vendor_map_assets.py first).
    python3 scripts/compare_harbour_dbs.py --old theirs.geojson --offline
"""

import argparse
import csv
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.cluster_formation import _connected_components  # noqa: E402
from utils.compare import (  # noqa: E402
    area_m2,
    calibrate,
    compare_names,
    components,
    coverage_cells,
    diagnose_only_in_old,
    in_coverage,
    links,
    load_interim_layers,
    load_new,
    load_old,
    metric_frame,
    nearest_other,
    oversized,
    primary_links,
    score,
    stratify,
)
from utils.map_assets import VENDOR_DIR, is_vendored, use_local_assets  # noqa: E402

# One res-11 cell is ~50 m across, so 2500 m² is about the smallest overlap
# that can be a shared berth rather than a boundary sliver.
MIN_OVERLAP_M2 = 2500.0
# Half of one shape inside the other is unambiguous even when the two draw
# harbours at completely different scales.
MIN_COVERAGE = 0.5
# Three times the Phase 4 outline buffer (75 m). Loose enough to bridge a
# convention mismatch, tight enough that Hellerup's 442 m neighbours — the
# closest pair in the reference run — do not chain into one group.
MAX_GAP_METERS = 250.0
# Beyond this a group is more likely a chain through an oversized polygon than
# one harbour complex, so it is reported but kept out of the medians.
MAX_COMPONENT = 8
# A polygon bigger than this across is a data error, not a harbour.
MAX_OLD_SPAN_KM = 50.0
# Tolerance for "was there any AIS evidence inside this harbour at all".
DIAG_BUFFER_METERS = 200.0
# A counterpart this close means the two sides disagree about extent, not
# about whether the harbour exists.
NEARBY_METERS = 1000.0
# ~8 km cells, dilated one ring → a ~25 km tolerance around real moorings.
COVERAGE_RESOLUTION = 5
# Display only. Never apply this to pipeline geometry: phase4's
# outline_simplify_meters must stay 0, because a tolerance of even 10 m can
# pull an outline inside a trafficked res-11 cell.
MAP_SIMPLIFY_METERS = 5.0

STYLES = {
    "only_in_old": {"color": "#d62728", "weight": 2, "fillOpacity": 0.35},
    "only_in_new": {"color": "#1f77b4", "weight": 2, "fillOpacity": 0.35},
    "matched_old": {"color": "#e377c2", "weight": 1, "fillOpacity": 0.12},
    "matched_new": {"color": "#2ca02c", "weight": 1, "fillOpacity": 0.12},
    "transit": {"color": "#ff7f0e", "weight": 3, "fillOpacity": 0.25},
}


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def _rule(title: str) -> None:
    print(f"\n{title}\n{'-' * len(title)}")


def _print_inputs(choice, n_old, n_new, invalid_old, invalid_new, big) -> None:
    _rule("Inputs")
    print(f"  old database : {n_old} usable harbour polygons")
    print(f"  new database : {n_new} usable harbour polygons")
    fill = choice.fill
    for kind, value in (("id", choice.id_field),
                        ("name", choice.name_field),
                        ("country", choice.country_field)):
        rate = fill.get(kind)
        if value is None:
            fallback = ("ids synthesised by position" if kind == "id"
                        else "not compared")
            print(f"  old {kind:<8}: not found — {fallback}")
        else:
            suffix = f" ({rate:.0%} filled)" if rate is not None else ""
            print(f"  old {kind:<8}: '{value}'{suffix}")
    if invalid_old or invalid_new:
        print(f"  dropped      : {len(invalid_old)} old and "
              f"{len(invalid_new)} new features enclosed no usable area")
    if big:
        print(f"  WARNING      : {len(big)} old polygon(s) span more than "
              f"{MAX_OLD_SPAN_KM:g} km — likely a data error: "
              f"{', '.join(big[:5])}")


def _print_calibration(cal) -> None:
    _rule("Do the two databases mean the same thing by 'harbour polygon'?")
    print(f"  verdict              : {cal.regime}  "
          f"(from {cal.n_pairs} unambiguous 1:1 pair(s))")
    print(f"  median area ratio    : {cal.median_area_ratio:.2f}  (new / old)")
    print(f"  median coverage_old  : {cal.median_coverage_old:.2f}  "
          "(how much of the old shape the new one covers)")
    print(f"  median coverage_new  : {cal.median_coverage_new:.2f}  "
          "(how much of the new shape the old one covers)")
    print(f"  median IoU           : {cal.median_iou:.2f}")
    print(f"\n  {cal.note}")
    print(f"  Headline geometry statistic: {cal.headline}")


def _print_scores(scores) -> None:
    _rule("Agreement")
    print(f"  old harbours corroborated by the pipeline : "
          f"{scores['matched_old']}/{scores['n_old']}  "
          f"({scores['agreement_old']:.1%})")
    print(f"  pipeline harbours corroborated by the old : "
          f"{scores['matched_new']}/{scores['n_new']}  "
          f"({scores['agreement_new']:.1%})")
    print(f"  only in old : {scores['only_in_old']}")
    print(f"  only in new : {scores['only_in_new']}")
    print("\n  No combined score: with no ground truth, an F1 would assert "
          "that\n  the two sides' omissions are the same kind of error.")

    _rule("How the two sides line up")
    labels = {
        "1:1": "one to one",
        "1:0": "only in old",
        "0:1": "only in new",
        "1:N": "one old  -> several new  (pipeline split it)",
        "N:1": "several old -> one new   (pipeline merged them)",
        "N:M": "many to many",
    }
    for kind, count in scores["cardinality"].items():
        print(f"  {kind:<5} {count:>5}   {labels.get(kind, '')}")
    if scores["n_tangled"]:
        print(f"\n  {scores['n_tangled']} group(s) exceeded {MAX_COMPONENT} "
              "members and were kept out of the medians.")


def _print_geometry(pairs, cal) -> None:
    if not pairs:
        return
    _rule("Shape agreement, over matched pairs")
    import numpy as np

    def quantiles(values):
        arr = np.asarray(values, dtype=float)
        q = np.percentile(arr, [25, 50, 75])
        return q[0], q[1], q[2]

    rows = (
        ("IoU", [p.iou for p in pairs], "{:.2f}"),
        ("coverage_old", [p.coverage_old for p in pairs], "{:.2f}"),
        ("coverage_new", [p.coverage_new for p in pairs], "{:.2f}"),
        ("area ratio new/old",
         [p.area_new_m2 / p.area_old_m2 for p in pairs if p.area_old_m2 > 0],
         "{:.2f}"),
        ("centroid shift (m)", [p.centroid_m for p in pairs], "{:.0f}"),
    )
    print(f"  {'':<22}{'p25':>10}{'median':>10}{'p75':>10}")
    for label, values, fmt in rows:
        if not values:
            continue
        lo, mid, hi = quantiles(values)
        marker = "  <- headline" if label.lower().startswith(
            cal.headline.split("_")[0]) and cal.headline in (
                "iou", "coverage_old", "coverage_new") and (
                    label.replace(" ", "_").lower().startswith(cal.headline)
                    or label.lower() == cal.headline) else ""
        print(f"  {label:<22}{fmt.format(lo):>10}{fmt.format(mid):>10}"
              f"{fmt.format(hi):>10}{marker}")


def _print_attributes(rows) -> None:
    compared = [r for r in rows if r["country_match"] != "unknown"]
    _rule("Attributes, over each old harbour's best counterpart")
    if compared:
        agree = sum(1 for r in compared if r["country_match"] == "same")
        print(f"  country agrees : {agree}/{len(compared)} "
              f"({agree / len(compared):.1%})")
        bad = [r for r in compared if r["country_match"] == "differs"]
        if bad:
            print("  The country is hashed into harbour_id, so each of these "
                  "would re-id\n  the harbour if the pipeline adopted the "
                  "other spelling:")
            for r in bad[:10]:
                print(f"    {str(r['old_name'] or '')[:22]:<22} "
                      f"{r['old_country']} vs {r['new_country']}  "
                      f"({r['new_id']})")
            if len(bad) > 10:
                print(f"    … and {len(bad) - 10} more in matched.csv")
    else:
        print("  country : not comparable (no country field on the old side)")

    named = [r for r in rows if r["name_match"] != "unknown"]
    if named:
        tally = {}
        for r in named:
            tally[r["name_match"]] = tally.get(r["name_match"], 0) + 1
        parts = ", ".join(f"{k} {v}" for k, v in sorted(tally.items()))
        print(f"\n  name vs nearest_city : {parts}  (of {len(named)})")
        print("  Not a name-accuracy score: the pipeline has no port name, "
              "only the\n  gazetteer's nearest_city, so this rates Phase 4's "
              "city pick.")


def _print_only_in_old(records, diag) -> None:
    _rule(f"Only in the old database ({len(records)})")
    if not records:
        print("  none")
        return
    legend = {
        "unpaired_nearby": "a pipeline harbour is right there, but the two "
                           "disagree on extent",
        "no_stops": "no AIS stop inside it (or one vessel only — the stop "
                    "file predates the Phase-2 floor, the cell file does not, "
                    "so the two cannot be told apart)",
        "stops_below_cell_floor": "stops, but no cell survived Phase 2",
        "below_cluster_floor": "cells, but no cluster survived Phase 3 — "
                               "INFERRED, not observed: the interim file "
                               "holds only surviving clusters",
        "detected_not_linked": "a cluster is there; nothing above explains it",
        "unknown": "an interim file was missing, so nothing could be checked",
    }
    tally: dict[str, int] = {}
    for r in records:
        b = diag[r.rec_id].bucket if r.rec_id in diag else "unknown"
        tally[b] = tally.get(b, 0) + 1
    for bucket, count in sorted(tally.items(), key=lambda kv: -kv[1]):
        print(f"  {count:>5}  {bucket}")
        print(f"         {legend.get(bucket, '')}")
    if tally.get("no_stops"):
        share = tally["no_stops"] / len(records)
        if share > 0.5:
            print(f"\n  NOTE: {share:.0%} of these had no AIS stop at all. "
                  "The two databases\n  probably do not cover the same ground "
                  "— re-run with --coverage-gate.")


def _print_only_in_new(records, nearest, transit_ids) -> None:
    _rule(f"Only in the pipeline output ({len(records)})")
    if not records:
        print("  none")
        return
    buckets = {"1-5": 0, "6-20": 0, "21-100": 0, "100+": 0}
    close = 0
    for r in records:
        n = int(r.props.get("n_unique_mmsi") or 0)
        key = ("1-5" if n <= 5 else "6-20" if n <= 20
               else "21-100" if n <= 100 else "100+")
        buckets[key] += 1
        gap = nearest.get(r.rec_id, (None, None))[1]
        if gap is not None and gap <= NEARBY_METERS:
            close += 1
    print("  by distinct vessels seen:")
    for key, count in buckets.items():
        print(f"    {key:<8} {count:>5}")
    if close:
        print(f"\n  {close} sit within {NEARBY_METERS:.0f} m of an old "
              "harbour — likely a split\n  of one site rather than a separate "
              "one.")
    busiest = sorted(records,
                     key=lambda r: -int(r.props.get("n_events") or 0))[:10]
    print("\n  Busiest, worth looking at first:")
    for r in busiest:
        gap = nearest.get(r.rec_id, (None, None))[1]
        gap_s = f"{gap:.0f} m to nearest old" if gap is not None else "—"
        flag = "  [transit_like]" if r.rec_id in transit_ids else ""
        print(f"    {str(r.name or '')[:20]:<20} {r.rec_id:<14} "
              f"{int(r.props.get('n_events') or 0):>7} events   "
              f"{gap_s}{flag}")


def _print_transit(new_records, matched_new, transit_ids) -> None:
    flagged = [r for r in new_records if r.rec_id in transit_ids]
    if not flagged:
        return
    _rule(f"Sites the pipeline flagged transit_like ({len(flagged)})")
    print("  The old database carries no type field, so lock detection cannot "
          "be\n  scored against it. Listed for review only.")
    for r in flagged:
        state = "matched" if r.rec_id in matched_new else "only in new"
        print(f"    {str(r.name or '')[:20]:<20} {r.rec_id:<14} {state}")


# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

def _write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _matched_rows(link_list, comp_of, old_by_id, new_by_id) -> list[dict]:
    rows = []
    for lk in link_list:
        old, new = old_by_id[lk.old_id], new_by_id[lk.new_id]
        rows.append({
            "component_id": comp_of.get(f"old:{lk.old_id}", -1),
            "cardinality": comp_of.get(f"card:old:{lk.old_id}", ""),
            "old_id": lk.old_id,
            "new_id": lk.new_id,
            "old_name": old.name or "",
            "new_nearest_city": new.name or "",
            "name_match": compare_names(old.name, new.name),
            "old_country": old.country or "",
            "new_country": new.country or "",
            "country_match": _country_match(old.country, new.country),
            "iou": round(lk.iou, 4),
            "coverage_old": round(lk.coverage_old, 4),
            "coverage_new": round(lk.coverage_new, 4),
            "area_old_m2": round(lk.area_old_m2, 1),
            "area_new_m2": round(lk.area_new_m2, 1),
            "inter_m2": round(lk.inter_m2, 1),
            "gap_m": round(lk.gap_m, 1),
            "centroid_m": round(lk.centroid_m, 1),
            "n_events": new.props.get("n_events"),
            "n_unique_mmsi": new.props.get("n_unique_mmsi"),
            "transit_like": bool(new.props.get("transit_like")),
        })
    rows.sort(key=lambda r: (-r["iou"], r["gap_m"]))
    return rows


def _country_match(old_country, new_country) -> str:
    if not old_country or not new_country:
        return "unknown"
    return "same" if old_country == new_country else "differs"


def _simplify(geom, lat, lon, tolerance_m):
    """
    Thin a polygon for drawing only.

    Display only, and kept well away from the pipeline: phase4's
    `outline_simplify_meters` must stay 0 because a tolerance of even 10 m can
    pull an outline inside a res-11 cell that really saw traffic. Nothing here
    feeds back into the data.
    """
    if tolerance_m <= 0:
        return geom
    from shapely.ops import transform
    to_m, to_deg = metric_frame(lat, lon)
    thinned = transform(to_m, geom).simplify(tolerance_m,
                                             preserve_topology=True)
    return transform(to_deg, thinned) if not thinned.is_empty else geom


def _build_map(out_path, old_records, new_records, matched_old, matched_new,
               best, diag, transit_ids, *, tiles, tiles_attr, simplify_m,
               offline):
    """Draw both databases, layer per category, so misses can be eyeballed."""
    import folium                                            # noqa: PLC0415
    from shapely.geometry import mapping

    if offline:
        if not is_vendored():
            print("  --offline: static/vendor/ is not filled — run "
                  "scripts/vendor_map_assets.py first. Falling back to CDNs.")
            offline = False
        else:
            shutil.copytree(VENDOR_DIR, out_path.parent / "vendor",
                            dirs_exist_ok=True)
            use_local_assets("vendor")

    everything = [r for r in old_records] + [r for r in new_records]
    if not everything:
        return None
    lats = [r.lat for r in everything]
    lons = [r.lon for r in everything]
    fmap = folium.Map(
        location=[sum(lats) / len(lats), sum(lons) / len(lons)],
        tiles=None if offline else tiles,
        attr=None if offline else tiles_attr,
        zoom_start=8,
    )

    n_lonely_old = len(old_records) - len(matched_old)
    n_lonely_new = len(new_records) - len(matched_new)
    groups = {
        key: folium.FeatureGroup(name=label, show=show)
        for key, label, show in (
            ("only_in_old", f"only in old ({n_lonely_old})", True),
            ("only_in_new", f"only in new ({n_lonely_new})", True),
            ("matched_old", f"matched — old ({len(matched_old)})", True),
            ("matched_new", f"matched — new ({len(matched_new)})", True),
            ("transit", f"transit_like ({len(transit_ids)})", False),
        )
    }

    def add(rec, key, html):
        geom = _simplify(rec.geom, rec.lat, rec.lon, simplify_m)
        layer = folium.GeoJson(
            mapping(geom),
            style_function=lambda _f, s=STYLES[key]: dict(s),
        )
        # A static popup must be attached, not passed as popup=: folium types
        # that argument as a GeoJsonPopup over per-feature fields.
        folium.Popup(html, max_width=380).add_to(layer)
        layer.add_to(groups[key])

    for rec in old_records:
        matched = rec.rec_id in matched_old
        lk = best.get(("old", rec.rec_id))
        rows = [f"<b>{rec.name or rec.rec_id}</b>", f"old · {rec.rec_id}"]
        if rec.country:
            rows.append(f"country: {rec.country}")
        if matched and lk is not None:
            rows += [f"matched: {lk.new_id}",
                     f"IoU {lk.iou:.2f} · coverage_old {lk.coverage_old:.2f}"
                     f" · coverage_new {lk.coverage_new:.2f}",
                     f"centroid shift {lk.centroid_m:.0f} m"]
        else:
            d = diag.get(rec.rec_id)
            if d is not None:
                rows += [f"<b>only in old</b> — {d.bucket}",
                         f"stops {d.n_stops} · cells {d.n_cells} · "
                         f"clusters {d.n_clusters}"]
                if d.nearest_id:
                    rows.append(f"nearest new: {d.nearest_id} "
                                f"({d.nearest_gap_m:.0f} m)")
            else:
                rows.append("<b>only in old</b>")
        add(rec, "matched_old" if matched else "only_in_old", "<br>".join(rows))

    for rec in new_records:
        matched = rec.rec_id in matched_new
        lk = best.get(("new", rec.rec_id))
        rows = [f"<b>{rec.name or rec.rec_id}</b>", f"new · {rec.rec_id}",
                f"{rec.props.get('n_events')} events · "
                f"{rec.props.get('n_unique_mmsi')} vessels"]
        if matched and lk is not None:
            rows += [f"matched: {lk.old_id}", f"IoU {lk.iou:.2f}"]
        else:
            rows.append("<b>only in new</b>")
        add(rec, "matched_new" if matched else "only_in_new", "<br>".join(rows))
        if rec.rec_id in transit_ids:
            add(rec, "transit", f"{rec.name or rec.rec_id}<br>transit_like")

    for group in groups.values():
        group.add_to(fmap)
    folium.LayerControl(collapsed=False).add_to(fmap)

    bounds = [[min(lats), min(lons)], [max(lats), max(lons)]]
    fmap.fit_bounds(bounds)
    fmap.save(str(out_path))
    return out_path


# ---------------------------------------------------------------------------

def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--old", type=Path, required=True,
                   help="the other program's harbour database (GeoJSON)")
    p.add_argument("--new", type=Path,
                   default=Path("data/output/harbours.geojson"),
                   help="the pipeline's output (default: %(default)s)")
    p.add_argument("--out-dir", type=Path, default=Path("data/output/compare"),
                   help="where the report files go (default: %(default)s)")
    p.add_argument("--old-id-field", help="override id property detection")
    p.add_argument("--old-name-field", help="override name property detection")
    p.add_argument("--old-country-field",
                   help="override country property detection")
    p.add_argument("--min-overlap-m2", type=float, default=MIN_OVERLAP_M2,
                   help="shared area that links a pair (default: %(default)s)")
    p.add_argument("--min-coverage", type=float, default=MIN_COVERAGE,
                   help="one-sided containment that links a pair "
                        "(default: %(default)s)")
    p.add_argument("--max-gap-m", type=float, default=MAX_GAP_METERS,
                   help="surface gap that still links a pair "
                        "(default: %(default)s)")
    p.add_argument("--max-component", type=int, default=MAX_COMPONENT,
                   help="group size above which a group is called tangled "
                        "and kept out of the medians (default: %(default)s)")
    p.add_argument("--max-old-span-km", type=float, default=MAX_OLD_SPAN_KM,
                   help="warn about old polygons wider than this")
    p.add_argument("--interim-dir", type=Path, default=Path("data/interim"),
                   help="where to look for the evidence behind a non-match")
    p.add_argument("--no-diagnose", action="store_true",
                   help="skip the interim-file diagnosis of unmatched records")
    p.add_argument("--coverage-gate", action="store_true",
                   help="exclude old harbours outside the area the AIS run "
                        "actually covered; off by default because dropping "
                        "rows silently inflates the agreement rate")
    p.add_argument("--countries", help="comma-separated ISO2 allowlist")
    p.add_argument("--bbox", help="MINLON,MINLAT,MAXLON,MAXLAT")
    p.add_argument("--exclude-transit", action="store_true",
                   help="drop transit_like sites from the new side, as a "
                        "sensitivity check on the new-side agreement rate")
    p.add_argument("--no-map", action="store_true", help="skip the HTML map")
    p.add_argument("--offline", action="store_true",
                   help="serve Leaflet from static/vendor/ next to the page; "
                        "note the basemap tiles still need internet, so "
                        "offline means geometry on a blank background")
    p.add_argument("--tiles-url", default="OpenStreetMap",
                   help="basemap tile layer (default: %(default)s)")
    p.add_argument("--tiles-attr", default=None, help="tile attribution")
    p.add_argument("--map-simplify-m", type=float,
                   default=MAP_SIMPLIFY_METERS,
                   help="thin outlines for drawing only (default: %(default)s)")
    p.add_argument("--min-agreement-old", type=float,
                   help="exit non-zero below this old-side agreement rate")
    p.add_argument("--min-agreement-new", type=float,
                   help="exit non-zero below this new-side agreement rate")
    return p


def main(argv=None) -> int:
    args = _parser().parse_args(argv)

    if not args.old.exists():
        print(f"No such file: {args.old}")
        return 1
    if not args.new.exists():
        print(f"No such file: {args.new}")
        return 1

    old_records, choice, invalid_old = load_old(
        args.old, id_field=args.old_id_field, name_field=args.old_name_field,
        country_field=args.old_country_field)
    new_records, invalid_new = load_new(args.new)
    big = oversized(old_records, args.max_old_span_km)
    _print_inputs(choice, len(old_records), len(new_records),
                  invalid_old, invalid_new, big)

    if not old_records:
        print("\nThe old database holds no usable harbour polygons. "
              "Nothing to compare.")
        return 1
    if not new_records:
        print("\nThe pipeline output holds no usable harbour polygons.")
        return 1

    excluded: dict[str, int] = {}
    if args.countries:
        allow = {c.strip().upper() for c in args.countries.split(",") if c}
        before = len(old_records)
        old_records = [r for r in old_records
                       if r.country is None or r.country in allow]
        new_records = [r for r in new_records
                       if r.country is None or r.country in allow]
        excluded["country allowlist"] = before - len(old_records)
    if args.bbox:
        minx, miny, maxx, maxy = (float(v) for v in args.bbox.split(","))
        before = len(old_records)
        old_records = [r for r in old_records
                       if minx <= r.lon <= maxx and miny <= r.lat <= maxy]
        new_records = [r for r in new_records
                       if minx <= r.lon <= maxx and miny <= r.lat <= maxy]
        excluded["bbox"] = before - len(old_records)

    layers = {} if args.no_diagnose else load_interim_layers(args.interim_dir)

    out_of_coverage = []
    if args.coverage_gate:
        stops = layers.get("stops")
        if stops is None:
            print("\n  --coverage-gate: no stops.parquet under "
                  f"{args.interim_dir}; gate not applied.")
        else:
            cells = coverage_cells(stops[0], stops[1],
                                   resolution=COVERAGE_RESOLUTION)
            old_records, out_of_coverage = in_coverage(
                old_records, cells, resolution=COVERAGE_RESOLUTION)
            excluded["coverage gate"] = len(out_of_coverage)

    if args.exclude_transit:
        before = len(new_records)
        new_records = [r for r in new_records
                       if not r.props.get("transit_like")]
        excluded["transit_like (new side)"] = before - len(new_records)

    if excluded:
        _rule("Records excluded before scoring")
        for label, count in excluded.items():
            print(f"  {label:<26}{count:>6}")
        print("  A gate that excludes nothing, or nearly everything, is "
              "misconfigured.")

    if not old_records or not new_records:
        print("\nThe filters left nothing to compare.")
        return 1

    link_list = links(old_records, new_records,
                      min_coverage=args.min_coverage,
                      max_gap_m=args.max_gap_m,
                      min_overlap_m2=args.min_overlap_m2)
    comps = components(old_records, new_records, link_list,
                       find_components=_connected_components,
                       max_component=args.max_component)
    cal = calibrate(link_list, comps)
    scores = score(old_records, new_records, comps)

    _print_calibration(cal)
    _print_scores(scores)

    best = primary_links(link_list)
    old_by_id = {r.rec_id: r for r in old_records}
    new_by_id = {r.rec_id: r for r in new_records}
    matched_old = {i for c in comps if c.new_ids for i in c.old_ids}
    matched_new = {i for c in comps if c.old_ids for i in c.new_ids}
    tangled = {i for c in comps if c.tangled
               for i in (c.old_ids + c.new_ids)}

    untangled_pairs = [
        lk for lk in link_list
        if lk.old_id not in tangled and lk.new_id not in tangled
    ]
    _print_geometry(untangled_pairs, cal)

    comp_of: dict[str, object] = {}
    for c in comps:
        for i in c.old_ids:
            comp_of[f"old:{i}"] = c.comp_id
            comp_of[f"card:old:{i}"] = c.cardinality
        for i in c.new_ids:
            comp_of[f"new:{i}"] = c.comp_id

    matched_rows = _matched_rows(link_list, comp_of, old_by_id, new_by_id)
    primary_rows = [
        r for r in matched_rows
        if best.get(("old", r["old_id"])) is not None
        and best[("old", r["old_id"])].new_id == r["new_id"]
    ]
    _print_attributes(primary_rows)

    lonely_old = [r for r in old_records if r.rec_id not in matched_old]
    lonely_new = [r for r in new_records if r.rec_id not in matched_new]
    diag = diagnose_only_in_old(lonely_old, new_records, layers,
                                buffer_m=DIAG_BUFFER_METERS,
                                nearby_m=NEARBY_METERS)
    _print_only_in_old(lonely_old, diag)

    nearest = {r.rec_id: nearest_other(r, old_records) for r in lonely_new}
    transit_ids = {r.rec_id for r in new_records
                   if r.props.get("transit_like")}
    _print_only_in_new(lonely_new, nearest, transit_ids)
    _print_transit(new_records, matched_new, transit_ids)

    if cal.regime == "comparable":
        _rule("Old-side agreement by harbour size")
        quart = _size_buckets(old_records)
        for key, stats in stratify(old_records, comps,
                                   lambda r: quart[r.rec_id]).items():
            print(f"  {key:<10}{stats['matched']:>5}/{stats['n']:<6}"
                  f"{stats['agreement']:>8.1%}")

    if any(r.country for r in old_records):
        _rule("Old-side agreement by country")
        for key, stats in stratify(old_records, comps,
                                   lambda r: r.country or "?").items():
            print(f"  {key:<10}{stats['matched']:>5}/{stats['n']:<6}"
                  f"{stats['agreement']:>8.1%}")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(args.out_dir / "matched.csv", matched_rows)
    _write_csv(args.out_dir / "only_in_old.csv", [
        {
            "old_id": r.rec_id, "name": r.name or "",
            "country": r.country or "",
            "centroid_lat": round(r.lat, 6), "centroid_lon": round(r.lon, 6),
            "area_m2": round(area_m2(r), 1),
            "reason": diag[r.rec_id].bucket,
            "n_stops": diag[r.rec_id].n_stops,
            "n_cells": diag[r.rec_id].n_cells,
            "n_clusters": diag[r.rec_id].n_clusters,
            "nearest_new_id": diag[r.rec_id].nearest_id or "",
            "nearest_new_m": (round(diag[r.rec_id].nearest_gap_m, 1)
                              if diag[r.rec_id].nearest_gap_m is not None
                              else ""),
        }
        for r in lonely_old
    ])
    _write_csv(args.out_dir / "only_in_new.csv", [
        {
            "new_id": r.rec_id, "nearest_city": r.name or "",
            "country": r.country or "",
            "centroid_lat": round(r.lat, 6), "centroid_lon": round(r.lon, 6),
            "area_m2": round(area_m2(r), 1),
            "n_events": r.props.get("n_events"),
            "n_unique_mmsi": r.props.get("n_unique_mmsi"),
            "mean_dwell_minutes": r.props.get("mean_dwell_minutes"),
            "transit_like": bool(r.props.get("transit_like")),
            "nearest_old_id": nearest[r.rec_id][0] or "",
            "nearest_old_m": (round(nearest[r.rec_id][1], 1)
                              if nearest[r.rec_id][1] is not None else ""),
        }
        for r in sorted(lonely_new,
                        key=lambda r: -int(r.props.get("n_events") or 0))
    ])

    summary = {
        "old_database": str(args.old),
        "new_database": str(args.new),
        "fields": {"id": choice.id_field, "name": choice.name_field,
                   "country": choice.country_field, "fill": choice.fill},
        "excluded": excluded,
        "out_of_coverage": [r.rec_id for r in out_of_coverage],
        "thresholds": {"min_overlap_m2": args.min_overlap_m2,
                       "min_coverage": args.min_coverage,
                       "max_gap_m": args.max_gap_m,
                       "max_component": args.max_component},
        "calibration": {"regime": cal.regime,
                        "headline": cal.headline,
                        "note": cal.note,
                        "median_area_ratio": cal.median_area_ratio,
                        "median_coverage_old": cal.median_coverage_old,
                        "median_coverage_new": cal.median_coverage_new,
                        "median_iou": cal.median_iou,
                        "n_pairs": cal.n_pairs},
        "scores": scores,
        "only_in_old_reasons": _tally(
            [diag[r.rec_id].bucket for r in lonely_old]),
    }
    (args.out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    _rule("Written")
    for name in ("summary.json", "matched.csv", "only_in_old.csv",
                 "only_in_new.csv"):
        print(f"  {args.out_dir / name}")
    if not args.no_map:
        written = _build_map(
            args.out_dir / "compare_map.html", old_records, new_records,
            matched_old, matched_new, best, diag, transit_ids,
            tiles=args.tiles_url, tiles_attr=args.tiles_attr,
            simplify_m=args.map_simplify_m, offline=args.offline)
        if written:
            print(f"  {written}")

    if args.min_agreement_old is not None and \
            scores["agreement_old"] < args.min_agreement_old:
        print(f"\nOld-side agreement {scores['agreement_old']:.1%} is below "
              f"the required {args.min_agreement_old:.1%}.")
        return 1
    if args.min_agreement_new is not None and \
            scores["agreement_new"] < args.min_agreement_new:
        print(f"\nNew-side agreement {scores['agreement_new']:.1%} is below "
              f"the required {args.min_agreement_new:.1%}.")
        return 1
    return 0


def _tally(values) -> dict[str, int]:
    out: dict[str, int] = {}
    for v in values:
        out[v] = out.get(v, 0) + 1
    return dict(sorted(out.items(), key=lambda kv: -kv[1]))


def _size_buckets(records) -> dict[str, str]:
    """Each record's area quartile, as a label."""
    import numpy as np
    areas = {r.rec_id: area_m2(r) for r in records}
    values = np.asarray(list(areas.values()))
    edges = np.percentile(values, [25, 50, 75]) if values.size else [0, 0, 0]
    labels = {}
    for rec_id, a in areas.items():
        labels[rec_id] = ("Q1 small" if a <= edges[0]
                          else "Q2" if a <= edges[1]
                          else "Q3" if a <= edges[2] else "Q4 large")
    return labels


if __name__ == "__main__":
    raise SystemExit(main())
