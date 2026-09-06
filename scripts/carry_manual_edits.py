#!/usr/bin/env python3
"""
Carry an operator's manual edits from one harbour database onto another whose
ids have changed.

Some changes re-issue every harbour_id — `phase3.centroid_id_resolution` is the
obvious one, since the id is hashed from that cell. A re-id throws away the
link between a harbour and the work someone did on it: the drawn outlines, the
corrected city names, the lock verdicts. Phase 5 carries those forward by
harbour_id, which is exactly the thing that just changed.

So this matches the two files **by geometry instead**: the harbour with the
greatest H3-cell overlap, tie-broken by centroid distance. When the re-id came
from changing the id resolution the match is exact — clustering is untouched,
so the cell sets are identical — but the same tool works after any re-id where
the geometry is broadly stable.

It refuses to guess. A record whose best candidate is below the overlap floor
and outside the distance cap is reported as unmatched and the script exits
non-zero, rather than attaching someone's hand-drawn outline to the wrong
harbour.

Usage:
    python3 scripts/carry_manual_edits.py --from old.geojson --to new.geojson
    python3 scripts/carry_manual_edits.py --from old.geojson --to new.geojson --dry-run
    python3 scripts/carry_manual_edits.py --from old.geojson --to new.geojson \\
        --out merged.geojson          # default: rewrite --to in place
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.id_matching import _jaccard  # noqa: E402
from utils.geo import haversine_meters  # noqa: E402
from utils.overrides import (  # noqa: E402
    MANUAL_LOCK_AREA_KEY,
    MANUAL_OUTLINE_KEY,
    MANUAL_TRANSIT_KEY,
    OVERRIDES_KEY,
    manual_lock_area,
    manual_outline,
    manual_transit,
    normalise_overrides,
    override_values,
)

# A harbour has to look like the same harbour: either most of its cells are
# shared, or its centroid barely moved. Both are generous, because the point is
# to catch a re-id, not to re-detect harbours.
MIN_JACCARD = 0.3
MAX_CENTROID_METERS = 500.0


def _cells(props: dict) -> set:
    value = props.get("h3_cells") or []
    return {str(c) for c in value}


def _has_manual_work(props: dict) -> bool:
    return bool(
        normalise_overrides(props.get(OVERRIDES_KEY))
        or manual_outline(props)
        or manual_lock_area(props)
        or manual_transit(props) is not None
    )


def _best_match(old: dict, new_features: list[dict]) -> tuple[dict | None, str]:
    """The new record that is the same harbour, plus how it was decided."""
    old_cells = _cells(old)
    best, best_score = None, 0.0
    for feat in new_features:
        score = _jaccard(old_cells, _cells(feat["properties"])) if old_cells else 0.0
        if score > best_score:
            best, best_score = feat, score
    if best is not None and best_score >= MIN_JACCARD:
        return best, f"{best_score:.0%} cell overlap"

    # No usable overlap — fall back to the nearest centroid.
    olat, olon = old.get("centroid_lat"), old.get("centroid_lon")
    if olat is None or olon is None:
        return None, "no cells and no centroid"

    nearest, nearest_d = None, None
    for feat in new_features:
        p = feat["properties"]
        if p.get("centroid_lat") is None:
            continue
        d = haversine_meters(olat, olon, p["centroid_lat"], p["centroid_lon"])
        if nearest_d is None or d < nearest_d:
            nearest, nearest_d = feat, d
    if nearest is not None and nearest_d <= MAX_CENTROID_METERS:
        return nearest, f"centroid {nearest_d:.0f} m"
    return None, f"nearest centroid {nearest_d:.0f} m" if nearest_d else "nothing near"


def carry(old_features: list[dict], new_features: list[dict]) -> tuple[list, list]:
    """Copy manual work onto the matching new records. Returns (moved, lost)."""
    moved, lost = [], []
    for old in old_features:
        props = old["properties"]
        if not _has_manual_work(props):
            continue

        target, why = _best_match(props, new_features)
        if target is None:
            lost.append((props.get("harbour_id"), props.get("nearest_city"), why))
            continue

        dest = target["properties"]
        # The corrected values travel with the list that names them, so the
        # pair cannot drift apart.
        corrected = override_values(props)
        dest.update(corrected)
        fields = normalise_overrides(props.get(OVERRIDES_KEY))
        if fields:
            dest[OVERRIDES_KEY] = fields

        drawn = manual_outline(props)
        if drawn:
            dest[MANUAL_OUTLINE_KEY] = drawn

        verdict = manual_transit(props)
        if verdict is not None:
            dest[MANUAL_TRANSIT_KEY] = verdict

        area = manual_lock_area(props)
        if area:
            dest[MANUAL_LOCK_AREA_KEY] = area

        carried = set(fields)
        if drawn:
            carried.add("outline")
        if verdict is not None:
            carried.add("lock verdict")
        if area:
            carried.add("lock area")
        moved.append((props.get("harbour_id"), dest.get("harbour_id"),
                      props.get("nearest_city"), why, sorted(carried)))
    return moved, lost


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--from", dest="source", type=Path, required=True,
                        help="database holding the manual edits (old ids)")
    parser.add_argument("--to", dest="target", type=Path, required=True,
                        help="freshly generated output (new ids)")
    parser.add_argument("--out", type=Path,
                        help="where to write (default: overwrite --to)")
    parser.add_argument("--dry-run", action="store_true",
                        help="report the mapping and write nothing")
    args = parser.parse_args()

    old = json.loads(args.source.read_text())
    new = json.loads(args.target.read_text())
    moved, lost = carry(old["features"], new["features"])

    if not moved and not lost:
        print("No manual edits found in --from; nothing to carry.")
        return 0

    print(f"{len(moved)} harbour(s) with manual work:\n")
    for old_id, new_id, city, why, what in moved:
        print(f"  {city or '':<18} {old_id} -> {new_id}")
        print(f"      matched on {why}; carried {', '.join(what)}")

    if lost:
        print(f"\n{len(lost)} could NOT be matched and would lose their edits:")
        for old_id, city, why in lost:
            print(f"  {city or '':<18} {old_id}  ({why})")
        print("\nNothing written — resolve these first.")
        return 1

    if args.dry_run:
        print("\n--dry-run: nothing written.")
        return 0

    out = args.out or args.target
    out.write_text(json.dumps(new))
    print(f"\nWrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
