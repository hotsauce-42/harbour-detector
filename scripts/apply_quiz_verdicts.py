#!/usr/bin/env python3
"""
Write harbour-quiz answers into the output GeoJSONs as manual verdicts.

The quiz (`streamlit run quiz.py`) records what a person called each site;
this turns those answers into the same tri-state properties app.py's lock and
offshore controls store, so Phase 5 carries them on every later run:

    harbour  → manual_transit_like = False, manual_offshore_like = False
    lock     → manual_transit_like = True   (offshore verdict untouched)
    offshore → manual_offshore_like = True  (lock verdict untouched)
    unsure   → nothing

The latest answer per harbour wins across all files given.

Usage:
    python3 scripts/apply_quiz_verdicts.py data/output/quiz/*.csv --dry-run
    python3 scripts/apply_quiz_verdicts.py data/output/quiz/*.csv
    python3 scripts/apply_quiz_verdicts.py quiz.csv --geojson /tmp/x/harbours.geojson
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import app  # noqa: E402
from quiz import read_answers  # noqa: E402

# answer → (lock verdict, offshore verdict); None leaves that verdict alone.
VERDICTS: dict[str, tuple[bool | None, bool | None]] = {
    "harbour":  (False, False),
    "lock":     (True, None),
    "offshore": (None, True),
}


def plan(answers: dict[str, dict]) -> dict[str, tuple[bool | None, bool | None]]:
    """harbour_id → the verdicts to store; unsure answers drop out."""
    return {hid: VERDICTS[row["answer"]] for hid, row in answers.items()
            if row["answer"] in VERDICTS}


def apply(changes: dict, paths: list[str]) -> tuple[list[str], list[str]]:
    """Store the verdicts. Returns (updated ids, ids found in no file)."""
    updated: list[str] = []
    missing: list[str] = []
    for hid, (lock, offshore) in sorted(changes.items()):
        touched = []
        if lock is not None:
            touched += app.save_harbour_transit(paths, hid, lock)
        if offshore is not None:
            touched += app.save_harbour_offshore(paths, hid, offshore)
        (updated if touched else missing).append(hid)
    return updated, missing


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("csv", nargs="+", type=Path, help="quiz_*.csv files")
    parser.add_argument("--geojson", default=None,
                        help="outline GeoJSON (default: gui.output_file); its "
                             "_cells companion is updated too")
    parser.add_argument("--dry-run", action="store_true",
                        help="print what would change, write nothing")
    args = parser.parse_args()

    gui_cfg = app.load_config().get("gui", {})
    output = args.geojson or gui_cfg.get("output_file",
                                         "data/output/harbours.geojson")
    cells = (app._cells_path(output, {}) if args.geojson
             else app._cells_path(output, gui_cfg))
    paths = [p for p in (output, cells) if Path(p).exists()]
    if output not in paths:
        raise SystemExit(f"{output} not found")

    answers = read_answers(args.csv)
    changes = plan(answers)
    counts: dict[str, int] = {}
    for row in answers.values():
        counts[row["answer"]] = counts.get(row["answer"], 0) + 1
    print(f"{len(answers)} harbours answered: "
          + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())))

    if args.dry_run:
        known = {f.get("properties", {}).get("harbour_id")
                 for f in json.loads(Path(output).read_text("utf-8"))["features"]}
        for hid, (lock, offshore) in sorted(changes.items()):
            note = "" if hid in known else "   (not in output)"
            print(f"  {hid:<14} lock={lock}  offshore={offshore}{note}")
        print("dry run — nothing written")
        return 0

    updated, missing = apply(changes, paths)
    print(f"updated {len(updated)} harbours in {', '.join(paths)}")
    if missing:
        print(f"warning: not in the output: {', '.join(missing)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
