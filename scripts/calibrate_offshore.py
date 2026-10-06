#!/usr/bin/env python3
"""
Pick `phase4.offshore_min_coast_km` from confirmed examples.

Phase 4 writes `coast_dist_km` (outline → nearest land) for every harbour, and
the GUI's offshore control stores what an operator decided in
`manual_offshore_like`: True = "offshore waiting area", False = "harbour". The
verdicts are labelled examples, so this reads a pipeline output and reports
the thresholds that separate the two groups, and what each one would flag.

Extra examples can be given by id, for sites nobody has marked in the GUI yet.
An id given here wins over the file's own verdict.

Nothing is written; put the chosen number into config/settings.yaml (or
PHASE4__OFFSHORE_MIN_COAST_KM) and re-run Phase 4.

Usage:
    python3 scripts/calibrate_offshore.py data/output/harbours.parquet
    python3 scripts/calibrate_offshore.py data/output/harbours.geojson \\
        --offshore DE-1234abcd DK-5678ef01 --harbour DK-cdd3fe35
"""

import argparse
import json
import math
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.overrides import manual_offshore  # noqa: E402

DEFAULT_CANDIDATES = (0.1, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0)


def load(path: Path) -> pd.DataFrame:
    """harbour_id, coast_dist_km and the stored verdict, from either output."""
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        features = json.loads(path.read_text(encoding="utf-8"))["features"]
        df = pd.DataFrame([f.get("properties", {}) for f in features])
    if "coast_dist_km" not in df.columns:
        raise SystemExit(f"{path} has no coast_dist_km — re-run Phases 4 and 5 "
                         "with phase4.coastline_path set")
    columns = ["harbour_id", "nearest_city", "coast_dist_km"]
    out = df[[c for c in columns if c in df.columns]].copy()
    out["coast_dist_km"] = pd.to_numeric(out["coast_dist_km"], errors="coerce")
    out["label"] = [manual_offshore(row) for _, row in df.iterrows()]
    return out


def separating_range(df: pd.DataFrame) -> tuple[float, float] | None:
    """
    (low, high): every threshold in [low, high) classifies all labels right.

    None when the groups overlap — some confirmed harbour is further from land
    than some confirmed waiting area, and no single distance separates them.
    A missing distance on a waiting area counts as infinitely far, the same
    way Phase 4 flags a site with no land in reach.
    """
    harbours = df.loc[df["label"].eq(False), "coast_dist_km"].dropna()
    offshore = df.loc[df["label"].eq(True), "coast_dist_km"].fillna(math.inf)
    low = float(harbours.max()) if len(harbours) else 0.0
    high = float(offshore.min()) if len(offshore) else math.inf
    return (low, high) if low < high else None


def evaluate(df: pd.DataFrame, threshold: float) -> dict:
    flagged = df["coast_dist_km"].isna() | (df["coast_dist_km"] > threshold)
    # A missing distance on an unlabelled site is "no land polygons", not the
    # open sea — Phase 4 only flags it when the file covers the site, which
    # this script cannot tell, so it is left out of the count.
    known = df["coast_dist_km"].notna()
    return {
        "threshold_km":     threshold,
        "flagged":          int((flagged & known).sum()),
        "unlabelled_flagged": int((flagged & known & df["label"].isna()).sum()),
        "harbours_flagged": int((flagged & df["label"].eq(False)).sum()),
        "offshore_missed":  int((~flagged & df["label"].eq(True)).sum()),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("path", type=Path,
                        help="harbours.parquet or harbours.geojson")
    parser.add_argument("--offshore", nargs="*", default=[], metavar="ID",
                        help="harbour ids confirmed as offshore waiting areas")
    parser.add_argument("--harbour", nargs="*", default=[], metavar="ID",
                        help="harbour ids confirmed as real harbours")
    parser.add_argument("--thresholds", nargs="*", type=float,
                        default=list(DEFAULT_CANDIDATES), metavar="KM")
    args = parser.parse_args()

    df = load(args.path)
    unknown = (set(args.offshore) | set(args.harbour)) - set(df["harbour_id"])
    if unknown:
        print(f"warning: not in {args.path.name}: {', '.join(sorted(unknown))}",
              file=sys.stderr)
    df.loc[df["harbour_id"].isin(args.harbour), "label"] = False
    df.loc[df["harbour_id"].isin(args.offshore), "label"] = True

    n_off = int(df["label"].eq(True).sum())
    n_harb = int(df["label"].eq(False).sum())
    dist = df["coast_dist_km"]
    print(f"{len(df)} harbours, {int(dist.notna().sum())} with a distance to land; "
          f"{n_off} confirmed offshore, {n_harb} confirmed harbours")
    print("distance to land, all sites (km): "
          + ", ".join(f"p{q:.0f}={dist.quantile(q / 100):.2f}"
                      for q in (50, 90, 99)) + f", max={dist.max():.2f}")

    for name, value in (("offshore", True), ("harbour", False)):
        group = df[df["label"].eq(value)].sort_values("coast_dist_km")
        if len(group):
            print(f"\nconfirmed {name}:")
            for _, r in group.iterrows():
                d = r["coast_dist_km"]
                print(f"  {r['harbour_id']:<14} {str(r.get('nearest_city', '')):<24}"
                      f" {'—' if pd.isna(d) else f'{d:.3f} km'}")

    print()
    if n_off and n_harb:
        found = separating_range(df)
        if found is None:
            print("The confirmed groups overlap: no single threshold separates "
                  "them. Check the outliers above — and remember a manual "
                  "verdict overrides the flag anyway.")
        else:
            low, high = found
            if not math.isfinite(high):
                # Every confirmed waiting area is out of reach of any land.
                print(f"Any threshold above {low:.3f} km separates the "
                      "confirmed examples.")
            else:
                print(f"Any threshold in [{low:.3f}, {high:.3f}) km separates "
                      "the confirmed examples. Midpoint: "
                      f"{(low + high) / 2:.2f} km.")
    else:
        print("Need at least one confirmed example of each kind to suggest a "
              "range — mark some in the GUI (Site type tab), or pass "
              "--offshore / --harbour.")

    print()
    table = pd.DataFrame([evaluate(df, t) for t in sorted(args.thresholds)])
    print(table.to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
