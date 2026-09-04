#!/usr/bin/env python3
"""
Download the map's Leaflet assets into static/vendor/ so the GUI works offline.

Without this, folium's page fetches Leaflet and Leaflet.Draw from public CDNs
and an air-gapped browser renders a blank box. Run it once on a machine with
internet; `static/vendor/` is gitignored, so either copy the directory to the
target host or run this during an image build.

The versions are read from folium itself, so a folium upgrade is picked up here
without editing a URL by hand.

Usage:
    python3 scripts/vendor_map_assets.py
    python3 scripts/vendor_map_assets.py --out-dir /srv/www/leaflet
    python3 scripts/vendor_map_assets.py --force        # re-download everything
"""

import argparse
import sys
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urljoin
from urllib.request import urlopen

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.map_assets import SUB_ASSETS, VENDOR_DIR, asset_urls  # noqa: E402

TIMEOUT = 30


def _plan(urls: dict[str, str]) -> list[tuple[str, Path]]:
    """(url, path relative to out_dir) for every file to fetch."""
    jobs: list[tuple[str, Path]] = []
    for key, url in urls.items():
        jobs.append((url, Path(url.rsplit("/", 1)[-1])))
        for rel in SUB_ASSETS.get(key, ()):
            # urljoin against the stylesheet's URL — exactly how the browser
            # resolves the url(...) references inside it.
            jobs.append((urljoin(url, rel), Path(rel)))
    return jobs


def _download(url: str, dest: Path) -> int:
    with urlopen(url, timeout=TIMEOUT) as response:  # noqa: S310 — fixed CDN URLs
        body = response.read()
    if not body:
        raise ValueError(f"empty response from {url}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(body)
    return len(body)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=VENDOR_DIR,
                        help=f"target directory (default: {VENDOR_DIR})")
    parser.add_argument("--force", action="store_true",
                        help="re-download files that are already present")
    args = parser.parse_args()

    urls = asset_urls()
    if not urls:
        print("No assets to vendor — folium exposed none of the expected keys.")
        return 1

    print(f"Vendoring {len(urls)} libraries into {args.out_dir}")
    failures = []
    for url, rel in _plan(urls):
        dest = args.out_dir / rel
        if dest.is_file() and not args.force:
            print(f"  skip  {rel}  (already there)")
            continue
        try:
            size = _download(url, dest)
        except (URLError, OSError, ValueError) as exc:
            print(f"  FAIL  {rel}  ← {url}\n        {exc}")
            failures.append(rel)
        else:
            print(f"  ok    {rel}  ({size / 1024:.0f} KB)")

    if failures:
        print(f"\n{len(failures)} file(s) failed — the map will not work offline.")
        return 1

    print("\nDone. Enable it with gui.local_map_assets: true in "
          "config/settings.yaml (or GUI__LOCAL_MAP_ASSETS=true).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
