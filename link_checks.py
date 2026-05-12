"""
make_latest_broken_links.py

Simple script: read ./inventories/_latest_resources_inventory.csv,
filter rows whose 'url_status' indicates a broken link, and write
./inventories/latest_broken_links.csv containing those full rows.

Broken criteria (minimal, adjustable):
  - url_status is NaN / empty
  - url_status == -1 (scanner error)
  - url_status >= 400 (HTTP client/server error)
Redirects (301/302/307/308) are NOT treated as broken.

Run:
  python make_latest_broken_links.py

You can also pass a custom inventories directory:
  python make_latest_broken_links.py inventories
"""
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd


def resolve_inventories_dir(arg: str | None) -> Path:
    if arg:
        return Path(arg).resolve()
    # Try CWD first, then script dir
    cwd_inv = Path.cwd() / "inventories"
    if (cwd_inv / "_latest_resources_inventory.csv").exists():
        return cwd_inv
    script_inv = Path(__file__).resolve().parent / "inventories"
    return script_inv


def main() -> int:
    inventories_dir = resolve_inventories_dir(sys.argv[1] if len(sys.argv) > 1 else None)
    in_csv = inventories_dir / "_latest_resources_inventory.csv"
    out_csv = inventories_dir / "latest_broken_links.csv"

    if not in_csv.exists():
        print(f"[error] Not found: {in_csv}", file=sys.stderr)
        return 1

    # Read everything as strings to preserve existing formatting/columns
    try:
        df = pd.read_csv(in_csv, dtype=str, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df = pd.read_csv(in_csv, dtype=str, encoding="latin-1")

    if "url_status" not in df.columns:
        print("[error] Column 'url_status' not found in input CSV.", file=sys.stderr)
        return 1

    # Make a numeric view of status for filtering
    status_num = pd.to_numeric(df["url_status"], errors="coerce")

    # Broken when: NaN OR -1 OR >= 400
    broken_mask = status_num.isna() | (status_num == -1) | (status_num >= 400)
    broken_df = df[broken_mask].copy()

    # Write all original columns/rows out
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    broken_df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    # Small console summary
    total = len(df)
    broken = len(broken_df)
    kept = total - broken
    print(f"[ok] Scanned: {in_csv}")
    print(f"[ok] Total rows: {total}")
    print(f"[ok] Broken rows: {broken}")
    print(f"[ok] Non-broken rows: {kept}")
    print(f"[ok] Wrote: {out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
