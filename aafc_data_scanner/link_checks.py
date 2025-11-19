"""
Function to read ./inventories/_latest_resources_inventory.csv,
filter rows whose 'url_status' indicates a broken link, and list a report in
./inventories/latest_broken_links.csv containing all broken links and their meta data.

Redirects (301/302/307/308) are NOT treated as broken but the filter
can be altered easily.

"""
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd
from colorama import Fore

def resolve_inventories_dir(arg: str | None) -> Path:
    if arg:
        return Path(arg).resolve()
    cwd_inv = Path.cwd() / "inventories"
    if (cwd_inv / "_latest_resources_inventory.csv").exists():
        return cwd_inv
    script_inv = Path(__file__).resolve().parent / "inventories"
    return script_inv


def main() -> int:
    inventories_dir = resolve_inventories_dir(sys.argv[1] if len(sys.argv) > 1 else None)
    in_csv = Path(__file__).resolve().parent.parent / "inventories" / "_latest_resources_inventory.csv"
    out_csv = Path(__file__).resolve().parent.parent / "inventories" / "_latest_broken_links.csv"


    if not in_csv.exists():
        print(f"[error] Not found: {in_csv}", file=sys.stderr)
        return 1

    try:
        df = pd.read_csv(in_csv, dtype=str, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df = pd.read_csv(in_csv, dtype=str, encoding="latin-1")

    if "url_status" not in df.columns:
        print("[error] Column 'url_status' not found in input CSV.", file=sys.stderr)
        return 1

    status_num = pd.to_numeric(df["url_status"], errors="coerce")

    # Broken when: NaN OR -1 OR >= 400
    broken_mask = status_num.isna() | (status_num == -1) | (status_num >= 400)
    broken_df = df[broken_mask].copy()


    out_csv.parent.mkdir(parents=True, exist_ok=True)
    broken_df.to_csv(out_csv, index=False, encoding="utf-8-sig")


    total = len(df)
    broken = len(broken_df)
    kept = total - broken
    print(f"Scanned: {in_csv}")
    print(f"Total rows: {total}")
    print(Fore.RED + f"Broken rows: {broken}" + Fore.RESET)
    print(Fore.GREEN + f"Non-broken rows: {kept}" + Fore.RESET)
    print(f"Wrote report to : {out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
