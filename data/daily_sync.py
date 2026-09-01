"""
Daily pipeline: pull one day of KXNCAAFGAME games, export the local SQLite
cache to Parquet, and sync it up to the Spaces bucket. Meant to be run once a
day (see the "Daily automation" section of CONTEXT.md for how this is
scheduled) against yesterday's games -- by the time this runs, yesterday's
games have finished and Kalshi has settled results for them.

This is a thin orchestrator over the three existing CLIs (bulk_pull.py,
spaces/spaces_export.py, spaces/spaces_sync.py) rather than a rewrite of
their logic -- each stays runnable and testable on its own.

Usage:
  python3 daily_sync.py                    # pull yesterday (UTC), export, sync
  python3 daily_sync.py --date 2026-08-30   # pull a specific day instead
  python3 daily_sync.py --dry-run           # pull + export for real, but only
                                             # list what spaces_sync.py would upload
"""
import argparse
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import kalshi_db

SCRIPT_DIR = Path(__file__).resolve().parent
SPACES_DIR = SCRIPT_DIR / "spaces"
STAGING_DIR = SPACES_DIR / "staging"


def run_step(name, cmd):
    print(f"\n=== {name} ===")
    print(f"$ {' '.join(str(c) for c in cmd)}")
    result = subprocess.run([sys.executable, *cmd])
    if result.returncode != 0:
        print(f"\n{name} failed (exit {result.returncode}); stopping pipeline.", file=sys.stderr)
        sys.exit(result.returncode)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", help="YYYY-MM-DD to pull (default: yesterday, UTC)")
    parser.add_argument("--db", default=kalshi_db.DEFAULT_DB_PATH, help="SQLite DB path to pull into and export from")
    parser.add_argument("--sleep", type=float, default=0.4, help="Seconds to sleep between games in bulk_pull.py")
    parser.add_argument("--dry-run", action="store_true", help="Run the pull + export for real, but only list what spaces_sync.py would upload")
    args = parser.parse_args()

    if args.date:
        target = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target = datetime.now(timezone.utc).date() - timedelta(days=1)
    date_str = target.isoformat()

    print(f"Daily sync for {date_str}")

    run_step("Pull", [
        str(SCRIPT_DIR / "bulk_pull.py"),
        "--start", date_str, "--end", date_str,
        "--db", str(args.db), "--sleep", str(args.sleep), "--skip-existing",
    ])

    run_step("Export to Parquet", [
        str(SPACES_DIR / "spaces_export.py"),
        "--db", str(args.db), "--out", str(STAGING_DIR),
    ])

    sync_cmd = [str(SPACES_DIR / "spaces_sync.py"), "--local-root", str(STAGING_DIR)]
    if args.dry_run:
        sync_cmd.append("--dry-run")
    run_step("Sync to Spaces", sync_cmd)

    print(f"\nDaily sync for {date_str} complete.")


if __name__ == "__main__":
    main()
