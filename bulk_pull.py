"""
Bulk-pull every KXNCAAFGAME event in a date window and persist it to the
SQLite store (kalshi_db.py). Built for "get everything in the next two
weeks so we can start modeling signals" -- skips per-game HTML report
rendering (that's cheap to do later for any one game via
kalshi_game_report.py) and just does event detail + trades + candles +
orderbook -> DB for as many games as match.

Usage:
  python3 bulk_pull.py --days 14
  python3 bulk_pull.py --start 2026-09-01 --end 2026-09-15
  python3 bulk_pull.py --days 14 --series KXNCAAFGAME --sleep 0.2

Safe to re-run: pulls are idempotent (see kalshi_db.py), so re-running
mid-window just refreshes prices/adds new trades for games still active.
"""
import argparse
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta

import kalshi_db
from kalshi_game_report import get_all_events, pull_event, write_raw_and_csvs, SCRIPT_DIR

MONTHS = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
          "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}


def parse_ticker_date(event_ticker, series_ticker):
    m = re.match(rf"{re.escape(series_ticker)}-(\d{{2}})([A-Z]{{3}})(\d{{2}})", event_ticker)
    if not m:
        return None
    yy, mon, dd = m.groups()
    if mon not in MONTHS:
        return None
    try:
        return datetime(2000 + int(yy), MONTHS[mon], int(dd)).date()
    except ValueError:
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--series", default="KXNCAAFGAME")
    parser.add_argument("--days", type=int, default=14, help="Days ahead from today (ignored if --start/--end given)")
    parser.add_argument("--start", help="YYYY-MM-DD, default today (UTC)")
    parser.add_argument("--end", help="YYYY-MM-DD, default start + --days")
    parser.add_argument("--db", default=kalshi_db.DEFAULT_DB_PATH)
    parser.add_argument("--sleep", type=float, default=0.4, help="Seconds to sleep between games")
    parser.add_argument("--skip-existing", action="store_true",
                        help="Skip events already stored with settled results -- lets a killed "
                             "backfill resume instead of re-pulling completed games")
    parser.add_argument("--write-files", action="store_true", help="Also write raw.json/csvs per game (default: DB only)")
    args = parser.parse_args()

    today = datetime.now(timezone.utc).date()
    start = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else today
    end = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else start + timedelta(days=args.days)

    print(f"Fetching event list for {args.series} ...")
    events = get_all_events(args.series)
    matched = []
    for e in events:
        d = parse_ticker_date(e["event_ticker"], args.series)
        if d and start <= d <= end:
            matched.append((d, e["event_ticker"], e.get("title", "")))
    matched.sort()
    print(f"{len(matched)} events between {start} and {end}")

    if args.skip_existing:
        import sqlite3
        done = set()
        if os.path.exists(args.db):
            conn = sqlite3.connect(args.db)
            # "complete" = every market for the event already carries a settled
            # result, so in-flight/unsettled games still get re-pulled for updates
            done = {row[0] for row in conn.execute("""
                SELECT event_ticker FROM markets
                GROUP BY event_ticker
                HAVING SUM(CASE WHEN result IN ('yes','no') THEN 1 ELSE 0 END) = COUNT(*)
                   AND COUNT(*) > 0
            """)}
            conn.close()
        before = len(matched)
        matched = [m for m in matched if m[1] not in done]
        print(f"Skipping {before - len(matched)} already-settled events; {len(matched)} to pull")
    print()

    ok, failed, total_new_trades = 0, [], 0
    t0 = time.time()
    for i, (d, ticker, title) in enumerate(matched, 1):
        try:
            raw = pull_event(ticker, args.series)
            if args.write_files:
                out_dir = f"{SCRIPT_DIR}/reports/{ticker}"
                os.makedirs(out_dir, exist_ok=True)
                write_raw_and_csvs(raw, out_dir)
            stats = kalshi_db.store_raw(raw, args.db)
            total_new_trades += stats["new_trades"]
            ok += 1
            print(f"[{i}/{len(matched)}] {d} {ticker:34} {title:38} +{stats['new_trades']} trades")
        except Exception as ex:
            failed.append((ticker, str(ex)))
            print(f"[{i}/{len(matched)}] {d} {ticker:34} FAILED: {ex}", file=sys.stderr)
        time.sleep(args.sleep)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s. {ok}/{len(matched)} games pulled, {total_new_trades} new trades persisted.")
    if failed:
        print(f"{len(failed)} failed:")
        for ticker, err in failed:
            print(f"  {ticker}: {err}")


if __name__ == "__main__":
    main()
