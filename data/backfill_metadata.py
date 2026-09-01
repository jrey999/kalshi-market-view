"""
One-time backfill for columns added to the markets table after games were
already pulled (currently: expected_expiration_time, added to approximate
kickoff for calibration -- see CONTEXT.md). Metadata-only: one API call per
event, no trades/candlesticks re-fetch, so this is cheap and fast even for
a whole season already in the local db.

Usage:
  python3 backfill_metadata.py --db kalshi_historical.db
"""
import argparse
import sqlite3
import sys
import time

import kalshi_db
import kalshi_game_report as kgr


def fetch_markets_meta(event_ticker):
    markets_meta = kgr.get("/historical/markets", event_ticker=event_ticker).get("markets", [])
    if not markets_meta:
        # Not yet in the historical archive -- still live/open.
        event_detail = kgr.get(f"/events/{event_ticker}", with_nested_markets="true")
        markets_meta = event_detail["event"].get("markets") or []
    return markets_meta


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default=kalshi_db.DEFAULT_DB_PATH)
    parser.add_argument("--sleep", type=float, default=0.2)
    args = parser.parse_args()

    conn = kalshi_db.connect(args.db)
    event_tickers = [r[0] for r in conn.execute("SELECT event_ticker FROM events ORDER BY event_ticker")]
    conn.close()

    print(f"Backfilling expected_expiration_time for {len(event_tickers)} events...")
    updated = failed = 0
    conn = sqlite3.connect(args.db)
    for i, ticker in enumerate(event_tickers, 1):
        try:
            for mm in fetch_markets_meta(ticker):
                conn.execute(
                    "UPDATE markets SET expected_expiration_time=? WHERE market_ticker=?",
                    (mm.get("expected_expiration_time"), mm["ticker"]),
                )
                updated += conn.execute("SELECT changes()").fetchone()[0]
            conn.commit()
        except Exception as e:
            failed += 1
            print(f"[{i}/{len(event_tickers)}] {ticker} FAILED: {e}", file=sys.stderr)
        if i % 100 == 0:
            print(f"[{i}/{len(event_tickers)}] ...")
        time.sleep(args.sleep)
    conn.close()

    print(f"\nDone. {updated} market rows updated, {failed} events failed.")


if __name__ == "__main__":
    main()
