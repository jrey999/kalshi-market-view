"""
Export the SQLite working cache into the file layout used by the
DigitalOcean Spaces bucket, as Parquet.

The bucket is the system of record. SQLite is only a local working cache
(it absorbs the incremental per-game churn of a pull), and the repo
carries code only -- the full historical dataset runs to ~1-2 GB, which
does not belong in git.

Layout (mirrored exactly in the bucket):

    kalshi/sport=<sport>/season=<yyyy>/
      events.parquet
      markets.parquet
      trades/week=<iso-year-week>/trades.parquet
      candlesticks/week=<iso-year-week>/candles.parquet
      orderbooks/week=<iso-year-week>/snapshots.parquet

Why week and not per-event: per-event partitioning produced ~2,800 tiny
files per season, and the dominant read pattern is "load a season to fit
a model" -- thousands of object-storage round trips for sub-20 KB files.
By week it's ~17 files per season at a workable size, with event_ticker
kept as a column so single-game filtering is just a predicate. Coarse
partitioning costs nothing on the write side because SQLite is already
the incremental buffer and this export is a batch pass.

Weeks are ISO year-week (e.g. week=2025-W35) so January bowl/playoff
games still sort correctly inside their own season.

Orderbook snapshots stay raw JSON in a single column -- they're cheap to
keep as-is and can be flattened in the database when actually needed.

Usage:
  python3 spaces_export.py --db kalshi_historical.db
  python3 spaces_export.py --db kalshi_market_data.db --out spaces
"""
import argparse
import os
import re
import sys
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import kalshi_db

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT = os.path.join(SCRIPT_DIR, "spaces")
CHUNK = 200_000  # rows per row-group, keeps peak memory flat on big weeks

MONTHS = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
          "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}

SPORT_BY_SERIES = {"KXNCAAFGAME": "cfb", "KXNCAAFCSGAME": "cfb"}


def event_date(event_ticker):
    m = re.search(r"-(\d{2})([A-Z]{3})(\d{2})", event_ticker)
    if not m:
        return None
    yy, mon, dd = m.groups()
    try:
        return date(2000 + int(yy), MONTHS[mon], int(dd))
    except (KeyError, ValueError):
        return None


def season_of(d):
    """A college football season spans Aug -> Jan, so a January game belongs
    to the previous calendar year's season (Jan 2026 playoff -> 2025)."""
    if d is None:
        return "unknown"
    return str(d.year if d.month >= 7 else d.year - 1)


def week_of(d):
    if d is None:
        return "unknown"
    iso = d.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def write_table(rows, columns, path):
    if not rows:
        return 0
    os.makedirs(os.path.dirname(path), exist_ok=True)
    table = pa.table({col: [r[i] for r in rows] for i, col in enumerate(columns)})
    pq.write_table(table, path, compression="snappy")
    return len(rows)


def write_streamed(cursor, columns, path):
    """Stream a cursor into one Parquet file in row-group chunks, so a week
    holding millions of trades never materializes fully in memory."""
    writer = None
    total = 0
    try:
        while True:
            rows = cursor.fetchmany(CHUNK)
            if not rows:
                break
            table = pa.table({col: [r[i] for r in rows] for i, col in enumerate(columns)})
            if writer is None:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                writer = pq.ParquetWriter(path, table.schema, compression="snappy")
            writer.write_table(table)
            total += len(rows)
    finally:
        if writer is not None:
            writer.close()
    return total


def export(db_path, out_root):
    conn = kalshi_db.connect(db_path)

    events = conn.execute(
        "SELECT event_ticker, series_ticker, title, sub_title, first_pulled_at, last_pulled_at FROM events"
    ).fetchall()
    if not events:
        print(f"No events in {db_path}; nothing to export.")
        return

    # group events by (sport, season) and, separately, map each event to its week
    by_season = {}
    week_of_event = {}
    for row in events:
        ticker, series = row[0], row[1]
        d = event_date(ticker)
        sport = SPORT_BY_SERIES.get(series, "other")
        key = (sport, season_of(d))
        by_season.setdefault(key, []).append(row)
        week_of_event[ticker] = (key, week_of(d))

    n_files = n_rows = 0
    for (sport, season), rows in by_season.items():
        base = f"{out_root}/kalshi/sport={sport}/season={season}"

        n_rows += write_table(rows, ["event_ticker", "series_ticker", "title", "sub_title",
                                     "first_pulled_at", "last_pulled_at"], f"{base}/events.parquet")
        n_files += 1

        tickers = tuple(r[0] for r in rows)
        ph = ",".join("?" * len(tickers))
        markets = conn.execute(
            f"""SELECT market_ticker, event_ticker, label, open_time, close_time,
                       status, result, last_pulled_at
                FROM markets WHERE event_ticker IN ({ph})""", tickers
        ).fetchall()
        n_rows += write_table(markets, ["market_ticker", "event_ticker", "label", "open_time",
                                        "close_time", "status", "result", "last_pulled_at"],
                              f"{base}/markets.parquet")
        n_files += 1

    # trades / candlesticks / orderbooks partition by week
    weeks = {}
    for ticker, (key, week) in week_of_event.items():
        weeks.setdefault((key, week), []).append(ticker)

    for ((sport, season), week), tickers in sorted(weeks.items()):
        base = f"{out_root}/kalshi/sport={sport}/season={season}"
        ph = ",".join("?" * len(tickers))

        cur = conn.execute(
            f"""SELECT trade_id, market_ticker, event_ticker, created_time,
                       yes_price_cents, no_price_cents, size, taker_side
                FROM trades WHERE event_ticker IN ({ph}) ORDER BY created_time""", tickers)
        n = write_streamed(cur, ["trade_id", "market_ticker", "event_ticker", "created_time",
                                 "yes_price_cents", "no_price_cents", "size", "taker_side"],
                           f"{base}/trades/week={week}/trades.parquet")
        if n:
            n_rows += n
            n_files += 1

        cur = conn.execute(
            f"""SELECT c.market_ticker, m.event_ticker, c.end_period_ts, c.open_cents,
                       c.close_cents, c.high_cents, c.low_cents, c.yes_bid_close_cents,
                       c.yes_ask_close_cents, c.volume, c.open_interest
                FROM candlesticks c JOIN markets m ON m.market_ticker = c.market_ticker
                WHERE m.event_ticker IN ({ph})
                ORDER BY c.market_ticker, c.end_period_ts""", tickers)
        n = write_streamed(cur, ["market_ticker", "event_ticker", "end_period_ts", "open_cents",
                                 "close_cents", "high_cents", "low_cents", "yes_bid_close_cents",
                                 "yes_ask_close_cents", "volume", "open_interest"],
                           f"{base}/candlesticks/week={week}/candles.parquet")
        if n:
            n_rows += n
            n_files += 1

        cur = conn.execute(
            f"""SELECT o.market_ticker, m.event_ticker, o.pulled_at, o.raw_json
                FROM orderbook_snapshots o JOIN markets m ON m.market_ticker = o.market_ticker
                WHERE m.event_ticker IN ({ph}) ORDER BY o.pulled_at""", tickers)
        n = write_streamed(cur, ["market_ticker", "event_ticker", "pulled_at", "raw_json"],
                           f"{base}/orderbooks/week={week}/snapshots.parquet")
        if n:
            n_rows += n
            n_files += 1

    conn.close()
    print(f"Exported {n_rows:,} rows across {n_files:,} parquet files to {out_root}/kalshi/")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", required=True, help="SQLite DB to export from")
    parser.add_argument("--out", default=DEFAULT_OUT, help=f"Local root for the bucket tree (default {DEFAULT_OUT})")
    args = parser.parse_args()
    export(args.db, args.out)


if __name__ == "__main__":
    main()
