"""
Persistent SQLite store for Kalshi market flow data (events, markets,
trades, candlesticks, order-book snapshots).

The database is a gitignored local working-cache file (kalshi_market_data.db)
that accumulates across runs -- every pull from kalshi_game_report.py upserts
into it rather than overwriting, so re-pulling the same game just adds any
new trades/candles and refreshes metadata, and pulling a new game adds
alongside what's already there. Nothing here ever drops or truncates a
table.

Trades are deduped by their Kalshi trade_id (append-only, immutable once
written). Candlesticks are upserted per (market_ticker, end_period_ts)
since the in-progress hour's candle changes as more trades land. Order
book snapshots are point-in-time and always appended, never overwritten,
so the table doubles as a time series of book depth.

The liquidity filter itself is NOT stored here -- it's a pure computation
over trades + candlesticks (see liquidity_filter.py / kalshi_game_report.py)
and is cheap to recompute from this raw data whenever needed.
"""
import json
import os
import sqlite3
from datetime import datetime, timezone

DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kalshi_market_data.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_ticker      TEXT PRIMARY KEY,
    series_ticker     TEXT,
    title             TEXT,
    sub_title         TEXT,
    first_pulled_at   TEXT,
    last_pulled_at    TEXT
);

CREATE TABLE IF NOT EXISTS markets (
    market_ticker            TEXT PRIMARY KEY,
    event_ticker             TEXT NOT NULL REFERENCES events(event_ticker),
    label                    TEXT,
    open_time                TEXT,
    close_time               TEXT,
    status                   TEXT,
    result                   TEXT,
    expected_expiration_time TEXT,
    last_pulled_at           TEXT
);

CREATE TABLE IF NOT EXISTS trades (
    trade_id          TEXT PRIMARY KEY,
    market_ticker     TEXT NOT NULL REFERENCES markets(market_ticker),
    event_ticker      TEXT NOT NULL,
    created_time      TEXT NOT NULL,
    yes_price_cents   REAL,
    no_price_cents    REAL,
    size              REAL,
    taker_side        TEXT
);
CREATE INDEX IF NOT EXISTS idx_trades_market_time ON trades(market_ticker, created_time);

CREATE TABLE IF NOT EXISTS candlesticks (
    market_ticker         TEXT NOT NULL REFERENCES markets(market_ticker),
    end_period_ts         INTEGER NOT NULL,
    open_cents            REAL,
    close_cents           REAL,
    high_cents            REAL,
    low_cents             REAL,
    yes_bid_close_cents   REAL,
    yes_ask_close_cents   REAL,
    volume                REAL,
    open_interest         REAL,
    PRIMARY KEY (market_ticker, end_period_ts)
);

CREATE TABLE IF NOT EXISTS orderbook_snapshots (
    market_ticker     TEXT NOT NULL REFERENCES markets(market_ticker),
    pulled_at         TEXT NOT NULL,
    raw_json          TEXT,
    PRIMARY KEY (market_ticker, pulled_at)
);
"""


def _migrate(conn):
    """Add columns introduced after the table already existed on disk --
    SQLite's CREATE TABLE IF NOT EXISTS won't add them to an existing table."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(markets)")}
    for col in ("status", "result", "expected_expiration_time"):
        if col not in existing:
            conn.execute(f"ALTER TABLE markets ADD COLUMN {col} TEXT")


def connect(db_path=DEFAULT_DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA)
    _migrate(conn)
    conn.commit()
    return conn


def _cents(dollars):
    return round(float(dollars) * 100, 2) if dollars not in (None, "") else None


def store_raw(raw, db_path=DEFAULT_DB_PATH):
    """Upsert one pull_event()-shaped `raw` dict into the persistent DB.
    Safe to call repeatedly -- never drops/truncates, only adds or refreshes."""
    conn = connect(db_path)
    now = datetime.now(timezone.utc).isoformat()
    event_ticker = raw["event_ticker"]
    series_ticker = raw["series_ticker"]
    event = raw["event_detail"]["event"]

    with conn:
        conn.execute("""
            INSERT INTO events (event_ticker, series_ticker, title, sub_title, first_pulled_at, last_pulled_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_ticker) DO UPDATE SET
                title=excluded.title, sub_title=excluded.sub_title, last_pulled_at=excluded.last_pulled_at
        """, (event_ticker, series_ticker, event.get("title", ""), event.get("sub_title", ""), now, now))

        n_trades = n_candles = n_books = 0
        for ticker, m in raw["markets"].items():
            conn.execute("""
                INSERT INTO markets (market_ticker, event_ticker, label, open_time, close_time, status, result, expected_expiration_time, last_pulled_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_ticker) DO UPDATE SET
                    label=excluded.label, status=excluded.status, result=excluded.result,
                    expected_expiration_time=excluded.expected_expiration_time,
                    last_pulled_at=excluded.last_pulled_at
            """, (ticker, event_ticker, m["label"], m.get("open_time", ""), m.get("close_time", ""),
                  m.get("status"), m.get("result"), m.get("expected_expiration_time"), now))

            for t in m["trades"]["trades"]:
                conn.execute("""
                    INSERT OR IGNORE INTO trades
                        (trade_id, market_ticker, event_ticker, created_time, yes_price_cents, no_price_cents, size, taker_side)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    t["trade_id"], ticker, event_ticker, t["created_time"],
                    _cents(t["yes_price_dollars"]), _cents(t["no_price_dollars"]),
                    float(t["count_fp"]), t["taker_side"],
                ))
                n_trades += conn.execute("SELECT changes()").fetchone()[0]

            for c in m["candlesticks"].get("candlesticks", []):
                price = c.get("price", {})
                yes_bid = c.get("yes_bid", {})
                yes_ask = c.get("yes_ask", {})
                conn.execute("""
                    INSERT INTO candlesticks
                        (market_ticker, end_period_ts, open_cents, close_cents, high_cents, low_cents,
                         yes_bid_close_cents, yes_ask_close_cents, volume, open_interest)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(market_ticker, end_period_ts) DO UPDATE SET
                        open_cents=excluded.open_cents, close_cents=excluded.close_cents,
                        high_cents=excluded.high_cents, low_cents=excluded.low_cents,
                        yes_bid_close_cents=excluded.yes_bid_close_cents,
                        yes_ask_close_cents=excluded.yes_ask_close_cents,
                        volume=excluded.volume, open_interest=excluded.open_interest
                """, (
                    ticker, c["end_period_ts"], _cents(price.get("open_dollars")), _cents(price.get("close_dollars")),
                    _cents(price.get("high_dollars")), _cents(price.get("low_dollars")),
                    _cents(yes_bid.get("close_dollars")), _cents(yes_ask.get("close_dollars")),
                    float(c.get("volume_fp", 0) or 0), float(c.get("open_interest_fp", 0) or 0),
                ))
                n_candles += 1

            if m.get("orderbook_snapshot") is not None:
                conn.execute("""
                    INSERT OR IGNORE INTO orderbook_snapshots (market_ticker, pulled_at, raw_json)
                    VALUES (?, ?, ?)
                """, (ticker, raw["pulled_at_utc"], json.dumps(m["orderbook_snapshot"])))
                n_books += conn.execute("SELECT changes()").fetchone()[0]

    conn.close()
    return {"new_trades": n_trades, "candlestick_rows_upserted": n_candles, "new_orderbook_snapshots": n_books}


def summary(db_path=DEFAULT_DB_PATH):
    """Print what's currently stored -- events, markets, and row counts."""
    conn = connect(db_path)
    events = conn.execute("SELECT event_ticker, title, first_pulled_at, last_pulled_at FROM events ORDER BY last_pulled_at DESC").fetchall()
    print(f"{len(events)} event(s) stored in {db_path}\n")
    for event_ticker, title, first_pulled, last_pulled in events:
        markets = conn.execute("SELECT market_ticker, label FROM markets WHERE event_ticker=?", (event_ticker,)).fetchall()
        n_trades = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE event_ticker=?", (event_ticker,)
        ).fetchone()[0]
        n_candles = conn.execute(
            "SELECT COUNT(*) FROM candlesticks WHERE market_ticker IN (SELECT market_ticker FROM markets WHERE event_ticker=?)",
            (event_ticker,),
        ).fetchone()[0]
        n_books = conn.execute(
            "SELECT COUNT(*) FROM orderbook_snapshots WHERE market_ticker IN (SELECT market_ticker FROM markets WHERE event_ticker=?)",
            (event_ticker,),
        ).fetchone()[0]
        print(f"  {event_ticker} -- {title}")
        print(f"    markets: {', '.join(f'{lbl} ({tk})' for tk, lbl in markets)}")
        print(f"    {n_trades} trades, {n_candles} candlestick rows, {n_books} orderbook snapshots")
        print(f"    first pulled {first_pulled[:19]}, last pulled {last_pulled[:19]}")
    conn.close()


if __name__ == "__main__":
    import sys
    summary(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_PATH)
