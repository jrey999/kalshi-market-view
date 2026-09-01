"""
Pulls Kalshi market data for the Southern Illinois @ Samford college
football game (Sept 3, 2026) and writes:
  - siu_samford_raw.json                (full raw API responses)
  - siu_samford_trades.csv               (every individual trade)
  - siu_samford_hourly_price_history.csv (hourly OHLC candlesticks)

Data source: Kalshi's public read API — no API key needed for these
read-only endpoints (events, trades, candlesticks, orderbook).
  Base URL: https://api.elections.kalshi.com/trade-api/v2
  NOTE: the older host https://trading-api.kalshi.com is decommissioned —
  it now returns HTTP 401 with a "moved" notice. If Kalshi changes the base
  URL again, check https://trading-api.readme.io/reference for the current one.

  Authenticated endpoints (placing orders, account/portfolio data) DO
  require an API key. If this script ever needs those, set:
    KALSHI_API_KEY_ID   - key ID from kalshi.com/account -> API Keys
    KALSHI_PRIVATE_KEY  - the RSA private key (PEM) generated alongside it
  and sign requests per Kalshi's docs. Not needed for anything pulled here.

Event: KXNCAAFGAME-26SEP03SIUSAM
  - KXNCAAFGAME-26SEP03SIUSAM-SIU  ("Southern Illinois wins")
  - KXNCAAFGAME-26SEP03SIUSAM-SAM  ("Samford wins")
"""
import csv
import json
import time
from datetime import datetime, timezone

import requests

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
OUT_DIR = "."
EVENT_TICKER = "KXNCAAFGAME-26SEP03SIUSAM"
SERIES_TICKER = "KXNCAAFGAME"
MARKETS = {
    "SIU": f"{EVENT_TICKER}-SIU",
    "SAM": f"{EVENT_TICKER}-SAM",
}


def get(path, **params):
    resp = requests.get(f"{BASE_URL}{path}", params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


def get_all_trades(ticker):
    trades, cursor = [], None
    while True:
        params = {"ticker": ticker, "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        data = get("/markets/trades", **params)
        trades.extend(data.get("trades", []))
        cursor = data.get("cursor")
        if not cursor:
            break
    return {"trades": trades}


def main():
    event_detail = get(f"/events/{EVENT_TICKER}", with_nested_markets="true")

    raw = {
        "source": BASE_URL,
        "pulled_at_utc": datetime.now(timezone.utc).isoformat(),
        "event_ticker": EVENT_TICKER,
        "series_ticker": SERIES_TICKER,
        "event_detail": event_detail,
        "markets": {},
    }

    now_ts = int(time.time())
    # market opened well before this; a wide start_ts just gets clamped by Kalshi
    start_ts = now_ts - 30 * 86400

    for side, ticker in MARKETS.items():
        trades = get_all_trades(ticker)
        candles = get(
            f"/series/{SERIES_TICKER}/markets/{ticker}/candlesticks",
            start_ts=start_ts, end_ts=now_ts, period_interval=60,
        )
        orderbook = get(f"/markets/{ticker}/orderbook", depth=10)
        raw["markets"][side] = {
            "ticker": ticker,
            "trades": trades,
            "candlesticks": candles,
            "orderbook_snapshot": orderbook,
        }

    with open(f"{OUT_DIR}/siu_samford_raw.json", "w") as f:
        json.dump(raw, f, indent=2)

    # ---- Cleaned CSV: one row per trade (both sides) ----
    trade_rows = []
    for side, m in raw["markets"].items():
        for t in m["trades"]["trades"]:
            trade_rows.append({
                "market_side": side,
                "timestamp_utc": t["created_time"],
                "price_yes_cents": round(float(t["yes_price_dollars"]) * 100, 2),
                "price_no_cents": round(float(t["no_price_dollars"]) * 100, 2),
                "volume": float(t["count_fp"]),
                "taker_side": t["taker_side"],
                "trade_id": t["trade_id"],
            })
    trade_rows.sort(key=lambda r: (r["market_side"], r["timestamp_utc"]))

    with open(f"{OUT_DIR}/siu_samford_trades.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "market_side", "timestamp_utc", "price_yes_cents", "price_no_cents",
            "volume", "taker_side", "trade_id",
        ])
        writer.writeheader()
        writer.writerows(trade_rows)

    # ---- Cleaned CSV: hourly candlesticks (price history) for both sides ----
    candle_rows = []
    for side, m in raw["markets"].items():
        for c in m["candlesticks"].get("candlesticks", []):
            price = c.get("price", {})
            yes_bid = c.get("yes_bid", {})
            yes_ask = c.get("yes_ask", {})
            ts = datetime.fromtimestamp(c["end_period_ts"], tz=timezone.utc).isoformat()

            def cents(d, key):
                v = d.get(key)
                return round(float(v) * 100, 2) if v else ""

            candle_rows.append({
                "market_side": side,
                "timestamp_utc": ts,
                "open_cents": cents(price, "open_dollars"),
                "close_cents": cents(price, "close_dollars"),
                "high_cents": cents(price, "high_dollars"),
                "low_cents": cents(price, "low_dollars"),
                "yes_bid_close_cents": cents(yes_bid, "close_dollars"),
                "yes_ask_close_cents": cents(yes_ask, "close_dollars"),
                "volume": float(c.get("volume_fp", 0) or 0),
                "open_interest": float(c.get("open_interest_fp", 0) or 0),
            })
    candle_rows.sort(key=lambda r: (r["market_side"], r["timestamp_utc"]))

    with open(f"{OUT_DIR}/siu_samford_hourly_price_history.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "market_side", "timestamp_utc", "open_cents", "close_cents",
            "high_cents", "low_cents", "yes_bid_close_cents", "yes_ask_close_cents",
            "volume", "open_interest",
        ])
        writer.writeheader()
        writer.writerows(candle_rows)

    print(f"Wrote {len(trade_rows)} trade rows and {len(candle_rows)} hourly candle rows")


if __name__ == "__main__":
    main()
