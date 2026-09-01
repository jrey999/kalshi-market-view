"""
Reusable Kalshi game-market report: give it an event ticker (or search
terms), it pulls trades/candlesticks/orderbook for every outcome in that
event, builds a liquidity-filtered price series (see liquidity_filter.py
for the filter's rationale), and renders a self-contained HTML report
with the same charts used for the SIU vs Samford analysis.

Usage:
  # If you already know the Kalshi event ticker:
  python3 kalshi_game_report.py --event KXNCAAFGAME-26SEP03SIUSAM

  # Otherwise, search by team names / keywords within a series:
  python3 kalshi_game_report.py --search "Southern Illinois Samford"
  python3 kalshi_game_report.py --search "Alabama Auburn" --series KXNCAAFGAME

Also upserts everything pulled into a persistent SQLite database
(kalshi_market_data.db by default -- see kalshi_db.py) that
accumulates across games and re-pulls; pass --no-db to skip that.

Output (written to reports/<event_ticker>/):
  raw.json                     -- full raw API responses, all markets
  trades.csv                   -- every individual trade, all markets
  hourly_price_history.csv     -- hourly OHLC candlesticks, all markets
  liquidity_filtered.csv       -- filtered price series, all markets
  report.html                  -- the standalone chart report

No API key is needed -- these are all public read endpoints.
"""
import argparse
import csv
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests

import kalshi_db

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_SERIES = "KXNCAAFGAME"

# Categorical slots 1 & 2 from the validated reference palette (color-formula.md
# / palette.md) -- fixed order, CVD-safe pair, used instead of team colors so
# the same tool works for any two teams without per-game palette work.
MARKET_COLORS = [
    {"light": "#2a78d6", "dark": "#3987e5"},
    {"light": "#eb6834", "dark": "#d95926"},
]

MIN_VOLUME_CONTRACTS = 10
MAX_SPREAD_CENTS = 10


def get(path, **params):
    """GET with backoff. Kalshi rate-limits bulk pulls with 429s, and long
    background runs also hit transient connection failures, so retry both
    rather than losing an entire multi-hour backfill to one blip."""
    delay = 1.0
    last_err = None
    for attempt in range(6):
        try:
            resp = requests.get(f"{BASE_URL}{path}", params=params, timeout=30)
            if resp.status_code == 429:
                time.sleep(delay)
                delay = min(delay * 2, 30)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            last_err = e
            time.sleep(delay)
            delay = min(delay * 2, 30)
    if last_err:
        raise last_err
    raise RuntimeError(f"Gave up after repeated 429s: {path}")


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
    return trades


def get_all_historical_trades(ticker):
    trades, cursor = [], None
    while True:
        params = {"ticker": ticker, "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        data = get("/historical/trades", **params)
        trades.extend(data.get("trades", []))
        cursor = data.get("cursor")
        if not cursor:
            break
    return trades


def _normalize_historical_candle(c):
    """Historical candlesticks use price.close/open/... (no _dollars suffix)
    and volume/open_interest (no _fp suffix), unlike the live endpoint's
    price.close_dollars / volume_fp. Reshape to the live schema so every
    downstream consumer (store_raw, liquidity_filter, the chart template)
    can stay written against one shape regardless of source."""
    def dollars(d):
        # Drop None values so an all-null price dict comes out {} (falsy),
        # matching the live schema's convention of omitting the key entirely
        # when nothing traded that hour -- downstream code branches on that.
        return {f"{k}_dollars": v for k, v in (d or {}).items() if v is not None}
    return {
        "end_period_ts": c["end_period_ts"],
        "price": dollars(c.get("price")),
        "yes_bid": dollars(c.get("yes_bid")),
        "yes_ask": dollars(c.get("yes_ask")),
        "volume_fp": c.get("volume", 0),
        "open_interest_fp": c.get("open_interest", 0),
    }


def get_historical_candlesticks(ticker, start_ts, end_ts, period_interval=60):
    data = get(f"/historical/markets/{ticker}/candlesticks",
               start_ts=start_ts, end_ts=end_ts, period_interval=period_interval)
    candles = [_normalize_historical_candle(c) for c in data.get("candlesticks", [])]
    return {"candlesticks": candles}


def get_all_events(series_ticker):
    events, cursor = [], None
    while True:
        params = {"series_ticker": series_ticker, "limit": 200}
        if cursor:
            params["cursor"] = cursor
        data = get("/events", **params)
        batch = data.get("events", [])
        events.extend(batch)
        cursor = data.get("cursor")
        if not cursor or not batch:
            break
    return events


def search_events(series_ticker, terms):
    terms_lower = [t.lower() for t in terms]
    events = get_all_events(series_ticker)
    matches = []
    for e in events:
        title = e.get("title", "") + " " + e.get("sub_title", "")
        title_lower = title.lower()
        if all(t in title_lower for t in terms_lower):
            matches.append(e)
    return matches


def resolve_event_ticker(args):
    if args.event:
        return args.event
    if args.search:
        terms = args.search.split()
        print(f"Searching {args.series} for: {' '.join(terms)} ...")
        matches = search_events(args.series, terms)
        if not matches:
            print("No matching events found.", file=sys.stderr)
            sys.exit(1)
        if len(matches) > 1:
            print(f"Found {len(matches)} matches -- re-run with --event <ticker>:")
            for m in matches:
                print(f"  {m['event_ticker']:38} {m.get('title', '')}")
            sys.exit(1)
        ticker = matches[0]["event_ticker"]
        print(f"Resolved to: {ticker} ({matches[0].get('title', '')})")
        return ticker
    print("Pass --event TICKER or --search 'team names'.", file=sys.stderr)
    sys.exit(1)


def pull_event(event_ticker, series_ticker):
    event_detail = get(f"/events/{event_ticker}", with_nested_markets="true")
    event = event_detail["event"]
    markets_meta = event.get("markets") or []
    if not markets_meta:
        # Settled/archived events don't come back with nested markets on the
        # live endpoint -- their market objects (and all trade/candle data)
        # have been moved to Kalshi's historical archive. Same host, /historical/*.
        markets_meta = get("/historical/markets", event_ticker=event_ticker).get("markets", [])
    if len(markets_meta) != 2:
        print(f"Warning: expected 2 outcome markets, found {len(markets_meta)}. "
              "Top-10 'direction' logic assumes exactly 2.", file=sys.stderr)

    now_ts = int(time.time())
    start_ts = now_ts - 45 * 86400

    markets = {}
    for mm in markets_meta:
        ticker = mm["ticker"]
        label = mm.get("yes_sub_title") or mm.get("title") or ticker
        is_historical = mm.get("status") in ("finalized", "settled")

        if is_historical:
            trades = get_all_historical_trades(ticker)
            open_ts = int(datetime.fromisoformat(mm["open_time"].replace("Z", "+00:00")).timestamp())
            close_ts = int(datetime.fromisoformat(mm["close_time"].replace("Z", "+00:00")).timestamp())
            candles = get_historical_candlesticks(ticker, open_ts, close_ts)
            orderbook = None  # no live book for a settled market
        else:
            trades = get_all_trades(ticker)
            candles = get(
                f"/series/{series_ticker}/markets/{ticker}/candlesticks",
                start_ts=start_ts, end_ts=now_ts, period_interval=60,
            )
            orderbook = get(f"/markets/{ticker}/orderbook", depth=10)

        markets[ticker] = {
            "ticker": ticker,
            "label": label,
            "status": mm.get("status"),
            "result": mm.get("result"),
            "open_time": mm.get("open_time", ""),
            "close_time": mm.get("close_time", ""),
            "trades": {"trades": trades},
            "candlesticks": candles,
            "orderbook_snapshot": orderbook,
        }

    return {
        "source": BASE_URL,
        "pulled_at_utc": datetime.now(timezone.utc).isoformat(),
        "event_ticker": event_ticker,
        "series_ticker": series_ticker,
        "event_detail": event_detail,
        "markets": markets,
    }


def write_raw_and_csvs(raw, out_dir):
    with open(f"{out_dir}/raw.json", "w") as f:
        json.dump(raw, f, indent=2)

    trade_rows = []
    for ticker, m in raw["markets"].items():
        for t in m["trades"]["trades"]:
            trade_rows.append({
                "market_ticker": ticker,
                "market_label": m["label"],
                "timestamp_utc": t["created_time"],
                "price_yes_cents": round(float(t["yes_price_dollars"]) * 100, 2),
                "price_no_cents": round(float(t["no_price_dollars"]) * 100, 2),
                "volume": float(t["count_fp"]),
                "taker_side": t["taker_side"],
                "trade_id": t["trade_id"],
            })
    trade_rows.sort(key=lambda r: (r["market_ticker"], r["timestamp_utc"]))
    with open(f"{out_dir}/trades.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "market_ticker", "market_label", "timestamp_utc", "price_yes_cents",
            "price_no_cents", "volume", "taker_side", "trade_id",
        ])
        writer.writeheader()
        writer.writerows(trade_rows)

    candle_rows = []
    for ticker, m in raw["markets"].items():
        for c in m["candlesticks"].get("candlesticks", []):
            price = c.get("price", {})
            yes_bid = c.get("yes_bid", {})
            yes_ask = c.get("yes_ask", {})
            ts = datetime.fromtimestamp(c["end_period_ts"], tz=timezone.utc).isoformat()

            def cents(d, key):
                v = d.get(key)
                return round(float(v) * 100, 2) if v else ""

            candle_rows.append({
                "market_ticker": ticker,
                "market_label": m["label"],
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
    candle_rows.sort(key=lambda r: (r["market_ticker"], r["timestamp_utc"]))
    with open(f"{out_dir}/hourly_price_history.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "market_ticker", "market_label", "timestamp_utc", "open_cents",
            "close_cents", "high_cents", "low_cents", "yes_bid_close_cents",
            "yes_ask_close_cents", "volume", "open_interest",
        ])
        writer.writeheader()
        writer.writerows(candle_rows)


def liquidity_filter(raw, out_dir):
    """Same logic as liquidity_filter.py: volume decides trust in a trade,
    spread only decides whether the quoted mid is a good fallback."""
    rows = []
    filtered_by_market = {}
    for ticker, m in raw["markets"].items():
        candles = sorted(m["candlesticks"]["candlesticks"], key=lambda c: c["end_period_ts"])
        last_liquid_price = None
        points = []
        for c in candles:
            ts_ms = c["end_period_ts"] * 1000
            ts_iso = datetime.fromtimestamp(c["end_period_ts"], tz=timezone.utc).isoformat()
            yes_bid = c.get("yes_bid", {}).get("close_dollars")
            yes_ask = c.get("yes_ask", {}).get("close_dollars")
            price = c.get("price", {})
            raw_close = price.get("close_dollars")
            volume = float(c.get("volume_fp", 0) or 0)
            open_interest = float(c.get("open_interest_fp", 0) or 0)

            bid_c = float(yes_bid) * 100 if yes_bid else None
            ask_c = float(yes_ask) * 100 if yes_ask else None
            mid_c = (bid_c + ask_c) / 2 if bid_c is not None and ask_c is not None else None
            spread_c = (ask_c - bid_c) if bid_c is not None and ask_c is not None else None
            raw_trade_c = float(raw_close) * 100 if raw_close else None

            is_liquid = raw_trade_c is not None and volume >= MIN_VOLUME_CONTRACTS
            tight_quote = spread_c is not None and spread_c <= MAX_SPREAD_CENTS

            if is_liquid:
                filtered_price = raw_trade_c
                source = "trade"
                last_liquid_price = raw_trade_c
            elif last_liquid_price is not None:
                filtered_price = last_liquid_price
                source = "carried_forward"
            else:
                filtered_price = mid_c
                source = "quoted_mid" if tight_quote else "quoted_mid_wide"

            rows.append({
                "market_ticker": ticker, "market_label": m["label"], "timestamp_utc": ts_iso,
                "yes_bid_cents": bid_c, "yes_ask_cents": ask_c, "quoted_mid_cents": mid_c,
                "spread_cents": spread_c, "raw_last_trade_cents": raw_trade_c, "volume": volume,
                "open_interest": open_interest, "is_liquid": is_liquid, "tight_quote": tight_quote,
                "filtered_price_cents": filtered_price, "price_source": source,
            })
            points.append({
                "t": ts_ms, "f": filtered_price, "raw": raw_trade_c,
                "bid": bid_c, "ask": ask_c, "vol": volume, "liq": is_liquid,
            })
        filtered_by_market[ticker] = points

    rows.sort(key=lambda r: (r["market_ticker"], r["timestamp_utc"]))
    fieldnames = [
        "market_ticker", "market_label", "timestamp_utc", "yes_bid_cents", "yes_ask_cents",
        "quoted_mid_cents", "spread_cents", "raw_last_trade_cents", "volume",
        "open_interest", "is_liquid", "tight_quote", "filtered_price_cents", "price_source",
    ]
    with open(f"{out_dir}/liquidity_filtered.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            r = dict(r)
            for k in ("yes_bid_cents", "yes_ask_cents", "quoted_mid_cents", "spread_cents",
                      "raw_last_trade_cents", "filtered_price_cents"):
                r[k] = round(r[k], 2) if r[k] is not None else ""
            writer.writerow(r)

    return filtered_by_market


def compute_kpis(raw, market_tickers):
    primary = market_tickers[0]
    candles = sorted(raw["markets"][primary]["candlesticks"]["candlesticks"], key=lambda c: c["end_period_ts"])
    real_candles = [c for c in candles if c.get("price")]
    label = raw["markets"][primary]["label"]

    items = []
    if real_candles:
        open_px = float(real_candles[0]["price"]["open_dollars"]) * 100
        close_px = float(real_candles[-1]["price"]["close_dollars"]) * 100
        items.append({"label": f"Opening price ({label})", "value": f"{open_px:.0f}¢", "sub": "first trade"})
        items.append({"label": f"Current price ({label})", "value": f"{close_px:.0f}¢", "sub": "as of data pull"})

        by_day = defaultdict(list)
        for c in candles:
            close = c.get("price", {}).get("close_dollars")
            if close is not None:
                ts = datetime.fromtimestamp(c["end_period_ts"], tz=timezone.utc)
                by_day[ts.date()].append((ts, float(close)))
        daily_last = {d: sorted(v)[-1][1] for d, v in by_day.items()}
        days = sorted(daily_last)
        biggest, prev = None, None
        for d in days:
            if prev:
                move = (daily_last[d] - daily_last[prev]) * 100
                if biggest is None or abs(move) > abs(biggest[2]):
                    biggest = (prev, d, move)
            prev = d
        if biggest:
            items.append({
                "label": "Biggest 1-day move",
                "value": f"{biggest[2]:+.0f}¢",
                "sub": f"{biggest[0].strftime('%b %-d')} → {biggest[1].strftime('%b %-d')}",
            })

    total_vol = sum(
        sum(float(t["count_fp"]) for t in m["trades"]["trades"])
        for m in raw["markets"].values()
    )
    items.append({"label": "Combined volume", "value": f"{total_vol:,.0f}", "sub": "contracts, all markets"})
    return {"items": items}


def compute_top10(raw, market_tickers, market_labels):
    if len(market_tickers) != 2:
        return []  # direction logic below assumes exactly 2 markets
    a, b = market_tickers
    all_trades = []
    for ticker in market_tickers:
        other = b if ticker == a else a
        trades = sorted(raw["markets"][ticker]["trades"]["trades"], key=lambda t: t["created_time"])
        for t in trades:
            price = float(t["yes_price_dollars"]) * 100
            size = float(t["count_fp"])
            taker = t["taker_side"]
            direction = ticker if taker == "yes" else other
            all_trades.append({
                "t": int(datetime.fromisoformat(t["created_time"].replace("Z", "+00:00")).timestamp() * 1000),
                "market": ticker,
                "price": round(price, 1),
                "size": round(size, 1),
                "taker": taker,
                "direction": direction,
            })
    all_trades.sort(key=lambda x: -x["size"])
    return all_trades[:10]


def render_html(raw, market_tickers, filtered_by_market, kpis, top10, out_dir):
    event = raw["event_detail"]["event"]
    title = event.get("title", raw["event_ticker"])
    sub_title = event.get("sub_title", "")

    colors = []
    for i, ticker in enumerate(market_tickers):
        c = MARKET_COLORS[i % len(MARKET_COLORS)]
        colors.append({
            "key": ticker,
            "label": raw["markets"][ticker]["label"],
            "color": f"var(--m{i+1})",
        })

    data = {
        "kpi": kpis,
        "markets": [
            {**colors[i], "points": filtered_by_market[ticker]}
            for i, ticker in enumerate(market_tickers)
        ],
        "top10": top10,
    }

    open_time = raw["markets"][market_tickers[0]].get("open_time", "")
    close_time = raw["markets"][market_tickers[0]].get("close_time", "")

    with open(f"{SCRIPT_DIR}/report_template.html") as f:
        tmpl = f.read()

    out = tmpl
    out = out.replace("@@TITLE@@", title)
    out = out.replace("@@KICKER@@", f"{raw['event_ticker']} · Kalshi prediction market")
    out = out.replace(
        "@@DEK@@",
        "Hourly implied win probability, filtered for liquidity. The faint dots are every trade "
        "Kalshi recorded; the heavy line only updates when a print clears real size."
    )
    meta_bits = [sub_title] if sub_title else []
    if open_time:
        meta_bits.append(f"Market opened {open_time[:10]}")
    if close_time:
        meta_bits.append(f"Closes {close_time[:10]}")
    meta_bits.append(f"Data pulled {raw['pulled_at_utc'][:10]}")
    meta_html = " &middot; ".join(meta_bits)
    out = out.replace("@@META@@", meta_html)
    out = out.replace("@@FOOTER@@", "Source: Kalshi public read API (api.elections.kalshi.com/trade-api/v2) · no API key required for market data")
    out = out.replace("@@DATA_JSON@@", json.dumps(data))

    out_path = f"{out_dir}/report.html"
    with open(out_path, "w") as f:
        f.write(out)
    return out_path


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--event", help="Kalshi event ticker, e.g. KXNCAAFGAME-26SEP03SIUSAM")
    parser.add_argument("--search", help="Search terms to find the event, e.g. 'Southern Illinois Samford'")
    parser.add_argument("--series", default=DEFAULT_SERIES, help=f"Series ticker to search within (default {DEFAULT_SERIES})")
    parser.add_argument("--out-dir", default=None, help="Output directory (default reports/<event_ticker>)")
    parser.add_argument("--db", default=kalshi_db.DEFAULT_DB_PATH, help="SQLite DB path to persist into (default kalshi_market_data.db)")
    parser.add_argument("--no-db", action="store_true", help="Skip writing to the persistent SQLite DB")
    args = parser.parse_args()

    event_ticker = resolve_event_ticker(args)
    out_dir = args.out_dir or f"{SCRIPT_DIR}/reports/{event_ticker}"
    os.makedirs(out_dir, exist_ok=True)

    print(f"Pulling {event_ticker} ...")
    raw = pull_event(event_ticker, args.series)
    market_tickers = list(raw["markets"].keys())

    write_raw_and_csvs(raw, out_dir)

    if not args.no_db:
        db_stats = kalshi_db.store_raw(raw, args.db)
        print(f"Persisted to {args.db}: +{db_stats['new_trades']} new trades, "
              f"{db_stats['candlestick_rows_upserted']} candlestick rows upserted, "
              f"+{db_stats['new_orderbook_snapshots']} orderbook snapshots")

    filtered_by_market = liquidity_filter(raw, out_dir)
    kpis = compute_kpis(raw, market_tickers)
    top10 = compute_top10(raw, market_tickers, {t: raw["markets"][t]["label"] for t in market_tickers})
    report_path = render_html(raw, market_tickers, filtered_by_market, kpis, top10, out_dir)

    print(f"\nWrote: {out_dir}/")
    print(f"  raw.json, trades.csv, hourly_price_history.csv, liquidity_filtered.csv, report.html")
    print(f"\n=== {raw['event_detail']['event'].get('title', event_ticker)} ===")
    for item in kpis["items"]:
        print(f"  {item['label']}: {item['value']} ({item['sub']})")
    print(f"\nOpen report.html in a browser, or ask Claude to publish it as an Artifact.")


if __name__ == "__main__":
    main()
