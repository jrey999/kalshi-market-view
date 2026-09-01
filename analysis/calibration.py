'''
First signal, per CONTEXT.md's modeling plan: bucket games by their
pre-game (kickoff) implied probability and check whether games priced
around X% actually win X% of the time. Cheap gut check of whether there's
mispricing worth chasing before building predictive features.

Kickoff isn't directly available from Kalshi -- approximated as
expected_expiration_time - GAME_DURATION_HOURS (see the commit that added
expected_expiration_time to kalshi_db.py for why that field, not
close_time/open_time, is the right basis). The price at that cut is the
liquidity-filtered price, not the raw last trade -- same is_liquid /
carry-forward logic as kalshi_game_report.py's liquidity_filter() and
liquidity_filter.py, replayed here against Parquet candlestick rows up to
the kickoff cutoff only (see CONTEXT.md if you're about to "simplify"
this: the two checks are deliberately not ANDed).

Usage:
  python3 analysis/calibration.py
'''
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import connect

MIN_VOLUME_CONTRACTS = 10
GAME_DURATION_HOURS = 3.5
BUCKET_WIDTH = 10  # cents


def filtered_price_at_or_before(candles, cutoff_ts):
    """candles: [(end_period_ts, close_cents, volume, yes_bid_close_cents,
    yes_ask_close_cents), ...] sorted ascending. Returns (filtered_price,
    source) as of the last candle at or before cutoff_ts, where source is
    "trade" | "carried_forward" | "quoted_mid", or (None, None) if nothing
    to go on yet (no liquid trade and no quote) by that point."""
    last_liquid_price = None
    filtered, source = None, None
    for ts, close_c, vol, bid_c, ask_c in candles:
        if ts > cutoff_ts:
            break
        is_liquid = close_c is not None and vol is not None and vol >= MIN_VOLUME_CONTRACTS
        if is_liquid:
            last_liquid_price = close_c
            filtered, source = close_c, "trade"
        elif last_liquid_price is not None:
            filtered, source = last_liquid_price, "carried_forward"
        elif bid_c is not None and ask_c is not None:
            filtered, source = (bid_c + ask_c) / 2, "quoted_mid"
    return filtered, source


def wilson_interval(k, n, z=1.959963984540054):
    if n == 0:
        return (None, None)
    phat = k / n
    denom = 1 + z * z / n
    center = (phat + z * z / (2 * n)) / denom
    margin = z * ((phat * (1 - phat) / n + z * z / (4 * n * n)) ** 0.5) / denom
    return (max(0.0, center - margin), min(1.0, center + margin))


def main():
    con, root = connect()

    markets = con.sql(f"""
        SELECT market_ticker, event_ticker, result, expected_expiration_time
        FROM read_parquet('{root}/season=2025/markets.parquet')
        WHERE status = 'finalized' AND result IN ('yes', 'no')
              AND expected_expiration_time IS NOT NULL
    """).fetchall()

    candle_rows = con.sql(f"""
        SELECT market_ticker, end_period_ts, close_cents, volume,
               yes_bid_close_cents, yes_ask_close_cents
        FROM read_parquet('{root}/season=2025/candlesticks/week=*/candles.parquet', hive_partitioning=true)
        ORDER BY market_ticker, end_period_ts
    """).fetchall()

    candles_by_market = defaultdict(list)
    for mt, ts, close_c, vol, bid_c, ask_c in candle_rows:
        candles_by_market[mt].append((ts, close_c, vol, bid_c, ask_c))

    events = defaultdict(dict)  # event_ticker -> market_ticker -> {result, price, source}
    skipped_no_candles = 0
    for market_ticker, event_ticker, result, exp_exp_iso in markets:
        exp_exp = datetime.fromisoformat(exp_exp_iso.replace("Z", "+00:00"))
        kickoff_ts = int((exp_exp - timedelta(hours=GAME_DURATION_HOURS)).timestamp())
        price, source = filtered_price_at_or_before(candles_by_market.get(market_ticker, []), kickoff_ts)
        if price is None:
            skipped_no_candles += 1
            continue
        events[event_ticker][market_ticker] = {"result": result, "price": price, "source": source}

    buckets = defaultdict(lambda: {"n": 0, "wins": 0})
    by_source = defaultdict(lambda: {"n": 0, "wins": 0, "price_sum": 0.0})
    skipped_incomplete = skipped_tie = 0
    for event_ticker, mkts in events.items():
        if len(mkts) != 2:
            skipped_incomplete += 1
            continue
        (mt_a, a), (mt_b, b) = mkts.items()
        if a["price"] == b["price"]:
            skipped_tie += 1
            continue
        favorite = a if a["price"] > b["price"] else b
        bucket = min(int(favorite["price"] // BUCKET_WIDTH) * BUCKET_WIDTH, 100 - BUCKET_WIDTH)
        buckets[bucket]["n"] += 1
        by_source[favorite["source"]]["n"] += 1
        by_source[favorite["source"]]["price_sum"] += favorite["price"]
        if favorite["result"] == "yes":
            buckets[bucket]["wins"] += 1
            by_source[favorite["source"]]["wins"] += 1

    print(f"Games: {len(events)} events with a kickoff-cut price on at least one side; "
          f"{skipped_incomplete} missing a side, {skipped_tie} exact ties, "
          f"{skipped_no_candles} markets with no data before kickoff cut.\n")

    print(f"{'bucket':>12} {'n':>5} {'win rate':>10} {'95% CI':>18}")
    total_n = 0
    for lo in sorted(buckets):
        b = buckets[lo]
        n, wins = b["n"], b["wins"]
        total_n += n
        rate = wins / n if n else 0
        lo_ci, hi_ci = wilson_interval(wins, n)
        ci_str = f"[{lo_ci:.2f}, {hi_ci:.2f}]" if n else "n/a"
        print(f"{lo:>4}-{lo+BUCKET_WIDTH:<6}¢ {n:>5} {rate:>9.1%} {ci_str:>18}")
    print(f"\nTotal games in buckets: {total_n}")

    print(f"\n--- by price source at the kickoff cut (avg predicted vs actual, all buckets pooled) ---")
    print(f"{'source':>17} {'n':>5} {'avg predicted':>14} {'win rate':>10} {'95% CI':>18}")
    for source in ("trade", "carried_forward", "quoted_mid"):
        s = by_source.get(source)
        if not s or s["n"] == 0:
            continue
        n, wins = s["n"], s["wins"]
        avg_pred = s["price_sum"] / n / 100
        rate = wins / n
        lo_ci, hi_ci = wilson_interval(wins, n)
        print(f"{source:>17} {n:>5} {avg_pred:>13.1%} {rate:>9.1%} [{lo_ci:.2f}, {hi_ci:.2f}]")


if __name__ == "__main__":
    main()
