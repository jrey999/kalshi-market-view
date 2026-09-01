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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import connect
from filters import wilson_interval
from kickoff import load_favorites

BUCKET_WIDTH = 10  # cents


def main():
    con, root = connect()
    favorites, _, stats = load_favorites(con, root)

    buckets = defaultdict(lambda: {"n": 0, "wins": 0})
    by_source = defaultdict(lambda: {"n": 0, "wins": 0, "price_sum": 0.0})
    for fav in favorites:
        price, source, result = fav["favorite_price"], fav["favorite_source"], fav["favorite_result"]
        bucket = min(int(price // BUCKET_WIDTH) * BUCKET_WIDTH, 100 - BUCKET_WIDTH)
        buckets[bucket]["n"] += 1
        by_source[source]["n"] += 1
        by_source[source]["price_sum"] += price
        if result == "yes":
            buckets[bucket]["wins"] += 1
            by_source[source]["wins"] += 1

    print(f"Games: {len(favorites)} usable; {stats['incomplete']} missing a side, "
          f"{stats['tie']} exact ties, {stats['no_price']} markets with no data before kickoff cut.\n")

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
