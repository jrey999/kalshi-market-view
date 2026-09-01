'''
Shared "load games with a kickoff-cut favorite price" setup, used by
calibration.py, order_flow.py, and sweep.py so each doesn't grow its own
copy of: fetch markets + candlesticks, approximate kickoff
(expected_expiration_time - GAME_DURATION_HOURS -- see the commit that
added that column for why), compute the liquidity-filtered price at that
cut, and pick the favorite side per game.
'''
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from filters import filtered_price_at

GAME_DURATION_HOURS = 3.5


def load_candles(con, root, season):
    rows = con.sql(f"""
        SELECT market_ticker, end_period_ts, close_cents, volume,
               yes_bid_close_cents, yes_ask_close_cents
        FROM read_parquet('{root}/season={season}/candlesticks/week=*/candles.parquet', hive_partitioning=true)
        ORDER BY market_ticker, end_period_ts
    """).fetchall()
    candles_by_market = defaultdict(list)
    for mt, ts, close_c, vol, bid_c, ask_c in rows:
        candles_by_market[mt].append((ts, close_c, vol, bid_c, ask_c))
    return candles_by_market


def load_favorites(con, root, season=2025):
    """Returns (favorites, candles_by_market, stats). favorites is a list of
    dicts: event_ticker, favorite_ticker, underdog_ticker, favorite_price,
    favorite_source, favorite_result, kickoff_ts -- one per event with a
    valid, non-tied kickoff-cut price on both sides. candles_by_market is
    returned too so callers needing more than the kickoff cut (e.g.
    order_flow.py's prior-window price) don't have to re-fetch. stats has
    skip counts for reporting: no_price, incomplete, tie."""
    markets = con.sql(f"""
        SELECT market_ticker, event_ticker, result, expected_expiration_time
        FROM read_parquet('{root}/season={season}/markets.parquet')
        WHERE status = 'finalized' AND result IN ('yes', 'no')
              AND expected_expiration_time IS NOT NULL
    """).fetchall()
    candles_by_market = load_candles(con, root, season)

    events = defaultdict(dict)
    skipped_no_price = 0
    for market_ticker, event_ticker, result, exp_exp_iso in markets:
        exp_exp = datetime.fromisoformat(exp_exp_iso.replace("Z", "+00:00"))
        kickoff_ts = int((exp_exp - timedelta(hours=GAME_DURATION_HOURS)).timestamp())
        price, source = filtered_price_at(candles_by_market.get(market_ticker, []), kickoff_ts)
        if price is None:
            skipped_no_price += 1
            continue
        events[event_ticker][market_ticker] = {
            "result": result, "price": price, "source": source, "kickoff_ts": kickoff_ts,
        }

    favorites = []
    skipped_incomplete = skipped_tie = 0
    for event_ticker, mkts in events.items():
        if len(mkts) != 2:
            skipped_incomplete += 1
            continue
        (mt_a, a), (mt_b, b) = mkts.items()
        if a["price"] == b["price"]:
            skipped_tie += 1
            continue
        fav_ticker, fav = (mt_a, a) if a["price"] > b["price"] else (mt_b, b)
        dog_ticker = mt_b if fav_ticker == mt_a else mt_a
        favorites.append({
            "event_ticker": event_ticker,
            "favorite_ticker": fav_ticker,
            "underdog_ticker": dog_ticker,
            "favorite_price": fav["price"],
            "favorite_source": fav["source"],
            "favorite_result": fav["result"],
            "kickoff_ts": fav["kickoff_ts"],
        })
    stats = {"no_price": skipped_no_price, "incomplete": skipped_incomplete, "tie": skipped_tie}
    return favorites, candles_by_market, stats
