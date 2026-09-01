'''
Second order-flow candidate: does a large individual trade (a "sweep") on
the favorite's market before kickoff predict the outcome beyond the
kickoff price level? Per CONTEXT.md's own finding, clusters of the
biggest trades are usually one order sweeping the book, not many
independent traders -- so trade size/direction is a genuinely different
kind of signal than the price-path momentum tested in order_flow.py
(which found nothing at any lookback window).

Four features on the favorite's own market, pre-kickoff:
  max_trade_size    -- size of the single largest individual trade, in
                        contracts (raw magnitude, direction-agnostic)
  max_trade_signed  -- signed by direction: positive if that trade bought
                        the favorite (taker_side="yes"), negative if
                        against (taker_side="no")
  max_burst_size    -- CONTEXT.md's actual definition of a sweep is a
                        cluster of trades in a short window, not
                        necessarily one giant trade_id -- a single market
                        order commonly fills as several trade records at
                        the same instant, walking through adjacent price
                        levels (confirmed in this data: e.g. 5 trades at
                        one identical timestamp, 45-47c, on the
                        FRESHAW-HAW game). max_burst_size sums size within
                        3-minute bins and takes the largest bin.
  max_burst_signed  -- same, signed by the burst's majority taker_side

Same evaluation approach as order_flow.py: logistic regression of
logit(P(favorite wins)) ~ logit(price) [+ feature], compared by
out-of-sample (5-fold CV) log-loss, since price alone is already close to
well-calibrated (see calibration.py) -- a feature only earns its keep if
it beats that baseline, not just correlates with the outcome.

Usage:
  python3 analysis/sweep.py
'''
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pyarrow as pa

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import connect
from kickoff import load_favorites
from order_flow import EPS, brier, cv_log_loss, fit_logistic, log_loss, logit, predict


def main():
    con, root = connect()
    favorites, _, stats = load_favorites(con, root)

    tickers = [f["favorite_ticker"] for f in favorites]
    kickoff_dts = [datetime.fromtimestamp(f["kickoff_ts"], tz=timezone.utc).replace(tzinfo=None) for f in favorites]
    kickoffs = pa.table({"market_ticker": tickers, "kickoff_ts": kickoff_dts})
    con.register("kickoffs", kickoffs)

    trade_stats = con.sql(f"""
        SELECT t.market_ticker,
               max(t.size) AS max_trade_size,
               arg_max(t.taker_side, t.size) AS max_trade_side,
               sum(t.size) AS total_volume,
               count(*) AS n_trades
        FROM read_parquet('{root}/season=2025/trades/week=*/trades.parquet', hive_partitioning=true) t
        JOIN kickoffs k ON t.market_ticker = k.market_ticker
        WHERE CAST(t.created_time AS TIMESTAMP) <= k.kickoff_ts
        GROUP BY t.market_ticker
    """).fetchall()
    by_ticker = {r[0]: r[1:] for r in trade_stats}

    # A sweep is usually several trade records in a short burst (one market
    # order filling across adjacent price levels), not one giant trade_id --
    # confirmed in this data (see module docstring). Bin into 3-minute
    # windows, matching CONTEXT.md's own worked example, and take the
    # largest bin per market.
    burst_stats = con.sql(f"""
        WITH pre_kickoff AS (
            SELECT t.market_ticker, t.size, t.taker_side,
                   CAST(t.created_time AS TIMESTAMP) AS ts
            FROM read_parquet('{root}/season=2025/trades/week=*/trades.parquet', hive_partitioning=true) t
            JOIN kickoffs k ON t.market_ticker = k.market_ticker
            WHERE CAST(t.created_time AS TIMESTAMP) <= k.kickoff_ts
        ),
        side_bins AS (
            SELECT market_ticker, floor(epoch(ts) / 180)::BIGINT AS bin,
                   taker_side, sum(size) AS side_size
            FROM pre_kickoff
            GROUP BY market_ticker, bin, taker_side
        ),
        bins AS (
            SELECT market_ticker, bin,
                   sum(side_size) AS bin_total,
                   arg_max(taker_side, side_size) AS dominant_side
            FROM side_bins
            GROUP BY market_ticker, bin
        )
        SELECT market_ticker,
               max(bin_total) AS max_burst_size,
               arg_max(dominant_side, bin_total) AS max_burst_side
        FROM bins
        GROUP BY market_ticker
    """).fetchall()
    burst_by_ticker = {r[0]: r[1:] for r in burst_stats}

    print(f"Games: {len(favorites)} usable; {stats['incomplete']} missing a side, "
          f"{stats['tie']} exact ties, {stats['no_price']} markets with no data before kickoff cut.\n")

    rows = []  # (price, max_trade_size, max_trade_signed, max_burst_size, max_burst_signed, won)
    skipped_no_trades = 0
    for fav in favorites:
        s = by_ticker.get(fav["favorite_ticker"])
        b = burst_by_ticker.get(fav["favorite_ticker"])
        if s is None or b is None:
            skipped_no_trades += 1
            continue
        max_size, max_side, total_vol, n_trades = s
        signed = max_size if max_side == "yes" else -max_size
        burst_size, burst_side = b
        burst_signed = burst_size if burst_side == "yes" else -burst_size
        won = 1.0 if fav["favorite_result"] == "yes" else 0.0
        rows.append((fav["favorite_price"], max_size, signed, burst_size, burst_signed, won))

    print(f"{len(rows)} games with pre-kickoff trades on the favorite's market "
          f"({skipped_no_trades} with none).\n")

    price = np.array([r[0] for r in rows])
    max_size = np.array([r[1] for r in rows])
    signed = np.array([r[2] for r in rows])
    burst_size = np.array([r[3] for r in rows])
    burst_signed = np.array([r[4] for r in rows])
    y = np.array([r[5] for r in rows])

    print("Largest pre-kickoff single trade (contracts):")
    print(f"  mean={max_size.mean():.1f}  median={np.median(max_size):.1f}  "
          f"p90={np.percentile(max_size, 90):.1f}  max={max_size.max():.0f}")
    print(f"  bought the favorite (positive): {int((signed > 0).sum())}, "
          f"bought against it (negative): {int((signed < 0).sum())}\n")

    print("Largest pre-kickoff 3-minute burst (contracts):")
    print(f"  mean={burst_size.mean():.1f}  median={np.median(burst_size):.1f}  "
          f"p90={np.percentile(burst_size, 90):.1f}  max={burst_size.max():.0f}")
    print(f"  ratio to largest single trade (median): {np.median(burst_size / max_size):.2f}x\n")

    logit_price = logit(price / 100)
    X_a = logit_price.reshape(-1, 1)

    features = (
        ("max_trade_size (magnitude)", max_size),
        ("max_trade_signed (direction)", signed),
        ("max_burst_size (magnitude)", burst_size),
        ("max_burst_signed (direction)", burst_signed),
    )
    for name, feature in features:
        feature_z = (feature - feature.mean()) / feature.std()
        X_b = np.column_stack([logit_price, feature_z])
        beta_b, se_b = fit_logistic(X_b, y)
        print(f"--- Model: price + {name} ---")
        print(f"  logit(price) coef={beta_b[1]:+.3f} (se {se_b[1]:.3f})   "
              f"feature coef={beta_b[2]:+.3f} (se {se_b[2]:.3f}, z={beta_b[2]/se_b[2]:+.2f})")
        cv_a = cv_log_loss(X_a, y)
        cv_b = cv_log_loss(X_b, y)
        print(f"  5-fold CV log-loss: price only={cv_a:.4f}  price+feature={cv_b:.4f}  "
              f"({'better' if cv_b < cv_a else 'worse'} than price alone)\n")


if __name__ == "__main__":
    main()
