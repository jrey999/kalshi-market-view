"""
Builds a liquidity-filtered price time series from the raw Kalshi
candlestick data in siu_samford_raw.json, per the "small order distorts
the last price" issue found in siu_samford_hourly_price_history.csv
(see the Aug 25/26 dip-and-bounce: a couple of $1-3 lots hit a wide,
static 66/90 bid-ask spread while the book itself never moved).

Method, per hourly candle. Two independent checks, not ANDed together:
volume tells you whether an actual trade is trustworthy; spread tells you
whether the *quoted* mid is trustworthy when nothing traded. (An earlier
version of this script ANDed them, which wrongly discarded the real
447-contract trade on Aug 26 just because the book around it was wide --
a large executed trade is real information regardless of how wide the
resting quote was.)

  - quoted_mid_cents   = (yes_bid_close + yes_ask_close) / 2 * 100
                         always available, reflects the resting book
                         regardless of whether anyone traded that hour.
  - spread_cents       = (yes_ask_close - yes_bid_close) * 100
                         confidence measure for the quoted mid only.
  - raw_last_trade_cents = candlestick close price (NaN if no trade
                         that hour) -- this is what a naive "last price"
                         chart uses, and what got yanked around Aug 25/26
                         by a couple of $1-3 lots.
  - is_liquid          = volume in that hour >= MIN_VOLUME_CONTRACTS.
                         A trade this size is trusted regardless of the
                         surrounding spread.
  - tight_quote        = spread_cents <= MAX_SPREAD_CENTS. Only matters
                         when nothing traded -- decides whether the
                         quoted mid is a good fair-value fallback.
  - filtered_price_cents = raw_last_trade_cents when is_liquid; else the
                         last liquid trade price carried forward; before
                         any liquid trade has occurred, falls back to
                         quoted_mid_cents (flagged low-confidence if the
                         quote itself isn't tight either).
  - price_source       = "trade" | "carried_forward" | "quoted_mid"

Output: siu_samford_liquidity_filtered.csv
"""
import csv
import json
from datetime import datetime, timezone

IN_PATH = "siu_samford_raw.json"
OUT_PATH = "siu_samford_liquidity_filtered.csv"

MIN_VOLUME_CONTRACTS = 10   # hourly volume must clear this to trust the print
MAX_SPREAD_CENTS = 10       # quoted bid/ask spread must be <= this (cents)

with open(IN_PATH) as f:
    raw = json.load(f)

rows = []
for side, m in raw["markets"].items():
    candles = sorted(m["candlesticks"]["candlesticks"], key=lambda c: c["end_period_ts"])

    last_liquid_price = None  # carried forward once we've seen a trustworthy print
    for c in candles:
        ts = datetime.fromtimestamp(c["end_period_ts"], tz=timezone.utc).isoformat()
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
            "market_side": side,
            "timestamp_utc": ts,
            "yes_bid_cents": round(bid_c, 2) if bid_c is not None else "",
            "yes_ask_cents": round(ask_c, 2) if ask_c is not None else "",
            "quoted_mid_cents": round(mid_c, 2) if mid_c is not None else "",
            "spread_cents": round(spread_c, 2) if spread_c is not None else "",
            "raw_last_trade_cents": round(raw_trade_c, 2) if raw_trade_c is not None else "",
            "volume": volume,
            "open_interest": open_interest,
            "is_liquid": is_liquid,
            "tight_quote": tight_quote,
            "filtered_price_cents": round(filtered_price, 2) if filtered_price is not None else "",
            "price_source": source,
        })

rows.sort(key=lambda r: (r["market_side"], r["timestamp_utc"]))

fieldnames = [
    "market_side", "timestamp_utc", "yes_bid_cents", "yes_ask_cents",
    "quoted_mid_cents", "spread_cents", "raw_last_trade_cents", "volume",
    "open_interest", "is_liquid", "tight_quote", "filtered_price_cents", "price_source",
]
with open(OUT_PATH, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

n_liquid = sum(1 for r in rows if r["is_liquid"])
n_total = len(rows)
print(f"Wrote {n_total} rows ({n_liquid} liquid prints, "
      f"{n_total - n_liquid} carried-forward/quoted-mid) to {OUT_PATH}")
print(f"Thresholds: volume >= {MIN_VOLUME_CONTRACTS} contracts, spread <= {MAX_SPREAD_CENTS} cents")
