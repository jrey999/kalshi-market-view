'''
Shared filtering/stats helpers for analysis scripts -- factored out of
calibration.py so order_flow.py (and whatever comes after) don't each grow
their own copy of the liquidity filter's carry-forward logic. See
CONTEXT.md's liquidity filter section before touching filtered_price_at:
the two checks (volume, spread) are deliberately not ANDed.
'''
MIN_VOLUME_CONTRACTS = 10


def filtered_price_at(candles, cutoff_ts):
    """candles: [(end_period_ts, close_cents, volume, yes_bid_close_cents,
    yes_ask_close_cents), ...] sorted ascending. Returns (filtered_price,
    source) as of the last candle at or before cutoff_ts, where source is
    "trade" | "carried_forward" | "quoted_mid", or (None, None) if nothing
    to go on yet (no liquid trade and no quote) by that point. Same
    is_liquid / carry-forward logic as kalshi_game_report.py's
    liquidity_filter() and liquidity_filter.py, replayed here against
    Parquet candlestick rows up to an arbitrary cutoff instead of the
    whole series."""
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
