# Kalshi CFB market research — project context

Pulling and analyzing Kalshi prediction-market data for college football,
building toward modeling signals from order flow. Started as research into
one game (Southern Illinois @ Samford, Sept 3 2026), generalized into a
reusable tool, then a persistent multi-season dataset.

**Repo move complete**: code was moved from `jrey999/Backtester` (branch
`claude/siu-samford-kalshi-data-59r9tn`, under `kalshi_data/`) into its own
repo, `jrey999/kalshi-market-view`. Git history was not carried over (the
old branch had a ~22 MB SQLite file committed in it).

**Layout**: pipeline code lives under `data/`; the subset of it that talks
to the Spaces bucket (`spaces_export.py`, `spaces_sync.py`) lives under
`data/spaces/`, which also holds `staging/` — the local Parquet tree
`spaces_export.py` writes and `spaces_sync.py` uploads from (gitignored,
regenerable). `data/daily_sync.py` orchestrates the three CLIs
(`bulk_pull.py` → `spaces/spaces_export.py` → `spaces/spaces_sync.py`) into
one daily job; see "Daily automation" below.

## Key API facts (learned the hard way)

- **Base URL**: `https://api.elections.kalshi.com/trade-api/v2`. The old
  `trading-api.kalshi.com` is decommissioned (401 + "moved" notice).
- **No API key needed** for reads — events, markets, trades, candlesticks,
  orderbook are all public.
- **`/historical/*` endpoints are the big unlock.** Kalshi purges settled
  markets from the live endpoints: `/markets?event_ticker=...` returns
  empty and the market ticker 404s for any past game, even recent ones.
  But `/historical/markets`, `/historical/trades`, and
  `/historical/markets/{ticker}/candlesticks` serve the full archive on the
  *same* host. Critically, historical market objects carry
  **`result` ("yes"/"no")** and `expiration_value` (winning team) — that's
  ground-truth outcome labels straight from Kalshi's own settlement, so
  win/loss labels do NOT require an external results API.
- **Historical schema differs subtly from live**: candlesticks use
  `price.close` / `volume` / `open_interest`, where live uses
  `price.close_dollars` / `volume_fp` / `open_interest_fp`. Values are the
  same dollar-string format. `_normalize_historical_candle()` in
  `kalshi_game_report.py` reshapes historical into the live schema so all
  downstream code stays written against one shape. It also drops
  None-valued keys, because live omits keys entirely when nothing traded
  and downstream code branches on that dict being falsy.
- **Series**: `KXNCAAFGAME` covers every game, all divisions (FBS, FCS,
  D-III). Each event has exactly 2 outcome markets, suffixed `-<TEAM>`.
- **Event ticker format**: `KXNCAAFGAME-YYMONDDAWAYHOME`, e.g.
  `26SEP03SIUSAM`. Parsed for date filtering in `bulk_pull.py`.
- **`/events` pagination**: must page with `cursor` until empty — a single
  page silently misses games that do exist.
- **`/markets?series_ticker=...`** is the fast way to scan every market's
  `last_price_dollars` / `previous_price_dollars` / `volume_fp` at once,
  for finding interesting games without pulling full trade history.
- **Rate limits are real**: bulk pulling at 0.1s between games triggered
  429s within 16 games. `get()` now retries with exponential backoff;
  default inter-game sleep is 0.4s.

## The core analytical finding: thin markets lie about price

Most of these markets are **very illiquid** — small-school games trade in
$1–3 lots. The "last traded price" you'd read off a chart is frequently
not a consensus at all, just whichever tiny odd-lot last crossed a wide,
stale, mostly-untouched spread.

Two worked examples:
- **SIU–Samford, Aug 25–26**: printed price swung 90¢ → 65¢ → 90¢ while
  the quoted book never moved (~66/90 the whole time, 23 straight hours of
  zero volume). Two $1–3 trades caused the entire "move."
- **Illinois St / Western Illinois**: an ask of 25–35¢ sat completely
  untraded for over a week. The only real trades in that window priced
  Western Illinois at 7–10% the whole time. A naive reading says
  "pickem → landslide"; the truth is the market always knew, and a big
  real sweep just confirmed it.

**The filter** (in `liquidity_filter.py`, and inline in
`kalshi_game_report.py`) — two *independent* checks, deliberately not ANDed:
- `is_liquid` = hourly volume ≥ 10 contracts → trust the traded price,
  **regardless of spread**. (Originally ANDed with a tight-spread check,
  which wrongly discarded a legitimate 447-contract trade that crossed a
  wide spread. A large trade is real information even in a thin book.)
- `tight_quote` = spread ≤ 10¢ → only decides whether the quoted midpoint
  is a usable stand-in when nothing traded.
- `filtered_price` = traded price on liquid hours; else carried forward
  from the last liquid trade; else the quoted mid (flagged low-confidence).

Also: clusters of "biggest trades" are usually **one order sweeping the
book**, not many independent traders — 8 of the 10 biggest SIU–Samford
trades landed in a single 3-minute window. The report's top-10 chart
auto-detects this.

## Architecture

**Spaces bucket is the system of record. SQLite is a local working cache.
The repo is code only.** The full historical dataset runs to ~1–2 GB,
which does not belong in git.

Bucket layout (`degenerate-cafe`, nyc3):

```
kalshi/sport=<sport>/season=<yyyy>/
  events.parquet
  markets.parquet
  trades/week=<iso-year-week>/trades.parquet
  candlesticks/week=<iso-year-week>/candles.parquet
  orderbooks/week=<iso-year-week>/snapshots.parquet
```

Week partitioning (not per-event) because the dominant read is "load a
season to fit a model" — per-event produced ~2,800 tiny files/season.
Restructuring cut the two-week dataset from 738 files / 9.2 MB to
8 files / 2.7 MB. `event_ticker` stays a column so single-game reads are
just a predicate. This costs nothing on write because SQLite absorbs the
incremental churn and the export is a batch pass. ISO year-week so January
bowl games sort correctly within their season. `sport=` exists so NBA/NFL
can land in the same bucket later without rewriting keys.

Orderbook snapshots stay raw JSON in one column — flatten in the database
if a use emerges. Compression is snappy; zstd was considered and rejected
since the $5 Spaces plan covers 250 GB / 1 TB transfer against a ~1 GB
dataset.

## Files

- `kalshi_game_report.py` — main CLI. `--event TICKER` or
  `--search "team names"`. Pulls both outcome markets (auto-dispatching
  live vs `/historical/*` by market status), applies the liquidity filter,
  writes per-game CSV/JSON + a self-contained HTML report, upserts to SQLite.
- `report_template.html` — the report: KPI row, a price panel per team
  (raw dots + filtered line + illiquid shading + hover), a ten-biggest-trades
  diverging bar chart. Uses a validated categorical palette, not team colors,
  so it works for any matchup.
- `kalshi_db.py` — SQLite store: `events`, `markets` (incl. `status`/`result`),
  `trades` (deduped by `trade_id`), `candlesticks` (upsert per market+hour),
  `orderbook_snapshots` (append-only). Never drops/truncates. Has a
  `_migrate()` for columns added after tables existed. `python3 kalshi_db.py`
  prints what's stored.
- `bulk_pull.py` — batch pull over a date window. `--skip-existing` skips
  events whose markets are all settled, so a killed run resumes cheaply
  (unsettled games are still re-pulled, since their prices move).
- `spaces/spaces_export.py` — SQLite → Parquet in the bucket layout, into
  `spaces/staging/`. Streams each week through a ParquetWriter in row-group
  chunks so big weeks don't materialize in memory.
- `spaces/spaces_sync.py` — boto3 against the DO endpoint (Spaces is
  S3-compatible). Credentials from env or gitignored `.env` (checked at the
  repo root, then next to the script), never logged. Skips objects whose
  size already matches. `--check`, `--dry-run`, `--prefix`.
- `daily_sync.py` — runs `bulk_pull.py` for one day (default: yesterday,
  UTC) then chains into `spaces/spaces_export.py` and `spaces/spaces_sync.py`.
  This is what the daily Routine below actually invokes.
- `build_dataset.py`, `liquidity_filter.py` — the original SIU–Samford
  one-off scripts, superseded by the above but kept as the original analysis.
  Unlike the rest, these use paths relative to the working directory, not
  the script's own directory, so run them from `data/`.

## Daily automation

A Claude Code Routine (scheduled trigger) fires daily, spins up a fresh
session in this environment, and runs `python3 data/daily_sync.py`. Because
each firing gets a fresh, ephemeral container with no persisted `.env`,
`SPACES_KEY` / `SPACES_SECRET` / `SPACES_REGION` / `SPACES_BUCKET` need to be
available some other way for that session to actually upload anything —
either as environment variables configured on this Claude Code environment
itself, or the fired session needs to be told where to get them. Check with
whoever owns this project whether that's set up before assuming the daily
job is actually uploading data.

## Credentials & environment

- `.env` (gitignored; repo root, or `data/spaces/`) holds `SPACES_KEY` /
  `SPACES_SECRET` / `SPACES_REGION=nyc3` / `SPACES_BUCKET=degenerate-cafe`.
  **These keys were shared through a chat transcript and should be
  rotated.**
- **Egress is allowlisted per environment.** `api.elections.kalshi.com` and
  `nyc3.digitaloceanspaces.com` are open. ESPN, TheOddsAPI, TheSportsDB,
  Sportradar and Kalshi's *docs* domains are blocked (403 from the proxy).
  `api.collegefootballdata.com` is open but needs a free API key
  (`CFBD_API_KEY`) that hasn't been created yet.
- Not installed by default here: `pyarrow`, `boto3` (pip install both);
  `python-dotenv` is absent, so `spaces_sync.py` parses `.env` itself.

## Current state

- **Bucket**: season=2026 (the current two-week slate, 252 games) and
  season=2025 (backfill, partial) are uploaded and verified readable.
- **Backfill**: `bulk_pull.py --start 2025-08-01 --end 2026-02-01
  --db kalshi_historical.db --skip-existing` was running at
  ~234/936 events, 1.24 M trades, 468 settled markets. **The container is
  ephemeral — if it was reclaimed, this needs re-running.** It's resumable
  and idempotent; re-run the same command and re-export/sync.
- Two published chart artifacts exist from the analysis phase (SIU–Samford
  "Homewood Line Watch", and Illinois St vs Western Illinois). Regenerable.

## Next phase: modeling signals (not started)

No modeling code exists yet. The plan discussed:

1. **Calibration first** — bucket games by closing price, check whether
   games priced at 90% actually win ~90%. Cheap to compute, and tells you
   whether there's any mispricing to chase before isolating features.
2. **Then predictive** — does an order-flow signal (a real sweep, filtered
   price diverging from naive last price, direction of the biggest trades)
   predict the winner better than the pre-game price alone?

Candidate features raised: liquidity-filtered price vs naive last price;
spread width as a confidence measure; volume/OI jumps; time-to-kickoff
(late movement likely more informative); YES(A)+YES(B) basis drift from
100¢; sweep-vs-broad-consensus detection. A vig-removed sportsbook line
comparison would need CFBD (key pending).

Labels come free from Kalshi's own `result` field — no external results
API strictly required.
