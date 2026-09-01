'''
DuckDB access to the Kalshi CFB Parquet dataset.

The dataset's system of record is the Spaces bucket (see data/CONTEXT.md
for the layout). DuckDB can query Parquet directly out of S3-compatible
storage via its httpfs extension -- but that extension has to be
downloaded from extensions.duckdb.org on first use, and this sandbox's
egress allowlist (api.elections.kalshi.com / nyc3.digitaloceanspaces.com /
api.collegefootballdata.com only) blocks that domain. Rather than depend on
that staying open in every future ephemeral container, connect() instead
syncs the Parquet files down into a local cache via boto3 (reusing
spaces_sync.py's client -- nyc3.digitaloceanspaces.com is allowed) and
points DuckDB at the local copy. Local Parquet reads need no extension.
Re-syncing is cheap: like spaces_sync.py, it skips files whose local size
already matches the bucket's.

Run from a machine with normal internet access (not this sandbox), the
httpfs + direct-S3 approach works fine and skips the local copy entirely
-- see the note at the bottom of this file.

Usage:
  from db import connect
  con, root = connect()
  con.sql(f"SELECT * FROM read_parquet('{root}/season=2025/events.parquet')").df()

  # Hive-partitioned glob across every season/week:
  con.sql(f"""
      SELECT * FROM read_parquet('{root}/season=*/trades/week=*/trades.parquet',
                                  hive_partitioning=true)
  """).df()

Run this file directly for a quick sanity check against whatever's
currently in the bucket: `python3 analysis/db.py`
'''
import os
import sys

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "spaces"))
from spaces_sync import load_env, make_client

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CACHE_DIR = os.path.join(SCRIPT_DIR, "cache")


def _sync_cache(cache_dir):
    cfg = load_env()
    client = make_client(cfg)
    bucket = cfg["SPACES_BUCKET"]

    paginator = client.get_paginator("list_objects_v2")
    downloaded = skipped = 0
    for page in paginator.paginate(Bucket=bucket, Prefix="kalshi/sport=cfb/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            local_path = os.path.join(cache_dir, os.path.relpath(key, "kalshi/"))
            if os.path.exists(local_path) and os.path.getsize(local_path) == obj["Size"]:
                skipped += 1
                continue
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            client.download_file(bucket, key, local_path)
            downloaded += 1
    print(f"Synced local cache: {downloaded} downloaded, {skipped} already current.")


def connect(cache_dir=DEFAULT_CACHE_DIR):
    """Returns (con, root). root is a local filesystem path standing in for
    the bucket's kalshi/sport=cfb/ prefix -- build query paths under it with
    plain string formatting, same as you would an s3:// URI."""
    _sync_cache(cache_dir)
    con = duckdb.connect()
    root = os.path.join(cache_dir, "sport=cfb")
    return con, root


if __name__ == "__main__":
    con, root = connect()

    print("\n=== events / markets per season ===")
    con.sql(f"""
        SELECT season, count(*) AS events
        FROM read_parquet('{root}/season=*/events.parquet', hive_partitioning=true)
        GROUP BY season ORDER BY season
    """).show()
    con.sql(f"""
        SELECT season, status, result, count(*) AS markets
        FROM read_parquet('{root}/season=*/markets.parquet', hive_partitioning=true)
        GROUP BY season, status, result ORDER BY season, status, result
    """).show()

    print("=== trades per season ===")
    con.sql(f"""
        SELECT season, count(*) AS trades, count(DISTINCT event_ticker) AS events_with_trades
        FROM read_parquet('{root}/season=*/trades/week=*/trades.parquet', hive_partitioning=true)
        GROUP BY season ORDER BY season
    """).show()

    print("=== candlesticks per season ===")
    con.sql(f"""
        SELECT season, count(*) AS candles
        FROM read_parquet('{root}/season=*/candlesticks/week=*/candles.parquet', hive_partitioning=true)
        GROUP BY season ORDER BY season
    """).show()

# --- Outside this sandbox, with normal internet access, use httpfs instead ---
# cfg = load_env()
# con = duckdb.connect()
# con.sql("INSTALL httpfs; LOAD httpfs;")
# con.sql(f"""
#     SET s3_endpoint='{cfg["SPACES_REGION"]}.digitaloceanspaces.com';
#     SET s3_region='{cfg["SPACES_REGION"]}';
#     SET s3_access_key_id='{cfg["SPACES_KEY"]}';
#     SET s3_secret_access_key='{cfg["SPACES_SECRET"]}';
#     SET s3_url_style='path';
# """)
# root = f's3://{cfg["SPACES_BUCKET"]}/kalshi/sport=cfb'
# # then query read_parquet(f'{root}/...') exactly as above -- no local cache needed
