"""
Sync the locally-exported Parquet tree (see spaces_export.py) up to the
DigitalOcean Spaces bucket, which is the system of record for the full
historical dataset.

Spaces is S3-compatible, so this is plain boto3 pointed at a DO endpoint.

Credentials are read from the environment (or a local .env, which is
gitignored) and are never logged, printed, or committed:

    SPACES_KEY=...            # Spaces access key
    SPACES_SECRET=...         # Spaces secret key
    SPACES_REGION=nyc3        # region slug, e.g. nyc3 / sfo3 / ams3
    SPACES_BUCKET=degenerate-cafe

Usage:
  python3 spaces_sync.py --check                 # verify credentials/bucket reachable
  python3 spaces_sync.py                         # upload everything new or changed
  python3 spaces_sync.py --prefix kalshi/trades  # upload just one subtree
  python3 spaces_sync.py --dry-run               # list what would upload

Uploads skip objects whose size already matches the local file, so a
re-run after a partial sync only sends what's missing.
"""
import argparse
import io
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Matches spaces_export.py's DEFAULT_OUT -- the local tree it stages before upload.
DEFAULT_LOCAL_ROOT = os.path.join(SCRIPT_DIR, "staging")


def _parse_env_file(path):
    """Minimal KEY=VALUE parser so this works without python-dotenv installed.
    Secrets routinely contain '=' and '/', so split on the first '=' only and
    don't try to be clever beyond stripping optional surrounding quotes."""
    values = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            val = val.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
                val = val[1:-1]
            if val:
                values[key.strip()] = val
    return values


def _find_repo_root(start):
    """Walk up from `start` looking for a .git directory. Bounded so a script
    run from somewhere unexpected can't wander off and read an unrelated .env."""
    d = start
    for _ in range(6):
        if os.path.isdir(os.path.join(d, ".git")):
            return d
        parent = os.path.dirname(d)
        if parent == d:
            return None
        d = parent
    return None


def load_env():
    """Read .env (repo root, or alongside this script) without overriding real
    env vars. Checked by repo root first, script dir second, so a script-local
    .env can override one at the root if both happen to exist."""
    values = {}
    repo_root = _find_repo_root(SCRIPT_DIR)
    candidates = ([os.path.join(repo_root, ".env")] if repo_root else []) + \
                 [os.path.join(SCRIPT_DIR, ".env")]
    for candidate in candidates:
        if os.path.exists(candidate):
            values.update(_parse_env_file(candidate))
    cfg = {}
    for key in ("SPACES_KEY", "SPACES_SECRET", "SPACES_REGION", "SPACES_BUCKET"):
        cfg[key] = os.environ.get(key) or values.get(key)
    return cfg


def make_client(cfg):
    import boto3

    missing = [k for k, v in cfg.items() if not v]
    if missing:
        print(f"Missing credentials: {', '.join(missing)}.\n"
              f"Set them in the environment or a .env file (see this module's docstring). "
              f"Never commit them -- .env is gitignored.", file=sys.stderr)
        sys.exit(1)

    region = cfg["SPACES_REGION"]
    return boto3.client(
        "s3",
        region_name=region,
        endpoint_url=f"https://{region}.digitaloceanspaces.com",
        aws_access_key_id=cfg["SPACES_KEY"],
        aws_secret_access_key=cfg["SPACES_SECRET"],
    )


def check(cfg):
    client = make_client(cfg)
    bucket = cfg["SPACES_BUCKET"]
    client.head_bucket(Bucket=bucket)
    resp = client.list_objects_v2(Bucket=bucket, Prefix="kalshi/", MaxKeys=5)
    n = resp.get("KeyCount", 0)
    print(f"Connected to bucket '{bucket}' ({cfg['SPACES_REGION']}). "
          f"{'No objects under kalshi/ yet.' if n == 0 else f'{n}+ objects already under kalshi/.'}")


# events.parquet and markets.parquet are one file per season (unlike trades/
# candlesticks/orderbooks, which are partitioned per week) -- keyed by their
# primary key so a merge can be found and applied before upload.
CATALOG_MERGE_KEYS = {"events.parquet": "event_ticker", "markets.parquet": "market_ticker"}


def _merge_catalog_file(client, bucket, key, local_path):
    """A plain overwrite-on-upload is wrong for events.parquet/markets.parquet:
    whoever runs the export only has whatever's in their local SQLite cache,
    which is frequently a subset of the season (e.g. a daily job's fresh,
    single-day local db). Uploading that verbatim would blow away every other
    day's rows already in the bucket. Download what's there first and merge
    by primary key -- local rows win on a conflicting key, since they're the
    fresher pull -- before this key ever reaches the upload step."""
    pk = CATALOG_MERGE_KEYS.get(os.path.basename(key))
    if pk is None:
        return
    import pyarrow as pa
    import pyarrow.parquet as pq
    from botocore.exceptions import ClientError

    try:
        resp = client.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return
        raise
    remote_table = pq.read_table(io.BytesIO(resp["Body"].read()))
    local_table = pq.read_table(local_path)
    if remote_table.num_rows == 0:
        return

    by_key = {}
    for row in remote_table.to_pylist():
        by_key[row[pk]] = row
    for row in local_table.to_pylist():  # local rows applied last, so they win
        by_key[row[pk]] = row
    merged = pa.Table.from_pylist(list(by_key.values()), schema=local_table.schema)
    pq.write_table(merged, local_path, compression="snappy")


def iter_local_files(local_root, prefix):
    for dirpath, _, filenames in os.walk(local_root):
        for fn in filenames:
            full = os.path.join(dirpath, fn)
            key = os.path.relpath(full, local_root).replace(os.sep, "/")
            if prefix and not key.startswith(prefix):
                continue
            yield full, key


def sync(cfg, local_root, prefix, dry_run):
    client = make_client(cfg)
    bucket = cfg["SPACES_BUCKET"]

    existing = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix or "kalshi/"):
        for obj in page.get("Contents", []):
            existing[obj["Key"]] = obj["Size"]

    uploaded = skipped = 0
    total_bytes = 0
    for full, key in iter_local_files(local_root, prefix):
        _merge_catalog_file(client, bucket, key, full)
        size = os.path.getsize(full)
        if existing.get(key) == size:
            skipped += 1
            continue
        if dry_run:
            print(f"  would upload {key} ({size:,} bytes)")
        else:
            client.upload_file(full, bucket, key)
        uploaded += 1
        total_bytes += size

    verb = "Would upload" if dry_run else "Uploaded"
    print(f"{verb} {uploaded:,} files ({total_bytes / 1e6:.1f} MB); {skipped:,} already current.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--local-root", default=DEFAULT_LOCAL_ROOT)
    parser.add_argument("--prefix", default="", help="Only sync keys under this prefix, e.g. kalshi/trades")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--check", action="store_true", help="Just verify credentials and bucket access")
    args = parser.parse_args()

    cfg = load_env()
    if args.check:
        check(cfg)
        return
    if not os.path.isdir(args.local_root):
        print(f"Nothing to sync: {args.local_root} does not exist. "
              f"Run spaces_export.py first.", file=sys.stderr)
        sys.exit(1)
    sync(cfg, args.local_root, args.prefix, args.dry_run)


if __name__ == "__main__":
    main()
