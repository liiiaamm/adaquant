# -*- coding: utf-8 -*-
"""
pipeline_status.py
ADAQuant — S3 Pipeline Status Report Generator.

Connects to the adaquant-data-bucket S3 bucket and inspects the Bronze,
Silver, and Gold layers, then writes PIPELINE_STATUS.md at the repo root
with per-layer metrics, a global summary, and a data-coverage gap analysis.

Run standalone:
    python scripts/pipeline_status.py
"""

import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

# =============================================================================
# CONFIGURATION
# =============================================================================
AWS_REGION = "eu-south-2"
S3_BUCKET  = "adaquant-data-bucket"
COIN       = "ada"

LAYERS = {
    "Bronze": f"bronze/{COIN}/",
    "Silver": f"silver/{COIN}/",
    "Gold":   f"gold/{COIN}/",
}

REPO_ROOT      = Path(__file__).resolve().parent.parent
OUTPUT_MD_PATH = REPO_ROOT / "PIPELINE_STATUS.md"

# Matches partition keys embedded in S3 keys:
#   bronze/ada/year=2024/month=01/adausdt_daily_2024_01.csv
YEAR_MONTH_RE = re.compile(r"year=(\d{4})/month=(\d{2})")


# =============================================================================
# STEP 1 — Verify AWS credentials
# =============================================================================
def check_aws_credentials() -> boto3.Session:
    print("[1/5] Checking AWS credentials...")
    try:
        session = boto3.Session()
        sts = session.client("sts", region_name=AWS_REGION)
        identity = sts.get_caller_identity()
        print(f"      Connected as: {identity['Arn']}")
        return session
    except (NoCredentialsError, PartialCredentialsError):
        print("\n  ERROR: AWS credentials not found or incomplete.")
        print("  Run: aws configure")
        print("  Region: eu-south-2  |  Output: json")
        sys.exit(1)
    except ClientError as exc:
        print(f"\n  ERROR connecting to AWS: {exc}")
        sys.exit(1)


# =============================================================================
# STEP 2 — List all S3 objects under a prefix
# =============================================================================
def _list_layer_objects(s3_client, prefix: str) -> list[dict]:
    """
    Returns every S3 object under *prefix* as a list of dicts with
    keys: Key, Size, LastModified.  Uses a paginator so there is no
    1 000-object cap.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    objects: list[dict] = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects.append(
                {
                    "Key":          obj["Key"],
                    "Size":         obj["Size"],          # bytes
                    "LastModified": obj["LastModified"],  # timezone-aware datetime
                }
            )
    return objects


# =============================================================================
# STEP 3 — Derive layer metrics from a list of S3 objects
# =============================================================================
def _compute_layer_metrics(objects: list[dict]) -> dict:
    """
    Returns a dict with:
      total_files     – int
      total_size_mb   – float (rounded to 3 dp)
      most_recent     – filename of the most recently modified object
      most_recent_ts  – human-readable last-modified timestamp (UTC)
      earliest        – "YYYY-MM" string of the oldest partition found
      latest          – "YYYY-MM" string of the newest partition found
      year_months     – set of (year_str, month_str) tuples
    """
    if not objects:
        return {
            "total_files":    0,
            "total_size_mb":  0.0,
            "most_recent":    "—",
            "most_recent_ts": "—",
            "earliest":       "—",
            "latest":         "—",
            "year_months":    set(),
        }

    total_files   = len(objects)
    total_size_mb = sum(o["Size"] for o in objects) / (1024 * 1024)

    # Most recently modified object
    newest_obj     = max(objects, key=lambda o: o["LastModified"])
    most_recent    = newest_obj["Key"].split("/")[-1]
    most_recent_ts = newest_obj["LastModified"].strftime("%Y-%m-%d %H:%M UTC")

    # Collect year/month partition pairs from the key structure
    year_months: set[tuple[str, str]] = set()
    for obj in objects:
        match = YEAR_MONTH_RE.search(obj["Key"])
        if match:
            year_months.add((match.group(1), match.group(2)))

    sorted_ym = sorted(year_months)
    earliest  = f"{sorted_ym[0][0]}-{sorted_ym[0][1]}"   if sorted_ym else "—"
    latest    = f"{sorted_ym[-1][0]}-{sorted_ym[-1][1]}"  if sorted_ym else "—"

    return {
        "total_files":    total_files,
        "total_size_mb":  round(total_size_mb, 3),
        "most_recent":    most_recent,
        "most_recent_ts": most_recent_ts,
        "earliest":       earliest,
        "latest":         latest,
        "year_months":    year_months,
    }


# =============================================================================
# STEP 4 — Scan all layers
# =============================================================================
def collect_all_layers(session: boto3.Session) -> dict[str, dict]:
    print("[2/5] Scanning S3 layers...")
    s3      = session.client("s3", region_name=AWS_REGION)
    results = {}
    for layer_name, prefix in LAYERS.items():
        print(f"      {layer_name:6s}  s3://{S3_BUCKET}/{prefix}")
        objects              = _list_layer_objects(s3, prefix)
        results[layer_name]  = _compute_layer_metrics(objects)
        m = results[layer_name]
        print(f"             {m['total_files']} files  |  {m['total_size_mb']:.3f} MB")
    return results


# =============================================================================
# STEP 5 — Build PIPELINE_STATUS.md content
# =============================================================================
def _build_markdown(metrics: dict[str, dict]) -> str:
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines: list[str] = [
        "# ADAQuant — Pipeline Status",
        "",
        f"> Auto-generated on **{now_utc}** by `scripts/pipeline_status.py`",
        "",
        "---",
        "",
    ]

    # ── Per-layer tables ──────────────────────────────────────────────────────
    print("[3/5] Building per-layer tables...")
    for layer_name, m in metrics.items():
        lines += [
            f"## {layer_name} Layer",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| Total files | {m['total_files']} |",
            f"| Total size | {m['total_size_mb']:.3f} MB |",
            f"| Most recent file | `{m['most_recent']}` |",
            f"| Most recent file date | {m['most_recent_ts']} |",
            f"| Earliest period | {m['earliest']} |",
            f"| Latest period | {m['latest']} |",
            "",
        ]

    # ── Global summary ────────────────────────────────────────────────────────
    print("[4/5] Building global summary...")
    total_files_all = sum(m["total_files"]   for m in metrics.values())
    total_size_all  = sum(m["total_size_mb"] for m in metrics.values())

    # Last pipeline run = latest timestamp seen across all layers
    candidate_dates = [
        m["most_recent_ts"]
        for m in metrics.values()
        if m["most_recent_ts"] != "—"
    ]
    last_pipeline_run = max(candidate_dates) if candidate_dates else "—"

    lines += [
        "---",
        "",
        "## Global Summary",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Total files (all layers) | {total_files_all} |",
        f"| Total storage used | {total_size_all:.3f} MB |",
        f"| Last pipeline run | {last_pipeline_run} |",
        f"| S3 bucket | `{S3_BUCKET}` |",
        f"| AWS region | `{AWS_REGION}` |",
        "",
    ]

    # ── Data coverage / gap analysis ─────────────────────────────────────────
    print("[5/5] Building data coverage / gap analysis...")
    bronze_ym = metrics.get("Bronze", {}).get("year_months", set())
    silver_ym = metrics.get("Silver", {}).get("year_months", set())
    gold_ym   = metrics.get("Gold",   {}).get("year_months", set())

    all_ym = sorted(bronze_ym | silver_ym | gold_ym)

    lines += [
        "---",
        "",
        "## Data Coverage",
        "",
        (
            "Year-month partitions present across layers. "
            "A **GAP** in the last column means the period exists in Bronze "
            "but has not yet been promoted all the way to the Gold layer."
        ),
        "",
        "| Period | Bronze | Silver | Gold | Gap? |",
        "|--------|:------:|:------:|:----:|------|",
    ]

    for year, month in all_ym:
        ym       = (year, month)
        b_mark   = "✓" if ym in bronze_ym else "✗"
        s_mark   = "✓" if ym in silver_ym else "✗"
        g_mark   = "✓" if ym in gold_ym   else "✗"
        gap_flag = "**GAP**" if (ym in bronze_ym and ym not in gold_ym) else ""
        lines.append(f"| {year}-{month} | {b_mark} | {s_mark} | {g_mark} | {gap_flag} |")

    if not all_ym:
        lines.append("| — | — | — | — | No data found in any layer |")

    lines += [
        "",
        "---",
        "",
        f"*Status last generated: {now_utc}*",
        "",
    ]

    return "\n".join(lines)


def write_markdown(content: str) -> None:
    OUTPUT_MD_PATH.write_text(content, encoding="utf-8")
    print(f"\n  Report written to: {OUTPUT_MD_PATH}")


# =============================================================================
# MAIN
# =============================================================================
def main() -> None:
    print("=" * 60)
    print("  ADAQuant — Pipeline Status Report")
    print(f"  Bucket : {S3_BUCKET}")
    print(f"  Region : {AWS_REGION}")
    print("=" * 60)

    session = check_aws_credentials()
    metrics = collect_all_layers(session)
    md      = _build_markdown(metrics)
    write_markdown(md)

    print("\nDone.")


if __name__ == "__main__":
    main()
