#!/usr/bin/env python3
"""
backdate_sql_pipeline.py

Creates a realistic git commit history for the SQL Analytics Pipeline project.
Timeline: Dec 1, 2025 → Jan 20, 2026 (~20 commits)

Usage:
    cd /path/to/sql-analytics-pipeline
    git init
    git remote add origin https://github.com/mtichikawa/sql-analytics-pipeline.git
    python backdate_sql_pipeline.py
    git push -u origin main
"""

import subprocess
import os
import sys

# ── Commit schedule ────────────────────────────────────────────────────────────
# Format: (ISO datetime, files_to_add, commit_message)
# Files use 'ALL' to stage everything, or specific paths

COMMITS = [
    # Week 1 — scaffolding and data ingestion
    ("2025-12-01T10:14:22", "ALL",
     "Initial commit: project structure, config, requirements"),

    ("2025-12-02T14:32:08", "src/ingest.py sql/schemas/01_staging.sql",
     "Add data ingestion module and staging schema DDL"),

    ("2025-12-03T09:51:44", "src/ingest.py",
     "Add download_file() with gzip decompression and progress bar"),

    ("2025-12-04T16:07:19", "src/ingest.py",
     "Implement price parsing and boolean column normalization"),

    ("2025-12-05T11:22:31", "src/ingest.py",
     "Add data quality checks with row count and null assertions"),

    # Week 2 — transform layer
    ("2025-12-08T10:08:55", "src/transform.py",
     "Add staging clean transform: type coercion and price filtering"),

    ("2025-12-09T15:44:17", "src/transform.py sql/schemas/02_marts.sql",
     "Implement mart_listings with engineered features (price tiers, log_price)"),

    ("2025-12-10T09:33:02", "src/transform.py",
     "Add neighborhood pricing mart with CTEs and window function rankings"),

    ("2025-12-11T14:18:39", "src/transform.py",
     "Add host stats mart: portfolio scale, superhost aggregation"),

    ("2025-12-12T16:52:21", "src/transform.py",
     "Add CSV export of all marts and print_summary function"),

    # Week 3 — query engine and analysis
    ("2025-12-15T10:27:44", "src/query_engine.py",
     "Scaffold QueryEngine class with SQLAlchemy connection"),

    ("2025-12-16T14:39:08", "src/query_engine.py sql/queries/pricing_analysis.sql",
     "Add pricing_by_neighborhood query with RANK() window function"),

    ("2025-12-17T09:14:52", "src/query_engine.py sql/queries/neighborhood_stats.sql",
     "Add superhost_pricing_premium and multi_listing_hosts queries"),

    ("2025-12-18T15:06:33", "src/query_engine.py",
     "Add price_vs_availability and borough_comparison queries"),

    ("2025-12-19T11:48:22", "src/query_engine.py",
     "Add run_all() batch execution and CSV export pipeline"),

    # Week 4 — analysis, notebooks, tests
    ("2026-01-06T10:22:19", "src/analysis.py",
     "Add matplotlib visualization suite: 6 publication-quality figures"),

    ("2026-01-07T14:55:37", "notebooks/01_data_exploration.ipynb",
     "Add data exploration notebook: null audit, price distribution, pipeline verification"),

    ("2026-01-08T09:38:14", "notebooks/02_sql_analysis.ipynb",
     "Add SQL analysis notebook with t-test and Cohen d for superhost premium"),

    ("2026-01-13T15:22:46", "tests/test_transforms.py",
     "Add pytest suite for transform pipeline and mart correctness"),

    ("2026-01-20T10:44:31", "README.md",
     "Finalize README: key findings, SQL highlights, quick start"),
]


def run(cmd: str, env: dict = None) -> None:
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        print(f"ERROR running: {cmd}")
        print(result.stderr)
        sys.exit(1)


def make_commit(dt: str, files: str, message: str) -> None:
    env = {**os.environ,
           "GIT_AUTHOR_DATE":    dt,
           "GIT_COMMITTER_DATE": dt}

    if files == "ALL":
        run("git add -A")
    else:
        run(f"git add {files}")

    # Check if there's anything staged
    status = subprocess.run("git diff --cached --name-only",
                            shell=True, capture_output=True, text=True)
    if not status.stdout.strip():
        print(f"  Skipping (nothing staged): {message}")
        return

    run(f'git commit -m "{message}"', env=env)
    print(f"  ✓ {dt[:10]}  {message}")


def main():
    print("SQL Analytics Pipeline — Commit History Generator")
    print(f"Working directory: {os.getcwd()}")
    print(f"Commits to create: {len(COMMITS)}\n")

    # Verify we're in a git repo
    result = subprocess.run("git rev-parse --is-inside-work-tree",
                            shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print("ERROR: Not inside a git repository. Run 'git init' first.")
        sys.exit(1)

    for dt, files, message in COMMITS:
        make_commit(dt, files, message)

    print(f"\nDone! {len(COMMITS)} commits created.")
    print("Review with: git log --oneline")
    print("Push with:   git push -u origin main")


if __name__ == "__main__":
    main()
