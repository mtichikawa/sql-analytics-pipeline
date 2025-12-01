"""
config.py — Database and pipeline configuration.

Defaults to SQLite for portability.
Set USE_POSTGRES=True and fill in POSTGRES_* vars to use PostgreSQL.
"""

import os
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT_DIR    = Path(__file__).parent
DATA_DIR    = ROOT_DIR / "data"
RAW_DIR     = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUTS_DIR = DATA_DIR / "outputs"
LOG_DIR     = ROOT_DIR / "logs"

for d in [RAW_DIR, PROCESSED_DIR, OUTPUTS_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Database ───────────────────────────────────────────────────────────────────
USE_POSTGRES = os.getenv("USE_POSTGRES", "false").lower() == "true"

if USE_POSTGRES:
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB   = os.getenv("POSTGRES_DB",   "airbnb_analytics")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASS = os.getenv("POSTGRES_PASS", "")
    DATABASE_URL  = (
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASS}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
else:
    SQLITE_PATH  = DATA_DIR / "airbnb_analytics.db"
    DATABASE_URL = f"sqlite:///{SQLITE_PATH}"

# ── Data Source ────────────────────────────────────────────────────────────────
# NYC Airbnb open data — Inside Airbnb (publicly available)
AIRBNB_DATA_URL = "https://data.insideairbnb.com/united-states/ny/new-york-city/2024-09-04/data/listings.csv.gz"
AIRBNB_CALENDAR_URL = "https://data.insideairbnb.com/united-states/ny/new-york-city/2024-09-04/data/calendar.csv.gz"
RAW_LISTINGS_PATH  = RAW_DIR / "listings.csv"
RAW_CALENDAR_PATH  = RAW_DIR / "calendar.csv"

# ── Pipeline settings ──────────────────────────────────────────────────────────
PRICE_FILTER_MIN = 10
PRICE_FILTER_MAX = 2000
MIN_REVIEWS      = 0   # include all listings
MIN_NEIGHBORHOOD_LISTINGS = 20   # for neighborhood stats

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_LEVEL = "INFO"
LOG_FILE  = LOG_DIR / "pipeline.log"
