"""
ingest.py — Raw data ingestion into staging database tables.

Pipeline stage 1: Download → validate → load into staging schema.

Usage:
    python src/ingest.py              # uses data already in data/raw/
    python src/ingest.py --download   # downloads fresh data first
"""

import argparse
import gzip
import io
import logging
import sys
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import (
    Column, Float, Integer, String, Text, Boolean, Date,
    create_engine, text, inspect
)
from sqlalchemy.orm import DeclarativeBase, Session
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent))
import config

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("ingest")


# ── ORM Base ───────────────────────────────────────────────────────────────────
class Base(DeclarativeBase):
    pass


class StagingListing(Base):
    """Raw listings table — minimal transformation, preserve source types."""
    __tablename__ = "stg_listings_raw"

    id                          = Column(Integer, primary_key=True)
    listing_url                 = Column(Text)
    name                        = Column(Text)
    description                 = Column(Text)
    neighbourhood_cleansed      = Column(String(100))
    neighbourhood_group_cleansed = Column(String(50))
    latitude                    = Column(Float)
    longitude                   = Column(Float)
    property_type               = Column(String(100))
    room_type                   = Column(String(50))
    accommodates                = Column(Integer)
    bathrooms                   = Column(Float)
    bedrooms                    = Column(Float)
    beds                        = Column(Float)
    price                       = Column(Float)
    minimum_nights              = Column(Integer)
    maximum_nights              = Column(Integer)
    number_of_reviews           = Column(Integer)
    number_of_reviews_ltm       = Column(Integer)
    review_scores_rating        = Column(Float)
    review_scores_cleanliness   = Column(Float)
    review_scores_location      = Column(Float)
    review_scores_value         = Column(Float)
    instant_bookable            = Column(Boolean)
    host_id                     = Column(Integer)
    host_name                   = Column(String(200))
    host_since                  = Column(String(20))
    host_is_superhost           = Column(Boolean)
    host_listings_count         = Column(Integer)
    host_total_listings_count   = Column(Integer)
    calculated_host_listings_count = Column(Integer)
    availability_30             = Column(Integer)
    availability_60             = Column(Integer)
    availability_90             = Column(Integer)
    availability_365            = Column(Integer)
    last_review                 = Column(String(20))
    reviews_per_month           = Column(Float)


# ── Download ───────────────────────────────────────────────────────────────────
def download_file(url: str, dest: Path, label: str = "file") -> Path:
    """Download a (possibly gzipped) file with progress bar."""
    if dest.exists():
        log.info(f"{label} already exists at {dest}, skipping download.")
        return dest

    log.info(f"Downloading {label} from {url}...")
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    raw_bytes = bytearray()

    with tqdm(total=total, unit="iB", unit_scale=True, desc=label) as bar:
        for chunk in resp.iter_content(chunk_size=8192):
            raw_bytes.extend(chunk)
            bar.update(len(chunk))

    # Decompress if gzipped
    if url.endswith(".gz"):
        log.info(f"Decompressing {label}...")
        raw_bytes = gzip.decompress(raw_bytes)

    dest.write_bytes(raw_bytes)
    log.info(f"Saved {label} to {dest} ({len(raw_bytes)/1e6:.1f} MB)")
    return dest


# ── Parse / Clean ──────────────────────────────────────────────────────────────
def parse_price(price_str) -> float | None:
    """Convert '$1,234.00' → 1234.0"""
    if pd.isna(price_str):
        return None
    try:
        cleaned = str(price_str).replace("$", "").replace(",", "").strip()
        return float(cleaned)
    except (ValueError, AttributeError):
        return None


def parse_bool(val) -> bool | None:
    if pd.isna(val):
        return None
    return str(val).strip().lower() in ("t", "true", "1", "yes")


COLUMN_MAP = {
    "id": "id",
    "listing_url": "listing_url",
    "name": "name",
    "description": "description",
    "neighbourhood_cleansed": "neighbourhood_cleansed",
    "neighbourhood_group_cleansed": "neighbourhood_group_cleansed",
    "latitude": "latitude",
    "longitude": "longitude",
    "property_type": "property_type",
    "room_type": "room_type",
    "accommodates": "accommodates",
    "bathrooms_text": "bathrooms",
    "bedrooms": "bedrooms",
    "beds": "beds",
    "price": "price",
    "minimum_nights": "minimum_nights",
    "maximum_nights": "maximum_nights",
    "number_of_reviews": "number_of_reviews",
    "number_of_reviews_ltm": "number_of_reviews_ltm",
    "review_scores_rating": "review_scores_rating",
    "review_scores_cleanliness": "review_scores_cleanliness",
    "review_scores_location": "review_scores_location",
    "review_scores_value": "review_scores_value",
    "instant_bookable": "instant_bookable",
    "host_id": "host_id",
    "host_name": "host_name",
    "host_since": "host_since",
    "host_is_superhost": "host_is_superhost",
    "host_listings_count": "host_listings_count",
    "host_total_listings_count": "host_total_listings_count",
    "calculated_host_listings_count": "calculated_host_listings_count",
    "availability_30": "availability_30",
    "availability_60": "availability_60",
    "availability_90": "availability_90",
    "availability_365": "availability_365",
    "last_review": "last_review",
    "reviews_per_month": "reviews_per_month",
}


def load_and_clean_listings(csv_path: Path) -> pd.DataFrame:
    """Load raw CSV, select and clean relevant columns."""
    log.info(f"Loading listings CSV from {csv_path}...")

    df = pd.read_csv(csv_path, low_memory=False)
    log.info(f"Raw shape: {df.shape}")

    # Select only columns we care about
    available = [c for c in COLUMN_MAP if c in df.columns]
    df = df[available].rename(columns=COLUMN_MAP)

    # Parse price
    df["price"] = df["price"].apply(parse_price)

    # Parse bathrooms (often "1 bath", "2 baths", etc.)
    if "bathrooms" in df.columns:
        df["bathrooms"] = (
            df["bathrooms"]
            .astype(str)
            .str.extract(r"(\d+\.?\d*)")
            [0]
            .astype(float)
        )

    # Parse booleans
    for col in ["instant_bookable", "host_is_superhost"]:
        if col in df.columns:
            df[col] = df[col].apply(parse_bool)

    # Numeric coerce
    for col in ["latitude", "longitude", "review_scores_rating",
                "review_scores_cleanliness", "review_scores_location",
                "review_scores_value", "reviews_per_month",
                "bedrooms", "beds", "host_listings_count",
                "host_total_listings_count", "calculated_host_listings_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    int_cols = ["id", "accommodates", "minimum_nights", "maximum_nights",
                "number_of_reviews", "number_of_reviews_ltm", "host_id",
                "availability_30", "availability_60", "availability_90",
                "availability_365"]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    log.info(f"Cleaned shape: {df.shape}")
    log.info(f"Price nulls: {df['price'].isna().sum()}")
    log.info(f"Price range: ${df['price'].min():.0f} – ${df['price'].max():.0f}")

    return df


# ── Write to DB ────────────────────────────────────────────────────────────────
def write_staging(df: pd.DataFrame, engine) -> None:
    """Write cleaned listings to staging table, replacing if exists."""
    log.info(f"Writing {len(df):,} rows to stg_listings_raw...")
    df.to_sql(
        "stg_listings_raw",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000,
    )
    log.info("Staging write complete.")


def run_data_quality_checks(engine) -> dict:
    """Basic data quality assertions. Returns dict of results."""
    results = {}
    with engine.connect() as conn:
        row = conn.execute(text("SELECT COUNT(*) FROM stg_listings_raw")).fetchone()
        results["total_rows"] = row[0]

        row = conn.execute(text(
            "SELECT COUNT(*) FROM stg_listings_raw WHERE id IS NULL"
        )).fetchone()
        results["null_ids"] = row[0]

        row = conn.execute(text(
            "SELECT COUNT(*) FROM stg_listings_raw WHERE price IS NULL OR price <= 0"
        )).fetchone()
        results["invalid_prices"] = row[0]

        row = conn.execute(text(
            "SELECT COUNT(DISTINCT neighbourhood_cleansed) FROM stg_listings_raw"
        )).fetchone()
        results["distinct_neighborhoods"] = row[0]

        row = conn.execute(text(
            "SELECT COUNT(DISTINCT host_id) FROM stg_listings_raw"
        )).fetchone()
        results["distinct_hosts"] = row[0]

    log.info("Data Quality Results:")
    for k, v in results.items():
        log.info(f"  {k}: {v:,}")

    assert results["null_ids"] == 0, "Found null IDs in staging!"
    assert results["total_rows"] > 10000, "Too few rows — check data download"
    log.info("All data quality checks passed.")
    return results


# ── Main ───────────────────────────────────────────────────────────────────────
def main(download: bool = False):
    engine = create_engine(config.DATABASE_URL, echo=False)
    Base.metadata.create_all(engine)

    if download:
        download_file(config.AIRBNB_DATA_URL, config.RAW_LISTINGS_PATH, "listings")

    if not config.RAW_LISTINGS_PATH.exists():
        log.error(
            f"Listings file not found at {config.RAW_LISTINGS_PATH}.\n"
            "Run with --download to fetch data, or place listings.csv in data/raw/"
        )
        sys.exit(1)

    df = load_and_clean_listings(config.RAW_LISTINGS_PATH)
    write_staging(df, engine)
    results = run_data_quality_checks(engine)

    # Save a summary
    summary_path = config.PROCESSED_DIR / "ingestion_summary.csv"
    pd.DataFrame([results]).to_csv(summary_path, index=False)
    log.info(f"Ingestion summary saved to {summary_path}")
    log.info("Ingestion complete. Run transform.py next.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest Airbnb data into staging DB")
    parser.add_argument("--download", action="store_true",
                        help="Download fresh data before ingestion")
    args = parser.parse_args()
    main(download=args.download)
