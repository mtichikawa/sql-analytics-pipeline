"""
transform.py — Staging → Analytical Marts transformation layer.

Implements a dbt-inspired two-layer pattern:
  Stage 1: stg_listings_raw → stg_listings_clean   (type coercion, null handling, filtering)
  Stage 2: stg_listings_clean → mart_listings       (business logic, feature engineering)
  Stage 3: mart_listings → mart_pricing_by_neighborhood, mart_host_stats (aggregations)

Usage:
    python src/transform.py
"""

import logging
import sys
from pathlib import Path

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))
import config

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("transform")


# ── Transform 1: Clean staging ─────────────────────────────────────────────────
CLEAN_STAGING_SQL = """
CREATE TABLE IF NOT EXISTS stg_listings_clean AS
SELECT
    id,
    listing_url,
    name,
    neighbourhood_cleansed,
    neighbourhood_group_cleansed AS borough,
    ROUND(latitude, 5)  AS latitude,
    ROUND(longitude, 5) AS longitude,
    property_type,
    room_type,
    CAST(accommodates AS INTEGER)   AS accommodates,
    CAST(bedrooms AS REAL)          AS bedrooms,
    CAST(beds AS REAL)              AS beds,
    CAST(bathrooms AS REAL)         AS bathrooms,
    ROUND(CAST(price AS REAL), 2)   AS price,
    CAST(minimum_nights AS INTEGER) AS minimum_nights,
    CAST(maximum_nights AS INTEGER) AS maximum_nights,
    CAST(number_of_reviews AS INTEGER)     AS number_of_reviews,
    CAST(number_of_reviews_ltm AS INTEGER) AS number_of_reviews_ltm,
    ROUND(CAST(review_scores_rating AS REAL), 2)     AS review_scores_rating,
    ROUND(CAST(review_scores_cleanliness AS REAL), 2) AS review_scores_cleanliness,
    ROUND(CAST(review_scores_location AS REAL), 2)   AS review_scores_location,
    ROUND(CAST(review_scores_value AS REAL), 2)      AS review_scores_value,
    CAST(instant_bookable AS INTEGER)   AS instant_bookable,
    CAST(host_id AS INTEGER)            AS host_id,
    host_name,
    host_since,
    CAST(host_is_superhost AS INTEGER)  AS host_is_superhost,
    CAST(host_listings_count AS INTEGER) AS host_listings_count,
    CAST(calculated_host_listings_count AS INTEGER) AS calculated_host_listings_count,
    CAST(availability_30 AS INTEGER)  AS availability_30,
    CAST(availability_60 AS INTEGER)  AS availability_60,
    CAST(availability_90 AS INTEGER)  AS availability_90,
    CAST(availability_365 AS INTEGER) AS availability_365,
    last_review,
    ROUND(CAST(reviews_per_month AS REAL), 2) AS reviews_per_month
FROM stg_listings_raw
WHERE
    id IS NOT NULL
    AND price IS NOT NULL
    AND price BETWEEN {price_min} AND {price_max}
    AND neighbourhood_cleansed IS NOT NULL
    AND room_type IS NOT NULL
""".format(price_min=config.PRICE_FILTER_MIN, price_max=config.PRICE_FILTER_MAX)


# ── Transform 2: Analytical mart with engineered features ─────────────────────
MART_LISTINGS_SQL = """
CREATE TABLE IF NOT EXISTS mart_listings AS
SELECT
    *,
    -- Price per guest
    ROUND(price / NULLIF(accommodates, 0), 2) AS price_per_guest,

    -- Listing size category
    CASE
        WHEN accommodates <= 2  THEN 'intimate'
        WHEN accommodates <= 4  THEN 'small'
        WHEN accommodates <= 8  THEN 'medium'
        ELSE 'large'
    END AS size_category,

    -- Price tier (quartile-based, computed in Python and back-filled)
    CASE
        WHEN price < 75  THEN 'budget'
        WHEN price < 125 THEN 'mid-range'
        WHEN price < 225 THEN 'premium'
        ELSE 'luxury'
    END AS price_tier,

    -- Review activity flag
    CASE
        WHEN number_of_reviews_ltm >= 10 THEN 'active'
        WHEN number_of_reviews_ltm >= 1  THEN 'occasional'
        ELSE 'inactive'
    END AS review_activity,

    -- Availability pattern
    CASE
        WHEN availability_365 < 30  THEN 'highly_restricted'
        WHEN availability_365 < 90  THEN 'restricted'
        WHEN availability_365 < 180 THEN 'moderate'
        WHEN availability_365 < 270 THEN 'available'
        ELSE 'always_available'
    END AS availability_pattern,

    -- Superhost flag as readable
    CASE WHEN host_is_superhost = 1 THEN 'superhost' ELSE 'regular' END AS host_type,

    -- Log price (useful for regression analysis)
    ROUND(LOG(price + 1), 4) AS log_price
FROM stg_listings_clean
"""


# ── Transform 3: Neighborhood pricing mart ────────────────────────────────────
MART_NEIGHBORHOOD_SQL = """
CREATE TABLE IF NOT EXISTS mart_pricing_by_neighborhood AS
WITH base AS (
    SELECT
        neighbourhood_cleansed AS neighborhood,
        borough,
        price,
        room_type,
        host_type,
        review_scores_rating,
        availability_365,
        number_of_reviews_ltm
    FROM mart_listings
),
stats AS (
    SELECT
        neighborhood,
        borough,
        COUNT(*)                         AS listing_count,
        ROUND(AVG(price), 2)             AS avg_price,
        ROUND(MIN(price), 2)             AS min_price,
        ROUND(MAX(price), 2)             AS max_price,
        ROUND(AVG(review_scores_rating), 3) AS avg_rating,
        ROUND(AVG(availability_365), 1)  AS avg_availability_365,
        SUM(CASE WHEN room_type = 'Entire home/apt' THEN 1 ELSE 0 END) AS entire_home_count,
        SUM(CASE WHEN room_type = 'Private room'    THEN 1 ELSE 0 END) AS private_room_count,
        SUM(CASE WHEN host_type = 'superhost'       THEN 1 ELSE 0 END) AS superhost_count,
        ROUND(
            100.0 * SUM(CASE WHEN host_type = 'superhost' THEN 1 ELSE 0 END) / COUNT(*),
        1) AS superhost_pct,
        ROUND(AVG(number_of_reviews_ltm), 2) AS avg_reviews_ltm
    FROM base
    GROUP BY neighborhood, borough
    HAVING COUNT(*) >= 20
)
SELECT
    *,
    ROUND(100.0 * entire_home_count / listing_count, 1) AS entire_home_pct,
    ROUND(100.0 * private_room_count / listing_count, 1) AS private_room_pct
FROM stats
ORDER BY avg_price DESC
"""


# ── Transform 4: Host behavior mart ──────────────────────────────────────────
MART_HOST_SQL = """
CREATE TABLE IF NOT EXISTS mart_host_stats AS
SELECT
    host_id,
    MAX(host_name)                          AS host_name,
    MAX(host_since)                         AS host_since,
    MAX(host_is_superhost)                  AS is_superhost,
    MAX(calculated_host_listings_count)     AS total_listings,
    COUNT(*)                                AS listings_in_dataset,
    ROUND(AVG(price), 2)                    AS avg_listing_price,
    ROUND(MIN(price), 2)                    AS min_listing_price,
    ROUND(MAX(price), 2)                    AS max_listing_price,
    SUM(number_of_reviews)                  AS total_reviews,
    ROUND(AVG(review_scores_rating), 3)     AS avg_rating,
    ROUND(AVG(availability_365), 1)         AS avg_availability,
    SUM(CASE WHEN room_type = 'Entire home/apt' THEN 1 ELSE 0 END) AS entire_home_listings,
    COUNT(DISTINCT neighbourhood_cleansed)  AS neighborhoods_count,
    MAX(last_review)                        AS most_recent_review
FROM mart_listings
GROUP BY host_id
HAVING COUNT(*) >= 1
ORDER BY total_listings DESC
"""


def drop_and_recreate(engine, table_name: str, create_sql: str) -> None:
    """Drop existing table and recreate from SQL statement."""
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        conn.execute(text(create_sql))
        conn.commit()

    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()
        log.info(f"  {table_name}: {row[0]:,} rows")


def run_transforms(engine) -> None:
    transforms = [
        ("stg_listings_clean",         CLEAN_STAGING_SQL),
        ("mart_listings",              MART_LISTINGS_SQL),
        ("mart_pricing_by_neighborhood", MART_NEIGHBORHOOD_SQL),
        ("mart_host_stats",            MART_HOST_SQL),
    ]

    for table_name, sql in transforms:
        log.info(f"Building {table_name}...")
        drop_and_recreate(engine, table_name, sql)

    log.info("All transforms complete.")


def export_marts(engine) -> None:
    """Export all mart tables to CSV for offline analysis / notebook use."""
    marts = ["mart_listings", "mart_pricing_by_neighborhood", "mart_host_stats"]

    for mart in marts:
        df = pd.read_sql(f"SELECT * FROM {mart}", con=engine)
        out_path = config.PROCESSED_DIR / f"{mart}.csv"
        df.to_csv(out_path, index=False)
        log.info(f"Exported {mart} → {out_path} ({len(df):,} rows)")


def print_summary(engine) -> None:
    """Print a quick summary of mart contents."""
    with engine.connect() as conn:
        print("\n" + "="*60)
        print("MART SUMMARY")
        print("="*60)

        row = conn.execute(text("SELECT COUNT(*), ROUND(AVG(price),2), ROUND(MIN(price),2), ROUND(MAX(price),2) FROM mart_listings")).fetchone()
        print(f"\nmart_listings")
        print(f"  Rows:         {row[0]:,}")
        print(f"  Price range:  ${row[2]} – ${row[3]}")
        print(f"  Avg price:    ${row[1]}")

        rows = conn.execute(text(
            "SELECT borough, COUNT(*), ROUND(AVG(price),2) FROM mart_listings GROUP BY borough ORDER BY AVG(price) DESC"
        )).fetchall()
        print(f"\nBy Borough:")
        for r in rows:
            print(f"  {str(r[0]):<20} {r[1]:>6,} listings   avg ${r[2]}")

        rows = conn.execute(text(
            "SELECT room_type, COUNT(*), ROUND(AVG(price),2) FROM mart_listings GROUP BY room_type ORDER BY AVG(price) DESC"
        )).fetchall()
        print(f"\nBy Room Type:")
        for r in rows:
            print(f"  {str(r[0]):<25} {r[1]:>6,} listings   avg ${r[2]}")

        row = conn.execute(text("SELECT COUNT(*) FROM mart_pricing_by_neighborhood")).fetchone()
        print(f"\nmart_pricing_by_neighborhood: {row[0]} neighborhoods")

        row = conn.execute(text("SELECT COUNT(*), SUM(CASE WHEN is_superhost=1 THEN 1 ELSE 0 END) FROM mart_host_stats")).fetchone()
        print(f"\nmart_host_stats: {row[0]:,} hosts ({row[1]:,} superhosts)")
        print("="*60 + "\n")


def main():
    engine = create_engine(config.DATABASE_URL, echo=False)

    # Check staging exists
    with engine.connect() as conn:
        try:
            conn.execute(text("SELECT COUNT(*) FROM stg_listings_raw")).fetchone()
        except Exception:
            log.error("stg_listings_raw not found. Run ingest.py first.")
            sys.exit(1)

    run_transforms(engine)
    export_marts(engine)
    print_summary(engine)
    log.info("Transform pipeline complete.")


if __name__ == "__main__":
    main()
