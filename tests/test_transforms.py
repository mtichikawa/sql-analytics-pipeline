"""
tests/test_transforms.py — Unit tests for the transformation layer.
"""

import sys
from pathlib import Path
import pytest
import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))
import config
from src.transform import run_transforms, export_marts


@pytest.fixture(scope="session")
def engine():
    """Create test engine using in-memory SQLite."""
    eng = create_engine("sqlite:///:memory:")
    # Create minimal staging data
    pd.DataFrame({
        "id": range(1, 101),
        "listing_url": [f"http://airbnb.com/{i}" for i in range(1, 101)],
        "name": [f"Listing {i}" for i in range(1, 101)],
        "description": ["Nice place"] * 100,
        "neighbourhood_cleansed": ["Williamsburg"] * 50 + ["Upper West Side"] * 50,
        "neighbourhood_group_cleansed": ["Brooklyn"] * 50 + ["Manhattan"] * 50,
        "latitude": [40.71 + i*0.001 for i in range(100)],
        "longitude": [-73.95 + i*0.001 for i in range(100)],
        "property_type": ["Entire rental unit"] * 100,
        "room_type": ["Entire home/apt"] * 60 + ["Private room"] * 40,
        "accommodates": [2, 4] * 50,
        "bathrooms": [1.0, 2.0] * 50,
        "bedrooms": [1.0, 2.0] * 50,
        "beds": [1.0, 2.0] * 50,
        "price": [100.0 + i*2 for i in range(100)],
        "minimum_nights": [2] * 100,
        "maximum_nights": [30] * 100,
        "number_of_reviews": [10 + i for i in range(100)],
        "number_of_reviews_ltm": [5 + i % 10 for i in range(100)],
        "review_scores_rating": [4.5 + (i % 5) * 0.1 for i in range(100)],
        "review_scores_cleanliness": [4.5] * 100,
        "review_scores_location": [4.7] * 100,
        "review_scores_value": [4.4] * 100,
        "instant_bookable": [1, 0] * 50,
        "host_id": [1000 + i % 20 for i in range(100)],
        "host_name": [f"Host{i % 20}" for i in range(100)],
        "host_since": ["2019-01-15"] * 100,
        "host_is_superhost": [1, 0] * 50,
        "host_listings_count": [1, 3, 5, 2, 1] * 20,
        "host_total_listings_count": [1, 3, 5, 2, 1] * 20,
        "calculated_host_listings_count": [1, 3, 5, 2, 1] * 20,
        "availability_30": [15] * 100,
        "availability_60": [30] * 100,
        "availability_90": [45] * 100,
        "availability_365": [120] * 100,
        "last_review": ["2024-08-01"] * 100,
        "reviews_per_month": [1.5] * 100,
    }).to_sql("stg_listings_raw", con=eng, if_exists="replace", index=False)
    return eng


def test_staging_has_data(engine):
    with engine.connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM stg_listings_raw")).fetchone()[0]
    assert n == 100


def test_transforms_run_without_error(engine):
    """All four transforms should complete without exception."""
    run_transforms(engine)


def test_clean_staging_filters_price(engine):
    with engine.connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM stg_listings_clean")).fetchone()[0]
    assert n > 0
    assert n <= 100


def test_mart_listings_has_engineered_features(engine):
    with engine.connect() as conn:
        cols = [row[1] for row in
                conn.execute(text("PRAGMA table_info(mart_listings)")).fetchall()]
    assert "price_tier" in cols
    assert "host_type" in cols
    assert "log_price" in cols
    assert "price_per_guest" in cols


def test_mart_no_null_ids(engine):
    with engine.connect() as conn:
        nulls = conn.execute(
            text("SELECT COUNT(*) FROM mart_listings WHERE id IS NULL")
        ).fetchone()[0]
    assert nulls == 0


def test_mart_pricing_neighborhood_aggregation(engine):
    with engine.connect() as conn:
        n = conn.execute(
            text("SELECT COUNT(*) FROM mart_pricing_by_neighborhood")
        ).fetchone()[0]
    assert n >= 1  # At least one neighborhood with ≥20 listings in test data


def test_mart_host_stats_correctness(engine):
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT * FROM mart_host_stats ORDER BY total_listings DESC LIMIT 5"),
            conn
        )
    assert len(df) > 0
    assert (df["total_listings"] >= 1).all()
    assert df["avg_listing_price"].between(0, 10000).all()
