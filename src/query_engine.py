"""
query_engine.py — Reusable analytical query interface using SQLAlchemy.

Provides a QueryEngine class wrapping common analytical patterns,
demonstrating both raw SQL and ORM-style querying.

Usage:
    from src.query_engine import QueryEngine
    qe = QueryEngine()
    df = qe.pricing_by_neighborhood(top_n=20)
"""

import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))
import config

log = logging.getLogger("query_engine")


class QueryEngine:
    """Analytical query interface over the mart layer."""

    def __init__(self):
        self.engine = create_engine(config.DATABASE_URL, echo=False)

    def _query(self, sql: str, params: dict = None) -> pd.DataFrame:
        with self.engine.connect() as conn:
            return pd.read_sql(text(sql), conn, params=params)

    # ── Pricing queries ────────────────────────────────────────────────────────

    def pricing_by_neighborhood(self, borough: Optional[str] = None,
                                top_n: int = 20) -> pd.DataFrame:
        """
        Top neighborhoods by median price.
        Uses a window-function approach to rank within borough.
        """
        where_clause = "WHERE borough = :borough" if borough else ""
        sql = f"""
        WITH ranked AS (
            SELECT
                neighborhood,
                borough,
                listing_count,
                avg_price,
                min_price,
                max_price,
                avg_rating,
                superhost_pct,
                entire_home_pct,
                RANK() OVER (
                    {'PARTITION BY borough' if not borough else ''}
                    ORDER BY avg_price DESC
                ) AS price_rank
            FROM mart_pricing_by_neighborhood
            {where_clause}
        )
        SELECT * FROM ranked
        WHERE price_rank <= :top_n
        ORDER BY {'borough, price_rank' if not borough else 'price_rank'}
        """
        params = {"top_n": top_n}
        if borough:
            params["borough"] = borough
        return self._query(sql, params)

    def price_distribution_by_room_type(self) -> pd.DataFrame:
        """Price percentiles broken down by room type."""
        sql = """
        SELECT
            room_type,
            COUNT(*)                                          AS listing_count,
            ROUND(AVG(price), 2)                              AS mean_price,
            ROUND(MIN(price), 2)                              AS p0_price,
            ROUND(AVG(CASE WHEN pct_rank <= 0.25 THEN price END), 2) AS p25_price,
            ROUND(AVG(CASE WHEN pct_rank <= 0.50 THEN price END), 2) AS p50_price,
            ROUND(AVG(CASE WHEN pct_rank <= 0.75 THEN price END), 2) AS p75_price,
            ROUND(MAX(price), 2)                              AS p100_price
        FROM (
            SELECT
                room_type,
                price,
                CUME_DIST() OVER (PARTITION BY room_type ORDER BY price) AS pct_rank
            FROM mart_listings
        ) sub
        GROUP BY room_type
        ORDER BY mean_price DESC
        """
        return self._query(sql)

    def price_vs_availability(self, min_listings: int = 50) -> pd.DataFrame:
        """
        Analyze the relationship between availability and price.
        Key insight: Is scarcity associated with higher prices?
        """
        sql = """
        SELECT
            availability_pattern,
            COUNT(*)                  AS listing_count,
            ROUND(AVG(price), 2)      AS avg_price,
            ROUND(AVG(review_scores_rating), 3) AS avg_rating,
            ROUND(AVG(number_of_reviews_ltm), 2) AS avg_reviews_ltm
        FROM mart_listings
        GROUP BY availability_pattern
        HAVING COUNT(*) >= :min_listings
        ORDER BY avg_price DESC
        """
        return self._query(sql, {"min_listings": min_listings})

    # ── Host queries ───────────────────────────────────────────────────────────

    def superhost_pricing_premium(self) -> pd.DataFrame:
        """Compare pricing between superhosts and regular hosts."""
        sql = """
        SELECT
            host_type,
            COUNT(*)                               AS listing_count,
            ROUND(AVG(price), 2)                   AS avg_price,
            ROUND(AVG(review_scores_rating), 3)    AS avg_rating,
            ROUND(AVG(number_of_reviews_ltm), 2)   AS avg_reviews_ltm,
            ROUND(AVG(availability_365), 1)        AS avg_availability,
            ROUND(
                100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
            1) AS pct_of_total
        FROM mart_listings
        GROUP BY host_type
        """
        return self._query(sql)

    def multi_listing_hosts(self, min_listings: int = 3) -> pd.DataFrame:
        """
        Hosts with multiple listings — potential commercial operators.
        Compares their pricing vs. single-listing hosts.
        """
        sql = """
        WITH host_scale AS (
            SELECT
                CASE
                    WHEN total_listings = 1  THEN '1 listing'
                    WHEN total_listings <= 3 THEN '2–3 listings'
                    WHEN total_listings <= 10 THEN '4–10 listings'
                    WHEN total_listings <= 50 THEN '11–50 listings'
                    ELSE '50+ listings'
                END AS host_scale,
                total_listings,
                avg_listing_price,
                avg_rating,
                is_superhost
            FROM mart_host_stats
        )
        SELECT
            host_scale,
            COUNT(*)                            AS host_count,
            ROUND(AVG(avg_listing_price), 2)    AS avg_price,
            ROUND(AVG(avg_rating), 3)           AS avg_rating,
            ROUND(100.0 * SUM(is_superhost) / COUNT(*), 1) AS superhost_pct
        FROM host_scale
        GROUP BY host_scale
        ORDER BY MIN(total_listings)
        """
        return self._query(sql)

    # ── Geographic queries ─────────────────────────────────────────────────────

    def borough_comparison(self) -> pd.DataFrame:
        """Full borough-level comparison across all metrics."""
        sql = """
        SELECT
            borough,
            COUNT(*)                                AS total_listings,
            ROUND(AVG(price), 2)                    AS avg_price,
            ROUND(AVG(review_scores_rating), 3)     AS avg_rating,
            ROUND(AVG(availability_365), 1)         AS avg_availability,
            SUM(CASE WHEN host_type='superhost' THEN 1 ELSE 0 END) AS superhost_count,
            ROUND(
                100.0 * SUM(CASE WHEN host_type='superhost' THEN 1 ELSE 0 END) / COUNT(*),
            1) AS superhost_pct,
            ROUND(
                100.0 * SUM(CASE WHEN room_type='Entire home/apt' THEN 1 ELSE 0 END) / COUNT(*),
            1) AS entire_home_pct
        FROM mart_listings
        WHERE borough IS NOT NULL
        GROUP BY borough
        ORDER BY avg_price DESC
        """
        return self._query(sql)

    def price_by_size_and_room_type(self) -> pd.DataFrame:
        """Price matrix: accommodation size × room type."""
        sql = """
        SELECT
            size_category,
            room_type,
            COUNT(*)               AS listing_count,
            ROUND(AVG(price), 2)   AS avg_price,
            ROUND(AVG(price_per_guest), 2) AS avg_price_per_guest
        FROM mart_listings
        GROUP BY size_category, room_type
        HAVING COUNT(*) >= 20
        ORDER BY
            CASE size_category
                WHEN 'intimate' THEN 1 WHEN 'small' THEN 2
                WHEN 'medium' THEN 3   ELSE 4 END,
            avg_price DESC
        """
        return self._query(sql)

    # ── Trend / time queries ────────────────────────────────────────────────────

    def review_activity_by_neighborhood(self, top_n: int = 15) -> pd.DataFrame:
        """Most actively-reviewed neighborhoods — proxy for booking demand."""
        sql = """
        SELECT
            neighbourhood_cleansed  AS neighborhood,
            borough,
            COUNT(*)                AS listing_count,
            ROUND(AVG(number_of_reviews_ltm), 2) AS avg_reviews_ltm,
            ROUND(AVG(price), 2)    AS avg_price,
            ROUND(AVG(review_scores_rating), 3)  AS avg_rating
        FROM mart_listings
        WHERE number_of_reviews_ltm IS NOT NULL
        GROUP BY neighbourhood_cleansed, borough
        HAVING COUNT(*) >= 10
        ORDER BY avg_reviews_ltm DESC
        LIMIT :top_n
        """
        return self._query(sql, {"top_n": top_n})

    # top_n=30 used here (vs default 20) to provide broader neighbourhood coverage in reports
def run_all(self) -> dict:
        """Run all analytical queries and return as dict of DataFrames."""
        log.info("Running full analytical query suite...")
        results = {
            "pricing_by_neighborhood":       self.pricing_by_neighborhood(top_n=30),
            "price_distribution_by_room":    self.price_distribution_by_room_type(),
            "price_vs_availability":         self.price_vs_availability(),
            "superhost_premium":             self.superhost_pricing_premium(),
            "multi_listing_hosts":           self.multi_listing_hosts(),
            "borough_comparison":            self.borough_comparison(),
            "price_size_matrix":             self.price_by_size_and_room_type(),
            "review_activity":               self.review_activity_by_neighborhood(),
        }
        for name, df in results.items():
            out = config.OUTPUTS_DIR / f"{name}.csv"
            df.to_csv(out, index=False)
            log.info(f"  {name}: {len(df)} rows → {out}")
        return results


if __name__ == "__main__":
    qe = QueryEngine()
    results = qe.run_all()
    print("\nQuery results exported to data/outputs/")
