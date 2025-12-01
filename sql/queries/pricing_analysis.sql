-- sql/queries/pricing_analysis.sql
-- Comprehensive pricing analysis using window functions and CTEs

-- ── Query 1: Top neighborhoods by median price ────────────────────────────────
WITH neighborhood_stats AS (
    SELECT
        neighbourhood_cleansed                                          AS neighborhood,
        borough,
        COUNT(*)                                                        AS listing_count,
        ROUND(AVG(price), 2)                                           AS avg_price,
        ROUND(MIN(price), 2)                                           AS min_price,
        ROUND(MAX(price), 2)                                           AS max_price,
        ROUND(AVG(review_scores_rating), 3)                            AS avg_rating,
        ROUND(STDDEV(price), 2)                                        AS price_stddev
    FROM mart_listings
    WHERE price BETWEEN 10 AND 2000
    GROUP BY neighbourhood_cleansed, borough
    HAVING COUNT(*) >= 20
),
ranked AS (
    SELECT
        *,
        RANK() OVER (ORDER BY avg_price DESC)          AS overall_rank,
        RANK() OVER (PARTITION BY borough ORDER BY avg_price DESC) AS borough_rank
    FROM neighborhood_stats
)
SELECT * FROM ranked
ORDER BY overall_rank
LIMIT 30;


-- ── Query 2: Price percentiles by room type ───────────────────────────────────
SELECT
    room_type,
    COUNT(*)                                                            AS n,
    ROUND(AVG(price), 2)                                               AS mean,
    ROUND(MIN(price), 2)                                               AS p0,
    ROUND(AVG(CASE WHEN pct <= 0.25 THEN price END), 2)               AS p25,
    ROUND(AVG(CASE WHEN pct <= 0.50 THEN price END), 2)               AS p50,
    ROUND(AVG(CASE WHEN pct <= 0.75 THEN price END), 2)               AS p75,
    ROUND(MAX(price), 2)                                               AS p100
FROM (
    SELECT
        room_type,
        price,
        CUME_DIST() OVER (PARTITION BY room_type ORDER BY price)      AS pct
    FROM mart_listings
    WHERE price BETWEEN 10 AND 2000
) sub
GROUP BY room_type
ORDER BY mean DESC;


-- ── Query 3: Hosts with largest portfolios and their pricing premium ──────────
WITH host_portfolio AS (
    SELECT
        host_id,
        MAX(host_name)                                                  AS host_name,
        MAX(host_is_superhost)                                         AS is_superhost,
        COUNT(*)                                                       AS listing_count,
        ROUND(AVG(price), 2)                                          AS avg_price,
        ROUND(AVG(review_scores_rating), 3)                           AS avg_rating,
        COUNT(DISTINCT neighbourhood_cleansed)                        AS neighborhoods,
        MAX(calculated_host_listings_count)                           AS reported_listings
    FROM mart_listings
    GROUP BY host_id
),
market_avg AS (
    SELECT ROUND(AVG(price), 2) AS market_avg_price FROM mart_listings
)
SELECT
    hp.*,
    ma.market_avg_price,
    ROUND(100.0 * (hp.avg_price - ma.market_avg_price) / ma.market_avg_price, 1) AS pct_vs_market
FROM host_portfolio hp
CROSS JOIN market_avg ma
WHERE hp.listing_count >= 5
ORDER BY hp.listing_count DESC
LIMIT 50;


-- ── Query 4: Availability patterns and their impact on pricing ────────────────
SELECT
    CASE
        WHEN availability_365 < 30  THEN '1. <30 days (very rare)'
        WHEN availability_365 < 90  THEN '2. 30-90 days (restricted)'
        WHEN availability_365 < 180 THEN '3. 90-180 days (moderate)'
        WHEN availability_365 < 270 THEN '4. 180-270 days (available)'
        ELSE                             '5. >270 days (always on)'
    END                                                                 AS availability_bucket,
    COUNT(*)                                                            AS listing_count,
    ROUND(AVG(price), 2)                                               AS avg_price,
    ROUND(AVG(number_of_reviews_ltm), 2)                               AS avg_reviews_ltm,
    ROUND(AVG(review_scores_rating), 3)                                AS avg_rating,
    ROUND(
        100.0 * SUM(CASE WHEN host_is_superhost=1 THEN 1 ELSE 0 END) / COUNT(*),
    1) AS superhost_pct
FROM mart_listings
WHERE availability_365 IS NOT NULL
GROUP BY availability_bucket
ORDER BY availability_bucket;
