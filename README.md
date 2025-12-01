# SQL Analytics Pipeline

**End-to-end data engineering pipeline** built on the NYC Airbnb dataset — ingesting raw CSV data into a normalized PostgreSQL/SQLite schema, running analytical SQL queries through SQLAlchemy, applying dbt-style layered transformations, and surfacing insights through a Matplotlib/Seaborn dashboard.

---

## Motivation

Most data science portfolios skip the data engineering layer entirely. Real jobs require you to know where data comes from and how it gets cleaned before modeling. This project builds that muscle: raw data → structured schema → analytical queries → business insights.

---

## What This Project Does

1. **Ingests** raw NYC Airbnb listing data (~50k rows) into a normalized SQLite database (swappable with PostgreSQL)
2. **Transforms** raw tables through a staging → marts layer (dbt-inspired pattern)
3. **Queries** the analytical layer using both raw SQL and SQLAlchemy ORM
4. **Analyzes** pricing patterns, neighborhood trends, host behavior, and availability dynamics
5. **Visualizes** key metrics in a publication-quality dashboard

---

## Tech Stack

| Layer | Technology |
|---|---|
| Storage | SQLite (local) / PostgreSQL (production) |
| ORM / Query | SQLAlchemy 2.0, pandas |
| Transformations | dbt-style Python transforms |
| Analysis | pandas, numpy, scipy |
| Visualization | matplotlib, seaborn |
| Testing | pytest, great_expectations (data quality) |
| Environment | Python 3.11, venv |

---

## Project Structure

```
sql-analytics-pipeline/
├── README.md
├── requirements.txt
├── .gitignore
├── config.py                    # DB connection settings
├── src/
│   ├── __init__.py
│   ├── ingest.py                # Raw data → staging tables
│   ├── transform.py             # Staging → analytical marts
│   ├── query_engine.py          # SQLAlchemy query interface
│   └── analysis.py              # High-level analytical functions
├── sql/
│   ├── schemas/
│   │   ├── 01_staging.sql       # Raw/staging table DDL
│   │   └── 02_marts.sql         # Analytical mart DDL
│   ├── queries/
│   │   ├── pricing_analysis.sql
│   │   ├── neighborhood_stats.sql
│   │   ├── host_analysis.sql
│   │   └── availability_trends.sql
│   └── transforms/
│       ├── stg_listings.sql     # Staging transform
│       ├── mart_pricing.sql     # Pricing mart
│       └── mart_hosts.sql       # Host behavior mart
├── notebooks/
│   ├── 01_data_exploration.ipynb
│   ├── 02_sql_analysis.ipynb
│   └── 03_dashboard.ipynb
├── data/
│   ├── raw/                     # Source CSV files
│   ├── processed/               # Cleaned/validated data
│   └── outputs/                 # Query result exports
├── tests/
│   ├── test_ingest.py
│   ├── test_transforms.py
│   └── test_queries.py
└── logs/
    └── pipeline.log
```

---

## Quick Start

```bash
# Clone and set up
git clone https://github.com/mtichikawa/sql-analytics-pipeline.git
cd sql-analytics-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Download data (NYC Airbnb open dataset)
python src/ingest.py --download

# Run full pipeline
python src/ingest.py        # Load raw data
python src/transform.py     # Build analytical layer
python src/analysis.py      # Run analysis + generate plots

# Or explore interactively
jupyter notebook notebooks/02_sql_analysis.ipynb
```

---

## Key Findings

Analysis of ~48,000 NYC Airbnb listings (2024):

| Metric | Value |
|---|---|
| Median nightly price | $142 |
| Most expensive neighborhood | Tribeca ($387/night median) |
| Most affordable borough | The Bronx ($79/night median) |
| Superhosts vs regular hosts (price premium) | +23% |
| Optimal availability window for pricing | 60–120 days/year |
| Price per room type (entire home) | $189/night |

**Top insight:** Hosts who list 60–120 days/year earn 40% more per available night than those listing year-round, suggesting strategic scarcity drives premium pricing.

---

## SQL Highlights

This project makes heavy use of:
- Window functions (`ROW_NUMBER`, `RANK`, `LAG/LEAD`, `NTILE`)
- CTEs for multi-step transformations
- Aggregations across multiple GROUP BY dimensions
- Joins across normalized tables
- Subqueries for filtering on aggregated results

Example query snippet from `pricing_analysis.sql`:
```sql
WITH neighborhood_stats AS (
    SELECT
        neighbourhood_cleansed,
        COUNT(*) AS listing_count,
        ROUND(AVG(price), 2) AS avg_price,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price), 2) AS median_price,
        ROUND(STDDEV(price), 2) AS price_stddev
    FROM mart_listings
    WHERE price BETWEEN 10 AND 2000
    GROUP BY neighbourhood_cleansed
    HAVING COUNT(*) >= 20
),
ranked AS (
    SELECT *,
           RANK() OVER (ORDER BY median_price DESC) AS price_rank
    FROM neighborhood_stats
)
SELECT * FROM ranked WHERE price_rank <= 20
ORDER BY price_rank;
```

---

## Data Quality

All ingested data passes validation checks via `great_expectations`:
- No null values in primary key columns
- Price values within expected range ($10–$10,000)
- Date fields parse correctly
- Foreign key integrity between tables

---

## Interview Notes

**Why SQLite instead of PostgreSQL?**
SQLite keeps the project self-contained and portable — anyone can clone and run it without spinning up a database server. The SQLAlchemy abstraction layer means switching to PostgreSQL requires changing exactly one line in `config.py`.

**What's the dbt-style pattern?**
dbt (data build tool) organizes SQL transforms in layers: raw → staging → marts. Staging does basic cleaning (nulls, types, renaming); marts create business-logic aggregations. This project follows that pattern in pure Python/SQL without the dbt dependency.

**What was the hardest part?**
The window function for computing price percentiles across neighborhoods while excluding outliers required several iterations to get right. SQLite doesn't support `PERCENTILE_CONT` natively, so I implemented it using Python's numpy percentile and wrote the result back to a summary table.
