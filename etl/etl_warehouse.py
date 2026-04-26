"""
ETL Pipeline 3: MySQL + MongoDB → PostgreSQL Star Schema
Builds dimension tables, merges stock and news data into the fact table,
and computes derived metrics (30-day moving average, 30-day volatility).
"""

import mysql.connector
import psycopg2
from pymongo import MongoClient
import pandas as pd
from collections import defaultdict


def get_mysql_connection():
    return mysql.connector.connect(
        host="localhost",
        port=3306,
        user="root",
        password="root",
        database="financial_market",
    )


def get_postgres_connection():
    return psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="financial_warehouse",
        user="postgres",
        password="postgres",
    )


def get_mongo_collection():
    client = MongoClient("mongodb://localhost:27017")
    db = client["financial_market"]
    return db["scraped_news"]


def build_dim_date(pg_cursor):
    """Generate dim_date from full date range."""
    print("Building dim_date...")
    from datetime import date
    dates = pd.date_range("2021-01-01", date.today())
    for d in dates:
        pg_cursor.execute(
            """
            INSERT INTO dim_date (full_date, year, quarter, month, day_of_week)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (full_date) DO NOTHING
            """,
            (d.date(), d.year, f"Q{d.quarter}", d.month, d.day_name()),
        )
    print(f"  {len(dates)} dates processed.")


def build_dim_asset(pg_cursor, mysql_cursor):
    """Copy asset metadata from MySQL into dim_asset."""
    print("Building dim_asset...")
    mysql_cursor.execute("SELECT ticker, name, sector, asset_type FROM assets")
    rows = mysql_cursor.fetchall()
    for ticker, name, sector, asset_type in rows:
        pg_cursor.execute(
            """
            INSERT INTO dim_asset (ticker, company_name, sector, asset_type)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ticker) DO NOTHING
            """,
            (ticker, name, sector, asset_type),
        )
    print(f"  {len(rows)} assets processed.")


def build_dim_sector(pg_cursor, mysql_cursor):
    """Build dim_sector from distinct sectors in MySQL."""
    print("Building dim_sector...")
    mysql_cursor.execute("SELECT DISTINCT sector FROM assets WHERE sector IS NOT NULL")
    sectors = mysql_cursor.fetchall()
    for (sector,) in sectors:
        pg_cursor.execute(
            """
            INSERT INTO dim_sector (sector_name)
            VALUES (%s)
            ON CONFLICT (sector_name) DO NOTHING
            """,
            (sector,),
        )
    print(f"  {len(sectors)} sectors processed.")


def load_dimension_keys(pg_cursor):
    """Pre-load dimension key lookups into dicts to avoid per-row SELECTs."""
    pg_cursor.execute("SELECT full_date, date_key FROM dim_date")
    date_keys = {row[0]: row[1] for row in pg_cursor.fetchall()}

    pg_cursor.execute("SELECT ticker, asset_key FROM dim_asset")
    asset_keys = {row[0]: row[1] for row in pg_cursor.fetchall()}

    return date_keys, asset_keys


def load_news_counts(mongo_collection):
    """
    Batch-load news counts from MongoDB into a dict keyed by (ticker, date_str).
    Much faster than per-row count_documents with $regex.
    """
    print("  Pre-loading news counts from MongoDB...")
    news_counts = defaultdict(int)
    for doc in mongo_collection.find({}, {"tickers": 1, "date": 1}):
        date_str = (doc.get("date") or "")[:10]  # Extract YYYY-MM-DD
        for ticker in doc.get("tickers", []):
            news_counts[(ticker, date_str)] += 1
    print(f"  {len(news_counts)} ticker-date news counts loaded.")
    return news_counts


def build_fact_table(pg_cursor, mysql_cursor, news_counts, date_keys, asset_keys):
    """Merge MySQL prices and MongoDB news counts into fact_market_data."""
    print("Building fact_market_data...")

    # Ensure the unique constraint exists (init SQL only runs on first container creation)
    pg_cursor.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'uq_fact_date_asset'
            ) THEN
                -- Remove duplicate rows, keeping the latest (highest id) per (date_key, asset_key)
                DELETE FROM fact_market_data
                WHERE id NOT IN (
                    SELECT MAX(id)
                    FROM fact_market_data
                    GROUP BY date_key, asset_key
                );
                ALTER TABLE fact_market_data
                ADD CONSTRAINT uq_fact_date_asset UNIQUE (date_key, asset_key);
            END IF;
        END $$;
    """)

    mysql_cursor.execute(
        "SELECT ticker, date, open, high, low, close, volume FROM daily_prices"
    )
    rows = mysql_cursor.fetchall()
    attempted = 0
    inserted = 0

    for ticker, date_val, open_, high, low, close, volume in rows:
        date_key = date_keys.get(date_val)
        asset_key = asset_keys.get(ticker)
        if not date_key or not asset_key:
            continue

        daily_return = ((float(close) - float(open_)) / float(open_)) * 100 if float(open_) != 0 else 0
        news_count = news_counts.get((ticker, str(date_val)), 0)

        pg_cursor.execute(
            """
            INSERT INTO fact_market_data
            (date_key, asset_key, open, high, low, close, volume, daily_return, news_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_key, asset_key) DO UPDATE
                SET news_count = EXCLUDED.news_count
            """,
            (date_key, asset_key, open_, high, low, close, volume, daily_return, news_count),
        )
        inserted += pg_cursor.rowcount
        attempted += 1

        if attempted % 5000 == 0:
            print(f"  {attempted} rows processed...")

    print(f"  {inserted} rows inserted, {attempted - inserted} skipped (already existed).")


def compute_window_metrics(pg_cursor):
    """Compute 30-day moving average and volatility using window functions."""
    print("Computing 30-day moving average and volatility...")
    pg_cursor.execute("""
        WITH windowed AS (
            SELECT f.id,
                   AVG(f.close) OVER w AS avg_close,
                   STDDEV(f.daily_return) OVER w AS std_return
            FROM fact_market_data f
            JOIN dim_date d ON f.date_key = d.date_key
            WINDOW w AS (
                PARTITION BY f.asset_key
                ORDER BY d.full_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            )
        )
        UPDATE fact_market_data
        SET ma_30 = windowed.avg_close,
            vol_30 = windowed.std_return
        FROM windowed
        WHERE fact_market_data.id = windowed.id
    """)
    print("  Done.")


def main():
    mysql_conn = get_mysql_connection()
    mysql_cursor = mysql_conn.cursor()

    pg_conn = get_postgres_connection()
    pg_cursor = pg_conn.cursor()

    mongo_collection = get_mongo_collection()

    try:
        # Step 1: Build dimension tables
        build_dim_date(pg_cursor)
        build_dim_asset(pg_cursor, mysql_cursor)
        build_dim_sector(pg_cursor, mysql_cursor)
        pg_conn.commit()

        # Step 2: Pre-load lookups
        date_keys, asset_keys = load_dimension_keys(pg_cursor)
        news_counts = load_news_counts(mongo_collection)

        # Step 3: Build fact table
        build_fact_table(pg_cursor, mysql_cursor, news_counts, date_keys, asset_keys)
        pg_conn.commit()

        # Step 4: Compute derived metrics (single pass with window functions)
        compute_window_metrics(pg_cursor)
        pg_conn.commit()

        print("\nETL Pipeline 3 complete: Warehouse populated in PostgreSQL.")
    finally:
        mysql_cursor.close()
        mysql_conn.close()
        pg_cursor.close()
        pg_conn.close()


if __name__ == "__main__":
    main()
