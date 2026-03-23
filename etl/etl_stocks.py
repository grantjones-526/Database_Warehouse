"""
ETL Pipeline 1: Stock Prices → MySQL
Extracts daily OHLCV data from yfinance, transforms it, and loads into MySQL.
"""

import yfinance as yf
import mysql.connector
import pandas as pd
from datetime import date

# Target tickers and metadata
TICKERS = {
    "AAPL": ("Apple Inc.", "Technology"),
    "MSFT": ("Microsoft Corporation", "Technology"),
    "GOOGL": ("Alphabet Inc.", "Technology"),
    "AMZN": ("Amazon.com Inc.", "Consumer Cyclical"),
    "TSLA": ("Tesla Inc.", "Consumer Cyclical"),
}

START_DATE = "2021-01-01"
END_DATE = "2026-01-01"


def get_mysql_connection():
    return mysql.connector.connect(
        host="localhost",
        port=3306,
        user="root",
        password="root",
        database="financial_market",
    )


def load_assets(cursor):
    """Insert asset metadata into the assets table."""
    for ticker, (name, sector) in TICKERS.items():
        cursor.execute(
            "INSERT IGNORE INTO assets (ticker, name, sector, asset_type) "
            "VALUES (%s, %s, %s, %s)",
            (ticker, name, sector, "stock"),
        )
    print(f"Loaded {len(TICKERS)} assets.")


def extract_and_load_prices(cursor):
    """Download price data from yfinance and load into daily_prices."""
    total_rows = 0

    for ticker in TICKERS:
        print(f"Downloading {ticker}...")
        df = yf.download(ticker, start=START_DATE, end=END_DATE, progress=False)

        if df.empty:
            print(f"  No data for {ticker}, skipping.")
            continue

        # Flatten multi-level columns from yfinance
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.reset_index()
        df = df.dropna()

        rows_inserted = 0
        for _, row in df.iterrows():
            trade_date = row["Date"]
            if hasattr(trade_date, "date"):
                trade_date = trade_date.date()

            cursor.execute(
                "INSERT IGNORE INTO daily_prices (ticker, date, open, high, low, close, volume) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    ticker,
                    trade_date,
                    float(row["Open"]),
                    float(row["High"]),
                    float(row["Low"]),
                    float(row["Close"]),
                    int(row["Volume"]),
                ),
            )
            rows_inserted += 1

        print(f"  Inserted {rows_inserted} rows for {ticker}.")
        total_rows += rows_inserted

    print(f"Total rows inserted: {total_rows}")


def main():
    conn = get_mysql_connection()
    cursor = conn.cursor()

    try:
        load_assets(cursor)
        conn.commit()

        extract_and_load_prices(cursor)
        conn.commit()

        print("ETL Pipeline 1 complete: Stock prices loaded into MySQL.")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
