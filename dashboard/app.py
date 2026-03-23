"""
Financial Market Intelligence Dashboard
Streamlit app with structured OLAP queries and Ollama natural language interface.
"""

import streamlit as st
import psycopg2
import pandas as pd
import sys
import os

# Add parent directory so we can import the query handler
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from ollama.query_handler import run_nl_query


def get_postgres_connection():
    return psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="financial_warehouse",
        user="postgres",
        password="postgres",
    )


def run_query(sql, params=None):
    """Execute a SQL query and return results as a DataFrame."""
    conn = get_postgres_connection()
    try:
        df = pd.read_sql_query(sql, conn, params=params)
        return df
    finally:
        conn.close()


@st.cache_data(ttl=300)
def get_available_tickers():
    df = run_query("SELECT ticker FROM dim_asset ORDER BY ticker")
    return df["ticker"].tolist()


@st.cache_data(ttl=300)
def get_available_sectors():
    df = run_query("SELECT sector_name FROM dim_sector ORDER BY sector_name")
    return df["sector_name"].tolist()


@st.cache_data(ttl=300)
def get_available_years():
    df = run_query("SELECT DISTINCT year FROM dim_date ORDER BY year")
    return df["year"].tolist()


# --- Pre-built OLAP Queries ---
# Each query returns (sql_string, params_list).
# Filters are injected via numbered placeholders built dynamically.

def build_filter_clause(year=None, sector=None, ticker=None):
    """Build a WHERE clause with parameterized placeholders."""
    conditions = []
    params = []
    if year:
        conditions.append("d.year = %s")
        params.append(year)
    if sector:
        conditions.append("a.sector = %s")
        params.append(sector)
    if ticker:
        conditions.append("a.ticker = %s")
        params.append(ticker)

    clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    return clause, params


QUERIES = {
    "Sector Performance by Quarter": """
        SELECT d.year, d.quarter, a.sector,
               ROUND(AVG(f.daily_return)::numeric, 4) AS avg_return,
               ROUND(AVG(f.vol_30)::numeric, 4) AS avg_volatility
        FROM fact_market_data f
        JOIN dim_date d ON f.date_key = d.date_key
        JOIN dim_asset a ON f.asset_key = a.asset_key
        {where}
        GROUP BY d.year, d.quarter, a.sector
        ORDER BY d.year, d.quarter, avg_return DESC
    """,

    "Sector Momentum (Acceleration/Deceleration)": """
        WITH quarterly AS (
            SELECT d.year, d.quarter, a.sector,
                   AVG(f.daily_return) AS avg_return
            FROM fact_market_data f
            JOIN dim_date d ON f.date_key = d.date_key
            JOIN dim_asset a ON f.asset_key = a.asset_key
            {where}
            GROUP BY d.year, d.quarter, a.sector
        ),
        with_lag AS (
            SELECT *,
                   LAG(avg_return) OVER (PARTITION BY sector ORDER BY year, quarter) AS prev_return
            FROM quarterly
        )
        SELECT year, quarter, sector,
               ROUND(avg_return::numeric, 4) AS avg_return,
               ROUND(prev_return::numeric, 4) AS prev_return,
               ROUND((avg_return - prev_return)::numeric, 4) AS momentum_change
        FROM with_lag
        WHERE prev_return IS NOT NULL
        ORDER BY sector, year, quarter
    """,

    "Period-over-Period Comparison": """
        WITH quarterly AS (
            SELECT a.ticker, d.year, d.quarter,
                   AVG(f.daily_return) AS avg_return
            FROM fact_market_data f
            JOIN dim_date d ON f.date_key = d.date_key
            JOIN dim_asset a ON f.asset_key = a.asset_key
            {where}
            GROUP BY a.ticker, d.year, d.quarter
        )
        SELECT q1.ticker, q1.year,
               ROUND(q1.avg_return::numeric, 4) AS q1_return,
               ROUND(q2.avg_return::numeric, 4) AS q2_return,
               ROUND(q3.avg_return::numeric, 4) AS q3_return
        FROM quarterly q1
        JOIN quarterly q2 ON q1.ticker = q2.ticker AND q1.year = q2.year AND q2.quarter = 'Q2'
        JOIN quarterly q3 ON q1.ticker = q3.ticker AND q1.year = q3.year AND q3.quarter = 'Q3'
        WHERE q1.quarter = 'Q1'
        ORDER BY q1.ticker, q1.year
    """,

    "News Lead/Lag Analysis": """
        WITH daily_data AS (
            SELECT a.ticker, d.full_date, f.daily_return, f.news_count,
                   LEAD(f.daily_return, 1) OVER (
                       PARTITION BY f.asset_key ORDER BY d.full_date
                   ) AS next_day_return
            FROM fact_market_data f
            JOIN dim_date d ON f.date_key = d.date_key
            JOIN dim_asset a ON f.asset_key = a.asset_key
            {where}
        )
        SELECT ticker,
               CASE WHEN news_count > 5 THEN 'high_news' ELSE 'low_news' END AS news_level,
               ROUND(AVG(daily_return)::numeric, 4) AS same_day_return,
               ROUND(AVG(next_day_return)::numeric, 4) AS next_day_return,
               COUNT(*) AS days
        FROM daily_data
        GROUP BY ticker, news_level
        ORDER BY ticker, news_level
    """,

    "Cross-Dimensional: Rate Impact by Sector": """
        SELECT d.year, d.quarter, a.sector,
               ROUND(AVG(f.daily_return)::numeric, 4) AS avg_return,
               ROUND(AVG(f.vol_30)::numeric, 4) AS avg_volatility
        FROM fact_market_data f
        JOIN dim_date d ON f.date_key = d.date_key
        JOIN dim_asset a ON f.asset_key = a.asset_key
        {where}
        GROUP BY d.year, d.quarter, a.sector
        ORDER BY d.year, d.quarter, avg_return
    """,

    "Multi-Factor Stock Screener": """
        SELECT a.ticker, a.sector,
               ROUND(AVG(f.daily_return)::numeric, 4) AS avg_return,
               ROUND(AVG(f.vol_30)::numeric, 4) AS avg_vol,
               ROUND(AVG(f.news_count)::numeric, 2) AS avg_news
        FROM fact_market_data f
        JOIN dim_date d ON f.date_key = d.date_key
        JOIN dim_asset a ON f.asset_key = a.asset_key
        {where}
        GROUP BY a.ticker, a.sector
        HAVING AVG(f.daily_return) > 0.001
           AND AVG(f.vol_30) < 0.02
           AND AVG(f.news_count) > 2
        ORDER BY avg_return DESC
    """,
}


# --- Streamlit App ---

st.set_page_config(page_title="Financial Market Intelligence", layout="wide")
st.title("Financial Market Intelligence Data Warehouse")

tab1, tab2 = st.tabs(["Structured Queries", "Natural Language"])

# --- Tab 1: Structured OLAP Queries ---
with tab1:
    st.subheader("Pre-Built Analytical Queries")

    query_type = st.selectbox("Analysis", list(QUERIES.keys()))

    col1, col2, col3 = st.columns(3)
    with col1:
        years = get_available_years()
        selected_year = st.selectbox("Year (optional)", [None] + years)
    with col2:
        sectors = get_available_sectors()
        selected_sector = st.selectbox("Sector (optional)", [None] + sectors)
    with col3:
        tickers = get_available_tickers()
        selected_ticker = st.selectbox("Ticker (optional)", [None] + tickers)

    if st.button("Run Query", key="structured"):
        where_clause, params = build_filter_clause(
            year=selected_year,
            sector=selected_sector,
            ticker=selected_ticker,
        )
        sql = QUERIES[query_type].format(where=where_clause)

        try:
            df = run_query(sql, params=params)
            if df.empty:
                st.warning("No results found for the selected filters.")
            else:
                st.dataframe(df, use_container_width=True)

                numeric_cols = df.select_dtypes(include="number").columns.tolist()
                if len(numeric_cols) >= 1:
                    st.bar_chart(df.set_index(df.columns[0])[numeric_cols[0]])
        except Exception as e:
            st.error(f"Query failed: {e}")

    with st.expander("View SQL"):
        where_clause, _ = build_filter_clause(
            year=selected_year,
            sector=selected_sector,
            ticker=selected_ticker,
        )
        st.code(QUERIES[query_type].format(where=where_clause), language="sql")

# --- Tab 2: Natural Language (Ollama) ---
with tab2:
    st.subheader("Ask a Question")
    st.caption(
        "Ask simple questions in plain English. Complex analytical queries "
        "are better served by the structured queries tab."
    )

    question = st.text_input(
        "e.g. Which sector had the lowest volatility last year?",
        key="nl_input",
    )

    if st.button("Ask", key="nl_ask") and question:
        with st.spinner("Generating and executing query..."):
            result = run_nl_query(question)

        if isinstance(result, list) and result and "error" in result[0]:
            st.error(result[0]["error"])
            if "generated_sql" in result[0]:
                st.code(result[0]["generated_sql"], language="sql")
        elif isinstance(result, dict) and "error" in result:
            st.error(result["error"])
            if "generated_sql" in result:
                st.code(result["generated_sql"], language="sql")
        elif isinstance(result, dict) and "data" in result:
            st.success("Query executed successfully.")
            with st.expander("Generated SQL"):
                st.code(result["sql"], language="sql")
            df = pd.DataFrame(result["data"])
            if df.empty:
                st.info("Query returned no results.")
            else:
                st.dataframe(df, use_container_width=True)
        else:
            st.warning("Unexpected response format.")
