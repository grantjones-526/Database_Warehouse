"""
Natural language query handler using Claude Haiku.
Translates plain-English questions into PostgreSQL SQL and returns results.
Context is intentionally minimal — only the warehouse schema is sent.
"""

import os
import re
import psycopg2
import anthropic

# Compact schema — just enough for SQL generation, nothing more
_SCHEMA = """\
PostgreSQL database: financial_warehouse
Data range: 2021-01-01 to 2026-01-01
Tickers: AAPL, MSFT, GOOGL, AMZN, TSLA

dim_date(date_key PK, full_date DATE, year INT, quarter VARCHAR, month INT, day_of_week VARCHAR)
dim_sector(sector_key PK, sector_name VARCHAR)
dim_asset(asset_key PK, ticker VARCHAR, company_name VARCHAR, sector VARCHAR, asset_type VARCHAR)
fact_market_data(
  id PK,
  date_key FK->dim_date,
  asset_key FK->dim_asset,
  open DECIMAL, high DECIMAL, low DECIMAL, close DECIMAL,
  volume BIGINT,
  daily_return DECIMAL,  -- (close-open)/open * 100
  news_count INT,
  ma_30 DECIMAL,         -- 30-day moving avg of close
  vol_30 DECIMAL         -- 30-day return volatility (stddev)
)"""

_SYSTEM = f"""\
You are a SQL expert. Write a single PostgreSQL SELECT query that answers the user's question.

Schema:
{_SCHEMA}

Rules:
- Output ONLY the raw SQL query — no explanation, no markdown, no code fences
- Always JOIN through dim_date and dim_asset when accessing fact_market_data
- LIMIT to 100 rows unless the question asks for more
- Cast numeric aggregates with ::numeric before ROUND()"""


def _get_conn():
    return psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="financial_warehouse",
        user="postgres",
        password="postgres",
    )


def _extract_sql(text: str) -> str:
    """Strip markdown code fences if present, otherwise return as-is."""
    match = re.search(r"```(?:sql)?\s*([\s\S]*?)```", text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return text.strip()


def run_nl_query(question: str) -> dict:
    """
    Translate *question* to SQL via Claude Haiku, execute it, and return results.

    Returns:
        {"sql": str, "data": list[dict]}  on success
        {"error": str, "generated_sql": str}  on failure
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return {"error": "ANTHROPIC_API_KEY environment variable is not set."}

    client = anthropic.Anthropic(api_key=api_key)

    try:
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=512,
            system=_SYSTEM,
            messages=[{"role": "user", "content": question}],
        )
    except anthropic.AuthenticationError:
        return {"error": "Invalid Anthropic API key."}
    except anthropic.RateLimitError:
        return {"error": "Anthropic rate limit reached. Please wait a moment and try again."}
    except anthropic.APIError as e:
        return {"error": f"Anthropic API error: {e}"}

    raw = response.content[0].text.strip() if response.content else ""
    sql = _extract_sql(raw)

    if not sql.lower().lstrip().startswith("select"):
        return {
            "error": "The model did not return a SELECT query.",
            "generated_sql": raw,
        }

    try:
        conn = _get_conn()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchmany(100)
            data = [dict(zip(columns, row)) for row in rows]
            return {"sql": sql, "data": data}
        finally:
            conn.close()
    except psycopg2.Error as e:
        return {"error": str(e), "generated_sql": sql}
