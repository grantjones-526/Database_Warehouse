"""
Natural language query handler using Claude Haiku.
Routes to either:
  - SQL generation (structured OLAP questions)
  - Price prediction (buy/forecast questions) using linear regression + brief LLM summary
Context is intentionally minimal to limit token usage.
"""

import os
import re
import psycopg2
import anthropic
from pymongo import MongoClient
from claude.regression import detect_prediction_intent, predict_next_week

# Compact schema — just enough for SQL generation, nothing more
_SCHEMA = """\
PostgreSQL database: financial_warehouse
Data range: 2021-01-01 to present
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


_PREDICT_SYSTEM = """\
You are a concise financial analyst assistant. You are given the output of a simple \
linear regression model on recent stock price data, plus the latest news headlines \
for that stock. Write 2-3 sentences that naturally answer the user's question. \
Acknowledge the model's limitations. Do not list every number — highlight the key \
takeaway and note any relevant news sentiment."""


def _fetch_recent_headlines(ticker: str, limit: int = 5) -> list[str]:
    """Return the most recent article headlines for *ticker* from MongoDB."""
    try:
        col = MongoClient("mongodb://localhost:27017").financial_market.scraped_news
        docs = list(
            col.find(
                {"tickers": ticker},
                {"headline": 1, "date": 1, "_id": 0},
            )
            .sort("date", -1)
            .limit(limit)
        )
        return [d["headline"] for d in docs if "headline" in d]
    except Exception:
        return []


def _run_prediction_query(question: str, ticker: str, client: anthropic.Anthropic) -> dict:
    """Run regression, then ask Claude to narrate the result in plain English."""
    model_result = predict_next_week(ticker)
    if "error" in model_result:
        return {"error": model_result["error"]}

    r = model_result
    vs_ma = ""
    if r["ma_30"]:
        vs_ma = (
            "above" if r["current_price"] > r["ma_30"] else "below"
        ) + f" its 30-day moving average (${r['ma_30']})"

    headlines = _fetch_recent_headlines(ticker)
    news_block = (
        "Recent news headlines:\n" + "\n".join(f"  - {h}" for h in headlines)
        if headlines else "No recent news found for this ticker."
    )

    context = (
        f"User asked: \"{question}\"\n\n"
        f"{r['model']} forecast on last {r['days_used']} trading days:\n"
        f"  Ticker: {r['ticker']} ({r['company']})\n"
        f"  Current close: ${r['current_price']}\n"
        f"  Predicted close tomorrow: ${r['predicted_price_1d']}\n"
        f"  Predicted close in 5 trading days: ${r['predicted_price_5d']} "
        f"({'+' if r['predicted_change_pct'] >= 0 else ''}{r['predicted_change_pct']}%)\n"
        f"  80% confidence interval (day 5): ${r['ci_low_5d']} – ${r['ci_high_5d']}\n"
        f"  AIC (lower = better fit): {r['aic']}\n"
        f"  30-day avg volatility: {r['avg_vol_30']}\n"
        + (f"  Price is currently {vs_ma}\n" if vs_ma else "")
        + f"\n{news_block}"
    )

    try:
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=220,
            system=_PREDICT_SYSTEM,
            messages=[{"role": "user", "content": context}],
        )
    except anthropic.APIError as e:
        return {"error": f"Anthropic API error: {e}"}

    narrative = response.content[0].text.strip() if response.content else ""
    return {"type": "prediction", "prediction": model_result, "response": narrative}


def run_nl_query(question: str) -> dict:
    """
    Route to prediction or SQL path based on question intent.

    Returns one of:
        {"type": "prediction", "prediction": dict, "response": str}
        {"type": "sql",        "sql": str,        "data": list[dict]}
        {"error": str, ...}
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return {"error": "ANTHROPIC_API_KEY environment variable is not set."}

    client = anthropic.Anthropic(api_key=api_key)

    is_prediction, ticker = detect_prediction_intent(question)
    if is_prediction:
        if not ticker:
            return {"error": "Please specify a stock ticker (AAPL, MSFT, GOOGL, AMZN, or TSLA)."}
        return _run_prediction_query(question, ticker, client)

    # --- SQL path ---
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
        return {"error": "The model did not return a SELECT query.", "generated_sql": raw}

    try:
        conn = _get_conn()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchmany(100)
            data = [dict(zip(columns, row)) for row in rows]
            return {"type": "sql", "sql": sql, "data": data}
        finally:
            conn.close()
    except psycopg2.Error as e:
        return {"error": str(e), "generated_sql": sql}
