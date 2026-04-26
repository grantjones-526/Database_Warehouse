"""
ARIMA-based next-week price prediction.
Uses PostgreSQL warehouse data (last 90 trading days).

Order selection: ARIMA(5,1,0) — industry-standard starting point for daily prices.
  d=1 differences the series to remove the upward trend (makes it stationary).
  p=5 captures autocorrelation over the past 5 trading days (one week).
  q=0 keeps it simple; MA terms add noise on short financial series.
"""

import warnings
import numpy as np
import psycopg2
from statsmodels.tsa.arima.model import ARIMA

warnings.filterwarnings("ignore")   # suppress convergence warnings in the UI

TICKERS = {
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "GOOGL": "Google / Alphabet",
    "AMZN": "Amazon",
    "TSLA": "Tesla",
}

_ALIASES = {
    "apple": "AAPL", "aapl": "AAPL",
    "microsoft": "MSFT", "msft": "MSFT",
    "google": "GOOGL", "googl": "GOOGL", "alphabet": "GOOGL",
    "amazon": "AMZN", "amzn": "AMZN",
    "tesla": "TSLA", "tsla": "TSLA",
}

_BUY_KEYWORDS = [
    "buy", "should i", "invest", "worth", "purchase",
    "next week", "predict", "forecast", "price target",
    "going up", "will it", "going to",
]


def detect_prediction_intent(question: str) -> tuple[bool, str | None]:
    """Return (is_prediction, ticker_or_None)."""
    q = question.lower()
    if not any(kw in q for kw in _BUY_KEYWORDS):
        return False, None
    for alias, ticker in _ALIASES.items():
        if alias in q:
            return True, ticker
    return False, None


def _get_conn():
    return psycopg2.connect(
        host="localhost", port=5433,
        dbname="financial_warehouse", user="postgres", password="postgres",
    )


def predict_next_week(ticker: str) -> dict:
    """
    Fit ARIMA(5,1,0) on the last 90 trading days of close prices and
    forecast the next 5 trading days.

    Returns a compact summary dict, or {"error": str} on failure.
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT d.full_date, f.close, f.vol_30, f.ma_30
            FROM fact_market_data f
            JOIN dim_date d ON f.date_key = d.date_key
            JOIN dim_asset a ON f.asset_key = a.asset_key
            WHERE a.ticker = %s
            ORDER BY d.full_date DESC
            LIMIT 90
        """, (ticker,))
        rows = list(reversed(cur.fetchall()))
    finally:
        conn.close()

    if len(rows) < 20:
        return {"error": f"Not enough historical data for {ticker} (need ≥20 days, got {len(rows)})"}

    closes = np.array([float(r[1]) for r in rows])
    vols   = np.array([float(r[2]) if r[2] else 0.0 for r in rows])
    mas    = np.array([float(r[3]) if r[3] else np.nan for r in rows])

    try:
        model  = ARIMA(closes, order=(5, 1, 0))
        fitted = model.fit()
        fc_obj   = fitted.get_forecast(steps=5)
        forecast = np.asarray(fc_obj.predicted_mean)
        ci       = np.asarray(fc_obj.conf_int(alpha=0.20))  # 80% CI, shape (5, 2)
    except Exception as e:
        return {"error": f"ARIMA fitting failed: {e}"}

    current          = float(closes[-1])
    predicted_5d     = float(forecast[-1])
    predicted_day1   = float(forecast[0])
    change_pct       = round((predicted_5d - current) / current * 100, 2)
    ci_low           = round(float(ci[-1, 0]), 2)
    ci_high          = round(float(ci[-1, 1]), 2)
    avg_vol          = round(float(np.nanmean(vols[-20:])), 4)
    last_ma          = float(mas[-1]) if not np.isnan(mas[-1]) else None

    # AIC — lower is better; gives a sense of model quality
    aic = round(float(fitted.aic), 1)

    return {
        "ticker":              ticker,
        "company":             TICKERS.get(ticker, ticker),
        "model":               "ARIMA(5,1,0)",
        "current_price":       round(current, 2),
        "predicted_price_1d":  round(predicted_day1, 2),
        "predicted_price_5d":  round(predicted_5d, 2),
        "predicted_change_pct": change_pct,
        "ci_low_5d":           ci_low,
        "ci_high_5d":          ci_high,
        "aic":                 aic,
        "avg_vol_30":          avg_vol,
        "ma_30":               round(last_ma, 2) if last_ma else None,
        "days_used":           len(closes),
    }
