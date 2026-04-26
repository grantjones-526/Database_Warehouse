# Financial Market Intelligence Data Warehouse

An academic data warehouse project that integrates stock price history, financial news, and a multi-database architecture with a natural language query interface powered by the Claude API.

## Architecture

| Layer | Technology | Purpose |
|---|---|---|
| Operational DB | MySQL | Raw daily OHLCV stock prices |
| Document Store | MongoDB | Financial news articles with ticker tags |
| Warehouse | PostgreSQL | Star schema — fact + dimension tables |
| Dashboard | Streamlit | OLAP queries and natural language interface |
| LLM | Claude Haiku (Anthropic) | SQL generation and price forecast narration |

### Star Schema
- `fact_market_data` — daily prices, returns, volatility, news count
- `dim_date` — date hierarchy (year, quarter, month, day)
- `dim_asset` — ticker metadata (company, sector, type)
- `dim_sector` — sector reference

## Features

**Structured Queries (Tab 1)**
- Sector performance by quarter
- Sector momentum (acceleration/deceleration)
- Period-over-period comparison
- News lead/lag analysis
- Cross-dimensional rate impact by sector
- Multi-factor stock screener

**Natural Language Interface (Tab 2)**
- Plain-English OLAP questions translated to SQL via Claude Haiku
- Stock price forecasting using ARIMA(5,1,0) for any tracked ticker
- Forecast narration enriched with recent MongoDB news headlines

### Decision Layer
When a user asks a buy/forecast question (e.g. *"Should I buy Apple stock next week?"*), the system produces a structured decision output:

1. **Data retrieval** — the last 90 trading days of closing prices are pulled from the PostgreSQL warehouse
2. **ARIMA(5,1,0) model** — fits a time-series model on the price history; `d=1` removes the upward trend, `p=5` captures weekly autocorrelation
3. **5-day forecast** — predicts tomorrow's price and the price 5 trading days out, with an 80% confidence interval
4. **News enrichment** — the 5 most recent MongoDB news headlines for that ticker are retrieved and passed alongside the model output
5. **LLM narration** — Claude Haiku receives only a compact ~100-token summary (model stats + headlines) and returns a 2–3 sentence plain-English recommendation that highlights the key trend, the confidence interval, and any relevant news sentiment
6. **Dashboard display** — the decision is surfaced as four metric cards (current price, predicted price with % delta, confidence interval, 30-day volatility) plus the LLM narrative and an AIC model quality score

## Data Sources
- **yfinance** — daily OHLCV prices for AAPL, MSFT, GOOGL, AMZN, TSLA (2021–present)
- **FRED API** — macroeconomic indicators
- **NewsAPI** — financial news articles tagged by ticker and topic

## Setup

### Prerequisites
- Docker
- Python 3.10+
- Anthropic API key
- NewsAPI key

### Run
```bash
cp .env.example .env   # add your API keys
python start.py
```

`start.py` handles everything: starts Docker containers (MySQL, PostgreSQL, MongoDB), installs dependencies, runs all three ETL pipelines, and launches the Streamlit dashboard.

### ETL Pipelines
1. `etl/etl_stocks.py` — downloads price history into MySQL
2. `etl/etl_news.py` — fetches news articles into MongoDB
3. `etl/etl_warehouse.py` — builds the PostgreSQL star schema, computes 30-day moving averages and volatility

## Project Structure
```
├── dashboard/app.py          # Streamlit dashboard
├── claude/
│   ├── query_handler.py      # NL → SQL routing and LLM calls
│   └── regression.py         # ARIMA price forecasting
├── etl/
│   ├── etl_stocks.py
│   ├── etl_news.py
│   └── etl_warehouse.py
├── sql/
│   ├── mysql_init.sql
│   └── postgres_init.sql
├── docker-compose.yml
└── start.py
```
