CREATE TABLE IF NOT EXISTS dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INT,
    quarter VARCHAR(2),
    month INT,
    day_of_week VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS dim_sector (
    sector_key SERIAL PRIMARY KEY,
    sector_name VARCHAR(50) UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_asset (
    asset_key SERIAL PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE,
    company_name VARCHAR(100),
    sector VARCHAR(50),
    asset_type VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS fact_market_data (
    id SERIAL PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    asset_key INT REFERENCES dim_asset(asset_key),
    open DECIMAL(10,2),
    high DECIMAL(10,2),
    low DECIMAL(10,2),
    close DECIMAL(10,2),
    volume BIGINT,
    daily_return DECIMAL(10,6),
    news_count INT DEFAULT 0,
    ma_30 DECIMAL(10,2),
    vol_30 DECIMAL(10,6),
    UNIQUE (date_key, asset_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_date_key ON fact_market_data(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_asset_key ON fact_market_data(asset_key);
CREATE INDEX IF NOT EXISTS idx_dim_date_year ON dim_date(year);
CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON dim_date(full_date);
