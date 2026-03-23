CREATE TABLE IF NOT EXISTS assets (
    ticker VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100),
    sector VARCHAR(50),
    asset_type VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS daily_prices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(10),
    date DATE,
    open DECIMAL(10,2),
    high DECIMAL(10,2),
    low DECIMAL(10,2),
    close DECIMAL(10,2),
    volume BIGINT,
    FOREIGN KEY (ticker) REFERENCES assets(ticker),
    UNIQUE KEY uq_ticker_date (ticker, date),
    INDEX idx_date (date)
);
