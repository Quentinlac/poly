-- Migration: 001_binance_prices
-- Creates table for storing Binance 1-second kline data

CREATE TABLE IF NOT EXISTS binance_prices (
    symbol VARCHAR(20) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open_price DECIMAL(20, 8) NOT NULL,
    high_price DECIMAL(20, 8) NOT NULL,
    low_price DECIMAL(20, 8) NOT NULL,
    close_price DECIMAL(20, 8) NOT NULL,
    volume DECIMAL(30, 8) NOT NULL,
    quote_volume DECIMAL(30, 8) NOT NULL,
    num_trades BIGINT NOT NULL,
    PRIMARY KEY (symbol, timestamp)
);

-- Index for time-based queries (get latest, get price at time)
CREATE INDEX IF NOT EXISTS idx_binance_prices_symbol_time
    ON binance_prices (symbol, timestamp DESC);

-- Log migration
DO $$
BEGIN
    RAISE NOTICE 'Migration 001_binance_prices completed';
END $$;
