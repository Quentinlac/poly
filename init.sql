-- PostgreSQL/TimescaleDB Schema for Polymarket Analyzer
-- Optimized for high-frequency trading workloads

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================================
-- USERS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS users (
    id VARCHAR(42) PRIMARY KEY,  -- Ethereum address
    username VARCHAR(255),
    address VARCHAR(42),
    total_trades INTEGER DEFAULT 0,
    total_pnl DECIMAL(20, 8) DEFAULT 0,
    win_rate DECIMAL(5, 4) DEFAULT 0,
    consistency DECIMAL(5, 4) DEFAULT 0,
    last_active TIMESTAMPTZ,
    last_synced_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for user queries
CREATE INDEX IF NOT EXISTS idx_users_total_pnl ON users(total_pnl DESC);
CREATE INDEX IF NOT EXISTS idx_users_total_trades ON users(total_trades DESC);
CREATE INDEX IF NOT EXISTS idx_users_win_rate ON users(win_rate DESC);
CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active DESC);

-- ============================================================================
-- USER SUBJECT METRICS
-- ============================================================================
CREATE TABLE IF NOT EXISTS user_subject_metrics (
    user_id VARCHAR(42) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    subject VARCHAR(50) NOT NULL,
    trades INTEGER DEFAULT 0,
    pnl DECIMAL(20, 8) DEFAULT 0,
    win_rate DECIMAL(5, 4) DEFAULT 0,
    consistency DECIMAL(5, 4) DEFAULT 0,
    PRIMARY KEY (user_id, subject)
);

-- Composite index for subject-based leaderboards
CREATE INDEX IF NOT EXISTS idx_metrics_subject_pnl ON user_subject_metrics(subject, pnl DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_subject_trades ON user_subject_metrics(subject, trades DESC);

-- ============================================================================
-- USER RED FLAGS
-- ============================================================================
CREATE TABLE IF NOT EXISTS user_red_flags (
    user_id VARCHAR(42) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    flag VARCHAR(100) NOT NULL,
    PRIMARY KEY (user_id, flag)
);

-- ============================================================================
-- USER TRADES (TimescaleDB Hypertable for time-series)
-- ============================================================================
CREATE TABLE IF NOT EXISTS user_trades (
    id VARCHAR(100) NOT NULL,
    user_id VARCHAR(42) NOT NULL,
    market_id VARCHAR(100),
    subject VARCHAR(50),
    type VARCHAR(20) DEFAULT 'TRADE',
    side VARCHAR(10),
    is_maker BOOLEAN DEFAULT FALSE,
    size DECIMAL(30, 18),
    usdc_size DECIMAL(20, 8),
    price DECIMAL(10, 8),
    outcome VARCHAR(255),
    timestamp TIMESTAMPTZ NOT NULL,
    title TEXT,
    slug VARCHAR(255),
    event_slug VARCHAR(255),
    transaction_hash VARCHAR(100),
    name VARCHAR(255),
    pseudonym VARCHAR(255),
    inserted_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, timestamp)
);

-- Convert to TimescaleDB hypertable for time-series optimization
SELECT create_hypertable('user_trades', 'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Optimized indexes for trade queries
CREATE INDEX IF NOT EXISTS idx_trades_user_time ON user_trades(user_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_trades_user_title_outcome ON user_trades(user_id, title, outcome);
CREATE INDEX IF NOT EXISTS idx_trades_user_side ON user_trades(user_id, side);
CREATE INDEX IF NOT EXISTS idx_trades_market ON user_trades(market_id);
CREATE INDEX IF NOT EXISTS idx_trades_subject ON user_trades(subject);

-- Partial indexes for common queries
CREATE INDEX IF NOT EXISTS idx_trades_buys ON user_trades(user_id, timestamp DESC) WHERE side = 'BUY';
CREATE INDEX IF NOT EXISTS idx_trades_sells ON user_trades(user_id, timestamp DESC) WHERE side = 'SELL';
CREATE INDEX IF NOT EXISTS idx_trades_redeems ON user_trades(user_id, timestamp DESC) WHERE type = 'REDEEM';

-- ============================================================================
-- GLOBAL TRADES (Platform-wide monitoring)
-- ============================================================================
CREATE TABLE IF NOT EXISTS global_trades (
    id VARCHAR(255) NOT NULL,
    user_address VARCHAR(42) NOT NULL,
    asset TEXT,
    type VARCHAR(20),
    side VARCHAR(10),
    size DECIMAL(30, 18),
    usdc_size DECIMAL(20, 8),
    price DECIMAL(10, 8),
    outcome VARCHAR(255),
    timestamp TIMESTAMPTZ NOT NULL,
    title TEXT,
    slug VARCHAR(255),
    transaction_hash VARCHAR(100),
    inserted_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, timestamp)
);

-- Convert to hypertable
SELECT create_hypertable('global_trades', 'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Indexes for global trade queries
CREATE INDEX IF NOT EXISTS idx_global_trades_user ON global_trades(user_address);
CREATE INDEX IF NOT EXISTS idx_global_trades_timestamp ON global_trades(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_global_trades_type ON global_trades(type);

-- ============================================================================
-- ANALYSIS CACHE
-- ============================================================================
CREATE TABLE IF NOT EXISTS analysis_cache (
    user_id VARCHAR(42) PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    result_json JSONB NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    trade_count INTEGER NOT NULL
);

-- GIN index for JSONB queries
CREATE INDEX IF NOT EXISTS idx_analysis_cache_json ON analysis_cache USING GIN(result_json);

-- ============================================================================
-- USER POSITIONS (Materialized aggregations)
-- ============================================================================
CREATE TABLE IF NOT EXISTS user_positions (
    user_id VARCHAR(42) NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    market_outcome VARCHAR(500) NOT NULL,
    title TEXT,
    outcome VARCHAR(255),
    subject VARCHAR(50),
    total_bought DECIMAL(20, 8),
    total_sold DECIMAL(20, 8),
    qty_bought DECIMAL(30, 18),
    qty_sold DECIMAL(30, 18),
    gain_loss DECIMAL(20, 8),
    buy_count INTEGER,
    sell_count INTEGER,
    first_buy_at TIMESTAMPTZ,
    last_buy_at TIMESTAMPTZ,
    first_sell_at TIMESTAMPTZ,
    last_sell_at TIMESTAMPTZ,
    duration_mins DECIMAL(15, 2),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, market_outcome)
);

-- Indexes for position queries
CREATE INDEX IF NOT EXISTS idx_positions_user ON user_positions(user_id);
CREATE INDEX IF NOT EXISTS idx_positions_gain_loss ON user_positions(user_id, gain_loss DESC);
CREATE INDEX IF NOT EXISTS idx_positions_subject ON user_positions(subject);

-- ============================================================================
-- TOKEN MAP CACHE
-- ============================================================================
CREATE TABLE IF NOT EXISTS token_map_cache (
    token_id VARCHAR(100) PRIMARY KEY,
    condition_id VARCHAR(100),
    outcome VARCHAR(255),
    title TEXT,
    slug VARCHAR(255),
    event_slug VARCHAR(255),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for token lookups
CREATE INDEX IF NOT EXISTS idx_token_condition ON token_map_cache(condition_id);

-- ============================================================================
-- CONTINUOUS AGGREGATES (Real-time materialized views)
-- ============================================================================

-- Hourly trade volume per user
CREATE MATERIALIZED VIEW IF NOT EXISTS user_hourly_volume
WITH (timescaledb.continuous) AS
SELECT
    user_id,
    time_bucket('1 hour', timestamp) AS bucket,
    COUNT(*) AS trade_count,
    SUM(usdc_size) AS total_volume,
    AVG(price) AS avg_price
FROM user_trades
GROUP BY user_id, time_bucket('1 hour', timestamp)
WITH NO DATA;

-- Daily P&L summary (refresh policy set below)
CREATE MATERIALIZED VIEW IF NOT EXISTS user_daily_summary
WITH (timescaledb.continuous) AS
SELECT
    user_id,
    time_bucket('1 day', timestamp) AS bucket,
    COUNT(*) AS trade_count,
    SUM(CASE WHEN side = 'BUY' THEN usdc_size ELSE 0 END) AS bought,
    SUM(CASE WHEN side = 'SELL' THEN usdc_size ELSE 0 END) AS sold
FROM user_trades
GROUP BY user_id, time_bucket('1 day', timestamp)
WITH NO DATA;

-- Set up refresh policies for continuous aggregates
SELECT add_continuous_aggregate_policy('user_hourly_volume',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy('user_daily_summary',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- ============================================================================
-- DATA RETENTION POLICIES
-- ============================================================================

-- Keep global trades for 90 days
SELECT add_retention_policy('global_trades', INTERVAL '90 days', if_not_exists => TRUE);

-- Keep user trades indefinitely (no retention policy)

-- ============================================================================
-- COMPRESSION POLICIES (for older data)
-- ============================================================================

-- Compress user_trades chunks older than 7 days
ALTER TABLE user_trades SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'user_id',
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT add_compression_policy('user_trades', INTERVAL '7 days', if_not_exists => TRUE);

-- Compress global_trades chunks older than 3 days
ALTER TABLE global_trades SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'user_address',
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT add_compression_policy('global_trades', INTERVAL '3 days', if_not_exists => TRUE);

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Function to get user trade stats quickly
CREATE OR REPLACE FUNCTION get_user_trade_stats(p_user_id VARCHAR)
RETURNS TABLE (
    total_trades BIGINT,
    total_bought DECIMAL,
    total_sold DECIMAL,
    first_trade TIMESTAMPTZ,
    last_trade TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COUNT(*)::BIGINT,
        COALESCE(SUM(CASE WHEN side = 'BUY' THEN usdc_size ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN side = 'SELL' THEN usdc_size ELSE 0 END), 0),
        MIN(timestamp),
        MAX(timestamp)
    FROM user_trades
    WHERE user_id = p_user_id;
END;
$$ LANGUAGE plpgsql;

-- Function for efficient batch upsert of trades
CREATE OR REPLACE FUNCTION upsert_trades(trades JSONB)
RETURNS INTEGER AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    WITH input_trades AS (
        SELECT
            (t->>'id')::VARCHAR AS id,
            (t->>'user_id')::VARCHAR AS user_id,
            (t->>'market_id')::VARCHAR AS market_id,
            (t->>'subject')::VARCHAR AS subject,
            COALESCE(t->>'type', 'TRADE')::VARCHAR AS type,
            (t->>'side')::VARCHAR AS side,
            COALESCE((t->>'is_maker')::BOOLEAN, FALSE) AS is_maker,
            (t->>'size')::DECIMAL AS size,
            (t->>'usdc_size')::DECIMAL AS usdc_size,
            (t->>'price')::DECIMAL AS price,
            (t->>'outcome')::VARCHAR AS outcome,
            (t->>'timestamp')::TIMESTAMPTZ AS timestamp,
            (t->>'title')::TEXT AS title,
            (t->>'slug')::VARCHAR AS slug,
            (t->>'event_slug')::VARCHAR AS event_slug,
            (t->>'transaction_hash')::VARCHAR AS transaction_hash,
            (t->>'name')::VARCHAR AS name,
            (t->>'pseudonym')::VARCHAR AS pseudonym
        FROM jsonb_array_elements(trades) AS t
    )
    INSERT INTO user_trades (
        id, user_id, market_id, subject, type, side, is_maker, size, usdc_size,
        price, outcome, timestamp, title, slug, event_slug, transaction_hash, name, pseudonym
    )
    SELECT * FROM input_trades
    ON CONFLICT (id, timestamp) DO UPDATE SET
        user_id = EXCLUDED.user_id,
        market_id = EXCLUDED.market_id,
        subject = EXCLUDED.subject,
        type = EXCLUDED.type,
        side = EXCLUDED.side,
        is_maker = EXCLUDED.is_maker,
        size = EXCLUDED.size,
        usdc_size = EXCLUDED.usdc_size,
        price = EXCLUDED.price,
        outcome = EXCLUDED.outcome,
        title = EXCLUDED.title,
        slug = EXCLUDED.slug,
        event_slug = EXCLUDED.event_slug,
        transaction_hash = EXCLUDED.transaction_hash,
        name = EXCLUDED.name,
        pseudonym = EXCLUDED.pseudonym;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- INITIAL DATA SETUP
-- ============================================================================

-- Create extension for UUID generation if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO polymarket;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO polymarket;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO polymarket;

-- Analyze tables for query planner
ANALYZE;

-- ============================================================================
-- COPY TRADING TABLES
-- ============================================================================

-- Track copy trades for audit
CREATE TABLE IF NOT EXISTS copy_trades (
    id SERIAL PRIMARY KEY,
    original_trade_id VARCHAR(255) NOT NULL,
    original_trader VARCHAR(42) NOT NULL,
    market_id VARCHAR(100) NOT NULL,
    token_id VARCHAR(100),
    outcome VARCHAR(255) NOT NULL,
    title TEXT,
    side VARCHAR(10) NOT NULL,
    intended_usdc DECIMAL(20, 8),
    actual_usdc DECIMAL(20, 8),
    price_paid DECIMAL(10, 8),
    size_bought DECIMAL(30, 18),
    status VARCHAR(20) DEFAULT 'pending',  -- pending, executed, partial, failed
    error_reason TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    executed_at TIMESTAMPTZ,
    order_id VARCHAR(255),
    tx_hash VARCHAR(100),
    detection_source VARCHAR(20) DEFAULT 'clob'  -- clob, polygon_ws, data_api
);

CREATE INDEX IF NOT EXISTS idx_copy_trades_status ON copy_trades(status);
CREATE INDEX IF NOT EXISTS idx_copy_trades_original ON copy_trades(original_trade_id);
CREATE INDEX IF NOT EXISTS idx_copy_trades_trader ON copy_trades(original_trader);
CREATE INDEX IF NOT EXISTS idx_copy_trades_created ON copy_trades(created_at DESC);

-- Track our positions from copy trading
CREATE TABLE IF NOT EXISTS my_positions (
    market_id VARCHAR(100) NOT NULL,
    token_id VARCHAR(100) NOT NULL,
    outcome VARCHAR(255) NOT NULL,
    title TEXT,
    size DECIMAL(30, 18) NOT NULL DEFAULT 0,
    avg_price DECIMAL(10, 8),
    total_cost DECIMAL(20, 8) DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (market_id, outcome)
);

CREATE INDEX IF NOT EXISTS idx_my_positions_token ON my_positions(token_id);

-- Track processed trades to avoid duplicates
CREATE TABLE IF NOT EXISTS processed_trades (
    trade_id VARCHAR(255) PRIMARY KEY,
    processed_at TIMESTAMPTZ DEFAULT NOW()
);

-- Function to check if trade was already processed
CREATE OR REPLACE FUNCTION is_trade_processed(p_trade_id VARCHAR)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN EXISTS (SELECT 1 FROM processed_trades WHERE trade_id = p_trade_id);
END;
$$ LANGUAGE plpgsql;

-- Per-user copy trading settings
CREATE TABLE IF NOT EXISTS user_copy_settings (
    user_address VARCHAR(42) PRIMARY KEY,
    multiplier DECIMAL(10, 6) NOT NULL DEFAULT 0.05,  -- Default 1/20th (5%)
    enabled BOOLEAN DEFAULT TRUE,
    min_usdc DECIMAL(20, 8) DEFAULT 1.0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_copy_settings_enabled ON user_copy_settings(enabled);

-- ============================================================================
-- PRIVILEGED KNOWLEDGE ANALYSIS (Pre-computed)
-- ============================================================================
CREATE TABLE IF NOT EXISTS privileged_analysis (
    id SERIAL PRIMARY KEY,
    time_window_minutes INTEGER NOT NULL,
    price_threshold_pct INTEGER NOT NULL,
    user_address VARCHAR(42) NOT NULL,
    hit_count INTEGER NOT NULL,
    total_buys INTEGER NOT NULL,
    hit_rate DECIMAL(10, 4) NOT NULL,
    hits_json JSONB NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(time_window_minutes, price_threshold_pct, user_address)
);

CREATE INDEX IF NOT EXISTS idx_privileged_window_threshold ON privileged_analysis(time_window_minutes, price_threshold_pct);
CREATE INDEX IF NOT EXISTS idx_privileged_hit_count ON privileged_analysis(time_window_minutes, price_threshold_pct, hit_count DESC);
CREATE INDEX IF NOT EXISTS idx_privileged_computed ON privileged_analysis(computed_at);

-- Track when each analysis was last computed
CREATE TABLE IF NOT EXISTS privileged_analysis_meta (
    time_window_minutes INTEGER NOT NULL,
    price_threshold_pct INTEGER NOT NULL,
    last_computed_at TIMESTAMPTZ NOT NULL,
    computation_duration_sec DECIMAL(10, 2),
    user_count INTEGER,
    PRIMARY KEY (time_window_minutes, price_threshold_pct)
);

-- ============================================================================
-- TRADING ACCOUNTS (Multi-account support)
-- ============================================================================

CREATE TABLE IF NOT EXISTS trading_accounts (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    private_key_env_var VARCHAR(100) NOT NULL,
    funder_address_env_var VARCHAR(100),
    signature_type INT DEFAULT 1,
    enabled BOOLEAN DEFAULT TRUE,
    is_default BOOLEAN DEFAULT FALSE,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(private_key_env_var),
    UNIQUE(name)
);

CREATE INDEX IF NOT EXISTS idx_trading_accounts_enabled ON trading_accounts(enabled);
CREATE INDEX IF NOT EXISTS idx_trading_accounts_default ON trading_accounts(is_default) WHERE is_default = TRUE;

-- Add trading_account_id to user_copy_settings if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'user_copy_settings' AND column_name = 'trading_account_id') THEN
        ALTER TABLE user_copy_settings ADD COLUMN trading_account_id INT REFERENCES trading_accounts(id) ON DELETE SET NULL;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'user_copy_settings' AND column_name = 'strategy_type') THEN
        ALTER TABLE user_copy_settings ADD COLUMN strategy_type INT DEFAULT 1;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'user_copy_settings' AND column_name = 'max_usd') THEN
        ALTER TABLE user_copy_settings ADD COLUMN max_usd DECIMAL(20, 8);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_user_copy_settings_account ON user_copy_settings(trading_account_id);

-- Add trading_account_id to copy_trades if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'copy_trades' AND column_name = 'trading_account_id') THEN
        ALTER TABLE copy_trades ADD COLUMN trading_account_id INT REFERENCES trading_accounts(id) ON DELETE SET NULL;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_copy_trades_account ON copy_trades(trading_account_id);

-- Add trading_account_id to my_positions if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'my_positions' AND column_name = 'trading_account_id') THEN
        ALTER TABLE my_positions ADD COLUMN trading_account_id INT REFERENCES trading_accounts(id) ON DELETE CASCADE;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_my_positions_account ON my_positions(trading_account_id);

-- Insert default account if not exists
INSERT INTO trading_accounts (name, private_key_env_var, funder_address_env_var, signature_type, enabled, is_default, description)
VALUES ('Default Account', 'POLYMARKET_PRIVATE_KEY', 'POLYMARKET_FUNDER_ADDRESS', 1, TRUE, TRUE, 'Default trading account')
ON CONFLICT (private_key_env_var) DO NOTHING;
