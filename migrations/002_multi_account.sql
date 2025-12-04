-- Migration: Multi-Account Support for Copy Trading
-- Date: December 2024
-- Description: Adds support for multiple Polymarket trading accounts

-- ============================================================================
-- TRADING ACCOUNTS TABLE
-- ============================================================================
-- Stores configuration for each trading account (private keys stored in env vars)

CREATE TABLE IF NOT EXISTS trading_accounts (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,                    -- Human-readable name: "Main", "Bot Account"
    private_key_env_var VARCHAR(100) NOT NULL,     -- Env var name: "POLYMARKET_PRIVATE_KEY_1"
    funder_address_env_var VARCHAR(100),           -- Env var name: "POLYMARKET_FUNDER_ADDRESS_1" (nullable for EOA)
    signature_type INT DEFAULT 1,                  -- 0=EOA, 1=Magic/Email wallet
    enabled BOOLEAN DEFAULT TRUE,
    is_default BOOLEAN DEFAULT FALSE,             -- Only one account should be default
    description TEXT,                              -- Optional notes about this account
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(private_key_env_var),
    UNIQUE(name)
);

-- Index for quick lookups
CREATE INDEX IF NOT EXISTS idx_trading_accounts_enabled ON trading_accounts(enabled);
CREATE INDEX IF NOT EXISTS idx_trading_accounts_default ON trading_accounts(is_default) WHERE is_default = TRUE;

-- ============================================================================
-- ALTER USER_COPY_SETTINGS
-- ============================================================================
-- Add trading_account_id to specify which account to use for each followed user

-- Add column (nullable to allow migration)
ALTER TABLE user_copy_settings
ADD COLUMN IF NOT EXISTS trading_account_id INT REFERENCES trading_accounts(id) ON DELETE SET NULL;

-- Add strategy_type if not exists (1=human, 2=bot, 3=btc_15m)
ALTER TABLE user_copy_settings
ADD COLUMN IF NOT EXISTS strategy_type INT DEFAULT 1;

-- Add max_usd if not exists (null = no cap)
ALTER TABLE user_copy_settings
ADD COLUMN IF NOT EXISTS max_usd DECIMAL(20, 8);

-- Index for account lookups
CREATE INDEX IF NOT EXISTS idx_user_copy_settings_account ON user_copy_settings(trading_account_id);

-- ============================================================================
-- ALTER COPY_TRADES (Audit Table)
-- ============================================================================
-- Track which account executed each copy trade

ALTER TABLE copy_trades
ADD COLUMN IF NOT EXISTS trading_account_id INT REFERENCES trading_accounts(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_copy_trades_account ON copy_trades(trading_account_id);

-- ============================================================================
-- ALTER MY_POSITIONS
-- ============================================================================
-- Track positions per account (each account has its own positions)

-- First, we need to change the primary key to include trading_account_id
-- This requires dropping and recreating the table or doing it carefully

-- Add the column first (nullable for existing data)
ALTER TABLE my_positions
ADD COLUMN IF NOT EXISTS trading_account_id INT REFERENCES trading_accounts(id) ON DELETE CASCADE;

-- Set default account_id to 1 for existing positions (will be created below)
-- This will be run after inserting the default account

-- Create new composite index
CREATE INDEX IF NOT EXISTS idx_my_positions_account ON my_positions(trading_account_id);
CREATE INDEX IF NOT EXISTS idx_my_positions_account_token ON my_positions(trading_account_id, token_id);

-- ============================================================================
-- COPY_TRADE_LOG TABLE (Create if not exists)
-- ============================================================================
-- Detailed logging of copy trade attempts

CREATE TABLE IF NOT EXISTS copy_trade_log (
    id SERIAL PRIMARY KEY,
    -- Following user info
    following_address VARCHAR(42) NOT NULL,
    following_trade_id VARCHAR(255),
    following_time TIMESTAMPTZ NOT NULL,
    following_shares DECIMAL(30, 18),  -- negative = buy, positive = sell
    following_price DECIMAL(10, 8),
    -- Follower (us) info
    follower_time TIMESTAMPTZ,
    follower_shares DECIMAL(30, 18),   -- negative = buy, positive = sell
    follower_price DECIMAL(10, 8),
    -- Trade info
    market_title TEXT,
    outcome VARCHAR(255),
    token_id VARCHAR(100),
    -- Status
    status VARCHAR(20) NOT NULL,       -- 'success', 'executed', 'failed', 'skipped'
    failed_reason TEXT,
    strategy_type INT DEFAULT 1,       -- 1=human, 2=bot, 3=btc_15m
    -- Trading account
    trading_account_id INT REFERENCES trading_accounts(id) ON DELETE SET NULL,
    -- Debug info (JSON)
    debug_log JSONB,
    timing_breakdown JSONB,
    -- Timing timestamps for latency analysis
    detected_at TIMESTAMPTZ,
    processing_started_at TIMESTAMPTZ,
    order_placed_at TIMESTAMPTZ,
    order_confirmed_at TIMESTAMPTZ,
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_copy_trade_log_following ON copy_trade_log(following_address);
CREATE INDEX IF NOT EXISTS idx_copy_trade_log_time ON copy_trade_log(following_time DESC);
CREATE INDEX IF NOT EXISTS idx_copy_trade_log_status ON copy_trade_log(status);
CREATE INDEX IF NOT EXISTS idx_copy_trade_log_account ON copy_trade_log(trading_account_id);

-- ============================================================================
-- COPY_TRADE_PNL TABLE (P&L tracking per market)
-- ============================================================================

CREATE TABLE IF NOT EXISTS copy_trade_pnl (
    id SERIAL PRIMARY KEY,
    market_title TEXT NOT NULL,
    outcome VARCHAR(255) NOT NULL,
    token_id VARCHAR(100),
    trading_account_id INT REFERENCES trading_accounts(id) ON DELETE SET NULL,
    -- Position info
    total_shares DECIMAL(30, 18) DEFAULT 0,
    avg_entry_price DECIMAL(10, 8),
    total_cost DECIMAL(20, 8) DEFAULT 0,
    -- Current/resolved value
    current_price DECIMAL(10, 8),
    current_value DECIMAL(20, 8),
    -- P&L
    unrealized_pnl DECIMAL(20, 8),
    realized_pnl DECIMAL(20, 8) DEFAULT 0,
    -- Market status
    is_resolved BOOLEAN DEFAULT FALSE,
    resolution_outcome VARCHAR(255),
    resolved_at TIMESTAMPTZ,
    -- Metadata
    first_trade_at TIMESTAMPTZ,
    last_trade_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(market_title, outcome, trading_account_id)
);

CREATE INDEX IF NOT EXISTS idx_copy_trade_pnl_account ON copy_trade_pnl(trading_account_id);
CREATE INDEX IF NOT EXISTS idx_copy_trade_pnl_resolved ON copy_trade_pnl(is_resolved);

-- ============================================================================
-- WALLET_BALANCES TABLE (Track balance per account)
-- ============================================================================

CREATE TABLE IF NOT EXISTS wallet_balances (
    id SERIAL PRIMARY KEY,
    trading_account_id INT REFERENCES trading_accounts(id) ON DELETE CASCADE,
    wallet_address VARCHAR(42) NOT NULL,
    usdc_balance DECIMAL(20, 8),
    matic_balance DECIMAL(20, 18),
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wallet_balances_account ON wallet_balances(trading_account_id);
CREATE INDEX IF NOT EXISTS idx_wallet_balances_time ON wallet_balances(recorded_at DESC);

-- ============================================================================
-- INSERT DEFAULT ACCOUNT (for migration)
-- ============================================================================
-- Create a default account using existing env vars

INSERT INTO trading_accounts (
    name,
    private_key_env_var,
    funder_address_env_var,
    signature_type,
    enabled,
    is_default,
    description
) VALUES (
    'Default Account',
    'POLYMARKET_PRIVATE_KEY',
    'POLYMARKET_FUNDER_ADDRESS',
    1,  -- Magic/Email wallet
    TRUE,
    TRUE,
    'Default trading account using original environment variables'
) ON CONFLICT (private_key_env_var) DO NOTHING;

-- Update existing user_copy_settings to use default account
UPDATE user_copy_settings
SET trading_account_id = (SELECT id FROM trading_accounts WHERE is_default = TRUE LIMIT 1)
WHERE trading_account_id IS NULL;

-- Update existing my_positions to use default account
UPDATE my_positions
SET trading_account_id = (SELECT id FROM trading_accounts WHERE is_default = TRUE LIMIT 1)
WHERE trading_account_id IS NULL;

-- Update existing copy_trades to use default account
UPDATE copy_trades
SET trading_account_id = (SELECT id FROM trading_accounts WHERE is_default = TRUE LIMIT 1)
WHERE trading_account_id IS NULL;

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Function to get account details with wallet address resolved from env
CREATE OR REPLACE FUNCTION get_trading_account_summary()
RETURNS TABLE (
    account_id INT,
    account_name VARCHAR,
    is_enabled BOOLEAN,
    is_default BOOLEAN,
    followed_users_count BIGINT,
    positions_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        ta.id,
        ta.name,
        ta.enabled,
        ta.is_default,
        COUNT(DISTINCT ucs.user_address),
        COUNT(DISTINCT mp.token_id)
    FROM trading_accounts ta
    LEFT JOIN user_copy_settings ucs ON ucs.trading_account_id = ta.id AND ucs.enabled = TRUE
    LEFT JOIN my_positions mp ON mp.trading_account_id = ta.id AND mp.size > 0
    GROUP BY ta.id, ta.name, ta.enabled, ta.is_default
    ORDER BY ta.is_default DESC, ta.name;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- GRANTS
-- ============================================================================

GRANT ALL PRIVILEGES ON trading_accounts TO polymarket;
GRANT ALL PRIVILEGES ON copy_trade_log TO polymarket;
GRANT ALL PRIVILEGES ON copy_trade_pnl TO polymarket;
GRANT ALL PRIVILEGES ON wallet_balances TO polymarket;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO polymarket;
