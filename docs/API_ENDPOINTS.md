# Polymarket Copy Trading Platform - API Endpoints Documentation

> **Generated**: December 2024
> **Purpose**: Complete reference of all API endpoints for multi-account implementation planning

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Main App Endpoints](#main-app-endpoints-port-8081)
3. [Internal API Endpoints](#internal-api-endpoints)
4. [Analytics Worker (Background)](#analytics-worker-background-services)
5. [Service Layer Methods](#service-layer-methods)
6. [Storage Layer Methods](#storage-layer-methods)
7. [Copy Trading Flow](#copy-trading-flow)
8. [Multi-Account Implementation Impact](#multi-account-implementation-impact)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         MAIN APP (main.go)                              │
│                         Port: 8081                                       │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────────────────┐   │
│  │ Public Routes │  │  API Routes   │  │    Internal API           │   │
│  │ GET /         │  │ /api/*        │  │ POST /api/internal/       │   │
│  └───────────────┘  └───────────────┘  │      execute-copy-trade   │   │
│                                        └───────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ HTTP calls
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    ANALYTICS WORKER (analytics-worker/main.go)          │
│                         Background Services                              │
│  ┌──────────────────┐  ┌─────────────────┐  ┌────────────────────────┐ │
│  │ IncrementalWorker│  │   CopyTrader    │  │   RealtimeDetector     │ │
│  │ (500ms polling)  │  │ (trade exec)    │  │   (Mempool + Polygon)  │ │
│  └──────────────────┘  └─────────────────┘  └────────────────────────┘ │
│  ┌──────────────────┐  ┌─────────────────┐  ┌────────────────────────┐ │
│  │ BalanceTracker   │  │  AutoRedeemer   │  │  BinancePriceSyncer    │ │
│  │ (30s interval)   │  │  (2min cycle)   │  │  (1hr updates)         │ │
│  └──────────────────┘  └─────────────────┘  └────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          STORAGE LAYER                                   │
│  ┌─────────────────────────────┐  ┌─────────────────────────────────┐  │
│  │      PostgreSQL             │  │           Redis                  │  │
│  │  - users                    │  │  - user:* (profile cache)        │  │
│  │  - user_trades              │  │  - trades:* (trade cache)        │  │
│  │  - user_copy_settings       │  │  - rankings:* (ranking cache)    │  │
│  │  - copy_trades              │  │                                  │  │
│  │  - copy_trade_log           │  │                                  │  │
│  │  - my_positions             │  │                                  │  │
│  │  - trading_accounts (NEW)   │  │                                  │  │
│  └─────────────────────────────┘  └─────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Main App Endpoints (Port 8081)

### Public Routes (No Additional Validation)

| Method | Endpoint | Handler | Description |
|--------|----------|---------|-------------|
| GET | `/` | `h.Index` | Serves main HTML page (`templates/index.html`) |
| GET | `/api/subjects` | `h.GetSubjects` | Returns list of available subjects (politics, sports, etc.) |
| GET | `/static/*` | Static files | Serves CSS/JS from `./static` directory |

---

### API Routes (`/api/*`)

#### User Discovery & Analytics

| Method | Endpoint | Handler | Description | Query Params |
|--------|----------|---------|-------------|--------------|
| GET | `/api/top-users` | `h.GetTopUsers` | Get ranked traders by subject | `subject`, `limit`, `min_trades`, `max_trades`, `min_win_rate`, `max_win_rate`, `min_pnl`, `max_pnl`, `hide_red_flags` |
| GET | `/api/users/imported` | `h.GetAllImportedUsers` | Get all manually imported users | - |
| GET | `/api/analytics` | `h.GetAnalyticsList` | Advanced analytics with filtering | `sort`, `order`, `limit`, `offset`, `min_pnl`, `max_pnl`, `min_volume`, `max_volume`, `min_bets`, `max_bets`, `hide_bots`, `data_complete` |
| GET | `/api/copy-trade-metrics` | `h.GetCopyTradeMetrics` | Real-time copy trading performance | - |

#### User Import

| Method | Endpoint | Handler | Description | Request Body |
|--------|----------|---------|-------------|--------------|
| POST | `/api/import-top-users` | `h.ImportTopUsers` | Bulk import user trade history | `{"addresses": ["0x...", "0x..."]}` |
| GET | `/api/import-status/:id` | `h.GetImportStatus` | Check import job status | - |

---

### User-Specific Routes (`/api/users/:id/*`)

> **Middleware**: `middleware.ValidateUserID()` - validates Ethereum address format

| Method | Endpoint | Handler | Description | Query Params |
|--------|----------|---------|-------------|--------------|
| GET | `/api/users/:id` | `h.GetUserProfile` | Get detailed user profile | - |
| GET | `/api/users/:id/trades` | `h.GetUserTrades` | Get user's trade history | `limit`, `live` (true=fetch from API) |
| GET | `/api/users/:id/positions` | `h.GetUserPositions` | Get aggregated positions by market | - |
| GET | `/api/users/:id/analysis` | `h.GetUserAnalysis` | Get behavioral pattern analysis | - |
| DELETE | `/api/users/:id` | `h.DeleteUser` | Remove user and all associated data | - |
| **GET** | **`/api/users/:id/copy-settings`** | **`h.GetUserCopySettings`** | **Get copy trading settings for followed user** | - |
| **PUT** | **`/api/users/:id/copy-settings`** | **`h.UpdateUserCopySettings`** | **Update copy trading settings** | - |

#### Copy Settings Request/Response

**GET `/api/users/:id/copy-settings`**
```json
// Response
{
  "settings": {
    "user_address": "0x...",
    "multiplier": 0.05,
    "enabled": true,
    "min_usdc": 1.0,
    "strategy_type": 1,    // 1=human, 2=bot, 3=btc_15m
    "max_usd": null        // null = no cap
    // MISSING: trading_account_id (to be added)
  }
}
```

**PUT `/api/users/:id/copy-settings`**
```json
// Request
{
  "multiplier": 0.05,      // Optional: 0-1 (fraction of copied trade)
  "enabled": true,         // Optional: enable/disable copying
  "min_usdc": 1.0          // Optional: minimum order size
  // MISSING: trading_account_id (to be added)
}

// Response
{
  "success": true,
  "settings": { ... }
}
```

---

### HTML Pages

| Method | Endpoint | Handler | Description |
|--------|----------|---------|-------------|
| GET | `/users/:id` | `h.UserProfilePage` | Renders HTML profile page (`templates/profile.html`) |

---

## Internal API Endpoints

> **Purpose**: Worker-to-main-app communication for critical trade execution path

| Method | Endpoint | Handler | Description |
|--------|----------|---------|-------------|
| POST | `/api/internal/execute-copy-trade` | `tradeExecutor.HandleExecuteRequest` | Execute a copy trade (critical path) |

#### Execute Copy Trade Request/Response

**Request:**
```json
{
  "following_address": "0x...",      // Trader being copied
  "following_trade_id": "...",       // Original trade ID
  "following_time": 1701234567890,   // Unix timestamp (ms)
  "token_id": "12345...",            // Polymarket token ID
  "side": "BUY",                     // BUY or SELL
  "following_price": 0.65,           // Price trader paid
  "following_shares": 100.0,         // Shares trader bought
  "market_title": "Will X happen?",  // Market name
  "outcome": "Yes",                  // Yes/No outcome
  "multiplier": 0.05,                // Our copy multiplier
  "min_order_usdc": 1.0              // Minimum order size
  // MISSING: trading_account_id (to be added)
}
```

**Response:**
```json
{
  "success": true,
  "status": "executed",              // executed, skipped, failed
  "order_id": "0x...",
  "follower_price": 0.66,            // Price we paid
  "follower_shares": 5.0,            // Shares we bought
  "execution_ms": 150,               // Execution time
  "debug_log": "..."
}
```

---

## Analytics Worker (Background Services)

The analytics worker runs as a separate process and does NOT expose HTTP endpoints. It runs background services:

### Services Started

| Service | Interval | Purpose | Config |
|---------|----------|---------|--------|
| **IncrementalWorker** | 500ms | Poll followed users for new trades | - |
| **CopyTrader** | On detection | Execute copy trades when enabled | `COPY_TRADER_ENABLED=true` |
| **RealtimeDetector** | Real-time | WebSocket trade detection (Mempool + Polygon) | Always enabled |
| **BalanceTracker** | 30s | Track wallet USDC balance | `POLYMARKET_FUNDER_ADDRESS` |
| **AutoRedeemer** | 2min | Redeem resolved positions | Always enabled |
| **BinancePriceSyncer** | 1hr | Sync BTC/USDT price data | `BINANCE_PRICE_SYNCER_ENABLED` |
| **P&L Refresh** | 15min | Refresh copy trade P&L data | Always enabled |

### Trade Detection Flow

```
1. Mempool WebSocket → Pending transactions (~3-5s before block)
        ↓
2. Polygon WebSocket → OrderFilled events (~1-2s after block)
        ↓
3. RealtimeDetector.handleMempoolTrade() / handleBlockchainTrade()
        ↓
4. Check if trader is in followedUsers map (in-memory cache)
        ↓
5. Deduplicate using TxHash:LogIndex
        ↓
6. Call onNewTrade callback → CopyTrader.handleRealtimeTrade()
        ↓
7. Get user settings (including trading_account_id - TO BE ADDED)
        ↓
8. Get ClobClient for that account (TO BE ADDED)
        ↓
9. Execute copy trade
```

---

## Service Layer Methods

**File**: `service/service.go`

| Method | Purpose | Used By |
|--------|---------|---------|
| `GetTopUsersBySubject()` | Get ranked users | `/api/top-users` |
| `GetUserProfile()` | Get single user | `/api/users/:id` |
| `GetUserTrades()` | Get trades from DB | `/api/users/:id/trades` |
| `GetUserTradesLive()` | Fetch trades from API | `/api/users/:id/trades?live=true` |
| `AggregateUserPositions()` | Aggregate positions | `/api/users/:id/positions` |
| `StartImportJob()` | Start async import | `/api/import-top-users` |
| `GetImportJob()` | Get job status | `/api/import-status/:id` |
| `GetCopyTradeMetrics()` | Copy trade stats | `/api/copy-trade-metrics` |
| `DeleteUser()` | Delete user data | `DELETE /api/users/:id` |
| **`GetUserCopySettings()`** | **Get copy settings** | **`/api/users/:id/copy-settings`** |
| **`SetUserCopySettings()`** | **Update copy settings** | **`PUT /api/users/:id/copy-settings`** |
| `GetUserAnalyticsList()` | Advanced analytics | `/api/analytics` |
| `GetUserAnalysis()` | Behavioral analysis | `/api/users/:id/analysis` |

---

## Storage Layer Methods

**File**: `storage/postgres.go`

### Copy Trading Related (🎯 Multi-Account Impact)

| Method | Purpose | Multi-Account Impact |
|--------|---------|---------------------|
| 🎯 `GetUserCopySettings()` | Get settings for followed user | **Add `trading_account_id` field** |
| 🎯 `SetUserCopySettings()` | Update settings | **Add `trading_account_id` field** |
| `SaveCopyTrade()` | Log copy trade (old table) | Add `trading_account_id` for audit |
| `SaveCopyTradeLog()` | Log copy trade (new detailed table) | Add `trading_account_id` for audit |
| `GetCopyTradeStats()` | Get aggregate stats | Group by account (optional) |
| `GetCopyTradeLogs()` | Get recent logs | Filter by account (optional) |

### Position Tracking (🎯 Multi-Account Impact)

| Method | Purpose | Multi-Account Impact |
|--------|---------|---------------------|
| 🎯 `UpdateMyPosition()` | Update our position in market | **Track per-account positions** |
| 🎯 `GetMyPosition()` | Get our position | **Filter by account** |
| 🎯 `ClearMyPosition()` | Clear position after sell | **Account-aware** |
| 🎯 `GetAllMyPositions()` | Get all our positions | **Filter by account** |

### User & Trade Management (No Change Needed)

| Method | Purpose |
|--------|---------|
| `SaveUserSnapshot()` | Upsert user data |
| `ReplaceAllUsers()` | Replace all users |
| `SaveTrades()` | Batch save trades |
| `ListUserTrades()` | Get user trades |
| `ListUsers()` | Get users by subject |
| `GetUser()` | Get single user |
| `SaveUserPositions()` | Save aggregated positions |
| `GetUserPositions()` | Get aggregated positions |

---

## Copy Trading Flow

### Current Flow (Single Account)

```
┌─────────────────────────────────────────────────────────────────────────┐
│  1. Trade Detected (RealtimeDetector)                                   │
│     ↓                                                                   │
│  2. Get UserCopySettings for followed trader                            │
│     - multiplier, enabled, min_usdc, strategy_type                      │
│     ↓                                                                   │
│  3. CopyTrader creates trade using SINGLE ClobClient                    │
│     - Uses POLYMARKET_PRIVATE_KEY env var                               │
│     - Uses POLYMARKET_FUNDER_ADDRESS env var                            │
│     ↓                                                                   │
│  4. Execute trade, log to copy_trades & copy_trade_log                  │
│     ↓                                                                   │
│  5. Update my_positions                                                 │
└─────────────────────────────────────────────────────────────────────────┘
```

### New Flow (Multi-Account)

```
┌─────────────────────────────────────────────────────────────────────────┐
│  1. Trade Detected (RealtimeDetector)                                   │
│     ↓                                                                   │
│  2. Get UserCopySettings for followed trader                            │
│     - multiplier, enabled, min_usdc, strategy_type                      │
│     - trading_account_id (NEW)                                          │
│     ↓                                                                   │
│  3. Get TradingAccount from trading_accounts table                      │
│     - name, private_key_env_var, funder_address_env_var                 │
│     ↓                                                                   │
│  4. AccountManager.GetClient(account_id)                                │
│     - Load private key from env var (e.g., POLYMARKET_PRIVATE_KEY_1)    │
│     - Create/cache ClobClient for this account                          │
│     ↓                                                                   │
│  5. Execute trade with account-specific ClobClient                      │
│     ↓                                                                   │
│  6. Log to copy_trades & copy_trade_log (with trading_account_id)       │
│     ↓                                                                   │
│  7. Update my_positions (with trading_account_id)                       │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Multi-Account Implementation Impact

### Database Changes

#### New Table: `trading_accounts`

```sql
CREATE TABLE trading_accounts (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,                    -- "Main", "Bot Account", etc.
    private_key_env_var VARCHAR(100) NOT NULL,     -- "POLYMARKET_PRIVATE_KEY_1"
    funder_address_env_var VARCHAR(100),           -- "POLYMARKET_FUNDER_ADDRESS_1" (nullable for EOA)
    signature_type INT DEFAULT 1,                  -- 0=EOA, 1=Magic/Email
    enabled BOOLEAN DEFAULT TRUE,
    is_default BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(private_key_env_var)
);
```

#### Modify: `user_copy_settings`

```sql
ALTER TABLE user_copy_settings
ADD COLUMN trading_account_id INT REFERENCES trading_accounts(id);
```

#### Modify: `copy_trade_log` (for audit trail)

```sql
ALTER TABLE copy_trade_log
ADD COLUMN trading_account_id INT REFERENCES trading_accounts(id);
```

#### Modify: `my_positions` (track per-account)

```sql
ALTER TABLE my_positions
ADD COLUMN trading_account_id INT REFERENCES trading_accounts(id);
-- Update primary key to include trading_account_id
```

---

### New API Endpoints Needed

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/api/trading-accounts` | List all trading accounts |
| POST | `/api/trading-accounts` | Create new trading account |
| PUT | `/api/trading-accounts/:id` | Update trading account |
| DELETE | `/api/trading-accounts/:id` | Delete trading account |
| GET | `/api/trading-accounts/:id/balance` | Get account balance |
| GET | `/api/trading-accounts/:id/positions` | Get positions for account |

---

### Code Changes Summary

| File | Changes Needed |
|------|----------------|
| `init.sql` | Add `trading_accounts` table, modify `user_copy_settings`, `copy_trade_log`, `my_positions` |
| `storage/postgres.go` | Add `TradingAccount` struct, CRUD methods, modify existing methods |
| **NEW** `api/account_manager.go` | `AccountManager` to create/cache ClobClients per account |
| `api/auth.go` | Add `NewAuthFromKey(privateKey string)` constructor |
| `syncer/copy_trader.go` | Use `AccountManager` instead of single `clobClient` |
| `syncer/trade_executor.go` | Accept `trading_account_id` in request |
| `handlers/handlers.go` | Add trading account endpoints, modify copy settings handlers |
| `service/service.go` | Add trading account service methods |
| `analytics-worker/syncer/copy_trader.go` | Same changes as main syncer |
| `analytics-worker/storage/postgres.go` | Same changes as main storage |

---

### Environment Variables

**Current:**
```bash
POLYMARKET_PRIVATE_KEY=0x...
POLYMARKET_FUNDER_ADDRESS=0x...
```

**New (Multi-Account):**
```bash
# Account 1 (default)
POLYMARKET_PRIVATE_KEY_1=0x...
POLYMARKET_FUNDER_ADDRESS_1=0x...

# Account 2
POLYMARKET_PRIVATE_KEY_2=0x...
POLYMARKET_FUNDER_ADDRESS_2=0x...

# Account N
POLYMARKET_PRIVATE_KEY_N=0x...
POLYMARKET_FUNDER_ADDRESS_N=0x...
```

---

## Files Reference

| File | Purpose |
|------|---------|
| `main.go` | Main app entry point, route definitions |
| `handlers/handlers.go` | HTTP request handlers |
| `service/service.go` | Business logic layer |
| `storage/postgres.go` | PostgreSQL data access |
| `api/auth.go` | EIP-712 authentication |
| `api/clob.go` | CLOB API client (order placement) |
| `syncer/copy_trader.go` | Copy trade execution (main) |
| `syncer/trade_executor.go` | Trade executor for main app |
| `analytics-worker/main.go` | Worker entry point |
| `analytics-worker/syncer/copy_trader.go` | Copy trade execution (worker) |
| `analytics-worker/syncer/realtime_detector.go` | Real-time trade detection |
| `analytics-worker/syncer/incremental.go` | Incremental sync polling |
| `init.sql` | Database schema |

---

## Summary

### Endpoints Affected by Multi-Account

1. **`GET /api/users/:id/copy-settings`** - Add `trading_account_id` to response
2. **`PUT /api/users/:id/copy-settings`** - Accept `trading_account_id` in request
3. **`POST /api/internal/execute-copy-trade`** - Accept `trading_account_id` in request
4. **`GET /api/copy-trade-metrics`** - Optionally group by account

### New Endpoints Needed

1. `GET /api/trading-accounts`
2. `POST /api/trading-accounts`
3. `PUT /api/trading-accounts/:id`
4. `DELETE /api/trading-accounts/:id`
5. `GET /api/trading-accounts/:id/balance`
6. `GET /api/trading-accounts/:id/positions`

### Key Components to Modify

1. **AccountManager** (NEW) - Manage multiple ClobClients
2. **CopyTrader** - Use AccountManager for account selection
3. **RealtimeDetector** - Pass account info to CopyTrader
4. **Storage** - Account-aware position tracking
