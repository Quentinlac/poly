# Copy Trading Strategies

## Overview

Two strategy types for copy trading:

| Type | Name | Use Case |
|------|------|----------|
| 1 | Human | Following human traders - uses market orders with slippage tolerance |
| 2 | Bot | Following bots - uses limit orders to match exact prices |

## Database Schema Changes

```sql
ALTER TABLE user_copy_settings
ADD COLUMN strategy_type INTEGER DEFAULT 1,  -- 1=human, 2=bot
ADD COLUMN multiplier DECIMAL(10,4) DEFAULT 0.05;  -- position size multiplier
```

---

## Strategy Type 1: Human Following (Existing)

### BUY
1. Copied user buys X tokens
2. We calculate: `our_amount = X × multiplier`
3. We place MARKET order with tiered slippage:
   - Price < $0.10: 200% slippage allowed
   - Price < $0.20: 100% slippage allowed
   - Price < $0.30: 50% slippage allowed
   - Price < $0.40: 30% slippage allowed
   - Price ≥ $0.40: 20% slippage allowed
4. Fill-or-kill immediate execution

### SELL
1. Copied user sells
2. We sell our ENTIRE position at market
3. No slippage protection (takes best available)

---

## Strategy Type 2: Bot Following (NEW)

The key insight: bots trade at specific prices, so we should match those prices rather than taking market orders.

### BUY Logic

```
Input: Copied user bought at price P, for amount A
Our target: A × multiplier USDC worth

Step 1: Try to buy at exact price P
  - Check order book asks for orders at price P
  - If available, take them (up to our target amount)

Step 2: If not filled (or partially filled)
  - Get all asks from price P to P + 10%
  - Sort by price ASCENDING (cheapest first!)
  - For each price level:
    - Take available liquidity (up to remaining target)
    - Stop when target reached
  - Important: Use < comparison, not = (handle 6 decimal precision)

Step 3: If nothing within 10%, ABORT
  - Don't overpay more than 10% above copied price
  - Log: "No liquidity within 10% of copied price"
```

**Example BUY:**
```
Copied user: Buys at $0.45
Our multiplier: 0.05
Copied amount: $100
Our target: $5

Order book asks:
- $0.45: 50 tokens available  ← Try this first
- $0.46: 100 tokens available
- $0.47: 200 tokens available
- $0.50: 500 tokens available  ← This is 11% higher, SKIP

We buy up to $5 worth starting at $0.45
```

### SELL Logic

```
Input: Copied user sold at price P
Our position: Check actual position on Polymarket

Step 1: Try to sell at exact price P
  - Check order book bids for orders at price P
  - If available, sell into them

Step 2: If not filled (or partially filled)
  - Get all bids from price P down to P - 10%
  - Sort by price DESCENDING (highest first!)
  - For each price level:
    - Sell available liquidity
    - Stop when position cleared

Step 3: If STILL not filled after orderbook sweep
  - Create 3 LIMIT SELL orders (max 5% below P):

    | Order | % of Position | Price |
    |-------|---------------|-------|
    | 1     | 20%          | P (same price) |
    | 2     | 40%          | P - 3% |
    | 3     | 40%          | P - 5% |

Step 4: Wait up to 3 minutes for limit orders to fill
  - Check every 30 seconds
  - Track which orders filled

Step 5: After 3 minutes, if unfilled:
  - Cancel remaining limit orders
  - Market sell everything remaining
  - Take best available bids (sorted highest first)
```

**Example SELL:**
```
Copied user: Sells at $0.60
Our position: 100 tokens

Order book bids:
- $0.60: 20 tokens available  ← Take this first (20 tokens sold)
- $0.58: 30 tokens available  ← Take this (30 more sold)
- $0.55: 0 tokens available
- $0.54: 0 tokens available   ← 10% below, stop here

50 tokens remaining, no more bids within 10%

Create limit orders:
- Order 1: 10 tokens (20%) at $0.60
- Order 2: 20 tokens (40%) at $0.582 ($0.60 - 3%)
- Order 3: 20 tokens (40%) at $0.57 ($0.60 - 5%)

Wait 3 minutes...

If still unfilled: cancel and market sell remaining
```

---

## Price Precision

Polymarket uses different precision for different tokens:
- Most tokens: 2 decimal places ($0.45)
- Some tokens: Up to 6 decimal places ($0.456789)

When comparing prices, use `<` or `>` instead of `=` and round appropriately:
```go
// Check if price is within 10% higher
maxPrice := copiedPrice * 1.10
if askPrice <= maxPrice {
    // Consider this ask
}

// Check if price is within 10% lower
minPrice := copiedPrice * 0.90
if bidPrice >= minPrice {
    // Consider this bid
}
```

---

## Flow Diagram

```
                    ┌─────────────────┐
                    │ Trade Detected  │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │ Get User Settings│
                    │ strategy_type?   │
                    └────────┬────────┘
                             │
            ┌────────────────┴────────────────┐
            │                                  │
   ┌────────▼────────┐              ┌─────────▼─────────┐
   │  Type 1: Human  │              │   Type 2: Bot     │
   └────────┬────────┘              └─────────┬─────────┘
            │                                  │
   ┌────────▼────────┐              ┌─────────▼─────────┐
   │ Market Order    │              │ Price-Match Order │
   │ + Slippage      │              │ + Limit Orders    │
   └─────────────────┘              └───────────────────┘
```

---

## Safety Checks

1. **BUY Safety:**
   - Never pay more than 10% above copied price
   - Abort if no liquidity within range

2. **SELL Safety:**
   - Never accept less than 10% below copied price (for immediate fills)
   - Limit orders max 5% below copied price
   - After 3 min timeout, market sell to avoid holding

3. **Position Tracking:**
   - Always check actual Polymarket position before selling
   - Don't try to sell more than we own
   - Handle partial fills correctly

---

## Implementation Checklist

- [x] Add `strategy_type` and `multiplier` columns to `user_copy_settings`
- [x] Update `GetUserCopySettings()` to fetch new columns
- [x] Create `executeBotBuy()` function for Type 2 buys
- [x] Create `executeBotSell()` function for Type 2 sells
- [x] Update `executeCopy()` to route based on strategy_type
- [x] Write unit tests for bot strategy logic
- [ ] Create `createLimitOrder()` function for placing limit orders (future enhancement)
- [ ] Add order tracking table for pending limit orders (future enhancement)
- [ ] Add background job to check limit order status after 3 min (future enhancement)

## How to Enable Bot Strategy for a User

```sql
-- Set user to bot strategy with 10% multiplier
UPDATE user_copy_settings
SET strategy_type = 2, multiplier = 0.10
WHERE user_address = '0x...';

-- Check current settings
SELECT user_address, strategy_type, multiplier, enabled
FROM user_copy_settings;
```

## Files Modified

- `storage/postgres.go` - Added `StrategyType` to `UserCopySettings` struct and updated queries
- `syncer/copy_trader.go` - Added `executeBotBuy()` and `executeBotSell()` functions
- `syncer/copy_trader_test.go` - Added tests for bot strategy logic
