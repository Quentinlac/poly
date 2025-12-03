# Trade Detection Analysis & Fix Documentation

## Overview

This document explains the different trade detection methods in our Polymarket copy trading system, the critical bug we discovered in mempool decoding, and how we fixed it.

---

## Trade Detection Methods

We have 5 different methods to detect trades, each with different latencies and trade-offs:

| Method | Latency | Description |
|--------|---------|-------------|
| **MEMPOOL** | ~2-3s before confirmation | Monitors pending transactions in Polygon mempool |
| **POLYGON_LOGS** | ~0-2s after confirmation | Subscribes to OrderFilled events on-chain |
| **LIVEDATA** | ~650ms after match | WebSocket from ws-live-data.polymarket.com |
| **DATA_API** | 30-80s after match | Polls data-api.polymarket.com/activity |
| **CLOB_API** | ~50ms after match | Polls clob.polymarket.com/data/trades |

### 1. MEMPOOL Detection (Fastest)
- **Source**: `wss://polygon-bor-rpc.publicnode.com`
- **Method**: `eth_subscribe` to `newPendingTransactions`
- **Advantage**: Detects trades 2-3 seconds BEFORE they're confirmed on-chain
- **Challenge**: Must decode raw transaction input data (ABI decoding)

### 2. POLYGON_LOGS Detection
- **Source**: `wss://polygon-bor-rpc.publicnode.com`
- **Method**: `eth_subscribe` to logs with OrderFilled topic
- **Topic**: `0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6`
- **Advantage**: Reliable, includes all filled order details in event logs

### 3. LIVEDATA WebSocket
- **Source**: `wss://ws-live-data.polymarket.com/`
- **Method**: Subscribe to `orders_matched` topic
- **Advantage**: Easy to parse, includes outcome (Up/Down)

### 4. DATA_API Polling
- **Source**: `https://data-api.polymarket.com/activity`
- **Method**: HTTP polling every 2 seconds
- **Disadvantage**: High latency (30-80 seconds)

### 5. CLOB_API Polling
- **Source**: `https://clob.polymarket.com/data/trades`
- **Method**: HTTP polling every 500ms
- **Note**: Currently not detecting trades (needs investigation)

---

## The Critical Bug: Wrong Shares Amount from Mempool

### Problem
Our mempool detection was returning **wildly incorrect shares amounts**. For example:
- Expected: 24.00 shares
- Got: 0.003520 shares (garbage value)

### Root Cause Analysis

#### Polymarket Contract Functions

Two main functions are used for order execution:

**1. `fillOrders` (0x2287e350)** - NegRiskAdapter
```
Word 0: offset to orders[]
Word 1: offset to fillAmounts[]
Word 2: takerFillAmountUsdc (TOTAL)
Word 3: takerFillAmountShares (TOTAL across ALL orders)
```

**2. `matchOrders` (0xa4a6c5a5)** - CTFExchange
```
Word 0: takerOrder offset
Word 1: makerOrders[] offset
Word 2: takerFillAmount (USDC)
Word 3: takerReceiveAmount (shares - for TAKER only)
Word 4: makerFillAmounts[] offset
Word 5: takerFeeAmount
Word 6: makerFeeAmounts[] offset
```

#### Order Struct Layout (384 bytes = 12 fields × 32 bytes)
```
Offset  Field
0x00    salt
0x20    maker (address)
0x40    signer (address)
0x60    taker (address)
0x80    tokenId
0xA0    makerAmount
0xC0    takerAmount
0xE0    expiration
0x100   nonce
0x120   feeRateBps
0x140   side (0=BUY, 1=SELL)
0x160   signatureType
```

### The Two Bugs

#### Bug 1: Using Total Fill Instead of Per-Order Fill

**OLD CODE (WRONG)**:
```go
// Reading Word 3 directly as the fill amount
fillAmount = new(big.Int).SetBytes(dataBytes[96:128])
```

**Problem**: Word 3 contains the TOTAL fill across ALL orders in the transaction, not the specific order's fill amount.

**Example from actual TX**:
```
Transaction: 0x5a56cb8b9ceca...
Word 3 (total) = 12.00 shares

But this TX had 2 maker orders:
  - Maker 0 (our target): 7.80 shares
  - Maker 1: 4.20 shares
  - Total: 12.00 shares

We were returning 12.00 when target only filled 7.80!
```

#### Bug 2: Scanning at Wrong Byte Offsets

**OLD CODE (WRONG)**:
```go
// Scanning every 32-byte offset looking for valid orders
for offset := 32; offset+384 <= len(dataBytes); offset += 32 {
    // Check if this looks like an order...
    orders = append(orders, foundOrder{index: len(orders), ...})
}
```

**Problem**: This finds "orders" at arbitrary positions that aren't real orders. When we find the target at index X, it's not the same X as in the `fillAmounts[]` array!

**Result**: `fillAmounts[targetOrderIdx]` returns garbage data.

---

## The Fix

### Proper ABI Parsing

Instead of scanning, we now parse at the **correct ABI offsets**:

#### For `fillOrders`:
```go
ordersOffset := int(dataBytes[0:32])      // Word 0 points to orders array
fillAmountsOffset := int(dataBytes[32:64]) // Word 1 points to fillAmounts array

// Orders array structure:
// ordersOffset -> [length][order0][order1][order2]...
ordersLen := dataBytes[ordersOffset : ordersOffset+32]

// Find target in the REAL orders array
for i := 0; i < ordersLen; i++ {
    orderStart := ordersOffset + 32 + i*384  // Each order is 384 bytes
    if orderMatchesTarget(orderStart) {
        fillAmount = fillAmounts[i]  // Correct index!
    }
}
```

#### For `matchOrders`:
```go
takerOrderOffset := int(dataBytes[0:32])
makerOrdersOffset := int(dataBytes[32:64])
takerReceiveAmount := dataBytes[96:128]        // Word 3 - taker's shares
makerFillAmountsOffset := int(dataBytes[128:160])

// Check if target is the TAKER
if targetInTakerOrder {
    fillAmount = takerReceiveAmount  // Use Word 3 for taker
}

// Check if target is a MAKER
for i := 0; i < makersLen; i++ {
    orderStart := makerOrdersOffset + 32 + i*384
    if orderMatchesTarget(orderStart) {
        fillAmount = makerFillAmounts[i]  // Per-maker fill
    }
}
```

---

## Comparison Script

### Location
`analytics-worker/cmd/compare_methods/main.go`

### Usage
```bash
cd analytics-worker/cmd/compare_methods
go build -o compare_methods .
./compare_methods <target_address> <duration_minutes> [output.csv]
```

### Example
```bash
./compare_methods 0x05c1882212a41aa8d7df5b70eebe03d9319345b7 5 detection.csv
```

### What It Does

1. **Starts 5 concurrent monitors**:
   - Mempool WebSocket subscriber
   - Polygon logs WebSocket subscriber
   - LiveData WebSocket subscriber
   - Data API HTTP poller
   - CLOB API HTTP poller

2. **Tracks all trades by TX hash**:
   - First detection time per method
   - Shares amount, side, price, outcome
   - Which method detected it first

3. **Outputs CSV with columns**:
   - `tx_hash` - Transaction hash
   - `timestamp_detected` - When first detected
   - `timestamp_tx` - On-chain block time
   - `method` - Which method detected first
   - `shares`, `side`, `price`, `outcome`, `token_id`
   - Per-method details: `mempool_*`, `livedata_*`, `polygon_*`, `data_api_*`, `clob_*`

### CSV Output Columns
```
tx_hash,timestamp_detected,timestamp_tx,method,shares,outcome,side,token_id,price,
mempool_detected_at,mempool_shares,mempool_side,mempool_price,mempool_outcome,mempool_role,
mempool_maker_amount,mempool_taker_amount,mempool_fill_amount,
livedata_detected_at,livedata_shares,livedata_side,livedata_price,livedata_outcome,
polygon_detected_at,polygon_shares,polygon_side,polygon_price,polygon_token_id,
data_api_detected_at,data_api_shares,data_api_side,data_api_price,data_api_outcome,data_api_token_id,
clob_detected_at,clob_shares,clob_side,clob_price,clob_outcome,clob_token_id,
first_method
```

---

## Files Modified

### `analytics-worker/api/mempool_ws.go`
- `DecodeTradeInputForTarget()` - New proper ABI decoder
- `decodeTradeInputLegacy()` - Fallback for unknown function types
- `DecodedOrder` struct - Added `FillAmount`, `TotalFill`, `OrderIndex` fields

### `analytics-worker/cmd/compare_methods/main.go`
- `decodeOrderFull()` - Same proper ABI decoding logic
- `decodeOrderFallback()` - Legacy fallback

---

## Key Learnings

1. **Never assume Word 3 is your fill amount** - It's the TOTAL for all orders in TX
2. **Parse arrays at their ABI offsets** - Don't scan for patterns
3. **matchOrders vs fillOrders have different structures** - Taker vs Maker logic differs
4. **Index alignment is critical** - `fillAmounts[i]` must match `orders[i]`
5. **Always validate with ground truth** - Compare against Polygon logs or Data API

---

## Test Results (2025-12-02)

### 9-Minute Test Summary

| Metric | Value |
|--------|-------|
| Total Trades Detected | 34 |
| MEMPOOL First | 21 (61.8%) |
| POLYGON_LOGS First | 10 (29.4%) |
| LIVEDATA First | 3 (8.8%) |
| DATA_API First | 0 (0.0%) |
| CLOB_API First | 0 (0.0%) |

### Key Findings

1. **MEMPOOL is fastest** - Detects 61.8% of trades before any other method
2. **Detection latency**: MEMPOOL detects 2-4 seconds before on-chain confirmation
3. **Shares extraction still needs work** - mempool_shares shows 0.00 in many cases

### Current Status

**Working**:
- MEMPOOL timing detection (detects TX hash early)
- POLYGON_LOGS shares/price extraction
- LIVEDATA shares/price/outcome extraction
- DATA_API full trade details

**Needs Investigation**:
- MEMPOOL shares extraction returning 0 for some function selectors
- CLOB_API not detecting any trades (endpoint/token issue?)

### Sample Output

```
TX: 0x61bd17b7eac3cc...
  Detected by:     MEMPOOL at 20:45:33.195 (FIRST!)
  Confirmed:       20:45:37 (4 seconds later)
  Shares:          46.00
  Side:            BUY
  Price:           0.48
  Outcome:         Up
```

---

## Next Steps

1. Debug why mempool shares extraction fails for certain transactions
2. Investigate CLOB_API endpoint
3. Consider hybrid approach: Use MEMPOOL for timing + Polygon for details
