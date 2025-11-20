# Import Top Users Feature

## Overview

The "Import Top Users" tab allows you to manually specify a list of Ethereum wallet addresses and fetch their **complete trading history** from Polymarket. All trades are stored in the local SQLite database for filtering and analysis.

## How to Use

### 1. Navigate to the Import Tab

Open http://localhost:8081 and click the **"Import Top Users"** tab at the top of the page.

### 2. Prepare Your Address List

You can provide wallet addresses in any of these formats:
- **One per line** (recommended):
  ```
  0x1234567890abcdef1234567890abcdef12345678
  0xabcdef1234567890abcdef1234567890abcdef12
  0x9876543210fedcba9876543210fedcba98765432
  ```

- **Comma-separated**:
  ```
  0x1234567890abcdef1234567890abcdef12345678, 0xabcdef1234567890abcdef1234567890abcdef12
  ```

- **Space-separated**:
  ```
  0x1234567890abcdef1234567890abcdef12345678 0xabcdef1234567890abcdef1234567890abcdef12
  ```

### 3. Start the Import

1. Paste your addresses into the text area
2. Click **"Start Import"**
3. Confirm the prompt (it will tell you how many addresses were detected)
4. Wait for completion

### 4. Monitor Progress

The import process will:
- Show a progress indicator while fetching data
- Display a detailed results table when complete, showing:
  - ✓/✗ Success/failure status per user
  - Number of trades imported
  - Time taken per user
  - Any error messages

## What Gets Imported

For each wallet address, the system fetches:
- **Up to 10,000 historical trades** per user
- **Up to 500 closed positions** (for P&L calculation)
- All trade metadata:
  - Market details (title, slug, event)
  - Trade side (BUY/SELL)
  - Size, price, outcome
  - Timestamp
  - Transaction hash
  - User profile (name, pseudonym, bio)

## Performance

- **Time per user**: ~30-60 seconds (depends on trade count and API rate limits)
- **Concurrency**: Sequential (one user at a time to respect API limits)
- **Example**: 10 users ≈ 5-10 minutes total

## API Limits

The import respects Polymarket's rate limits:
- Automatic retry on 429 (rate limit) errors
- Exponential backoff with up to 3 retries
- Configurable delays between requests

## After Import

Once imported, users appear in the **Leaderboard** tab:
1. Switch back to the "Leaderboard" tab
2. Use filters to find specific users
3. Click on any username to view their complete profile and trade history

## Troubleshooting

### Invalid Addresses
- Addresses must start with `0x`
- Must be exactly 42 characters (40 hex digits + `0x` prefix)
- Invalid addresses are automatically filtered out before import

### API Errors
Common errors and solutions:
- **"API client not available"**: Check that your `POLYMARKET_PRIVATE_KEY` is set in `.env`
- **"429 Too Many Requests"**: The system will automatically retry. If persistent, reduce the number of users per batch.
- **"Context deadline exceeded"**: Import timeout. Try importing fewer users at once.

### Missing Data
If a user has very few or no trades:
- They may be a new trader
- Their trades may be on markets outside your configured subjects
- Their wallet may not have traded on Polymarket

## Technical Details

### Backend Endpoint
```
POST /api/import-top-users
Content-Type: application/json

{
  "addresses": [
    "0x1234567890abcdef1234567890abcdef12345678",
    "0xabcdef1234567890abcdef1234567890abcdef12"
  ]
}
```

### Response Format
```json
{
  "total": 2,
  "success_count": 2,
  "failed_count": 0,
  "results": [
    {
      "address": "0x1234567890abcdef1234567890abcdef12345678",
      "success": true,
      "trade_count": 1234,
      "duration_sec": 45.2
    },
    {
      "address": "0xabcdef1234567890abcdef1234567890abcdef12",
      "success": true,
      "trade_count": 567,
      "duration_sec": 28.7
    }
  ]
}
```

## Database Storage

Imported trades are stored in the `user_trades` table:
```sql
CREATE TABLE user_trades (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    market_id TEXT,
    subject TEXT,
    side TEXT,
    size REAL,
    price REAL,
    outcome TEXT,
    timestamp TEXT,
    title TEXT,
    slug TEXT,
    event_slug TEXT,
    transaction_hash TEXT,
    name TEXT,
    pseudonym TEXT
);
```

Trades are **upserted** (updated if they already exist), so you can safely re-import the same users to refresh their data.

## Example Use Cases

1. **Track Specific Whales**: Import known high-volume traders and monitor their strategies
2. **Build Custom Leaderboard**: Import your own curated list of traders
3. **Historical Analysis**: Fetch complete history for specific addresses you're researching
4. **Refresh Data**: Re-import users periodically to update their latest trades

## Limitations

- Maximum **10,000 trades** per user (configurable in `service.ImportTopUsers`)
- Maximum **500 closed positions** per user (for P&L)
- Sequential processing (not parallelized to respect API limits)
- No real-time progress updates (results shown at the end)

## Configuration

To adjust import limits, edit `service/service.go`:

```go
// In GetUserTradesLive:
trades, err := s.GetUserTradesLive(ctx, normalized, 10000) // Change limit here

// In fetchAndStoreClosedPositions:
const maxPositions = 500 // Change closed positions limit here
```

