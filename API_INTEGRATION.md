# Polymarket API Integration Guide

## Overview

This application needs to integrate with Polymarket's API to fetch real user data, trades, and market information. The current implementation includes placeholder/mock data that needs to be replaced with actual API calls.

## Polymarket API Resources

1. **Official API Documentation**: https://docs.polymarket.com/
2. **Developer Resources**: https://www.polymarketexchange.com/developers.html
3. **Community SDK**: https://polymarket-data.com/ (TypeScript SDK - can be used as reference)

## Required API Endpoints

Based on the application requirements, you'll need to integrate with the following endpoints:

### 1. User Data Endpoints
- Get user information (username, address, etc.)
- Get user trade history
- Get user performance metrics

### 2. Market Data Endpoints
- List markets (with filtering by category/subject)
- Get market details (question, resolution, volume, etc.)
- Get market trades

### 3. Leaderboard/Top Users
- Get top users by various metrics
- Note: This endpoint may not exist directly - you may need to aggregate from user data

## Implementation Steps

### Step 1: Apply for API Access

1. Visit https://www.polymarketexchange.com/developers.html
2. Apply for API access
3. Complete integration testing
4. Obtain API keys/credentials

### Step 2: Update API Client

The `api/client.go` file contains placeholder implementations. You'll need to:

1. **Update Base URL**: Set the correct Polymarket API base URL
2. **Add Authentication**: Add API key/authentication headers
3. **Implement Real Endpoints**: Replace placeholder methods with actual API calls
4. **Handle Rate Limiting**: Implement rate limiting and retry logic
5. **Error Handling**: Add proper error handling for API responses

### Step 3: Data Mapping

Map Polymarket API responses to our internal models:

- **User Model**: Map API user data to `models.User`
- **Trade Model**: Map API trade data to `models.Trade`
- **Market Model**: Map API market data to `models.Market`

### Step 4: Market Classification

The `analyzer/processor.go` file includes a `ClassifyMarket` function that uses keyword matching. You may need to:

1. Use Polymarket's market categories if available
2. Enhance keyword matching with NLP
3. Use market tags/labels from the API

### Step 5: Data Processing Pipeline

1. **Fetch Markets**: Get all markets and classify by subject
2. **Fetch User Trades**: For each user, fetch their trade history
3. **Calculate Metrics**: Use `analyzer.Processor` to calculate:
   - Total trades per subject
   - P&L per subject
   - Win rate per subject
   - Consistency score
4. **Detect Red Flags**: Use `analyzer.Ranker` to identify problematic traders

### Step 6: Caching & Persistence Strategy

Data now flows through both SQLite (`storage.Store`) and in-memory caches (`service.Service`). When wiring the API, make sure to:

1. **Persist Snapshots**: Call `store.SaveUserSnapshot` for every processed user so rankings survive restarts.
2. **Hydrate Caches**: After persistence, invalidate/refresh `Service` caches (profile + rankings).
3. **Market Data**: Prepare a similar storage path once market schemas are added.
4. **Configurable Paths**: `config/*.yaml` allows changing the SQLite location via `data.db_path`.

### Step 7: Background Sync Hooks (`syncer/worker.go`)

Use the provided worker to schedule ingestion:

1. `syncMarkets`:
   - Fetch batches of markets based on `cfg.Sync.BatchSizeMarkets`
   - Classify subjects via `analyzer.Processor.ClassifyMarket`
   - Persist markets (add new tables as needed)
2. `syncUsers`:
   - Pull candidate trader lists (top performers, followed wallets, etc.)
   - Fetch their trades, run through `Processor`, persist via `store.SaveUserSnapshot`
   - Let the service cache provide fresh rankings to the UI
3. Tune intervals via `sync.market_refresh_minutes` and `sync.user_refresh_minutes`.

## Example API Client Update

```go
// In api/client.go

func (c *Client) GetUserTrades(userID string) ([]models.Trade, error) {
    url := fmt.Sprintf("%s/v1/users/%s/trades", c.BaseURL, userID)
    
    req, err := http.NewRequest("GET", url, nil)
    if err != nil {
        return nil, err
    }
    
    // Add authentication
    req.Header.Set("Authorization", "Bearer "+c.APIKey)
    req.Header.Set("Content-Type", "application/json")
    
    resp, err := c.HTTPClient.Do(req)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()
    
    // Parse response and map to models.Trade
    // ...
}
```

## Rate Limiting Considerations

Polymarket API may have rate limits. Implement:

1. Request throttling
2. Exponential backoff for retries
3. Batch processing for large datasets
4. Background jobs for data updates

## Data Updates

Since this is for personal use, consider:

1. **Initial Load**: Fetch all data on first run
2. **Incremental Updates**: Periodically update only changed data
3. **Background Sync**: Run updates in background goroutines
4. **Manual Refresh**: Add refresh button in UI

## Testing

Before going live:

1. Test with small datasets
2. Verify all metrics calculations
3. Test filtering and ranking
4. Verify red flag detection
5. Test error handling

## Alternative: Using Community SDK

If Polymarket's official API is complex, consider:

1. Using the community TypeScript SDK as reference
2. Creating a Go wrapper around their endpoints
3. Using GraphQL if available (Polymarket may have GraphQL endpoints)

## Notes
- The repository now ships with the official OpenAPI specs from [docs.polymarket.com](https://docs.polymarket.com):
  - `docs/polymarket/gamma-openapi.json` – REST endpoints for markets, tags, events (`https://gamma-api.polymarket.com`)
  - `docs/polymarket/data-api-openapi.yaml` – REST endpoints for trades, positions, builders (`https://data-api.polymarket.com`)
- The background worker consumes those endpoints directly (see `syncer/worker.go` + `syncer/leaderboard.go`).
- Configure ingestion limits (markets per subject, trades per market, closed positions per user, max tracked users) via the `ingestion` block in `config/default.yaml`.

- The actual Polymarket API structure may differ from what's assumed here
- You may need to aggregate data from multiple endpoints
- Some data might require web scraping if not available via API
- Consider using Polymarket's public data feeds if available

