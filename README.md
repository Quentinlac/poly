# Polymarket Top Traders Analyzer

A web application to identify and rank skilled traders on Polymarket across different subjects (politics, sports, finance, geopolitics, tech, culture, economy).

## Features

- **Top 10,000 Users Ranking**: Lists top traders based on comprehensive metrics
- **Subject Filtering**: Filter by politics, sports, finance, geopolitics, tech, culture, economy
- **Advanced Metrics**:
  - Trade count (100+ trades for experience)
  - Profit & Loss (P&L) over time (derived from closed positions)
  - Win rate (55-60%+)
  - Consistency across market types
- **Red Flag Detection**: Identifies traders to avoid (obvious bets, few trades, meme markets)
- **Market Selection**: Focus on information asymmetry markets
- **Live Data Sync**: Background worker pulls markets from the Gamma API and trades/positions from the Data API
- **Persistent Cache**: SQLite backing store + in-memory cache with background refresh stubs
- **Interactive Filters**: Layer win-rate, consistency, trade-count, P&L, and red-flag filters on top of any subject ranking

## Setup

1. **Install Go 1.21 or later**
   - Download from https://golang.org/dl/

2. **Install dependencies:**
   ```bash
   make deps
   # or manually:
   GOWORK=off go mod download
   ```

3. **Configure environment + app settings (optional):**
   ```bash
   cp .env.example .env
   # Edit .env with your Polymarket API credentials if available
   cp config/default.yaml config/local.yaml
   # Adjust scoring thresholds, cache TTLs, sync cadence, etc.
   export POLYMARKET_CONFIG=config/local.yaml
   # Set data.db_path if you want the SQLite file elsewhere
   ```

4. **Build the application:**
   ```bash
   make build
   # or manually:
   GOWORK=off go build -o polymarket-analyzer .
   ```

5. **Run the application:**
   ```bash
   make run
   # or manually:
   GOWORK=off go run main.go
   # or if built:
   ./polymarket-analyzer
   ```

6. **Open browser to `http://localhost:8081`**

## Note on Workspace Files

If you encounter errors about `go.work` files, use `GOWORK=off` prefix for Go commands, or use the Makefile which handles this automatically.

## Architecture

- **Backend**: Go with Gin web framework
- **Frontend**: HTML templates with vanilla JavaScript
- **API Client**: Polymarket Gamma API (markets/tags) + Data API (trades/positions)
- **Service Layer**: Ranking + caching orchestration
- **Storage**: SQLite database (`data/polymarket.db`) for aggregated leaderboards
- **Syncer**: Background worker that fetches fresh data on a schedule
- **Data Processing**: Ranking and filtering algorithms powered by `analyzer.Ranker`

## Background Sync

- `syncer.Worker` periodically:
  - downloads the latest markets from `https://gamma-api.polymarket.com`
  - pulls trades and closed positions from `https://data-api.polymarket.com`
  - aggregates per-user/per-subject metrics and stores snapshots in SQLite
- Refresh cadence and ingestion limits are configurable via `config/default.yaml`

## Usage

1. Select a subject filter from the dropdown
2. Optionally set minimum trades, win rate %, consistency %, P&L $, or hide red-flagged traders
3. Click “Load Traders” to fetch the filtered leaderboard
4. View ranked results and use sortable columns to reorder them
5. Click on a user to see detailed profile

## Polymarket API References

The repo includes the latest OpenAPI specs from the official docs (`docs/polymarket/*`):

- `gamma-openapi.json` – public endpoints for markets, events, tags (base: `https://gamma-api.polymarket.com`)
- `data-api-openapi.yaml` – data endpoints (trades, positions, holders, builders) (base: `https://data-api.polymarket.com`)

The worker uses these endpoints directly and can be tuned via the `ingestion` section in `config/default.yaml`:

- `subjects`: which categories to track
- `max_markets_per_subject`: top markets (by volume) fetched per subject
- `max_trades_per_market`: number of trades pulled for each market
- `max_users`: cap on addresses tracked per refresh
- `max_closed_positions_per_user`: how many realized positions to fetch per user (used to compute P&L/win rate)

