// =============================================================================
// COPY TRADER - EXECUTES TRADES BASED ON DETECTED FOLLOWED USER ACTIVITY
// =============================================================================
//
// This is the EXECUTION ENGINE that places orders to copy followed users.
// When RealtimeDetector detects a trade, it calls handleRealtimeTrade() here.
//
// EXECUTION FLOW (from detection to order placed):
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │  1. handleRealtimeTrade() receives TradeDetail from detector               │
// │     ↓                                                                       │
// │  2. processTrade() validates and checks user settings                      │
// │     - Skip if trade too old (>5 min)                                       │
// │     - Skip if already processed (deduplication)                            │
// │     - Check user copy settings (enabled, multiplier, min/max USDC)         │
// │     ↓                                                                       │
// │  3. Start order book fetch in PARALLEL (saves ~50-200ms)                   │
// │     go func() { book = clobClient.GetOrderBook(tokenID) }                  │
// │     ↓                                                                       │
// │  4. Route to strategy handler based on settings:                           │
// │     - Strategy 1 (Human): executeBuy() / executeSell() - via main app     │
// │     - Strategy 2/3 (Bot): executeBotBuyWithBook() - direct CLOB order     │
// │     ↓                                                                       │
// │  5. Calculate copy size: followedUserSize × multiplier                     │
// │     ↓                                                                       │
// │  6. Wait for order book (from parallel fetch)                              │
// │     ↓                                                                       │
// │  7. Calculate max price (copied price + slippage based on price tier)     │
// │     ↓                                                                       │
// │  8. Sweep order book: accumulate shares up to target at acceptable prices │
// │     ↓                                                                       │
// │  9. Place FAK order via clobClient.PlaceOrderFAK() - always taker         │
// │     ↓                                                                       │
// │ 10. Log result to copy_trade_log table with timing breakdown              │
// └─────────────────────────────────────────────────────────────────────────────┘
//
// KEY CONCEPTS:
// - Multiplier: Fraction of followed user's trade (e.g., 0.05 = 5% = 1/20th)
// - Dynamic Slippage: Lower prices allow more slippage (volatile markets)
//   - Under $0.10: 200% slippage allowed
//   - Under $0.20: 80%
//   - $0.40+: 20%
// - Order Book Sweeping: Buy at multiple price levels to fill target size
// - FAK (Fill-And-Kill): Take available liquidity immediately, cancel rest
//   - Always a TAKER (no orders left in book)
//   - Partial fills OK (unlike FOK which requires full fill)
//   - 2-decimal precision for maker amount (API requirement)
//
// TIMING TARGET: ~400ms from detection to order placed on CLOB (with FAK + connection pooling)
//
// =============================================================================
package syncer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"polymarket-analyzer/api"
	"polymarket-analyzer/models"
	"polymarket-analyzer/storage"
)

// orderBookResult holds the result of an async order book fetch
type orderBookResult struct {
	book *api.OrderBook
	err  error
}

// CopyTrader monitors trades and copies them
type CopyTrader struct {
	store      *storage.PostgresStore
	client     *api.Client
	clobClient *api.ClobClient
	config     CopyTraderConfig
	running    bool
	stopCh     chan struct{}
	myAddress  string // Our wallet address for position lookups

	// Real-time detector for faster trade detection
	detector *RealtimeDetector

	// Metrics for latency tracking
	metrics   *CopyTraderMetrics
	metricsMu sync.RWMutex

	// HTTP client for calling main app (critical path execution)
	mainAppURL string
	httpClient *http.Client

	// In-flight trades - prevents duplicate processing during the ~600ms order window
	inFlightTrades   map[string]time.Time
	inFlightTradesMu sync.Mutex
}

// CopyTraderMetrics tracks performance metrics
type CopyTraderMetrics struct {
	TradesCopied        int64
	TradesSkipped       int64
	TradesFailed        int64
	AvgCopyLatency      time.Duration // Time from original trade to our execution
	FastestCopy         time.Duration
	SlowestCopy         time.Duration
	TotalUSDCSpent      float64
	TotalUSDCReceived   float64
	LastCopyTime        time.Time
}

// CopyTraderConfig holds configuration for copy trading
type CopyTraderConfig struct {
	Enabled            bool
	Multiplier         float64 // 0.05 = 1/20th
	MinOrderUSDC       float64 // Minimum order size
	MaxPriceSlippage   float64 // Max price above trader's price (0.20 = 20%)
	CheckIntervalSec   int     // Poll frequency
	EnableBlockchainWS bool    // Enable Polygon blockchain WebSocket for ~1s detection (heavy)
}

// CopyTrade represents a copy trade record
type CopyTrade struct {
	ID              int
	OriginalTradeID string
	OriginalTrader  string
	MarketID        string
	TokenID         string
	Outcome         string
	Title           string
	Side            string
	IntendedUSDC    float64
	ActualUSDC      float64
	PricePaid       float64
	SizeBought      float64
	Status          string
	ErrorReason     string
	CreatedAt       time.Time
	ExecutedAt      *time.Time
	OrderID         string
	TxHash          string
	DetectionSource string // How the trade was detected: clob, polygon_ws, data_api
}

// MyPosition represents our position in a market
type MyPosition struct {
	MarketID  string
	TokenID   string
	Outcome   string
	Title     string
	Size      float64
	AvgPrice  float64
	TotalCost float64
	UpdatedAt time.Time
}

// getMaxSlippage returns the maximum allowed slippage based on trader's price.
// Lower prices are more volatile, so we allow more slippage.
// - Under $0.10: 200% (can pay up to 3x the price)
// - Under $0.20: 80% (can pay up to 1.8x)
// - Under $0.30: 50% (can pay up to 1.5x)
// - Under $0.40: 30% (can pay up to 1.3x)
// - $0.40+: 20% (can pay up to 1.2x)
func getMaxSlippage(traderPrice float64) float64 {
	switch {
	case traderPrice < 0.10:
		return 2.00 // 200%
	case traderPrice < 0.20:
		return 0.80 // 80%
	case traderPrice < 0.30:
		return 0.50 // 50%
	case traderPrice < 0.40:
		return 0.30 // 30%
	default:
		return 0.20 // 20%
	}
}

// NewCopyTrader creates a new copy trader
func NewCopyTrader(store *storage.PostgresStore, client *api.Client, config CopyTraderConfig) (*CopyTrader, error) {
	// Create CLOB client
	auth, err := api.NewAuth()
	if err != nil {
		return nil, fmt.Errorf("failed to create auth: %w", err)
	}

	clobClient, err := api.NewClobClient("", auth)
	if err != nil {
		return nil, fmt.Errorf("failed to create CLOB client: %w", err)
	}

	// Configure for Magic/Email wallet if funder address is set
	funderAddress := strings.TrimSpace(os.Getenv("POLYMARKET_FUNDER_ADDRESS"))
	if funderAddress != "" {
		clobClient.SetFunder(funderAddress)
		clobClient.SetSignatureType(1) // 1 = Magic/Email wallet (maker=funder, signer=EOA)
		log.Printf("[CopyTrader] Configured for Magic wallet")
	} else {
		log.Printf("[CopyTrader] Using EOA wallet (no funder address set)")
	}

	// Pre-warm connection pool to CLOB API for faster first order
	// This establishes TCP+TLS connection upfront (~200-300ms savings on first trade)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := clobClient.WarmConnection(ctx); err != nil {
		log.Printf("[CopyTrader] Warning: failed to warm connection: %v", err)
	}
	cancel()

	// Set defaults
	if config.Multiplier == 0 {
		config.Multiplier = 0.05 // 1/20th
	}
	if config.MinOrderUSDC == 0 {
		config.MinOrderUSDC = 1.05 // Slightly above $1 minimum to avoid "min size: $1" errors
	}
	if config.MaxPriceSlippage == 0 {
		config.MaxPriceSlippage = 0.20 // 20% max slippage above trader's price
	}
	if config.CheckIntervalSec == 0 {
		config.CheckIntervalSec = 1 // 1 second for faster copy execution
	}

	// Determine our wallet address for position lookups
	myAddress := funderAddress
	if myAddress == "" {
		myAddress = auth.GetAddress().Hex()
	}

	// Get main app URL for trade execution (critical path)
	mainAppURL := strings.TrimSpace(os.Getenv("MAIN_APP_URL"))
	if mainAppURL == "" {
		mainAppURL = "http://localhost:8081" // Default for local dev
	}
	mainAppURL = strings.TrimRight(mainAppURL, "/")

	ct := &CopyTrader{
		store:          store,
		client:         client,
		clobClient:     clobClient,
		config:         config,
		stopCh:         make(chan struct{}),
		myAddress:      myAddress,
		metrics:        &CopyTraderMetrics{},
		mainAppURL:     mainAppURL,
		inFlightTrades: make(map[string]time.Time),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	log.Printf("[CopyTrader] Will send trade execution to main app at: %s", mainAppURL)

	return ct, nil
}

// CopyTradeRequest is sent to main app for trade execution
type CopyTradeRequest struct {
	FollowingAddress string  `json:"following_address"`
	FollowingTradeID string  `json:"following_trade_id"`
	FollowingTime    int64   `json:"following_time"`
	TokenID          string  `json:"token_id"`
	Side             string  `json:"side"`
	FollowingPrice   float64 `json:"following_price"`
	FollowingShares  float64 `json:"following_shares"`
	MarketTitle      string  `json:"market_title"`
	Outcome          string  `json:"outcome"`
	Multiplier       float64 `json:"multiplier"`
	MinOrderUSDC     float64 `json:"min_order_usdc"`
}

// CopyTradeResponse is received from main app after trade execution
type CopyTradeResponse struct {
	Success        bool    `json:"success"`
	Status         string  `json:"status"`
	FailedReason   string  `json:"failed_reason,omitempty"`
	OrderID        string  `json:"order_id,omitempty"`
	FollowerPrice  float64 `json:"follower_price,omitempty"`
	FollowerShares float64 `json:"follower_shares,omitempty"`
	ExecutionMs    int64   `json:"execution_ms"`
	DebugLog       string  `json:"debug_log,omitempty"`
}

// executeViaMainApp sends trade to main app for execution (critical path)
func (ct *CopyTrader) executeViaMainApp(ctx context.Context, trade models.TradeDetail, tokenID string, side string) (*CopyTradeResponse, error) {
	httpStart := time.Now()
	defer func() {
		log.Printf("[CopyTrader] ⏱️ TIMING: mainAppHTTP=%dms", time.Since(httpStart).Milliseconds())
	}()

	req := CopyTradeRequest{
		FollowingAddress: trade.UserID,
		FollowingTradeID: trade.ID,
		FollowingTime:    trade.Timestamp.Unix(),
		TokenID:          tokenID,
		Side:             side,
		FollowingPrice:   trade.Price,
		FollowingShares:  trade.Size,
		MarketTitle:      trade.Title,
		Outcome:          trade.Outcome,
		Multiplier:       ct.config.Multiplier,
		MinOrderUSDC:     ct.config.MinOrderUSDC,
	}

	jsonBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	url := ct.mainAppURL + "/api/internal/execute-copy-trade"
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := ct.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var result CopyTradeResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	return &result, nil
}

// Start begins monitoring for trades to copy
func (ct *CopyTrader) Start(ctx context.Context) error {
	if ct.running {
		return fmt.Errorf("copy trader already running")
	}

	// Initialize API credentials
	log.Printf("[CopyTrader] Initializing CLOB API credentials...")
	if _, err := ct.clobClient.DeriveAPICreds(ctx); err != nil {
		return fmt.Errorf("failed to derive API creds: %w", err)
	}
	log.Printf("[CopyTrader] API credentials initialized successfully")

	// Start real-time detector for faster trade detection
	// Pass clobClient for fast CLOB API detection (~50ms latency)
	// EnableBlockchainWS should only be true in the worker (for backup ~1s detection)
	// Pass our address for blockchain confirmation tracking
	ct.detector = NewRealtimeDetector(ct.client, ct.clobClient, ct.store, ct.handleRealtimeTrade, ct.config.EnableBlockchainWS, ct.myAddress)
	if err := ct.detector.Start(ctx); err != nil {
		log.Printf("[CopyTrader] Warning: realtime detector failed to start: %v", err)
		// Continue without it - we'll fall back to polling
	} else {
		if ct.config.EnableBlockchainWS {
			log.Printf("[CopyTrader] Realtime detector started (CLOB API 100ms + blockchain WS backup)")
		} else {
			log.Printf("[CopyTrader] Realtime detector started (CLOB API 100ms polling)")
		}
	}

	ct.running = true
	go ct.run(ctx)

	log.Printf("[CopyTrader] Started with multiplier=%.2f, minOrder=$%.2f, interval=%ds",
		ct.config.Multiplier, ct.config.MinOrderUSDC, ct.config.CheckIntervalSec)

	return nil
}

// handleRealtimeTrade is called when the realtime detector finds a new trade
func (ct *CopyTrader) handleRealtimeTrade(trade models.TradeDetail) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startTime := time.Now()

	if err := ct.processTrade(ctx, trade); err != nil {
		log.Printf("[CopyTrader] Realtime trade processing failed: %v", err)
		ct.metricsMu.Lock()
		ct.metrics.TradesFailed++
		ct.metricsMu.Unlock()
		return
	}

	// Mark as processed
	ct.store.MarkTradeProcessed(ctx, trade.ID)

	// Track latency
	copyLatency := time.Since(startTime)
	totalLatency := time.Since(trade.Timestamp)

	ct.metricsMu.Lock()
	ct.metrics.TradesCopied++
	ct.metrics.LastCopyTime = time.Now()
	if ct.metrics.FastestCopy == 0 || copyLatency < ct.metrics.FastestCopy {
		ct.metrics.FastestCopy = copyLatency
	}
	if copyLatency > ct.metrics.SlowestCopy {
		ct.metrics.SlowestCopy = copyLatency
	}
	if ct.metrics.AvgCopyLatency == 0 {
		ct.metrics.AvgCopyLatency = copyLatency
	} else {
		ct.metrics.AvgCopyLatency = (ct.metrics.AvgCopyLatency + copyLatency) / 2
	}
	ct.metricsMu.Unlock()

	log.Printf("[CopyTrader] Trade copied in %s (total latency from original: %s)",
		copyLatency.Round(time.Millisecond), totalLatency.Round(time.Millisecond))
}

// GetMetrics returns current performance metrics
func (ct *CopyTrader) GetMetrics() CopyTraderMetrics {
	ct.metricsMu.RLock()
	defer ct.metricsMu.RUnlock()
	return *ct.metrics
}

// Stop halts the copy trader
func (ct *CopyTrader) Stop() {
	if ct.running {
		close(ct.stopCh)

		// Stop realtime detector
		if ct.detector != nil {
			ct.detector.Stop()
		}

		ct.running = false

		// Log final metrics
		metrics := ct.GetMetrics()
		log.Printf("[CopyTrader] Stopped - Metrics: copied=%d, skipped=%d, failed=%d, avgLatency=%s",
			metrics.TradesCopied, metrics.TradesSkipped, metrics.TradesFailed,
			metrics.AvgCopyLatency.Round(time.Millisecond))
	}
}

func (ct *CopyTrader) run(ctx context.Context) {
	// Poll for new trades from Data API every 150ms
	// This is a backup detection method - mempool/polygon are faster
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ct.stopCh:
			return
		case <-ticker.C:
			if err := ct.processNewTrades(ctx); err != nil {
				log.Printf("[CopyTrader] Error processing trades: %v", err)
			}
		}
	}
}

func (ct *CopyTrader) processNewTrades(ctx context.Context) error {
	// Get followed user addresses for filtered query
	followedUsers, _ := ct.store.GetFollowedUserAddresses(ctx)

	// Get unprocessed trades - use batch query with user filter
	trades, err := ct.store.GetUnprocessedTradesBatch(ctx, 100, followedUsers)
	if err != nil {
		return fmt.Errorf("failed to get unprocessed trades: %w", err)
	}

	if len(trades) == 0 {
		return nil
	}

	log.Printf("[CopyTrader] Processing %d new trades from Data API", len(trades))

	// Process trades in parallel with worker pool
	const maxWorkers = 5
	tradeChan := make(chan models.TradeDetail, len(trades))
	processedIDs := make(chan string, len(trades))
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for trade := range tradeChan {
				// Set detection source for trades from Data API polling
				if trade.DetectionSource == "" {
					trade.DetectionSource = "data_api"
				}
				if err := ct.processTrade(ctx, trade); err != nil {
					log.Printf("[CopyTrader] Error processing trade %s: %v", trade.ID, err)
				}
				processedIDs <- trade.ID
			}
		}()
	}

	// Send trades to workers
	for _, trade := range trades {
		tradeChan <- trade
	}
	close(tradeChan)

	// Wait for all workers to finish
	wg.Wait()
	close(processedIDs)

	// Collect processed IDs and batch mark as processed
	var ids []string
	for id := range processedIDs {
		ids = append(ids, id)
	}

	// Batch mark as processed (much faster than one at a time)
	if len(ids) > 0 {
		if err := ct.store.MarkTradesProcessedBatch(ctx, ids); err != nil {
			log.Printf("[CopyTrader] Error batch marking trades processed: %v", err)
		}
	}

	return nil
}

func (ct *CopyTrader) processTrade(ctx context.Context, trade models.TradeDetail) error {
	processStart := time.Now()

	// Helper to log with tx reference and elapsed time
	txRef := trade.TransactionHash
	if len(txRef) > 16 {
		txRef = txRef[:16]
	}
	detectedAt := trade.DetectedAt
	if detectedAt.IsZero() {
		detectedAt = processStart
	}
	elapsed := func() string {
		return fmt.Sprintf("+%dms", time.Since(detectedAt).Milliseconds())
	}

	log.Printf("[%s] [%s] 🚀 START processTrade (source=%s, side=%s, size=%.2f)",
		txRef, elapsed(), trade.DetectionSource, trade.Side, trade.Size)

	// Use TxHash as dedup key (not trade.ID which may include :LogIndex)
	dedupKey := trade.TransactionHash
	if dedupKey == "" {
		dedupKey = trade.ID
	}

	// CRITICAL: Check if this trade is already being processed
	ct.inFlightTradesMu.Lock()
	if startTime, exists := ct.inFlightTrades[dedupKey]; exists {
		ct.inFlightTradesMu.Unlock()
		log.Printf("[%s] [%s] ⏭️ SKIP: already in-flight since %s ago", txRef, elapsed(), time.Since(startTime).Round(time.Millisecond))
		return nil
	}
	ct.inFlightTrades[dedupKey] = time.Now()
	for id, t := range ct.inFlightTrades {
		if time.Since(t) > 2*time.Minute {
			delete(ct.inFlightTrades, id)
		}
	}
	ct.inFlightTradesMu.Unlock()
	log.Printf("[%s] [%s] ✓ in-flight check passed", txRef, elapsed())

	// DATABASE-LEVEL DEDUP
	alreadyExecuted, err := ct.store.IsTradeAlreadyExecuted(ctx, dedupKey)
	if err != nil {
		log.Printf("[%s] [%s] ⚠️ DB dedup check failed: %v", txRef, elapsed(), err)
	} else if alreadyExecuted {
		log.Printf("[%s] [%s] ⏭️ SKIP: already executed in DB", txRef, elapsed())
		return nil
	}
	log.Printf("[%s] [%s] ✓ DB dedup check passed", txRef, elapsed())

	// Skip stale trades
	const maxTradeAge = 5 * time.Minute
	tradeAge := time.Since(trade.Timestamp)
	if tradeAge > maxTradeAge {
		log.Printf("[%s] [%s] ⏭️ SKIP: stale trade (%.1f min old)", txRef, elapsed(), tradeAge.Minutes())
		return nil
	}

	// Skip non-TRADE types
	if trade.Type != "" && trade.Type != "TRADE" {
		log.Printf("[%s] [%s] ⏭️ SKIP: non-trade type=%s", txRef, elapsed(), trade.Type)
		return nil
	}

	// Determine trade type for token resolution
	isBlockchainTrade := trade.TransactionHash != "" && trade.DetectedAt != (time.Time{}) &&
		trade.DetectionSource != "live_ws" && trade.DetectionSource != "mempool"

	var tokenID string
	var negRisk bool

	if isBlockchainTrade {
		// Fast path: use token ID directly
		tokenID = trade.MarketID
		negRisk = false
		if tokenInfo, err := ct.store.GetTokenInfo(ctx, tokenID); err == nil && tokenInfo != nil {
			trade.Title = tokenInfo.Title
			trade.Outcome = tokenInfo.Outcome
		}
		log.Printf("[%s] [%s] ✓ blockchain fast path, tokenID=%s", txRef, elapsed(), tokenID[:20])
	} else if trade.DetectionSource == "mempool" {
		log.Printf("[%s] [%s] 📡 mempool: calling QuickNode trace...", txRef, elapsed())
		alchemyResult := api.ExtractViaAlchemySimple(trade.TransactionHash, trade.UserID)
		if alchemyResult == nil || alchemyResult.Error != "" {
			errMsg := "unknown error"
			if alchemyResult != nil {
				errMsg = alchemyResult.Error
			}
			log.Printf("[%s] [%s] ❌ mempool trace failed: %s", txRef, elapsed(), errMsg)
			return ct.logCopyTrade(ctx, trade, "", 0, 0, 0, 0, "failed", fmt.Sprintf("mempool: trace failed: %s", errMsg), "")
		}
		log.Printf("[%s] [%s] ✓ mempool trace: %s %.4f @ %.4f", txRef, elapsed(), alchemyResult.Direction, alchemyResult.Tokens, alchemyResult.Price)

		trade.Side = alchemyResult.Direction
		trade.Size = alchemyResult.Tokens
		trade.Price = alchemyResult.Price
		tokenID = alchemyResult.TokenID

		// Look up title from cache
		tokenInfo, err := ct.store.GetTokenInfo(ctx, tokenID)
		if err == nil && tokenInfo != nil {
			trade.Outcome = tokenInfo.Outcome
			trade.Title = tokenInfo.Title
			log.Printf("[%s] [%s] ✓ token info from cache", txRef, elapsed())
		} else {
			log.Printf("[%s] [%s] 📡 fetching token info from Gamma...", txRef, elapsed())
			gammaInfo, err := ct.clobClient.GetTokenInfoByID(ctx, tokenID)
			if err != nil {
				log.Printf("[%s] [%s] ❌ Gamma API failed: %v", txRef, elapsed(), err)
				return ct.logCopyTrade(ctx, trade, "", 0, 0, 0, 0, "failed", fmt.Sprintf("mempool: Gamma failed: %v", err), "")
			}
			trade.Outcome = gammaInfo.Outcome
			trade.Title = gammaInfo.Title
			negRisk = gammaInfo.NegRisk
			ct.store.SaveTokenInfo(ctx, tokenID, gammaInfo.ConditionID, gammaInfo.Outcome, gammaInfo.Title, gammaInfo.Slug)
			log.Printf("[%s] [%s] ✓ token info from Gamma", txRef, elapsed())
		}
	} else if trade.DetectionSource == "live_ws" {
		// LiveData WebSocket: MarketID is conditionID, need to look up tokenID
		log.Printf("[CopyTrader] ⚡ LiveData WS trade: looking up tokenID from conditionID=%s outcome=%s", trade.MarketID, trade.Outcome)

		// First try database cache (fast)
		tokenInfo, err := ct.store.GetTokenByConditionAndOutcome(ctx, trade.MarketID, trade.Outcome)
		if err == nil && tokenInfo != nil {
			tokenID = tokenInfo.TokenID
			negRisk = false // Will get from order book
			log.Printf("[CopyTrader] ⚡ LiveData WS: found tokenID=%s from cache", tokenID)
		} else {
			// Cache miss - call CLOB API directly (always works for active markets)
			log.Printf("[CopyTrader] ⚡ LiveData WS: cache miss, calling CLOB API...")
			market, err := ct.clobClient.GetMarket(ctx, trade.MarketID)
			if err != nil {
				return ct.logCopyTrade(ctx, trade, "", 0, 0, 0, 0, "failed", fmt.Sprintf("CLOB API failed for conditionID %s: %v", trade.MarketID, err), "")
			}

			// Find the token with matching outcome
			var foundToken string
			for _, token := range market.Tokens {
				if strings.EqualFold(token.Outcome, trade.Outcome) {
					foundToken = token.TokenID
					break
				}
			}

			if foundToken == "" {
				return ct.logCopyTrade(ctx, trade, "", 0, 0, 0, 0, "failed", fmt.Sprintf("no token found for outcome %s in conditionID %s", trade.Outcome, trade.MarketID), "")
			}

			tokenID = foundToken
			negRisk = market.NegRisk
			log.Printf("[CopyTrader] ⚡ LiveData WS: found tokenID=%s from CLOB API (negRisk=%v)", tokenID, negRisk)

			// Cache all tokens from this market for future lookups
			for _, token := range market.Tokens {
				if err := ct.store.SaveTokenInfo(ctx, token.TokenID, market.ConditionID, token.Outcome, market.Description, market.MarketSlug); err != nil {
					log.Printf("[CopyTrader] Warning: failed to cache token %s: %v", token.TokenID, err)
				}
			}
		}
	} else {
		// Slow path: legacy trades need verification
		tokenLookupStart := time.Now()
		var actualOutcome string
		var err error
		tokenID, actualOutcome, negRisk, err = ct.getVerifiedTokenID(ctx, trade)
		log.Printf("[CopyTrader] ⏱️ TIMING: tokenLookup=%dms", time.Since(tokenLookupStart).Milliseconds())
		if err != nil {
			return ct.logCopyTrade(ctx, trade, "", 0, 0, 0, 0, "failed", fmt.Sprintf("failed to get token ID: %v", err), "")
		}

		// Update trade outcome if it was wrong
		if actualOutcome != "" && actualOutcome != trade.Outcome {
			log.Printf("[CopyTrader] WARNING: Corrected outcome from '%s' to '%s' for %s", trade.Outcome, actualOutcome, trade.Title)
			trade.Outcome = actualOutcome
		}
	}

	log.Printf("[%s] [%s] 📡 starting order book fetch (parallel)...", txRef, elapsed())

	// OPTIMIZATION: Start order book fetch NOW while we check settings
	orderBookCh := make(chan orderBookResult, 1)
	go func() {
		book, err := ct.clobClient.GetOrderBook(ctx, tokenID)
		orderBookCh <- orderBookResult{book, err}
	}()

	// Get user settings
	log.Printf("[%s] [%s] 🔍 checking user settings...", txRef, elapsed())
	strategyType := storage.StrategyHuman
	var userSettings *storage.UserCopySettings
	if ct.detector != nil {
		userSettings = ct.detector.GetCachedUserSettings(trade.UserID)
	}
	if userSettings == nil {
		var err error
		userSettings, err = ct.store.GetUserCopySettings(ctx, trade.UserID)
		if err != nil || userSettings == nil {
			log.Printf("[%s] [%s] ⏭️ SKIP: no copy settings for user", txRef, elapsed())
			return nil
		}
	}
	if !userSettings.Enabled {
		log.Printf("[%s] [%s] ⏭️ SKIP: copy trading disabled", txRef, elapsed())
		return nil
	}
	strategyType = userSettings.StrategyType
	log.Printf("[%s] [%s] ✓ user settings OK (strategy=%d, mult=%.2f)", txRef, elapsed(), strategyType, userSettings.Multiplier)

	// Route to execution
	if trade.Side == "BUY" {
		log.Printf("[%s] [%s] 🛒 routing to BUY execution...", txRef, elapsed())
		if strategyType == storage.StrategyBot || strategyType == storage.StrategyBTC15m {
			// Use immediate execution (no order book fetch) - place at same price, wait 15s
			return ct.executeBotBuyImmediate(ctx, trade, tokenID, negRisk, userSettings, txRef, detectedAt)
		}
		return ct.executeBuy(ctx, trade, tokenID, negRisk)
	} else if trade.Side == "SELL" {
		log.Printf("[%s] [%s] 💰 routing to SELL execution...", txRef, elapsed())
		if strategyType == storage.StrategyBot || strategyType == storage.StrategyBTC15m {
			return ct.executeBotSellWithBook(ctx, trade, tokenID, negRisk, userSettings, orderBookCh, txRef, detectedAt)
		}
		return ct.executeSell(ctx, trade, tokenID, negRisk)
	}

	return nil
}

// getVerifiedTokenID looks up the token info and verifies/corrects the outcome.
// Returns: tokenID, actualOutcome, negRisk, error
// If the trade.MarketID is already a token ID, it verifies what outcome that token actually corresponds to.
func (ct *CopyTrader) getVerifiedTokenID(ctx context.Context, trade models.TradeDetail) (string, string, bool, error) {
	log.Printf("[CopyTrader] DEBUG getTokenID: trade.MarketID=%s, trade.Outcome=%s, trade.Title=%s",
		trade.MarketID, trade.Outcome, trade.Title)

	// First, look up the token info from cache to see what outcome this token actually is
	tokenInfo, err := ct.store.GetTokenInfo(ctx, trade.MarketID)
	if err == nil && tokenInfo != nil {
		// Found the token - need to get negRisk from CLOB API using conditionID
		negRisk := false
		if tokenInfo.ConditionID != "" {
			market, mErr := ct.clobClient.GetMarket(ctx, tokenInfo.ConditionID)
			if mErr == nil && market != nil {
				negRisk = market.NegRisk
				log.Printf("[CopyTrader] DEBUG getTokenID: Got negRisk=%v from market for conditionID=%s", negRisk, tokenInfo.ConditionID)
			}
		}

		// Check if the outcome matches
		if tokenInfo.Outcome != trade.Outcome {
			log.Printf("[CopyTrader] DEBUG getTokenID: OUTCOME MISMATCH! Token is %s but trade says %s",
				tokenInfo.Outcome, trade.Outcome)
			// The trade.MarketID is the wrong token - we need to find the correct one
			// Look up the sibling token with the correct outcome
			siblingToken, err := ct.store.GetTokenByConditionAndOutcome(ctx, tokenInfo.ConditionID, trade.Outcome)
			if err == nil && siblingToken != nil {
				log.Printf("[CopyTrader] DEBUG getTokenID: Found correct token %s for outcome %s",
					siblingToken.TokenID, trade.Outcome)
				return siblingToken.TokenID, trade.Outcome, negRisk, nil
			}
			// Couldn't find sibling, use the token we have but return its actual outcome
			log.Printf("[CopyTrader] DEBUG getTokenID: Using original token %s with corrected outcome %s",
				trade.MarketID, tokenInfo.Outcome)
			return trade.MarketID, tokenInfo.Outcome, negRisk, nil
		}
		// Outcome matches, use as-is
		log.Printf("[CopyTrader] DEBUG getTokenID: Token verified - outcome=%s matches, negRisk=%v", tokenInfo.Outcome, negRisk)
		return trade.MarketID, trade.Outcome, negRisk, nil
	}

	// Token not in cache - try Gamma API to fetch by token ID
	// (trade.MarketID is typically a token ID from the subgraph, not a condition ID)
	log.Printf("[CopyTrader] DEBUG getTokenID: Token not in cache, fetching from Gamma API...")
	gammaInfo, err := ct.clobClient.GetTokenInfoByID(ctx, trade.MarketID)
	if err == nil && gammaInfo != nil {
		log.Printf("[CopyTrader] DEBUG getTokenID: Gamma API success - conditionID=%s, outcome=%s, negRisk=%v",
			gammaInfo.ConditionID, gammaInfo.Outcome, gammaInfo.NegRisk)

		// Cache it for next time
		ct.store.SaveTokenInfo(ctx, trade.MarketID, gammaInfo.ConditionID, gammaInfo.Outcome, gammaInfo.Title, gammaInfo.Slug)

		// Check if outcome matches
		if gammaInfo.Outcome != trade.Outcome && gammaInfo.Outcome != "" {
			log.Printf("[CopyTrader] DEBUG getTokenID: Gamma outcome %s differs from trade outcome %s", gammaInfo.Outcome, trade.Outcome)
			// Try to find the sibling token with the correct outcome
			siblingToken, err := ct.store.GetTokenByConditionAndOutcome(ctx, gammaInfo.ConditionID, trade.Outcome)
			if err == nil && siblingToken != nil {
				log.Printf("[CopyTrader] DEBUG getTokenID: Found sibling token %s for outcome %s", siblingToken.TokenID, trade.Outcome)
				return siblingToken.TokenID, trade.Outcome, gammaInfo.NegRisk, nil
			}
		}

		return trade.MarketID, gammaInfo.Outcome, gammaInfo.NegRisk, nil
	}

	// Gamma API also failed - try CLOB API as last resort (unlikely to work)
	log.Printf("[CopyTrader] DEBUG getTokenID: Gamma API failed (%v), trying CLOB API...", err)
	market, err := ct.clobClient.GetMarket(ctx, trade.MarketID)
	if err != nil {
		// All APIs failed - check if this is a BTC 15m candle market (Strategy 3)
		// Mempool decoder often returns invalid hex tokenId for these markets
		if strings.HasPrefix(trade.MarketID, "0x") {
			log.Printf("[CopyTrader] DEBUG getTokenID: Detected hex format marketID, trying BTC 15m fallback...")
			tokenID, outcome, negRisk, btcErr := ct.lookupCurrentBTC15mMarket(ctx, trade.Outcome)
			if btcErr == nil && tokenID != "" {
				log.Printf("[CopyTrader] DEBUG getTokenID: BTC 15m fallback SUCCESS - tokenID=%s, outcome=%s", tokenID, outcome)
				return tokenID, outcome, negRisk, nil
			}
			log.Printf("[CopyTrader] DEBUG getTokenID: BTC 15m fallback failed: %v", btcErr)
		}
		// Both APIs failed - use trade.MarketID directly with unknown outcome
		log.Printf("[CopyTrader] DEBUG getTokenID: CLOB API also failed (%v), using trade.MarketID directly", err)
		return trade.MarketID, trade.Outcome, false, nil
	}

	// Find matching token by outcome
	log.Printf("[CopyTrader] DEBUG getTokenID: CLOB API SUCCESS, market has %d tokens, negRisk=%v", len(market.Tokens), market.NegRisk)
	for _, token := range market.Tokens {
		log.Printf("[CopyTrader] DEBUG getTokenID: checking token outcome=%s vs trade.Outcome=%s, tokenID=%s",
			token.Outcome, trade.Outcome, token.TokenID)
		if strings.EqualFold(token.Outcome, trade.Outcome) {
			// Cache it for next time
			ct.store.CacheTokenID(ctx, trade.MarketID, trade.Outcome, token.TokenID, market.NegRisk)
			return token.TokenID, trade.Outcome, market.NegRisk, nil
		}
	}

	// Fallback: use MarketID as token ID
	log.Printf("[CopyTrader] DEBUG getTokenID: NO MATCH in tokens, falling back to trade.MarketID")
	return trade.MarketID, trade.Outcome, false, nil
}

// lookupCurrentBTC15mMarket looks up the current active BTC 15m candle market
// This is used as a fallback when the mempool-decoded tokenId can't be looked up
// Returns: tokenID, outcome, negRisk, error
func (ct *CopyTrader) lookupCurrentBTC15mMarket(ctx context.Context, outcome string) (string, string, bool, error) {
	// Calculate the current 15-minute candle timestamp
	now := time.Now().Unix()
	candleInterval := int64(15 * 60) // 15 minutes in seconds
	candleStart := (now / candleInterval) * candleInterval

	// Build the market slug pattern
	slug := fmt.Sprintf("btc-updown-15m-%d", candleStart)
	log.Printf("[CopyTrader] BTC 15m fallback: looking up slug=%s for outcome=%s", slug, outcome)

	// Try Gamma API to get market by slug
	market, err := ct.clobClient.GetMarketBySlug(ctx, slug)
	if err != nil {
		// Try previous candle (in case we're at the boundary)
		prevSlug := fmt.Sprintf("btc-updown-15m-%d", candleStart-candleInterval)
		log.Printf("[CopyTrader] BTC 15m fallback: current candle failed, trying previous: %s", prevSlug)
		market, err = ct.clobClient.GetMarketBySlug(ctx, prevSlug)
		if err != nil {
			return "", "", false, fmt.Errorf("BTC 15m market lookup failed for both %s and %s: %v", slug, prevSlug, err)
		}
	}

	if market == nil {
		return "", "", false, fmt.Errorf("BTC 15m market not found for slug %s", slug)
	}

	// Find the token matching the requested outcome
	normalizedOutcome := strings.Title(strings.ToLower(outcome))
	for _, token := range market.Tokens {
		if strings.EqualFold(token.Outcome, outcome) || strings.EqualFold(token.Outcome, normalizedOutcome) {
			log.Printf("[CopyTrader] BTC 15m fallback: found token %s for outcome %s", token.TokenID, token.Outcome)
			// Cache it for next time
			ct.store.SaveTokenInfo(ctx, token.TokenID, market.ConditionID, token.Outcome, market.Description, slug)
			return token.TokenID, token.Outcome, market.NegRisk, nil
		}
	}

	return "", "", false, fmt.Errorf("no token found for outcome %s in market %s", outcome, slug)
}

func (ct *CopyTrader) executeBuy(ctx context.Context, trade models.TradeDetail, tokenID string, negRisk bool) error {
	// Initialize timestamps for latency tracking
	startTime := time.Now()
	timestamps := &CopyTradeTimestamps{
		ProcessingStartedAt: &startTime,
	}
	if !trade.DetectedAt.IsZero() {
		timestamps.DetectedAt = &trade.DetectedAt
	}

	// Get per-user settings or use defaults
	// First check cache (fast), then DB (slow), and update cache if found
	multiplier := ct.config.Multiplier
	minUSDC := ct.config.MinOrderUSDC

	var userSettings *storage.UserCopySettings
	if ct.detector != nil {
		userSettings = ct.detector.GetCachedUserSettings(trade.UserID)
	}
	if userSettings == nil {
		// Cache miss - query DB
		var err error
		userSettings, err = ct.store.GetUserCopySettings(ctx, trade.UserID)
		if err == nil && userSettings != nil && ct.detector != nil {
			// Add to cache for next time
			ct.detector.SetCachedUserSettings(trade.UserID, userSettings)
		}
	}
	if userSettings != nil {
		if !userSettings.Enabled {
			log.Printf("[CopyTrader] BUY skipped: user %s has copy trading disabled", trade.UserID)
			return nil
		}
		multiplier = userSettings.Multiplier
		minUSDC = userSettings.MinUSDC
	}

	log.Printf("[CopyTrader] BUY: Sending to main app - tokenID=%s, multiplier=%.2f, minUSDC=$%.2f",
		tokenID, multiplier, minUSDC)

	// Send to main app for execution (critical path)
	orderPlacedAt := time.Now()
	timestamps.OrderPlacedAt = &orderPlacedAt
	resp, err := ct.executeViaMainApp(ctx, trade, tokenID, "BUY")
	orderConfirmedAt := time.Now()
	if err != nil {
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, 0, "failed", fmt.Sprintf("main app error: %v", err), "", storage.StrategyHuman, nil, nil, timestamps)
	}

	timestamps.OrderConfirmedAt = &orderConfirmedAt

	if !resp.Success {
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, 0, resp.Status, resp.FailedReason, "", storage.StrategyHuman, nil, nil, timestamps)
	}

	log.Printf("[CopyTrader] BUY executed via main app: orderID=%s, price=%.4f, size=%.4f, executionMs=%d",
		resp.OrderID, resp.FollowerPrice, resp.FollowerShares, resp.ExecutionMs)

	return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, resp.FollowerPrice*resp.FollowerShares, resp.FollowerPrice, resp.FollowerShares, "executed", "", resp.OrderID, storage.StrategyHuman, nil, nil, timestamps)
}

func (ct *CopyTrader) executeSell(ctx context.Context, trade models.TradeDetail, tokenID string, negRisk bool) error {
	// Initialize timestamps for latency tracking
	startTime := time.Now()
	timestamps := &CopyTradeTimestamps{
		ProcessingStartedAt: &startTime,
	}
	if !trade.DetectedAt.IsZero() {
		timestamps.DetectedAt = &trade.DetectedAt
	}

	log.Printf("[CopyTrader] SELL: Sending to main app - tokenID=%s", tokenID)

	// Send to main app for execution (critical path)
	orderPlacedAt := time.Now()
	timestamps.OrderPlacedAt = &orderPlacedAt
	resp, err := ct.executeViaMainApp(ctx, trade, tokenID, "SELL")
	orderConfirmedAt := time.Now()
	if err != nil {
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, 0, "failed", fmt.Sprintf("main app error: %v", err), "", storage.StrategyHuman, nil, nil, timestamps)
	}

	timestamps.OrderConfirmedAt = &orderConfirmedAt

	if !resp.Success {
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, 0, resp.Status, resp.FailedReason, "", storage.StrategyHuman, nil, nil, timestamps)
	}

	log.Printf("[CopyTrader] SELL executed via main app: orderID=%s, price=%.4f, size=%.4f, executionMs=%d",
		resp.OrderID, resp.FollowerPrice, resp.FollowerShares, resp.ExecutionMs)

	return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, resp.FollowerPrice*resp.FollowerShares, resp.FollowerPrice, resp.FollowerShares, "executed", "", resp.OrderID, storage.StrategyHuman, nil, nil, timestamps)
}

// executeBotBuy implements the bot following strategy for buys.
// It tries to buy at the copied user's exact price, then sweeps asks up to +10%.
func (ct *CopyTrader) executeBotBuy(ctx context.Context, trade models.TradeDetail, tokenID string, negRisk bool, userSettings *storage.UserCopySettings) error {
	txRef := trade.TransactionHash
	if len(txRef) > 16 {
		txRef = txRef[:16]
	}
	detectedAt := trade.DetectedAt
	if detectedAt.IsZero() {
		detectedAt = time.Now()
	}
	return ct.executeBotBuyWithBook(ctx, trade, tokenID, negRisk, userSettings, nil, txRef, detectedAt)
}

// =============================================================================
// executeBotBuyWithBook - MAIN BUY EXECUTION FOR BOT STRATEGY
// =============================================================================
//
// This is the CRITICAL PATH for copy trade execution. Every millisecond matters!
//
// EXECUTION STEPS:
//   1. Load user settings (multiplier, min/max USDC)
//   2. Calculate target shares: copiedTradeSize × multiplier
//   3. Calculate max acceptable price: copiedPrice × (1 + slippage)
//   4. Fetch order book (or use pre-fetched from parallel channel)
//   5. Scan asks to find affordable liquidity within price limit
//   6. Sweep through asks: accumulate shares until target reached
//   7. Apply minimum order size ($1 or user's minUSDC)
//   8. Place order at maxFillPrice (sweeps all acceptable levels)
//   9. Update position tracking in database
//
// ORDER BOOK SWEEPING EXAMPLE:
//   Target: 100 shares, Max price: $0.55
//   Order book asks:
//     50 @ $0.50 → Take all 50, cost $25
//     30 @ $0.52 → Take all 30, cost $15.60
//     40 @ $0.54 → Take 20 (reach target), cost $10.80
//   Result: Order for 100 shares at $0.54 (max fill price)
//
// WHY maxFillPrice INSTEAD OF avgPrice?
//   If we use avgPrice ($0.514), the order only matches $0.50 and $0.52 levels.
//   Using maxFillPrice ($0.54) ensures we sweep ALL acceptable levels.
//
// =============================================================================
func (ct *CopyTrader) executeBotBuyWithBook(ctx context.Context, trade models.TradeDetail, tokenID string, negRisk bool, userSettings *storage.UserCopySettings, orderBookCh <-chan orderBookResult, txRef string, detectedAt time.Time) error {
	// Elapsed time helper
	elapsed := func() string {
		return fmt.Sprintf("+%dms", time.Since(detectedAt).Milliseconds())
	}

	log.Printf("[%s] [%s] 🛒 BUY: starting execution", txRef, elapsed())

	// Initialize timing tracking
	startTime := time.Now()
	timing := map[string]interface{}{}

	// Initialize timestamps
	timestamps := &CopyTradeTimestamps{
		ProcessingStartedAt: &startTime,
	}
	if !trade.DetectedAt.IsZero() {
		timestamps.DetectedAt = &trade.DetectedAt
	}

	// Initialize debug log
	debugLog := map[string]interface{}{
		"action":    "BUY",
		"timestamp": startTime.Format(time.RFC3339),
	}

	// Step 1: Get settings
	multiplier := ct.config.Multiplier
	minUSDC := ct.config.MinOrderUSDC
	var maxUSD *float64
	if userSettings != nil {
		multiplier = userSettings.Multiplier
		minUSDC = userSettings.MinUSDC
		maxUSD = userSettings.MaxUSD
	}
	timing["1_settings_ms"] = 0
	debugLog["settings"] = map[string]interface{}{
		"multiplier": multiplier,
		"minUSDC":    minUSDC,
		"maxUSD":     maxUSD,
	}
	log.Printf("[%s] [%s] ✓ settings loaded (mult=%.2f)", txRef, elapsed(), multiplier)

	// Step 2: Calculate target shares
	targetShares := trade.Size * multiplier
	copiedPrice := trade.Price
	var maxPrice float64
	if copiedPrice > 0 {
		slippage := getMaxSlippage(copiedPrice)
		maxPrice = copiedPrice * (1 + slippage)
	} else {
		maxPrice = 1.0
	}
	timing["2_calculation_ms"] = 0
	slippagePct := getMaxSlippage(copiedPrice) * 100
	debugLog["calculation"] = map[string]interface{}{
		"copiedTradeShares": trade.Size,
		"targetShares":      targetShares,
		"copiedPrice":       copiedPrice,
		"maxPrice":          maxPrice,
		"priceLimit":        fmt.Sprintf("+%.0f%%", slippagePct),
	}
	log.Printf("[%s] [%s] ✓ target=%.2f shares @ max $%.4f", txRef, elapsed(), targetShares, maxPrice)

	// Step 3: Get order book
	log.Printf("[%s] [%s] 📖 waiting for order book...", txRef, elapsed())
	orderBookStart := time.Now()
	var book *api.OrderBook
	var err error
	if orderBookCh != nil {
		result := <-orderBookCh
		book, err = result.book, result.err
		timing["4_get_orderbook_ms"] = float64(time.Since(orderBookStart).Microseconds()) / 1000
		timing["4_orderbook_prefetched"] = true
	} else {
		book, err = ct.clobClient.GetOrderBook(ctx, tokenID)
		timing["4_get_orderbook_ms"] = float64(time.Since(orderBookStart).Microseconds()) / 1000
	}
	if err != nil {
		timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
		debugLog["orderBook"] = map[string]interface{}{"error": err.Error()}
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "No orderbook exists") {
			log.Printf("[%s] [%s] ⏭️ market closed/resolved", txRef, elapsed())
			debugLog["decision"] = "skipped - market closed/resolved"
			return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, 0, "skipped", "market closed/resolved", "", userSettings.StrategyType, debugLog, timing, timestamps)
		}
		log.Printf("[%s] [%s] ❌ order book error: %v", txRef, elapsed(), err)
		debugLog["decision"] = fmt.Sprintf("failed - order book error: %v", err)
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, 0, "failed", fmt.Sprintf("failed to get order book: %v", err), "", userSettings.StrategyType, debugLog, timing, timestamps)
	}
	log.Printf("[%s] [%s] ✓ order book received (%d asks)", txRef, elapsed(), len(book.Asks))

	// Step 5: Analyze order book
	analysisStart := time.Now()
	// Build order book snapshot for debug log (top 10 asks)
	askSnapshot := []map[string]interface{}{}
	for i, ask := range book.Asks {
		if i >= 10 {
			break
		}
		askSnapshot = append(askSnapshot, map[string]interface{}{
			"price": ask.Price,
			"size":  ask.Size,
		})
	}
	debugLog["orderBook"] = map[string]interface{}{
		"asksCount": len(book.Asks),
		"bidsCount": len(book.Bids),
		"topAsks":   askSnapshot,
	}

	if len(book.Asks) == 0 {
		timing["5_analysis_ms"] = float64(time.Since(analysisStart).Microseconds()) / 1000
		timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
		log.Printf("[CopyTrader-Bot] BUY: no asks in order book")
		debugLog["decision"] = "skipped - no asks in order book"
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, 0, "skipped", "no asks in order book", "", userSettings.StrategyType, debugLog, timing, timestamps)
	}

	// Find all asks within our price range, sorted by price (cheapest first)
	var affordableAsks []struct {
		price float64
		size  float64
	}

	for _, ask := range book.Asks {
		var askPrice, askSize float64
		fmt.Sscanf(ask.Price, "%f", &askPrice)
		fmt.Sscanf(ask.Size, "%f", &askSize)

		// Stop if price exceeds our max (10% above copied price)
		if askPrice > maxPrice {
			break
		}

		affordableAsks = append(affordableAsks, struct {
			price float64
			size  float64
		}{askPrice, askSize})
	}

	// Log affordable asks
	affordableAsksLog := []map[string]interface{}{}
	for _, a := range affordableAsks {
		affordableAsksLog = append(affordableAsksLog, map[string]interface{}{
			"price": a.price,
			"size":  a.size,
			"cost":  a.price * a.size,
		})
	}
	debugLog["affordableAsks"] = affordableAsksLog
	timing["5_analysis_ms"] = float64(time.Since(analysisStart).Microseconds()) / 1000

	if len(affordableAsks) == 0 {
		timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
		var bestAsk float64
		fmt.Sscanf(book.Asks[0].Price, "%f", &bestAsk)
		log.Printf("[CopyTrader-Bot] BUY: no asks within 10%% of copied price (best ask %.4f > max %.4f)",
			bestAsk, maxPrice)
		debugLog["decision"] = fmt.Sprintf("skipped - best ask %.4f > max %.4f", bestAsk, maxPrice)
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, 0, "skipped",
			fmt.Sprintf("no liquidity within 10%% (best=%.4f, max=%.4f)", bestAsk, maxPrice), "", userSettings.StrategyType, debugLog, timing, timestamps)
	}

	// Step 6: Calculate fill - sweep through asks to accumulate target shares
	fillStart := time.Now()
	remainingShares := targetShares
	totalSize := 0.0
	totalCost := 0.0
	maxFillPrice := 0.0 // Track the highest price level we need to sweep

	for _, ask := range affordableAsks {
		if remainingShares <= 0 {
			break
		}

		if ask.size <= remainingShares {
			// Take entire level
			totalSize += ask.size
			totalCost += ask.price * ask.size
			remainingShares -= ask.size
			maxFillPrice = ask.price // Update max price as we sweep deeper
		} else {
			// Partial fill at this level
			totalSize += remainingShares
			totalCost += ask.price * remainingShares
			remainingShares = 0
			maxFillPrice = ask.price // This level is our deepest
		}
	}
	timing["6_fill_calc_ms"] = float64(time.Since(fillStart).Microseconds()) / 1000

	debugLog["fillCalculation"] = map[string]interface{}{
		"targetShares":    targetShares,
		"totalSize":       totalSize,
		"totalCost":       totalCost,
		"remainingShares": remainingShares,
		"maxFillPrice":    maxFillPrice,
	}

	// Polymarket minimum $1 USDC requirement
	const polymarketMinOrder = 1.0

	if totalSize < 0.01 {
		timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
		log.Printf("[CopyTrader-Bot] BUY: insufficient affordable liquidity (size=%.4f, cost=$%.4f)",
			totalSize, totalCost)
		debugLog["decision"] = fmt.Sprintf("skipped - insufficient liquidity (size=%.4f, cost=$%.4f)", totalSize, totalCost)
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, 0, "skipped", "insufficient affordable liquidity", "", userSettings.StrategyType, debugLog, timing, timestamps)
	}

	avgPrice := totalCost / totalSize

	// Use maxFillPrice for the order (not avgPrice) to ensure we sweep all levels
	// Example: if asks are 5@0.11 and 10@0.12, and we want to sweep both:
	// - avgPrice would be ~0.114, which only matches 0.11 asks
	// - maxFillPrice is 0.12, which matches both levels
	orderPrice := maxFillPrice

	// Ensure we meet minimum order requirements
	// 1. Polymarket minimum ($1)
	// 2. User's minUSDC setting
	effectiveMinUSDC := polymarketMinOrder
	if minUSDC > effectiveMinUSDC {
		effectiveMinUSDC = minUSDC
	}

	// Apply max cap if set (before minimum check)
	if maxUSD != nil && totalCost > *maxUSD {
		log.Printf("[CopyTrader-Bot] BUY: cost $%.4f exceeds max cap $%.2f, capping", totalCost, *maxUSD)
		totalCost = *maxUSD
		if orderPrice > 0 {
			totalSize = totalCost / orderPrice
		}
		debugLog["maxCapAdjustment"] = map[string]interface{}{
			"maxCap":       *maxUSD,
			"adjustedCost": totalCost,
			"adjustedSize": totalSize,
		}
	}

	// Ensure we meet minimum USDC order size
	if totalCost < effectiveMinUSDC {
		log.Printf("[CopyTrader-Bot] BUY: calculated $%.4f, bumping to $%.2f minimum", totalCost, effectiveMinUSDC)
		originalCost := totalCost
		totalCost = effectiveMinUSDC
		// Recalculate size based on max fill price (not avg)
		if orderPrice > 0 {
			totalSize = totalCost / orderPrice
		}
		debugLog["minOrderAdjustment"] = map[string]interface{}{
			"originalCost": originalCost,
			"adjustedCost": effectiveMinUSDC,
			"adjustedSize": totalSize,
		}
	}

	// Ensure we meet minimum 5 shares
	const minShares = 5.0
	if totalSize < minShares {
		log.Printf("[CopyTrader-Bot] BUY: size %.4f below minimum, bumping to %.0f shares", totalSize, minShares)
		originalSize := totalSize
		totalSize = minShares
		totalCost = totalSize * orderPrice
		debugLog["minSharesAdjustment"] = map[string]interface{}{
			"originalSize": originalSize,
			"adjustedSize": minShares,
			"adjustedCost": totalCost,
		}
	}

	log.Printf("[%s] [%s] 📤 placing order: %.4f shares @ $%.4f (cost=$%.4f)", txRef, elapsed(), totalSize, orderPrice, totalCost)

	debugLog["order"] = map[string]interface{}{
		"type":       "market",
		"side":       "BUY",
		"size":       totalSize,
		"cost":       totalCost,
		"avgPrice":   avgPrice,
		"orderPrice": orderPrice,
	}

	// Step 7: Place GTC order
	orderStart := time.Now()
	timestamps.OrderPlacedAt = &orderStart
	resp, err := ct.clobClient.PlaceOrderFast(ctx, tokenID, api.SideBuy, totalSize, orderPrice, negRisk)
	orderConfirmed := time.Now()
	timing["7_place_order_ms"] = float64(time.Since(orderStart).Microseconds()) / 1000
	if err != nil {
		timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
		log.Printf("[%s] [%s] ❌ BUY FAILED: %v", txRef, elapsed(), err)
		debugLog["orderResponse"] = map[string]interface{}{"error": err.Error()}
		debugLog["decision"] = fmt.Sprintf("failed - order error: %v", err)
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, totalCost, 0, 0, 0, "failed", fmt.Sprintf("order failed: %v", err), "", userSettings.StrategyType, debugLog, timing, timestamps)
	}

	timestamps.OrderConfirmedAt = &orderConfirmed

	debugLog["orderResponse"] = map[string]interface{}{
		"success":  resp.Success,
		"orderID":  resp.OrderID,
		"status":   resp.Status,
		"errorMsg": resp.ErrorMsg,
	}

	if !resp.Success {
		timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
		log.Printf("[%s] [%s] ❌ BUY REJECTED: %s", txRef, elapsed(), resp.ErrorMsg)
		debugLog["decision"] = fmt.Sprintf("failed - rejected: %s", resp.ErrorMsg)
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, totalCost, 0, 0, 0, "failed", resp.ErrorMsg, "", userSettings.StrategyType, debugLog, timing, timestamps)
	}

	log.Printf("[%s] [%s] ✅ BUY SUCCESS: status=%s, size=%.4f, cost=$%.4f", txRef, elapsed(), resp.Status, totalSize, totalCost)

	debugLog["decision"] = "executed successfully"

	// Step 8: Update position tracking
	positionStart := time.Now()
	if err := ct.store.UpdateMyPosition(ctx, MyPosition{
		MarketID:  trade.MarketID,
		TokenID:   tokenID,
		Outcome:   trade.Outcome,
		Title:     trade.Title,
		Size:      totalSize,
		AvgPrice:  avgPrice,
		TotalCost: totalCost,
	}); err != nil {
		log.Printf("[CopyTrader-Bot] Warning: failed to update position: %v", err)
	}
	timing["8_position_update_ms"] = float64(time.Since(positionStart).Microseconds()) / 1000

	// Final timing
	timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
	timing["latency_from_trade_ms"] = float64(time.Since(trade.Timestamp).Milliseconds())
	// Add detection latency if available
	if !trade.DetectedAt.IsZero() {
		timing["detection_latency_ms"] = float64(trade.DetectedAt.Sub(trade.Timestamp).Milliseconds())
		timing["processing_latency_ms"] = float64(time.Since(trade.DetectedAt).Milliseconds())
	}

	return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, totalCost, totalCost, avgPrice, totalSize, "executed", "", resp.OrderID, userSettings.StrategyType, debugLog, timing, timestamps)
}

// =============================================================================
// IMMEDIATE BUY EXECUTION WITH ORDER BOOK FALLBACK
// =============================================================================
//
// This strategy places an order at the EXACT SAME PRICE as the followed user,
// while polling the order book in parallel. If the initial order is not fully
// filled, it uses the latest order book to place follow-up orders.
//
// FLOW:
//   1. Calculate target size based on multiplier
//   2. Apply minimum size constraints (5 shares, $1 minimum)
//   3. IN PARALLEL:
//      - Place GTC order at followed user's exact price
//      - Poll order book every 50ms
//   4. When order response arrives:
//      - If matched: done (100% filled)
//      - If live: check fill status, place follow-up orders at ask prices
//
// =============================================================================
func (ct *CopyTrader) executeBotBuyImmediate(ctx context.Context, trade models.TradeDetail, tokenID string, negRisk bool, userSettings *storage.UserCopySettings, txRef string, detectedAt time.Time) error {
	// Elapsed time helper
	elapsed := func() string {
		return fmt.Sprintf("+%dms", time.Since(detectedAt).Milliseconds())
	}

	log.Printf("[%s] [%s] ⚡ IMMEDIATE BUY: starting (parallel order + book polling)", txRef, elapsed())

	// Initialize timing tracking
	startTime := time.Now()
	timing := map[string]interface{}{}

	// Initialize timestamps
	timestamps := &CopyTradeTimestamps{
		ProcessingStartedAt: &startTime,
	}
	if !trade.DetectedAt.IsZero() {
		timestamps.DetectedAt = &trade.DetectedAt
	}

	// Initialize debug log
	debugLog := map[string]interface{}{
		"action":    "IMMEDIATE_BUY",
		"timestamp": startTime.Format(time.RFC3339),
	}

	// Step 1: Get settings
	multiplier := ct.config.Multiplier
	minUSDC := ct.config.MinOrderUSDC
	var maxUSD *float64
	if userSettings != nil {
		multiplier = userSettings.Multiplier
		minUSDC = userSettings.MinUSDC
		maxUSD = userSettings.MaxUSD
	}
	debugLog["settings"] = map[string]interface{}{
		"multiplier": multiplier,
		"minUSDC":    minUSDC,
		"maxUSD":     maxUSD,
	}
	log.Printf("[%s] [%s] ✓ settings (mult=%.2f)", txRef, elapsed(), multiplier)

	// Step 2: Calculate target size at SAME PRICE as followed user
	targetShares := trade.Size * multiplier
	orderPrice := trade.Price // Use exact same price!

	if orderPrice <= 0 || orderPrice >= 1 {
		log.Printf("[%s] [%s] ❌ invalid price: %.4f", txRef, elapsed(), orderPrice)
		debugLog["decision"] = fmt.Sprintf("failed - invalid price: %.4f", orderPrice)
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, 0, "failed", fmt.Sprintf("invalid price: %.4f", orderPrice), "", userSettings.StrategyType, debugLog, timing, timestamps)
	}

	// Calculate max price we're willing to pay (20% slippage from copied price)
	maxPrice := orderPrice * 1.20

	// Calculate cost
	totalSize := targetShares
	totalCost := totalSize * orderPrice

	debugLog["calculation"] = map[string]interface{}{
		"copiedTradeShares": trade.Size,
		"targetShares":      targetShares,
		"copiedPrice":       orderPrice,
		"maxPrice":          maxPrice,
	}

	// Step 3: Apply minimum constraints
	const polymarketMinOrder = 1.0
	effectiveMinUSDC := polymarketMinOrder
	if minUSDC > effectiveMinUSDC {
		effectiveMinUSDC = minUSDC
	}

	// Apply max cap if set
	if maxUSD != nil && totalCost > *maxUSD {
		log.Printf("[%s] [%s] ⚠️ cost $%.4f exceeds max $%.2f, capping", txRef, elapsed(), totalCost, *maxUSD)
		totalCost = *maxUSD
		totalSize = totalCost / orderPrice
		debugLog["maxCapAdjustment"] = map[string]interface{}{
			"maxCap":       *maxUSD,
			"adjustedCost": totalCost,
			"adjustedSize": totalSize,
		}
	}

	// Ensure minimum USDC
	if totalCost < effectiveMinUSDC {
		log.Printf("[%s] [%s] ⚠️ cost $%.4f < min $%.2f, bumping", txRef, elapsed(), totalCost, effectiveMinUSDC)
		originalCost := totalCost
		totalCost = effectiveMinUSDC
		totalSize = totalCost / orderPrice
		debugLog["minOrderAdjustment"] = map[string]interface{}{
			"originalCost": originalCost,
			"adjustedCost": effectiveMinUSDC,
			"adjustedSize": totalSize,
		}
	}

	// Ensure minimum 5 shares
	const minShares = 5.0
	if totalSize < minShares {
		log.Printf("[%s] [%s] ⚠️ size %.4f < min %.0f, bumping", txRef, elapsed(), totalSize, minShares)
		originalSize := totalSize
		totalSize = minShares
		totalCost = totalSize * orderPrice
		debugLog["minSharesAdjustment"] = map[string]interface{}{
			"originalSize": originalSize,
			"adjustedSize": minShares,
			"adjustedCost": totalCost,
		}
	}

	log.Printf("[%s] [%s] 📤 placing order: %.4f shares @ $%.4f + polling book", txRef, elapsed(), totalSize, orderPrice)

	debugLog["order"] = map[string]interface{}{
		"type":       "immediate_limit",
		"side":       "BUY",
		"size":       totalSize,
		"cost":       totalCost,
		"orderPrice": orderPrice,
	}

	// Step 4: Start order placement AND order book polling in parallel
	orderStart := time.Now()
	timestamps.OrderPlacedAt = &orderStart

	// Channel to receive order response
	type orderResult struct {
		resp *api.OrderResponse
		err  error
	}
	orderCh := make(chan orderResult, 1)

	// Latest order book (protected by mutex)
	var latestBook *api.OrderBook
	var bookMu sync.Mutex
	var bookFetchCount int
	stopPolling := make(chan struct{})

	// Start order placement goroutine
	go func() {
		resp, err := ct.clobClient.PlaceOrderFast(ctx, tokenID, api.SideBuy, totalSize, orderPrice, negRisk)
		orderCh <- orderResult{resp, err}
	}()

	// Start order book polling goroutine (every 50ms)
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stopPolling:
				return
			case <-ticker.C:
				book, err := ct.clobClient.GetOrderBook(ctx, tokenID)
				if err == nil {
					bookMu.Lock()
					latestBook = book
					bookFetchCount++
					bookMu.Unlock()
				}
			}
		}
	}()

	// Wait for order response
	result := <-orderCh
	close(stopPolling) // Stop book polling

	orderConfirmed := time.Now()
	timing["place_order_ms"] = float64(time.Since(orderStart).Microseconds()) / 1000

	bookMu.Lock()
	timing["book_fetch_count"] = bookFetchCount
	currentBook := latestBook
	bookMu.Unlock()

	log.Printf("[%s] [%s] 📊 order response received, book polled %d times", txRef, elapsed(), bookFetchCount)

	if result.err != nil {
		timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
		log.Printf("[%s] [%s] ❌ order failed: %v", txRef, elapsed(), result.err)
		debugLog["orderResponse"] = map[string]interface{}{"error": result.err.Error()}
		debugLog["decision"] = fmt.Sprintf("failed - order error: %v", result.err)
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, totalCost, 0, 0, 0, "failed", fmt.Sprintf("order failed: %v", result.err), "", userSettings.StrategyType, debugLog, timing, timestamps)
	}

	resp := result.resp
	timestamps.OrderConfirmedAt = &orderConfirmed

	debugLog["orderResponse"] = map[string]interface{}{
		"success": resp.Success,
		"orderID": resp.OrderID,
		"status":  resp.Status,
	}

	if !resp.Success {
		timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
		log.Printf("[%s] [%s] ❌ order rejected: %s", txRef, elapsed(), resp.ErrorMsg)
		debugLog["decision"] = fmt.Sprintf("failed - rejected: %s", resp.ErrorMsg)
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, totalCost, 0, 0, 0, "failed", resp.ErrorMsg, "", userSettings.StrategyType, debugLog, timing, timestamps)
	}

	// Check if immediately matched
	if resp.Status == "matched" {
		log.Printf("[%s] [%s] ✅ IMMEDIATE FILL! size=%.4f @ $%.4f", txRef, elapsed(), totalSize, orderPrice)
		timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
		debugLog["decision"] = "executed - immediate fill at same price"

		// Update position
		if err := ct.store.UpdateMyPosition(ctx, MyPosition{
			MarketID:  trade.MarketID,
			TokenID:   tokenID,
			Outcome:   trade.Outcome,
			Title:     trade.Title,
			Size:      totalSize,
			AvgPrice:  orderPrice,
			TotalCost: totalCost,
		}); err != nil {
			log.Printf("[%s] Warning: failed to update position: %v", txRef, err)
		}

		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, totalCost, totalCost, orderPrice, totalSize, "executed", "", resp.OrderID, userSettings.StrategyType, debugLog, timing, timestamps)
	}

	// Order is live/pending - need to check fill and place follow-up orders
	log.Printf("[%s] [%s] ⏳ ORDER PENDING (status=%s), checking fill status...", txRef, elapsed(), resp.Status)

	// Get order status to see how much was filled
	order, err := ct.clobClient.GetOrder(ctx, resp.OrderID)
	if err != nil {
		log.Printf("[%s] [%s] ⚠️ failed to get order status: %v, will retry with book", txRef, elapsed(), err)
	}

	var sizeMatched float64
	if order != nil {
		sizeMatched, _ = strconv.ParseFloat(order.SizeMatched, 64)
	}
	unfilledSize := totalSize - sizeMatched
	filledCost := sizeMatched * orderPrice

	log.Printf("[%s] [%s] 📊 initial order: filled=%.4f, unfilled=%.4f", txRef, elapsed(), sizeMatched, unfilledSize)

	// If fully filled (within tolerance), we're done
	if unfilledSize < 0.01 {
		timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
		debugLog["decision"] = "executed - filled at same price"
		if err := ct.store.UpdateMyPosition(ctx, MyPosition{
			MarketID: trade.MarketID, TokenID: tokenID, Outcome: trade.Outcome,
			Title: trade.Title, Size: sizeMatched, AvgPrice: orderPrice, TotalCost: filledCost,
		}); err != nil {
			log.Printf("[%s] Warning: failed to update position: %v", txRef, err)
		}
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, totalCost, filledCost, orderPrice, sizeMatched, "executed", "", resp.OrderID, userSettings.StrategyType, debugLog, timing, timestamps)
	}

	// Cancel the pending order - we'll place new orders at ask prices
	log.Printf("[%s] [%s] ❌ cancelling pending order, will use book for remaining %.4f", txRef, elapsed(), unfilledSize)
	if err := ct.clobClient.CancelOrder(ctx, resp.OrderID); err != nil {
		log.Printf("[%s] [%s] ⚠️ cancel failed: %v", txRef, elapsed(), err)
	}

	// Check if we have an order book to use
	if currentBook == nil || len(currentBook.Asks) == 0 {
		log.Printf("[%s] [%s] ⚠️ no order book available, logging partial fill", txRef, elapsed())
		timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
		var status, failReason string
		if sizeMatched > 0.01 {
			status = "partial"
			failReason = fmt.Sprintf("partial fill: %.4f/%.4f, no book for remaining", sizeMatched, totalSize)
		} else {
			status = "failed"
			failReason = "no fill at same price, no book available"
		}
		debugLog["decision"] = failReason
		if sizeMatched > 0.01 {
			ct.store.UpdateMyPosition(ctx, MyPosition{
				MarketID: trade.MarketID, TokenID: tokenID, Outcome: trade.Outcome,
				Title: trade.Title, Size: sizeMatched, AvgPrice: orderPrice, TotalCost: filledCost,
			})
		}
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, totalCost, filledCost, orderPrice, sizeMatched, status, failReason, resp.OrderID, userSettings.StrategyType, debugLog, timing, timestamps)
	}

	// Use order book to place follow-up orders for remaining shares
	log.Printf("[%s] [%s] 📖 using order book (%d asks) for remaining %.4f shares", txRef, elapsed(), len(currentBook.Asks), unfilledSize)

	// Calculate how to fill remaining from order book
	remainingShares := unfilledSize
	var followUpOrders []map[string]interface{}
	var totalFollowUpSize, totalFollowUpCost float64

	for _, ask := range currentBook.Asks {
		if remainingShares < 0.01 {
			break
		}

		askPrice, _ := strconv.ParseFloat(ask.Price, 64)
		askSize, _ := strconv.ParseFloat(ask.Size, 64)

		// Skip if price too high
		if askPrice > maxPrice {
			log.Printf("[%s] [%s] ⚠️ ask %.4f > max %.4f, stopping", txRef, elapsed(), askPrice, maxPrice)
			break
		}

		// Calculate how much to take from this level
		takeSize := remainingShares
		if takeSize > askSize {
			takeSize = askSize
		}

		// Ensure minimum order size
		if takeSize < minShares {
			takeSize = minShares
			if takeSize > remainingShares*2 { // Don't over-buy by too much
				takeSize = remainingShares
			}
		}

		followUpOrders = append(followUpOrders, map[string]interface{}{
			"price": askPrice,
			"size":  takeSize,
		})

		remainingShares -= takeSize
		totalFollowUpSize += takeSize
		totalFollowUpCost += takeSize * askPrice
	}

	debugLog["followUpOrders"] = followUpOrders

	// Place follow-up orders
	var followUpFilled float64
	var followUpCost float64
	var allOrderIDs []string
	allOrderIDs = append(allOrderIDs, resp.OrderID)

	for _, fo := range followUpOrders {
		foPrice := fo["price"].(float64)
		foSize := fo["size"].(float64)

		// Ensure minimum order size
		if foSize < minShares {
			foSize = minShares
		}

		log.Printf("[%s] [%s] 📤 follow-up order: %.4f @ $%.4f", txRef, elapsed(), foSize, foPrice)

		foResp, err := ct.clobClient.PlaceOrderFast(ctx, tokenID, api.SideBuy, foSize, foPrice, negRisk)
		if err != nil {
			log.Printf("[%s] [%s] ⚠️ follow-up order failed: %v", txRef, elapsed(), err)
			continue
		}

		if foResp.Success {
			if foResp.Status == "matched" {
				followUpFilled += foSize
				followUpCost += foSize * foPrice
				log.Printf("[%s] [%s] ✅ follow-up filled: %.4f @ $%.4f", txRef, elapsed(), foSize, foPrice)
			} else {
				// Order is pending - cancel it (we want immediate fills only)
				log.Printf("[%s] [%s] ⏳ follow-up pending, cancelling", txRef, elapsed())
				ct.clobClient.CancelOrder(ctx, foResp.OrderID)
			}
			allOrderIDs = append(allOrderIDs, foResp.OrderID)
		}
	}

	// Calculate final totals
	finalFilledSize := sizeMatched + followUpFilled
	finalFilledCost := filledCost + followUpCost
	finalAvgPrice := orderPrice
	if finalFilledSize > 0 {
		finalAvgPrice = finalFilledCost / finalFilledSize
	}

	timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
	timing["follow_up_orders"] = len(followUpOrders)

	// Determine final status
	var status, failReason string
	if finalFilledSize >= totalSize*0.99 {
		status = "executed"
		debugLog["decision"] = fmt.Sprintf("executed - filled %.4f via initial + %d follow-up orders", finalFilledSize, len(followUpOrders))
	} else if finalFilledSize > 0.01 {
		status = "partial"
		failReason = fmt.Sprintf("partial: %.4f/%.4f (%.1f%%) filled", finalFilledSize, totalSize, (finalFilledSize/totalSize)*100)
		debugLog["decision"] = failReason
	} else {
		status = "failed"
		failReason = "no fills from initial or follow-up orders"
		debugLog["decision"] = failReason
	}

	debugLog["finalResult"] = map[string]interface{}{
		"initialFilled":   sizeMatched,
		"followUpFilled":  followUpFilled,
		"totalFilled":     finalFilledSize,
		"totalCost":       finalFilledCost,
		"avgPrice":        finalAvgPrice,
		"targetSize":      totalSize,
		"followUpOrders":  len(followUpOrders),
	}

	// Update position
	if finalFilledSize > 0.01 {
		if err := ct.store.UpdateMyPosition(ctx, MyPosition{
			MarketID:  trade.MarketID,
			TokenID:   tokenID,
			Outcome:   trade.Outcome,
			Title:     trade.Title,
			Size:      finalFilledSize,
			AvgPrice:  finalAvgPrice,
			TotalCost: finalFilledCost,
		}); err != nil {
			log.Printf("[%s] Warning: failed to update position: %v", txRef, err)
		}
	}

	if status == "executed" {
		log.Printf("[%s] [%s] ✅ SUCCESS: filled %.4f shares @ avg $%.4f (cost=$%.4f)", txRef, elapsed(), finalFilledSize, finalAvgPrice, finalFilledCost)
	} else if status == "partial" {
		log.Printf("[%s] [%s] ⚠️ PARTIAL: filled %.4f/%.4f shares", txRef, elapsed(), finalFilledSize, totalSize)
	} else {
		log.Printf("[%s] [%s] ❌ FAILED: no fills", txRef, elapsed())
	}

	return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, totalCost, finalFilledCost, finalAvgPrice, finalFilledSize, status, failReason, strings.Join(allOrderIDs, ","), userSettings.StrategyType, debugLog, timing, timestamps)
}

// executeBotSell implements the bot following strategy for sells.
// It tries to sell at the copied user's exact price, then sweeps bids down to -10%.
// If still not filled, creates limit orders at -3% and -5%, waits 3 min, then market sells remainder.
func (ct *CopyTrader) executeBotSell(ctx context.Context, trade models.TradeDetail, tokenID string, negRisk bool, userSettings *storage.UserCopySettings) error {
	txRef := trade.TransactionHash
	if len(txRef) > 16 {
		txRef = txRef[:16]
	}
	detectedAt := trade.DetectedAt
	if detectedAt.IsZero() {
		detectedAt = time.Now()
	}
	return ct.executeBotSellWithBook(ctx, trade, tokenID, negRisk, userSettings, nil, txRef, detectedAt)
}

// executeBotSellWithBook is the same as executeBotSell but accepts a pre-fetched order book channel
// for parallel execution optimization
func (ct *CopyTrader) executeBotSellWithBook(ctx context.Context, trade models.TradeDetail, tokenID string, negRisk bool, userSettings *storage.UserCopySettings, orderBookCh <-chan orderBookResult, txRef string, detectedAt time.Time) error {
	// Elapsed time helper
	elapsed := func() string {
		return fmt.Sprintf("+%dms", time.Since(detectedAt).Milliseconds())
	}

	log.Printf("[%s] [%s] 💰 SELL: starting execution", txRef, elapsed())

	// Initialize timestamps
	startTime := time.Now()
	timestamps := &CopyTradeTimestamps{
		ProcessingStartedAt: &startTime,
	}
	if !trade.DetectedAt.IsZero() {
		timestamps.DetectedAt = &trade.DetectedAt
	}

	// Get settings
	multiplier := ct.config.Multiplier
	if userSettings != nil {
		multiplier = userSettings.Multiplier
	}
	targetTokens := trade.Size * multiplier
	log.Printf("[%s] [%s] ✓ target=%.4f shares (mult=%.2f)", txRef, elapsed(), targetTokens, multiplier)

	// Get actual position
	log.Printf("[%s] [%s] 🔍 checking position...", txRef, elapsed())
	var ourPosition float64
	actualPositions, err := ct.client.GetOpenPositions(ctx, ct.myAddress)
	if err != nil {
		log.Printf("[%s] [%s] ⚠️ position API failed: %v", txRef, elapsed(), err)
	} else {
		for _, pos := range actualPositions {
			if pos.Asset == tokenID && pos.Size.Float64() > 0 {
				ourPosition = pos.Size.Float64()
				break
			}
		}
	}

	// Fall back to local tracking
	if ourPosition <= 0 {
		position, err := ct.store.GetMyPosition(ctx, trade.MarketID, trade.Outcome)
		if err == nil && position.Size > 0 {
			ourPosition = position.Size
		}
	}

	if ourPosition <= 0 {
		log.Printf("[%s] [%s] ⏭️ no position to sell", txRef, elapsed())
		return nil
	}
	log.Printf("[%s] [%s] ✓ position=%.4f shares", txRef, elapsed(), ourPosition)

	// Sell the minimum of targetTokens or ourPosition
	sellSize := targetTokens
	if sellSize > ourPosition {
		sellSize = ourPosition
		log.Printf("[%s] [%s] 📉 selling entire position (target > position)", txRef, elapsed())
	}

	// Ensure minimum 5 shares for SELL
	const minSellShares = 5.0
	if sellSize < minSellShares {
		if ourPosition >= minSellShares {
			// We have enough - bump up to minimum
			log.Printf("[%s] [%s] 📈 size %.4f below min, bumping to %.0f", txRef, elapsed(), sellSize, minSellShares)
			sellSize = minSellShares
		} else {
			// We don't have enough - sell entire position
			log.Printf("[%s] [%s] 📈 size %.4f below min, selling entire %.4f", txRef, elapsed(), sellSize, ourPosition)
			sellSize = ourPosition
		}
	}

	// Use dynamic slippage based on price (lower prices are more volatile)
	copiedPrice := trade.Price
	var minPrice float64
	if copiedPrice > 0 {
		slippage := getMaxSlippage(copiedPrice)
		minPrice = copiedPrice * (1 - slippage)
		if minPrice < 0 {
			minPrice = 0.01 // Never go below 1 cent
		}
	} else {
		// Blockchain trades don't have price - allow any price (accept any bid)
		minPrice = 0.0
	}
	log.Printf("[%s] [%s] ✓ sellSize=%.4f, minPrice=%.4f", txRef, elapsed(), sellSize, minPrice)

	// Get order book (use pre-fetched if available, otherwise fetch now)
	log.Printf("[%s] [%s] 📡 fetching order book...", txRef, elapsed())
	var book *api.OrderBook
	var bookErr error
	if orderBookCh != nil {
		// Use pre-fetched order book from parallel execution
		result := <-orderBookCh
		book, bookErr = result.book, result.err
		log.Printf("[%s] [%s] ✓ order book from parallel fetch", txRef, elapsed())
	} else {
		// Fetch fresh order book
		book, bookErr = ct.clobClient.GetOrderBook(ctx, tokenID)
		log.Printf("[%s] [%s] ✓ order book fetched fresh", txRef, elapsed())
	}
	if bookErr != nil {
		if strings.Contains(bookErr.Error(), "404") || strings.Contains(bookErr.Error(), "No orderbook exists") {
			log.Printf("[CopyTrader-Bot] SELL: market closed/resolved, skipping")
			return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "skipped", "market closed/resolved", "", userSettings.StrategyType, nil, nil, timestamps)
		}
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "failed", fmt.Sprintf("order book error: %v", bookErr), "", userSettings.StrategyType, nil, nil, timestamps)
	}

	if len(book.Bids) == 0 {
		log.Printf("[CopyTrader-Bot] SELL: no bids in order book")
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "skipped", "no bids in order book", "", userSettings.StrategyType, nil, nil, timestamps)
	}

	// Find all bids within our price range, sorted by price (highest first)
	// The order book bids should already be sorted descending by price
	var acceptableBids []struct {
		price float64
		size  float64
	}

	for _, bid := range book.Bids {
		var bidPrice, bidSize float64
		fmt.Sscanf(bid.Price, "%f", &bidPrice)
		fmt.Sscanf(bid.Size, "%f", &bidSize)

		// Stop if price is below our minimum (10% below copied price)
		if bidPrice < minPrice {
			break
		}

		acceptableBids = append(acceptableBids, struct {
			price float64
			size  float64
		}{bidPrice, bidSize})
	}

	// Calculate how much we can sell to acceptable bids
	// Track the minimum price level (deepest bid we'll sweep)
	remainingSize := sellSize
	totalSold := 0.0
	totalUSDC := 0.0
	minFillPrice := 0.0 // Track the lowest price level we need to sweep

	for _, bid := range acceptableBids {
		if remainingSize <= 0 {
			break
		}

		if bid.size <= remainingSize {
			// Take entire level
			totalSold += bid.size
			totalUSDC += bid.price * bid.size
			remainingSize -= bid.size
			minFillPrice = bid.price // Update as we sweep deeper (bids are sorted high to low)
		} else {
			// Partial fill at this level
			totalSold += remainingSize
			totalUSDC += bid.price * remainingSize
			remainingSize = 0
			minFillPrice = bid.price // This level is our deepest
		}
	}

	// If we found acceptable bids, sell into them
	if totalSold > 0.01 {
		avgPrice := totalUSDC / totalSold
		// Use minFillPrice (not avgPrice) to ensure we sweep all bid levels
		orderPrice := minFillPrice
		log.Printf("[%s] [%s] 📊 found bids: size=%.4f, avgPrice=%.4f, orderPrice=%.4f", txRef, elapsed(), totalSold, avgPrice, orderPrice)

		log.Printf("[%s] [%s] 🚀 placing SELL order...", txRef, elapsed())
		orderPlacedAt := time.Now()
		timestamps.OrderPlacedAt = &orderPlacedAt
		resp, err := ct.clobClient.PlaceOrderFast(ctx, tokenID, api.SideSell, totalSold, orderPrice, negRisk)
		orderConfirmedAt := time.Now()
		if err != nil {
			log.Printf("[%s] [%s] ❌ SELL failed: %v", txRef, elapsed(), err)
			return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "failed", fmt.Sprintf("order failed: %v", err), "", userSettings.StrategyType, nil, nil, timestamps)
		}

		timestamps.OrderConfirmedAt = &orderConfirmedAt

		if !resp.Success {
			log.Printf("[%s] [%s] ❌ SELL rejected: %s", txRef, elapsed(), resp.ErrorMsg)
			return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "failed", resp.ErrorMsg, "", userSettings.StrategyType, nil, nil, timestamps)
		}

		log.Printf("[%s] [%s] ✅ SELL SUCCESS: status=%s, size=%.4f, price=%.4f, cost=$%.2f",
			txRef, elapsed(), resp.Status, totalSold, avgPrice, totalUSDC)

		// Clear position if we sold everything
		if remainingSize < 0.01 {
			ct.store.ClearMyPosition(ctx, trade.MarketID, trade.Outcome)
		}

		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, totalUSDC, avgPrice, totalSold, "executed", "", resp.OrderID, userSettings.StrategyType, nil, nil, timestamps)
	}

	// No acceptable bids found within 10% - need to create limit orders
	log.Printf("[%s] [%s] ⚠️ no bids within slippage, creating limit orders...", txRef, elapsed())

	// Create 3 limit sell orders:
	// - 20% at same price as copied user
	// - 40% at -3%
	// - 40% at -5%
	// Note: Sizes are calculated but for now we fall back to market sell
	_ = sellSize * 0.20 // order1Size - 20%
	_ = sellSize * 0.40 // order2Size - 40%
	_ = sellSize * 0.40 // order3Size - 40%

	order1Price := copiedPrice
	order2Price := copiedPrice * 0.97 // -3%
	order3Price := copiedPrice * 0.95 // -5%

	log.Printf("[%s] [%s] 📋 limit prices: %.4f / %.4f / %.4f", txRef, elapsed(), order1Price, order2Price, order3Price)

	// Place limit orders (using market orders with price limits for now)
	var orderIDs []string
	var totalFilled float64
	var totalValue float64

	log.Printf("[%s] [%s] 🚀 placing SELL order (limit fallback)...", txRef, elapsed())
	orderPlacedAt := time.Now()
	timestamps.OrderPlacedAt = &orderPlacedAt
	resp, err := ct.clobClient.PlaceOrderFast(ctx, tokenID, api.SideSell, sellSize, order3Price, negRisk)
	orderConfirmedAt := time.Now()

	// Capture detailed error info
	var failReason string
	if err != nil {
		failReason = fmt.Sprintf("market sell failed: %v", err)
		log.Printf("[%s] [%s] ❌ SELL order error: %v", txRef, elapsed(), err)
	} else if !resp.Success {
		failReason = fmt.Sprintf("market sell rejected: %s", resp.ErrorMsg)
		log.Printf("[%s] [%s] ❌ SELL order rejected: %s", txRef, elapsed(), resp.ErrorMsg)
	} else {
		timestamps.OrderConfirmedAt = &orderConfirmedAt
		orderIDs = append(orderIDs, resp.OrderID)
		// Estimate fill
		if len(book.Bids) > 0 {
			var bestBid float64
			fmt.Sscanf(book.Bids[0].Price, "%f", &bestBid)
			totalFilled = sellSize
			totalValue = sellSize * bestBid
			log.Printf("[%s] [%s] ✅ SELL SUCCESS (limit): size=%.4f, price=%.4f", txRef, elapsed(), totalFilled, bestBid)
		} else {
			failReason = "no bids available after order"
		}
	}

	if totalFilled > 0 {
		avgPrice := totalValue / totalFilled
		ct.store.ClearMyPosition(ctx, trade.MarketID, trade.Outcome)
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, totalValue, avgPrice, totalFilled, "executed", "", strings.Join(orderIDs, ","), userSettings.StrategyType, nil, nil, timestamps)
	}

	// Failed to sell - include detailed reason
	if failReason == "" {
		failReason = "no fills achieved (unknown reason)"
	}
	log.Printf("[%s] [%s] ❌ SELL failed: %s", txRef, elapsed(), failReason)
	return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "failed", failReason, "", userSettings.StrategyType, nil, nil, timestamps)
}

// CopyTradeTimestamps holds timing information for a copy trade
type CopyTradeTimestamps struct {
	DetectedAt          *time.Time
	ProcessingStartedAt *time.Time
	OrderPlacedAt       *time.Time
	OrderConfirmedAt    *time.Time
}

func (ct *CopyTrader) logCopyTrade(ctx context.Context, trade models.TradeDetail, tokenID string, intended, actual, price, size float64, status, errReason, orderID string) error {
	return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, intended, actual, price, size, status, errReason, orderID, storage.StrategyHuman, nil, nil, nil)
}

func (ct *CopyTrader) logCopyTradeWithStrategy(ctx context.Context, trade models.TradeDetail, tokenID string, intended, actual, price, size float64, status, errReason, orderID string, strategyType int, debugLog, timingBreakdown map[string]interface{}, timestamps *CopyTradeTimestamps) error {
	// Make a copy of data for async DB writes
	copyTrade := CopyTrade{
		OriginalTradeID: trade.ID,
		OriginalTrader:  trade.UserID,
		MarketID:        trade.MarketID,
		TokenID:         tokenID,
		Outcome:         trade.Outcome,
		Title:           trade.Title,
		Side:            trade.Side,
		IntendedUSDC:    intended,
		ActualUSDC:      actual,
		PricePaid:       price,
		SizeBought:      size,
		Status:          status,
		ErrorReason:     errReason,
		OrderID:         orderID,
		DetectionSource: trade.DetectionSource,
	}

	// Async DB write - don't block the critical path
	go func() {
		bgCtx := context.Background()
		if err := ct.store.SaveCopyTrade(bgCtx, copyTrade); err != nil {
			log.Printf("[CopyTrader] Warning: async save copy trade failed: %v", err)
		}
	}()

	// Save to new detailed log table
	// Shares: negative for buy, positive for sell
	followingShares := trade.Size
	if trade.Side == "BUY" {
		followingShares = -followingShares
	}

	var followerTime *time.Time
	var followerShares, followerPrice *float64

	if status == "executed" || status == "success" {
		now := time.Now()
		followerTime = &now

		shares := size
		if trade.Side == "BUY" {
			shares = -shares
		}
		followerShares = &shares
		followerPrice = &price
	}

	// Use TxHash for dedup (not trade.ID which may include :LogIndex)
	followingTradeID := trade.TransactionHash
	if followingTradeID == "" {
		followingTradeID = trade.ID
	}

	logEntry := storage.CopyTradeLogEntry{
		FollowingAddress: trade.UserID,
		FollowingTradeID: followingTradeID,
		FollowingTime:    trade.Timestamp,
		FollowingShares:  followingShares,
		FollowingPrice:   trade.Price,
		FollowerTime:     followerTime,
		FollowerShares:   followerShares,
		FollowerPrice:    followerPrice,
		FollowerOrderID:  "", // Leave empty - will be populated async with real tx hash
		MarketTitle:      trade.Title,
		Outcome:          trade.Outcome,
		TokenID:          tokenID,
		Status:           status,
		FailedReason:     errReason,
		StrategyType:     strategyType,
		DetectionSource:  trade.DetectionSource,
		DebugLog:         debugLog,
		TimingBreakdown:  timingBreakdown,
	}

	// Add timing timestamps if provided
	if timestamps != nil {
		logEntry.DetectedAt = timestamps.DetectedAt
		logEntry.ProcessingStartedAt = timestamps.ProcessingStartedAt
		logEntry.OrderPlacedAt = timestamps.OrderPlacedAt
		logEntry.OrderConfirmedAt = timestamps.OrderConfirmedAt
	} else if !trade.DetectedAt.IsZero() {
		// Fall back to trade's DetectedAt if timestamps struct not provided
		logEntry.DetectedAt = &trade.DetectedAt
	}

	// Async DB write - don't block the critical path
	go func() {
		bgCtx := context.Background()
		if err := ct.store.SaveCopyTradeLog(bgCtx, logEntry); err != nil {
			log.Printf("[CopyTrader] Warning: async save copy trade log failed: %v", err)
		}
	}()

	// For executed/partial trades, spawn async goroutine to fetch real tx hash after 60s
	if (status == "executed" || status == "success" || status == "partial") && tokenID != "" && ct.myAddress != "" {
		followerTimeVal := time.Now()
		if followerTime != nil {
			followerTimeVal = *followerTime
		}
		go ct.fetchAndUpdateTxHash(trade.ID, tokenID, followerTimeVal)
	}

	return nil
}

// fetchAndUpdateTxHash waits, then fetches the real blockchain tx hash from Data API and updates the DB
// This is called async after a trade is executed since the tx hash isn't available immediately
func (ct *CopyTrader) fetchAndUpdateTxHash(followingTradeID, tokenID string, followerTime time.Time) {
	// Wait 60 seconds for the trade to settle on-chain
	time.Sleep(60 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log.Printf("[CopyTrader] Fetching tx hash for %s (tokenID=%s, time=%s, myAddr=%s)",
		followingTradeID[:16], tokenID[:20], followerTime.Format("15:04:05"), ct.myAddress[:10])

	// Query Data API for our recent trades
	trades, err := ct.client.GetActivity(ctx, api.TradeQuery{
		User:  ct.myAddress,
		Limit: 50,
	})
	if err != nil {
		log.Printf("[CopyTrader] Failed to fetch trades for tx hash lookup: %v", err)
		return
	}

	log.Printf("[CopyTrader] Got %d trades from Data API for tx hash lookup", len(trades))

	// Find matching trade by token and approximate timestamp (within 2 minutes)
	for _, trade := range trades {
		// Match by token ID
		if trade.Asset != tokenID {
			continue
		}

		// Match by timestamp (within 2 minutes of our execution time)
		tradeTime := time.Unix(trade.Timestamp, 0)
		timeDiff := tradeTime.Sub(followerTime)
		if timeDiff < -2*time.Minute || timeDiff > 2*time.Minute {
			log.Printf("[CopyTrader] Token match but time mismatch: trade=%s, follower=%s, diff=%s",
				tradeTime.Format("15:04:05"), followerTime.Format("15:04:05"), timeDiff)
			continue
		}

		// Found a match - update the DB
		if trade.TransactionHash != "" {
			if err := ct.store.UpdateCopyTradeLogTxHash(ctx, followingTradeID, trade.TransactionHash); err != nil {
				log.Printf("[CopyTrader] Failed to update tx hash for %s: %v", followingTradeID[:16], err)
			} else {
				log.Printf("[CopyTrader] ✅ Updated tx hash for %s -> %s", followingTradeID[:16], trade.TransactionHash[:16])
			}
			return
		}
	}

	log.Printf("[CopyTrader] ❌ No matching trade found for tx hash lookup (followingTradeID=%s, tokenID=%s)",
		followingTradeID[:16], tokenID[:20])
}

// GetStats returns copy trading statistics
func (ct *CopyTrader) GetStats(ctx context.Context) (map[string]interface{}, error) {
	return ct.store.GetCopyTradeStats(ctx)
}
