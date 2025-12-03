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
// │  9. Place order via clobClient.PlaceOrderFast() (FOK then GTC fallback)   │
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
// - FOK vs GTC: Try Fill-Or-Kill first (immediate), fall back to Good-Till-Cancel
//
// TIMING TARGET: ~600ms from detection to order placed on CLOB
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

	// Use TxHash as dedup key (not trade.ID which may include :LogIndex)
	// This ensures blockchain-detected trades (ID=TxHash:LogIndex) and API-detected
	// trades (ID=TxHash) are properly deduplicated against each other.
	// Multiple fills from the same tx = one trading decision = one copy trade.
	dedupKey := trade.TransactionHash
	if dedupKey == "" {
		dedupKey = trade.ID // Fallback for trades without TransactionHash
	}

	// CRITICAL: Check if this trade is already being processed (prevents duplicate orders)
	ct.inFlightTradesMu.Lock()
	if startTime, exists := ct.inFlightTrades[dedupKey]; exists {
		ct.inFlightTradesMu.Unlock()
		log.Printf("[CopyTrader] Skipping duplicate trade %s (already in-flight since %s ago)",
			dedupKey[:16], time.Since(startTime).Round(time.Millisecond))
		return nil
	}
	ct.inFlightTrades[dedupKey] = time.Now()
	// Cleanup old entries (older than 2 minutes)
	for id, t := range ct.inFlightTrades {
		if time.Since(t) > 2*time.Minute {
			delete(ct.inFlightTrades, id)
		}
	}
	ct.inFlightTradesMu.Unlock()

	// DATABASE-LEVEL DEDUP: Check if this trade was already executed (prevents duplicates across pods)
	// Use dedupKey (TxHash) to match across detection methods
	alreadyExecuted, err := ct.store.IsTradeAlreadyExecuted(ctx, dedupKey)
	if err != nil {
		log.Printf("[CopyTrader] Warning: failed to check for duplicate trade %s: %v", trade.ID[:16], err)
		// Continue anyway - better to risk a duplicate than miss a trade
	} else if alreadyExecuted {
		log.Printf("[CopyTrader] Skipping trade %s (already executed in DB)", trade.ID[:16])
		return nil
	}

	// Skip trades that are too old (e.g., server restart catch-up)
	// These are stale - markets have moved, prices changed, no point copying
	const maxTradeAge = 5 * time.Minute
	tradeAge := time.Since(trade.Timestamp)
	if tradeAge > maxTradeAge {
		log.Printf("[CopyTrader] Skipping stale trade %s (%.1f min old, max=%.1f min)",
			trade.ID[:16], tradeAge.Minutes(), maxTradeAge.Minutes())
		return nil
	}

	// Skip non-TRADE types (REDEEM, SPLIT, MERGE)
	if trade.Type != "" && trade.Type != "TRADE" {
		log.Printf("[CopyTrader] Skipping non-trade: %s (type=%s)", trade.ID, trade.Type)
		return nil
	}

	// FAST PATH: For blockchain-detected trades, we already have the token ID
	// Skip all the Gamma/CLOB lookups - they just slow us down
	// NOTE: LiveData WebSocket trades have conditionID, not tokenID - need to look it up
	// NOTE: Mempool trades have UNRELIABLE decoded tokenID/side - MUST verify!
	isBlockchainTrade := trade.TransactionHash != "" && trade.DetectedAt != (time.Time{}) &&
		trade.DetectionSource != "live_ws" && trade.DetectionSource != "mempool"

	var tokenID string
	var negRisk bool

	if isBlockchainTrade {
		// Fast path: use token ID directly (already in decimal format from realtime_detector)
		tokenID = trade.MarketID
		negRisk = false // Default, will be overridden by order book if needed
		log.Printf("[CopyTrader] ⚡ FAST PATH: blockchain trade, skipping API lookups, tokenID=%s", tokenID)
	} else if trade.DetectionSource == "mempool" {
		// Mempool trades: use Alchemy simulation/receipt for ACCURATE data
		// The decoded calldata is unreliable - Alchemy gives us real OrderFilled events
		log.Printf("[CopyTrader] ⚡ Mempool trade: extracting accurate data via Alchemy (txHash=%s)", trade.TransactionHash)

		// Call Alchemy to get accurate Side, Size, Price, TokenID
		alchemyResult := api.ExtractViaAlchemySimple(trade.TransactionHash, trade.UserID)
		if alchemyResult == nil || alchemyResult.Error != "" {
			errMsg := "unknown error"
			if alchemyResult != nil {
				errMsg = alchemyResult.Error
			}
			log.Printf("[CopyTrader] ⚠️ Mempool: Alchemy extraction failed: %s", errMsg)
			return ct.logCopyTrade(ctx, trade, "", 0, 0, 0, 0, "failed", fmt.Sprintf("mempool: Alchemy extraction failed: %s", errMsg), "")
		}

		log.Printf("[CopyTrader] ⚡ Mempool: Alchemy extracted (%s) - %s %.4f tokens @ %.4f, tokenID=%s",
			alchemyResult.Method, alchemyResult.Direction, alchemyResult.Tokens, alchemyResult.Price, alchemyResult.TokenID)

		// Update trade with accurate Alchemy data
		trade.Side = alchemyResult.Direction
		trade.Size = alchemyResult.Tokens
		trade.Price = alchemyResult.Price
		tokenID = alchemyResult.TokenID

		// Look up outcome from token cache (tokenID is now accurate from Alchemy)
		tokenInfo, err := ct.store.GetTokenInfo(ctx, tokenID)
		if err == nil && tokenInfo != nil {
			trade.Outcome = tokenInfo.Outcome
			log.Printf("[CopyTrader] ⚡ Mempool: outcome from cache = %s", trade.Outcome)
		} else {
			// Fetch from Gamma API
			gammaInfo, err := ct.clobClient.GetTokenInfoByID(ctx, tokenID)
			if err != nil {
				return ct.logCopyTrade(ctx, trade, "", 0, 0, 0, 0, "failed", fmt.Sprintf("mempool: failed to get token info for %s: %v", tokenID, err), "")
			}
			trade.Outcome = gammaInfo.Outcome
			negRisk = gammaInfo.NegRisk
			ct.store.SaveTokenInfo(ctx, tokenID, gammaInfo.ConditionID, gammaInfo.Outcome, gammaInfo.Title, gammaInfo.Slug)
			log.Printf("[CopyTrader] ⚡ Mempool: outcome from Gamma = %s", trade.Outcome)
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

	log.Printf("[CopyTrader] ⏱️ TIMING: preExecution=%dms", time.Since(processStart).Milliseconds())

	// OPTIMIZATION: Start order book fetch NOW while we check settings
	// This runs in parallel - saves ~50-200ms on the critical path
	orderBookCh := make(chan orderBookResult, 1)
	go func() {
		book, err := ct.clobClient.GetOrderBook(ctx, tokenID)
		orderBookCh <- orderBookResult{book, err}
	}()

	// Get user settings - first try cached settings (faster, no DB query), then fall back to DB
	// IMPORTANT: Users without explicit settings are DISABLED by default
	strategyType := storage.StrategyHuman
	var userSettings *storage.UserCopySettings
	if ct.detector != nil {
		userSettings = ct.detector.GetCachedUserSettings(trade.UserID)
	}
	if userSettings == nil {
		// Fall back to DB query if not in cache
		var err error
		userSettings, err = ct.store.GetUserCopySettings(ctx, trade.UserID)
		if err != nil || userSettings == nil {
			// No settings found - default to DISABLED (don't copy unknown users)
			log.Printf("[CopyTrader] Skipping: user %s has no copy settings (disabled by default)", trade.UserID)
			return nil
		}
	}
	if !userSettings.Enabled {
		log.Printf("[CopyTrader] Skipping: user %s has copy trading disabled", trade.UserID)
		return nil
	}
	strategyType = userSettings.StrategyType

	// Route based on strategy type and side
	// Strategy 3 (BTC 15m) uses the same execution as Strategy 2 (Bot)
	if trade.Side == "BUY" {
		if strategyType == storage.StrategyBot || strategyType == storage.StrategyBTC15m {
			return ct.executeBotBuyWithBook(ctx, trade, tokenID, negRisk, userSettings, orderBookCh)
		}
		return ct.executeBuy(ctx, trade, tokenID, negRisk)
	} else if trade.Side == "SELL" {
		if strategyType == storage.StrategyBot || strategyType == storage.StrategyBTC15m {
			return ct.executeBotSellWithBook(ctx, trade, tokenID, negRisk, userSettings, orderBookCh)
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
	return ct.executeBotBuyWithBook(ctx, trade, tokenID, negRisk, userSettings, nil)
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
func (ct *CopyTrader) executeBotBuyWithBook(ctx context.Context, trade models.TradeDetail, tokenID string, negRisk bool, userSettings *storage.UserCopySettings, orderBookCh <-chan orderBookResult) error {
	// Initialize timing tracking
	startTime := time.Now()
	timing := map[string]interface{}{}

	// Initialize timestamps for full latency tracking
	timestamps := &CopyTradeTimestamps{
		ProcessingStartedAt: &startTime,
	}
	// Capture detected_at from trade if available
	if !trade.DetectedAt.IsZero() {
		timestamps.DetectedAt = &trade.DetectedAt
	}

	// Initialize debug log
	debugLog := map[string]interface{}{
		"action":    "BUY",
		"timestamp": startTime.Format(time.RFC3339),
	}

	// Step 1: Get settings
	settingsStart := time.Now()
	multiplier := ct.config.Multiplier
	minUSDC := ct.config.MinOrderUSDC
	var maxUSD *float64
	if userSettings != nil {
		multiplier = userSettings.Multiplier
		minUSDC = userSettings.MinUSDC
		maxUSD = userSettings.MaxUSD
	}
	timing["1_settings_ms"] = float64(time.Since(settingsStart).Microseconds()) / 1000

	debugLog["settings"] = map[string]interface{}{
		"multiplier": multiplier,
		"minUSDC":    minUSDC,
		"maxUSD":     maxUSD,
	}

	// Step 2: Calculate target shares (we follow number of shares, not dollar amount)
	calcStart := time.Now()
	targetShares := trade.Size * multiplier

	// Copied user's price is our target price
	// Use dynamic slippage based on price (lower prices are more volatile)
	copiedPrice := trade.Price
	var maxPrice float64
	if copiedPrice > 0 {
		slippage := getMaxSlippage(copiedPrice)
		maxPrice = copiedPrice * (1 + slippage)
		log.Printf("[CopyTrader-Bot] BUY: Using dynamic slippage +%.0f%% for price $%.2f", slippage*100, copiedPrice)
	} else {
		// Blockchain trades don't have price - allow any price up to $1
		maxPrice = 1.0
		log.Printf("[CopyTrader-Bot] BUY: No price from blockchain, allowing any price up to $1.00")
	}
	timing["2_calculation_ms"] = float64(time.Since(calcStart).Microseconds()) / 1000

	slippagePct := getMaxSlippage(copiedPrice) * 100
	debugLog["calculation"] = map[string]interface{}{
		"copiedTradeShares":  trade.Size,
		"targetShares":       targetShares,
		"copiedPrice":        copiedPrice,
		"maxPrice":           maxPrice,
		"priceLimit":         fmt.Sprintf("+%.0f%%", slippagePct),
	}

	log.Printf("[CopyTrader-Bot] BUY: Copied shares=%.4f, targetShares=%.4f, copiedPrice=%.4f, maxPrice=%.4f (+%.0f%%), market=%s",
		trade.Size, targetShares, copiedPrice, maxPrice, slippagePct, trade.Title)

	// Step 3: Get order book (use pre-fetched if available, otherwise fetch now)
	orderBookStart := time.Now()
	var book *api.OrderBook
	var err error
	if orderBookCh != nil {
		// Use pre-fetched order book from parallel execution
		result := <-orderBookCh
		book, err = result.book, result.err
		timing["4_get_orderbook_ms"] = float64(time.Since(orderBookStart).Microseconds()) / 1000
		timing["4_orderbook_prefetched"] = true
	} else {
		// Fetch fresh order book
		book, err = ct.clobClient.GetOrderBook(ctx, tokenID)
		timing["4_get_orderbook_ms"] = float64(time.Since(orderBookStart).Microseconds()) / 1000
	}
	if err != nil {
		timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
		debugLog["orderBook"] = map[string]interface{}{"error": err.Error()}
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "No orderbook exists") {
			log.Printf("[CopyTrader-Bot] BUY: market closed/resolved, skipping")
			debugLog["decision"] = "skipped - market closed/resolved"
			return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, 0, "skipped", "market closed/resolved", "", userSettings.StrategyType, debugLog, timing, timestamps)
		}
		debugLog["decision"] = fmt.Sprintf("failed - order book error: %v", err)
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, 0, "failed", fmt.Sprintf("failed to get order book: %v", err), "", userSettings.StrategyType, debugLog, timing, timestamps)
	}

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

	log.Printf("[CopyTrader-Bot] BUY: placing order - size=%.4f, cost=$%.4f, avgPrice=%.4f, orderPrice=%.4f (max fill)",
		totalSize, totalCost, avgPrice, orderPrice)

	debugLog["order"] = map[string]interface{}{
		"type":       "market",
		"side":       "BUY",
		"size":       totalSize,
		"cost":       totalCost,
		"avgPrice":   avgPrice,
		"orderPrice": orderPrice,
	}

	// Step 7: Place order at maxFillPrice to sweep all affordable levels
	orderStart := time.Now()
	timestamps.OrderPlacedAt = &orderStart // Track when we sent the order
	resp, err := ct.clobClient.PlaceOrderFast(ctx, tokenID, api.SideBuy, totalSize, orderPrice, negRisk)
	orderConfirmed := time.Now()
	timing["7_place_order_ms"] = float64(time.Since(orderStart).Microseconds()) / 1000
	if err != nil {
		timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
		log.Printf("[CopyTrader-Bot] BUY failed: %v", err)
		debugLog["orderResponse"] = map[string]interface{}{"error": err.Error()}
		debugLog["decision"] = fmt.Sprintf("failed - order error: %v", err)
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, totalCost, 0, 0, 0, "failed", fmt.Sprintf("order failed: %v", err), "", userSettings.StrategyType, debugLog, timing, timestamps)
	}

	// Order confirmed by Polymarket
	timestamps.OrderConfirmedAt = &orderConfirmed

	debugLog["orderResponse"] = map[string]interface{}{
		"success":  resp.Success,
		"orderID":  resp.OrderID,
		"status":   resp.Status, // matched, live, delayed, unmatched - CRITICAL for timing analysis
		"errorMsg": resp.ErrorMsg,
	}

	if !resp.Success {
		timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
		log.Printf("[CopyTrader-Bot] BUY rejected: %s", resp.ErrorMsg)
		debugLog["decision"] = fmt.Sprintf("failed - rejected: %s", resp.ErrorMsg)
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, totalCost, 0, 0, 0, "failed", resp.ErrorMsg, "", userSettings.StrategyType, debugLog, timing, timestamps)
	}

	log.Printf("[CopyTrader-Bot] BUY success: OrderID=%s, Status=%s, Size=%.4f, AvgPrice=%.4f, Cost=$%.4f",
		resp.OrderID, resp.Status, totalSize, avgPrice, totalCost)

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

// executeBotSell implements the bot following strategy for sells.
// It tries to sell at the copied user's exact price, then sweeps bids down to -10%.
// If still not filled, creates limit orders at -3% and -5%, waits 3 min, then market sells remainder.
func (ct *CopyTrader) executeBotSell(ctx context.Context, trade models.TradeDetail, tokenID string, negRisk bool, userSettings *storage.UserCopySettings) error {
	return ct.executeBotSellWithBook(ctx, trade, tokenID, negRisk, userSettings, nil)
}

// executeBotSellWithBook is the same as executeBotSell but accepts a pre-fetched order book channel
// for parallel execution optimization
func (ct *CopyTrader) executeBotSellWithBook(ctx context.Context, trade models.TradeDetail, tokenID string, negRisk bool, userSettings *storage.UserCopySettings, orderBookCh <-chan orderBookResult) error {
	// Initialize timestamps for latency tracking
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

	// Calculate how many tokens to sell based on copied trade
	// Sell tokens = copied tokens × multiplier
	targetTokens := trade.Size * multiplier

	// Get actual position from Polymarket API
	var ourPosition float64
	actualPositions, err := ct.client.GetOpenPositions(ctx, ct.myAddress)
	if err != nil {
		log.Printf("[CopyTrader-Bot] SELL: Warning: failed to fetch positions: %v", err)
	} else {
		log.Printf("[CopyTrader-Bot] SELL: Found %d positions, looking for tokenID=%s", len(actualPositions), tokenID)
		for _, pos := range actualPositions {
			if pos.Asset == tokenID && pos.Size.Float64() > 0 {
				ourPosition = pos.Size.Float64()
				log.Printf("[CopyTrader-Bot] SELL: Found matching position: %.4f shares", ourPosition)
				break
			}
		}
	}

	// Fall back to local tracking
	if ourPosition <= 0 {
		position, err := ct.store.GetMyPosition(ctx, trade.MarketID, trade.Outcome)
		if err == nil && position.Size > 0 {
			ourPosition = position.Size
			log.Printf("[CopyTrader-Bot] SELL: Found position in local DB: %.4f shares", ourPosition)
		}
	}

	if ourPosition <= 0 {
		log.Printf("[CopyTrader-Bot] SELL: no position to sell for %s/%s (tokenID=%s)", trade.Title, trade.Outcome, tokenID)
		return nil
	}

	// Sell the minimum of targetTokens or ourPosition
	sellSize := targetTokens
	if sellSize > ourPosition {
		sellSize = ourPosition
		log.Printf("[CopyTrader-Bot] SELL: target %.4f > position %.4f, selling entire position", targetTokens, ourPosition)
	}

	// Minimum sell size - can't bump up for SELL (we may not have more shares)
	const minSellSize = 0.1
	if sellSize < minSellSize {
		log.Printf("[CopyTrader-Bot] SELL: size %.4f below minimum %.2f shares, skipping", sellSize, minSellSize)
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "skipped",
			fmt.Sprintf("size %.4f below minimum %.2f shares", sellSize, minSellSize), "", userSettings.StrategyType, nil, nil, timestamps)
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
		log.Printf("[CopyTrader-Bot] SELL: Using dynamic slippage -%.0f%% for price $%.2f", slippage*100, copiedPrice)
	} else {
		// Blockchain trades don't have price - allow any price (accept any bid)
		minPrice = 0.0
		log.Printf("[CopyTrader-Bot] SELL: No price from blockchain, accepting any bid")
	}

	log.Printf("[CopyTrader-Bot] SELL: Copied price=%.4f, minPrice=%.4f, sellSize=%.4f, market=%s",
		copiedPrice, minPrice, sellSize, trade.Title)

	// Get order book (use pre-fetched if available, otherwise fetch now)
	var book *api.OrderBook
	var bookErr error
	if orderBookCh != nil {
		// Use pre-fetched order book from parallel execution
		result := <-orderBookCh
		book, bookErr = result.book, result.err
	} else {
		// Fetch fresh order book
		book, bookErr = ct.clobClient.GetOrderBook(ctx, tokenID)
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
		// Example: if bids are 10@0.12 and 5@0.11, and we want to sweep both:
		// - avgPrice would be ~0.115, which only matches 0.12 bids
		// - minFillPrice is 0.11, which matches both levels
		orderPrice := minFillPrice
		log.Printf("[CopyTrader-Bot] SELL: selling %.4f tokens at avgPrice=%.4f, orderPrice=%.4f (min fill) for $%.4f",
			totalSold, avgPrice, orderPrice, totalUSDC)

		orderPlacedAt := time.Now()
		timestamps.OrderPlacedAt = &orderPlacedAt
		// Try FOK first (immediate), fall back to GTC if needed
		resp, err := ct.clobClient.PlaceOrderFast(ctx, tokenID, api.SideSell, totalSold, orderPrice, negRisk)
		orderConfirmedAt := time.Now()
		if err != nil {
			log.Printf("[CopyTrader-Bot] SELL failed: %v", err)
			return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "failed", fmt.Sprintf("order failed: %v", err), "", userSettings.StrategyType, nil, nil, timestamps)
		}

		timestamps.OrderConfirmedAt = &orderConfirmedAt

		if !resp.Success {
			log.Printf("[CopyTrader-Bot] SELL rejected: %s", resp.ErrorMsg)
			return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "failed", resp.ErrorMsg, "", userSettings.StrategyType, nil, nil, timestamps)
		}

		log.Printf("[CopyTrader-Bot] SELL success: OrderID=%s, Status=%s, Size=%.4f, AvgPrice=%.4f, USDC=$%.4f",
			resp.OrderID, resp.Status, totalSold, avgPrice, totalUSDC)

		// Clear position if we sold everything
		if remainingSize < 0.01 {
			ct.store.ClearMyPosition(ctx, trade.MarketID, trade.Outcome)
		}

		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, totalUSDC, avgPrice, totalSold, "executed", "", resp.OrderID, userSettings.StrategyType, nil, nil, timestamps)
	}

	// No acceptable bids found within 10% - need to create limit orders
	log.Printf("[CopyTrader-Bot] SELL: no bids within 10%%, creating limit orders...")

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

	log.Printf("[CopyTrader-Bot] SELL: Creating limit orders - 20%% @ %.4f, 40%% @ %.4f, 40%% @ %.4f",
		order1Price, order2Price, order3Price)

	// Place limit orders (using market orders with price limits for now)
	// TODO: Implement proper limit order API when available
	var orderIDs []string
	var totalFilled float64
	var totalValue float64

	// Try FOK first (immediate), fall back to GTC if needed
	orderPlacedAt := time.Now()
	timestamps.OrderPlacedAt = &orderPlacedAt
	sellUSDC := sellSize * order3Price
	log.Printf("[CopyTrader-Bot] SELL: Placing order for %.4f tokens at $%.4f (total ~$%.2f)",
		sellSize, order3Price, sellUSDC)
	resp, err := ct.clobClient.PlaceOrderFast(ctx, tokenID, api.SideSell, sellSize, order3Price, negRisk)
	orderConfirmedAt := time.Now()

	// Capture detailed error info
	var failReason string
	if err != nil {
		failReason = fmt.Sprintf("market sell failed: %v", err)
		log.Printf("[CopyTrader-Bot] SELL: Order error: %v", err)
	} else if !resp.Success {
		failReason = fmt.Sprintf("market sell rejected: %s", resp.ErrorMsg)
		log.Printf("[CopyTrader-Bot] SELL: Order rejected: %s", resp.ErrorMsg)
	} else {
		timestamps.OrderConfirmedAt = &orderConfirmedAt
		orderIDs = append(orderIDs, resp.OrderID)
		// Estimate fill
		if len(book.Bids) > 0 {
			var bestBid float64
			fmt.Sscanf(book.Bids[0].Price, "%f", &bestBid)
			totalFilled = sellSize
			totalValue = sellSize * bestBid
			log.Printf("[CopyTrader-Bot] SELL: Order successful, estimated fill %.4f @ $%.4f", totalFilled, bestBid)
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
	log.Printf("[CopyTrader-Bot] SELL: failed - %s", failReason)
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
		FollowerOrderID:  orderID,
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

	// For executed trades, spawn async goroutine to fetch real tx hash after 60s
	if (status == "executed" || status == "success") && tokenID != "" && ct.myAddress != "" {
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

	// Query Data API for our recent trades
	trades, err := ct.client.GetActivity(ctx, api.TradeQuery{
		User:  ct.myAddress,
		Limit: 50,
	})
	if err != nil {
		log.Printf("[CopyTrader] Failed to fetch trades for tx hash lookup: %v", err)
		return
	}

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
			continue
		}

		// Found a match - update the DB
		if trade.TransactionHash != "" {
			if err := ct.store.UpdateCopyTradeLogTxHash(ctx, followingTradeID, trade.TransactionHash); err != nil {
				log.Printf("[CopyTrader] Failed to update tx hash for %s: %v", followingTradeID[:16], err)
			} else {
				log.Printf("[CopyTrader] Updated tx hash for %s -> %s", followingTradeID[:16], trade.TransactionHash[:16])
			}
			return
		}
	}

	log.Printf("[CopyTrader] No matching trade found for tx hash lookup (followingTradeID=%s)", followingTradeID[:16])
}

// GetStats returns copy trading statistics
func (ct *CopyTrader) GetStats(ctx context.Context) (map[string]interface{}, error) {
	return ct.store.GetCopyTradeStats(ctx)
}
