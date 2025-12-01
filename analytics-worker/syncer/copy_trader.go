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

	// Start order book caching for faster execution
	ct.clobClient.StartOrderBookCaching()

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
		ct.clobClient.StopOrderBookCaching()

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
	ticker := time.NewTicker(time.Duration(ct.config.CheckIntervalSec) * time.Second)
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
	// DISABLED: This backup polling mechanism is no longer needed.
	// Trades are now detected in real-time via blockchain WebSocket (~300ms latency).
	// This query was scanning 221k+ rows every 2 seconds, causing DB bottleneck.
	return nil

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

	log.Printf("[CopyTrader] Processing %d new trades in parallel", len(trades))

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

	// CRITICAL: Check if this trade is already being processed (prevents duplicate orders)
	ct.inFlightTradesMu.Lock()
	if startTime, exists := ct.inFlightTrades[trade.ID]; exists {
		ct.inFlightTradesMu.Unlock()
		log.Printf("[CopyTrader] Skipping duplicate trade %s (already in-flight since %s ago)",
			trade.ID[:16], time.Since(startTime).Round(time.Millisecond))
		return nil
	}
	ct.inFlightTrades[trade.ID] = time.Now()
	// Cleanup old entries (older than 2 minutes)
	for id, t := range ct.inFlightTrades {
		if time.Since(t) > 2*time.Minute {
			delete(ct.inFlightTrades, id)
		}
	}
	ct.inFlightTradesMu.Unlock()

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
	isBlockchainTrade := trade.TransactionHash != "" && trade.DetectedAt != (time.Time{}) && trade.DetectionSource != "live_ws"

	var tokenID string
	var negRisk bool

	if isBlockchainTrade {
		// Fast path: use token ID directly (already in decimal format from realtime_detector)
		tokenID = trade.MarketID
		negRisk = false // Default, will be overridden by order book if needed
		log.Printf("[CopyTrader] ⚡ FAST PATH: blockchain trade, skipping API lookups, tokenID=%s", tokenID)
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
			return ct.executeBotBuy(ctx, trade, tokenID, negRisk, userSettings)
		}
		return ct.executeBuy(ctx, trade, tokenID, negRisk)
	} else if trade.Side == "SELL" {
		if strategyType == storage.StrategyBot || strategyType == storage.StrategyBTC15m {
			return ct.executeBotSell(ctx, trade, tokenID, negRisk, userSettings)
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
	multiplier := ct.config.Multiplier
	minUSDC := ct.config.MinOrderUSDC

	userSettings, err := ct.store.GetUserCopySettings(ctx, trade.UserID)
	if err == nil && userSettings != nil {
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

	// Step 2: Calculate target amount
	calcStart := time.Now()
	originalTargetUSDC := trade.UsdcSize * multiplier
	targetUSDC := originalTargetUSDC
	if targetUSDC < minUSDC {
		targetUSDC = minUSDC
		log.Printf("[CopyTrader-Bot] BUY amount below minimum, using $%.2f", targetUSDC)
	}

	// Apply max cap if set
	if maxUSD != nil && targetUSDC > *maxUSD {
		log.Printf("[CopyTrader-Bot] BUY amount $%.2f exceeds max cap $%.2f, capping", targetUSDC, *maxUSD)
		targetUSDC = *maxUSD
	}

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
		"copiedTradeUSDC":    trade.UsdcSize,
		"originalTargetUSDC": originalTargetUSDC,
		"finalTargetUSDC":    targetUSDC,
		"copiedPrice":        copiedPrice,
		"maxPrice":           maxPrice,
		"priceLimit":         fmt.Sprintf("+%.0f%%", slippagePct),
	}

	log.Printf("[CopyTrader-Bot] BUY: Copied price=%.4f, maxPrice=%.4f (+%.0f%%), targetUSDC=$%.2f, market=%s",
		copiedPrice, maxPrice, slippagePct, targetUSDC, trade.Title)

	// Step 3: Add token to cache
	cacheStart := time.Now()
	ct.clobClient.AddTokenToCache(tokenID)
	timing["3_token_cache_ms"] = float64(time.Since(cacheStart).Microseconds()) / 1000

	// Step 4: Get order book (API call - usually slowest)
	orderBookStart := time.Now()
	book, err := ct.clobClient.GetOrderBook(ctx, tokenID)
	timing["4_get_orderbook_ms"] = float64(time.Since(orderBookStart).Microseconds()) / 1000
	if err != nil {
		timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
		debugLog["orderBook"] = map[string]interface{}{"error": err.Error()}
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "No orderbook exists") {
			log.Printf("[CopyTrader-Bot] BUY: market closed/resolved, skipping")
			debugLog["decision"] = "skipped - market closed/resolved"
			return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, targetUSDC, 0, 0, 0, "skipped", "market closed/resolved", "", storage.StrategyBot, debugLog, timing, timestamps)
		}
		debugLog["decision"] = fmt.Sprintf("failed - order book error: %v", err)
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, targetUSDC, 0, 0, 0, "failed", fmt.Sprintf("failed to get order book: %v", err), "", storage.StrategyBot, debugLog, timing, timestamps)
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
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, targetUSDC, 0, 0, 0, "skipped", "no asks in order book", "", storage.StrategyBot, debugLog, timing, timestamps)
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
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, targetUSDC, 0, 0, 0, "skipped",
			fmt.Sprintf("no liquidity within 10%% (best=%.4f, max=%.4f)", bestAsk, maxPrice), "", storage.StrategyBot, debugLog, timing, timestamps)
	}

	// Step 6: Calculate fill
	fillStart := time.Now()
	remainingUSDC := targetUSDC
	totalSize := 0.0
	totalCost := 0.0

	for _, ask := range affordableAsks {
		if remainingUSDC <= 0 {
			break
		}

		levelCost := ask.price * ask.size
		if levelCost <= remainingUSDC {
			totalSize += ask.size
			totalCost += levelCost
			remainingUSDC -= levelCost
		} else {
			partialSize := remainingUSDC / ask.price
			totalSize += partialSize
			totalCost += remainingUSDC
			remainingUSDC = 0
		}
	}
	timing["6_fill_calc_ms"] = float64(time.Since(fillStart).Microseconds()) / 1000

	debugLog["fillCalculation"] = map[string]interface{}{
		"totalSize":     totalSize,
		"totalCost":     totalCost,
		"remainingUSDC": remainingUSDC,
	}

	// Polymarket requires minimum $1 order
	const polymarketMinOrder = 1.0

	if totalSize < 0.01 {
		timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
		log.Printf("[CopyTrader-Bot] BUY: insufficient affordable liquidity (size=%.4f, cost=$%.4f)",
			totalSize, totalCost)
		debugLog["decision"] = fmt.Sprintf("skipped - insufficient liquidity (size=%.4f, cost=$%.4f)", totalSize, totalCost)
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, targetUSDC, 0, 0, 0, "skipped", "insufficient affordable liquidity", "", storage.StrategyBot, debugLog, timing, timestamps)
	}

	avgPrice := totalCost / totalSize

	// Ensure we meet Polymarket's minimum order size ($1)
	// If we calculated less than $1 but have liquidity, bump up to $1
	if totalCost < polymarketMinOrder {
		log.Printf("[CopyTrader-Bot] BUY: calculated $%.4f, bumping to $%.2f minimum", totalCost, polymarketMinOrder)
		totalCost = polymarketMinOrder
		// Recalculate size based on average price
		if avgPrice > 0 {
			totalSize = totalCost / avgPrice
		}
		debugLog["minOrderAdjustment"] = map[string]interface{}{
			"originalCost": totalCost,
			"adjustedCost": polymarketMinOrder,
			"adjustedSize": totalSize,
		}
	}
	log.Printf("[CopyTrader-Bot] BUY: placing order - size=%.4f, cost=$%.4f, avgPrice=%.4f",
		totalSize, totalCost, avgPrice)

	debugLog["order"] = map[string]interface{}{
		"type":     "market",
		"side":     "BUY",
		"size":     totalSize,
		"cost":     totalCost,
		"avgPrice": avgPrice,
	}

	// Step 7: Place market order (API call - usually slowest)
	orderStart := time.Now()
	timestamps.OrderPlacedAt = &orderStart // Track when we sent the order
	resp, err := ct.clobClient.PlaceMarketOrder(ctx, tokenID, api.SideBuy, totalCost, negRisk)
	orderConfirmed := time.Now()
	timing["7_place_order_ms"] = float64(time.Since(orderStart).Microseconds()) / 1000
	if err != nil {
		timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
		log.Printf("[CopyTrader-Bot] BUY failed: %v", err)
		debugLog["orderResponse"] = map[string]interface{}{"error": err.Error()}
		debugLog["decision"] = fmt.Sprintf("failed - order error: %v", err)
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, targetUSDC, 0, 0, 0, "failed", fmt.Sprintf("order failed: %v", err), "", storage.StrategyBot, debugLog, timing, timestamps)
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
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, targetUSDC, 0, 0, 0, "failed", resp.ErrorMsg, "", storage.StrategyBot, debugLog, timing, timestamps)
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

	return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, targetUSDC, totalCost, avgPrice, totalSize, "executed", "", resp.OrderID, storage.StrategyBot, debugLog, timing, timestamps)
}

// executeBotSell implements the bot following strategy for sells.
// It tries to sell at the copied user's exact price, then sweeps bids down to -10%.
// If still not filled, creates limit orders at -3% and -5%, waits 3 min, then market sells remainder.
func (ct *CopyTrader) executeBotSell(ctx context.Context, trade models.TradeDetail, tokenID string, negRisk bool, userSettings *storage.UserCopySettings) error {
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

	// Add token to cache
	ct.clobClient.AddTokenToCache(tokenID)

	// Get order book
	book, err := ct.clobClient.GetOrderBook(ctx, tokenID)
	if err != nil {
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "No orderbook exists") {
			log.Printf("[CopyTrader-Bot] SELL: market closed/resolved, skipping")
			return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "skipped", "market closed/resolved", "", storage.StrategyBot, nil, nil, timestamps)
		}
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "failed", fmt.Sprintf("order book error: %v", err), "", storage.StrategyBot, nil, nil, timestamps)
	}

	if len(book.Bids) == 0 {
		log.Printf("[CopyTrader-Bot] SELL: no bids in order book")
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "skipped", "no bids in order book", "", storage.StrategyBot, nil, nil, timestamps)
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
	remainingSize := sellSize
	totalSold := 0.0
	totalUSDC := 0.0

	for _, bid := range acceptableBids {
		if remainingSize <= 0 {
			break
		}

		if bid.size <= remainingSize {
			// Take entire level
			totalSold += bid.size
			totalUSDC += bid.price * bid.size
			remainingSize -= bid.size
		} else {
			// Partial fill at this level
			totalSold += remainingSize
			totalUSDC += bid.price * remainingSize
			remainingSize = 0
		}
	}

	// If we found acceptable bids, sell into them
	if totalSold > 0.01 {
		avgPrice := totalUSDC / totalSold
		log.Printf("[CopyTrader-Bot] SELL: selling %.4f tokens at avg price %.4f for $%.4f",
			totalSold, avgPrice, totalUSDC)

		orderPlacedAt := time.Now()
		timestamps.OrderPlacedAt = &orderPlacedAt
		resp, err := ct.clobClient.PlaceMarketOrder(ctx, tokenID, api.SideSell, totalUSDC, negRisk)
		orderConfirmedAt := time.Now()
		if err != nil {
			log.Printf("[CopyTrader-Bot] SELL failed: %v", err)
			return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "failed", fmt.Sprintf("order failed: %v", err), "", storage.StrategyBot, nil, nil, timestamps)
		}

		timestamps.OrderConfirmedAt = &orderConfirmedAt

		if !resp.Success {
			log.Printf("[CopyTrader-Bot] SELL rejected: %s", resp.ErrorMsg)
			return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "failed", resp.ErrorMsg, "", storage.StrategyBot, nil, nil, timestamps)
		}

		log.Printf("[CopyTrader-Bot] SELL success: OrderID=%s, Status=%s, Size=%.4f, AvgPrice=%.4f, USDC=$%.4f",
			resp.OrderID, resp.Status, totalSold, avgPrice, totalUSDC)

		// Clear position if we sold everything
		if remainingSize < 0.01 {
			ct.store.ClearMyPosition(ctx, trade.MarketID, trade.Outcome)
		}

		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, totalUSDC, avgPrice, totalSold, "executed", "", resp.OrderID, storage.StrategyBot, nil, nil, timestamps)
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

	// For now, we'll use market orders with our best effort at the lowest acceptable price
	// In production, this should be replaced with actual limit order placement
	orderPlacedAt := time.Now()
	timestamps.OrderPlacedAt = &orderPlacedAt
	sellUSDC := sellSize * order3Price
	log.Printf("[CopyTrader-Bot] SELL: Placing market sell for %.4f tokens at ~$%.4f (total ~$%.2f)",
		sellSize, order3Price, sellUSDC)
	resp, err := ct.clobClient.PlaceMarketOrder(ctx, tokenID, api.SideSell, sellUSDC, negRisk)
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
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, totalValue, avgPrice, totalFilled, "executed", "", strings.Join(orderIDs, ","), storage.StrategyBot, nil, nil, timestamps)
	}

	// Failed to sell - include detailed reason
	if failReason == "" {
		failReason = "no fills achieved (unknown reason)"
	}
	log.Printf("[CopyTrader-Bot] SELL: failed - %s", failReason)
	return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "failed", failReason, "", storage.StrategyBot, nil, nil, timestamps)
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

	logEntry := storage.CopyTradeLogEntry{
		FollowingAddress: trade.UserID,
		FollowingTradeID: trade.ID,
		FollowingTime:    trade.Timestamp,
		FollowingShares:  followingShares,
		FollowingPrice:   trade.Price,
		FollowerTime:     followerTime,
		FollowerShares:   followerShares,
		FollowerPrice:    followerPrice,
		MarketTitle:      trade.Title,
		Outcome:          trade.Outcome,
		TokenID:          tokenID,
		Status:           status,
		FailedReason:     errReason,
		StrategyType:     strategyType,
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

	return nil
}

// GetStats returns copy trading statistics
func (ct *CopyTrader) GetStats(ctx context.Context) (map[string]interface{}, error) {
	return ct.store.GetCopyTradeStats(ctx)
}
