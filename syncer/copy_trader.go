package syncer

import (
	"context"
	"fmt"
	"log"
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
	clobClient *api.ClobClient // Default client (backward compatibility)
	config     CopyTraderConfig
	running    bool
	stopCh     chan struct{}
	myAddress  string // Our wallet address for position lookups

	// Multi-account support
	accountManager *api.AccountManager

	// Real-time detector for faster trade detection
	detector *RealtimeDetector

	// Metrics for latency tracking
	metrics   *CopyTraderMetrics
	metricsMu sync.RWMutex
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
	return NewCopyTraderWithAccountManager(store, client, config, nil)
}

// NewCopyTraderWithAccountManager creates a new copy trader with multi-account support
func NewCopyTraderWithAccountManager(store *storage.PostgresStore, client *api.Client, config CopyTraderConfig, accountManager *api.AccountManager) (*CopyTrader, error) {
	// Create default CLOB client (for backward compatibility)
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

	ct := &CopyTrader{
		store:          store,
		client:         client,
		clobClient:     clobClient,
		config:         config,
		stopCh:         make(chan struct{}),
		myAddress:      myAddress,
		metrics:        &CopyTraderMetrics{},
		accountManager: accountManager,
	}

	// Log multi-account status
	if accountManager != nil {
		log.Printf("[CopyTrader] Multi-account support enabled via AccountManager")
	}

	return ct, nil
}

// getClobClientForUser returns the appropriate ClobClient for a user based on their settings.
// If AccountManager is available and user has a specific account assigned, uses that account.
// Otherwise falls back to the default clobClient.
func (ct *CopyTrader) getClobClientForUser(ctx context.Context, userAddress string) (*api.ClobClient, int, error) {
	// If we don't have an account manager, use default client
	if ct.accountManager == nil {
		return ct.clobClient, 0, nil
	}

	// Get the client for this user
	client, accountID, err := ct.accountManager.GetClientForUser(ctx, userAddress)
	if err != nil {
		// Fall back to default client on error
		log.Printf("[CopyTrader] Warning: failed to get client for user %s: %v, using default", userAddress, err)
		return ct.clobClient, 0, nil
	}

	return client, accountID, nil
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
	// EnableBlockchainWS should only be true in the worker (heavy processing)
	ct.detector = NewRealtimeDetector(ct.client, ct.clobClient, ct.store, ct.handleRealtimeTrade, ct.config.EnableBlockchainWS)
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
	// Skip non-TRADE types (REDEEM, SPLIT, MERGE)
	if trade.Type != "" && trade.Type != "TRADE" {
		log.Printf("[CopyTrader] Skipping non-trade: %s (type=%s)", trade.ID, trade.Type)
		return nil
	}

	// Verify the token matches the intended outcome
	// trade.MarketID is the token ID, but trade.Outcome might not match the actual token's outcome
	tokenID, actualOutcome, negRisk, err := ct.getVerifiedTokenID(ctx, trade)
	if err != nil {
		return ct.logCopyTrade(ctx, trade, "", 0, 0, 0, 0, "failed", fmt.Sprintf("failed to get token ID: %v", err), "")
	}

	// Update trade outcome if it was wrong
	if actualOutcome != "" && actualOutcome != trade.Outcome {
		log.Printf("[CopyTrader] WARNING: Corrected outcome from '%s' to '%s' for %s", trade.Outcome, actualOutcome, trade.Title)
		trade.Outcome = actualOutcome
	}

	// Get user settings to determine strategy type
	// First try cached settings (faster, no DB query), then fall back to DB
	strategyType := storage.StrategyHuman // Default to human
	var userSettings *storage.UserCopySettings
	if ct.detector != nil {
		userSettings = ct.detector.GetCachedUserSettings(trade.UserID)
	}
	if userSettings == nil {
		// Fall back to DB query if not in cache
		var err error
		userSettings, err = ct.store.GetUserCopySettings(ctx, trade.UserID)
		if err != nil {
			log.Printf("[CopyTrader] Warning: failed to get user settings: %v", err)
		}
	}
	if userSettings != nil {
		if !userSettings.Enabled {
			log.Printf("[CopyTrader] Skipping: user %s has copy trading disabled", trade.UserID)
			return nil
		}
		strategyType = userSettings.StrategyType
	}

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
	// Get per-user settings or use defaults
	// First try cached settings (faster, no DB query), then fall back to DB
	multiplier := ct.config.Multiplier
	minUSDC := ct.config.MinOrderUSDC
	var maxUSD *float64

	var userSettings *storage.UserCopySettings
	if ct.detector != nil {
		userSettings = ct.detector.GetCachedUserSettings(trade.UserID)
	}
	if userSettings == nil {
		var err error
		userSettings, err = ct.store.GetUserCopySettings(ctx, trade.UserID)
		if err != nil {
			log.Printf("[CopyTrader] Warning: failed to get user settings in executeBuy: %v", err)
		}
	}
	if userSettings != nil {
		if !userSettings.Enabled {
			log.Printf("[CopyTrader] BUY skipped: user %s has copy trading disabled", trade.UserID)
			return nil
		}
		multiplier = userSettings.Multiplier
		minUSDC = userSettings.MinUSDC
		maxUSD = userSettings.MaxUSD
		log.Printf("[CopyTrader] Using custom settings for %s: multiplier=%.4f, minUSDC=$%.2f, maxUSD=%v",
			trade.UserID, multiplier, minUSDC, maxUSD)
	}

	// Calculate amount to buy
	intendedUSDC := trade.UsdcSize * multiplier

	// Ensure minimum order
	if intendedUSDC < minUSDC {
		intendedUSDC = minUSDC
		log.Printf("[CopyTrader] BUY amount below minimum, using $%.2f", intendedUSDC)
	}

	// Apply max cap if set
	if maxUSD != nil && intendedUSDC > *maxUSD {
		log.Printf("[CopyTrader] BUY amount $%.2f exceeds max cap $%.2f, capping", intendedUSDC, *maxUSD)
		intendedUSDC = *maxUSD
	}

	// Calculate max allowed price based on tiered slippage
	maxSlippage := getMaxSlippage(trade.Price)
	maxAllowedPrice := trade.Price * (1 + maxSlippage)

	log.Printf("[CopyTrader] DEBUG executeBuy: trade.MarketID=%s, tokenID=%s", trade.MarketID, tokenID)
	log.Printf("[CopyTrader] DEBUG executeBuy: trade.Price=%.4f, maxSlippage=%.0f%%, maxAllowedPrice=%.4f",
		trade.Price, maxSlippage*100, maxAllowedPrice)

	// Add token to cache for faster order book lookups
	ct.clobClient.AddTokenToCache(tokenID)

	// Retry loop: check every second for up to 3 minutes for affordable liquidity
	const maxRetryDuration = 3 * time.Minute
	const retryInterval = 1 * time.Second
	startTime := time.Now()
	attempt := 0
	remainingUSDC := intendedUSDC
	totalSizeBought := 0.0
	totalUSDCSpent := 0.0
	var lastOrderID string

	for remainingUSDC >= minUSDC && time.Since(startTime) < maxRetryDuration {
		attempt++

		// Get order book - use cached for first attempt (speed), fresh for retries (accuracy)
		var book *api.OrderBook
		var err error
		if attempt == 1 {
			book, err = ct.clobClient.GetCachedOrderBook(ctx, tokenID)
		} else {
			book, err = ct.clobClient.GetOrderBook(ctx, tokenID)
		}
		if err != nil {
			// If market doesn't exist (404), skip immediately - it's likely closed/resolved
			if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "No orderbook exists") {
				log.Printf("[CopyTrader] BUY: market closed/resolved, skipping: %v", err)
				return ct.logCopyTrade(ctx, trade, tokenID, intendedUSDC, 0, 0, 0, "skipped", "market closed/resolved", "")
			}
			log.Printf("[CopyTrader] BUY attempt %d: failed to get order book: %v", attempt, err)
			time.Sleep(retryInterval)
			continue
		}

		// DEBUG: Log order book details
		if attempt == 1 {
			log.Printf("[CopyTrader] DEBUG executeBuy: OrderBook response - asset_id=%s, market=%s, numAsks=%d, numBids=%d",
				book.AssetID, book.Market, len(book.Asks), len(book.Bids))
			if book.AssetID != tokenID {
				log.Printf("[CopyTrader] WARNING: OrderBook asset_id MISMATCH! requested=%s, got=%s", tokenID, book.AssetID)
			}
		}

		if len(book.Asks) == 0 {
			if attempt == 1 {
				log.Printf("[CopyTrader] BUY attempt %d: no asks in order book, will retry...", attempt)
			}
			time.Sleep(retryInterval)
			continue
		}

		// Find how much we can buy at or below maxAllowedPrice
		affordableSize := 0.0
		affordableUSDC := 0.0
		bestAskPrice := 0.0

		for i, ask := range book.Asks {
			price, _ := fmt.Sscanf(ask.Price, "%f", &bestAskPrice)
			if price == 0 {
				continue
			}
			var askPrice, askSize float64
			fmt.Sscanf(ask.Price, "%f", &askPrice)
			fmt.Sscanf(ask.Size, "%f", &askSize)

			if i == 0 {
				bestAskPrice = askPrice
			}

			if askPrice > maxAllowedPrice {
				break // No more affordable liquidity at this level or beyond
			}

			levelCost := askPrice * askSize
			if affordableUSDC+levelCost <= remainingUSDC {
				affordableSize += askSize
				affordableUSDC += levelCost
			} else {
				// Partial fill at this level
				remainingForLevel := remainingUSDC - affordableUSDC
				partialSize := remainingForLevel / askPrice
				affordableSize += partialSize
				affordableUSDC += remainingForLevel
				break
			}
		}

		// Log timing info on first attempt
		if attempt == 1 {
			delay := time.Since(trade.Timestamp)
			log.Printf("[CopyTrader] DEBUG executeBuy: trade.Timestamp=%s, delay=%s",
				trade.Timestamp.Format("15:04:05.000"), delay.Round(time.Millisecond))
			// Log top 3 asks for debugging
			for i, ask := range book.Asks {
				if i >= 3 {
					break
				}
				log.Printf("[CopyTrader] DEBUG executeBuy: Ask[%d] price=%s size=%s", i, ask.Price, ask.Size)
			}
		}

		// If no affordable liquidity, wait and retry
		if affordableSize < 0.01 || affordableUSDC < minUSDC {
			if attempt == 1 {
				log.Printf("[CopyTrader] BUY attempt %d: price too high (best ask %.4f > max %.4f), will retry for up to 3 min...",
					attempt, bestAskPrice, maxAllowedPrice)
			} else if attempt%30 == 0 { // Log every 30 seconds
				log.Printf("[CopyTrader] BUY attempt %d: still waiting for affordable price (best ask %.4f > max %.4f), elapsed=%s",
					attempt, bestAskPrice, maxAllowedPrice, time.Since(startTime).Round(time.Second))
			}
			time.Sleep(retryInterval)
			continue
		}

		// We have affordable liquidity - place the order
		log.Printf("[CopyTrader] BUY attempt %d: found affordable liquidity - size=%.4f, cost=$%.4f, bestAsk=%.4f, maxAllowed=%.4f",
			attempt, affordableSize, affordableUSDC, bestAskPrice, maxAllowedPrice)

		log.Printf("[CopyTrader] BUY: Original=$%.2f@%.4f, Copy=$%.4f, CurrentAsk=%.4f, MaxPrice=%.4f, Market=%s, Outcome=%s",
			trade.UsdcSize, trade.Price, affordableUSDC, bestAskPrice, maxAllowedPrice, trade.Title, trade.Outcome)

		// Place order for affordable amount
		resp, err := ct.clobClient.PlaceMarketOrder(ctx, tokenID, api.SideBuy, affordableUSDC, negRisk)
		if err != nil {
			log.Printf("[CopyTrader] BUY attempt %d: order failed: %v", attempt, err)
			time.Sleep(retryInterval)
			continue
		}

		if !resp.Success {
			log.Printf("[CopyTrader] BUY attempt %d: order rejected: %s", attempt, resp.ErrorMsg)
			time.Sleep(retryInterval)
			continue
		}

		// Order succeeded
		sizeBought, avgPrice, actualUSDC := api.CalculateOptimalFill(book, api.SideBuy, affordableUSDC)
		totalSizeBought += sizeBought
		totalUSDCSpent += actualUSDC
		remainingUSDC -= actualUSDC
		lastOrderID = resp.OrderID

		log.Printf("[CopyTrader] BUY success: OrderID=%s, Size=%.4f, AvgPrice=%.4f, Spent=$%.4f, Remaining=$%.4f",
			resp.OrderID, sizeBought, avgPrice, actualUSDC, remainingUSDC)

		// If we've filled enough or remaining is below minimum, we're done
		if remainingUSDC < minUSDC {
			break
		}

		// Small delay before next attempt to fill remaining
		time.Sleep(retryInterval)
	}

	// Log final result and update position
	if totalSizeBought > 0 {
		log.Printf("[CopyTrader] BUY completed: TotalSize=%.4f, TotalSpent=$%.4f, Attempts=%d, Duration=%s",
			totalSizeBought, totalUSDCSpent, attempt, time.Since(startTime).Round(time.Millisecond))

		avgPrice := totalUSDCSpent / totalSizeBought
		if err := ct.store.UpdateMyPosition(ctx, MyPosition{
			MarketID:  trade.MarketID,
			TokenID:   tokenID,
			Outcome:   trade.Outcome,
			Title:     trade.Title,
			Size:      totalSizeBought,
			AvgPrice:  avgPrice,
			TotalCost: totalUSDCSpent,
		}); err != nil {
			log.Printf("[CopyTrader] Warning: failed to update position: %v", err)
		}

		return ct.logCopyTrade(ctx, trade, tokenID, intendedUSDC, totalUSDCSpent, avgPrice, totalSizeBought, "executed", "", lastOrderID)
	}

	// No fills after 3 minutes - log as skipped
	log.Printf("[CopyTrader] BUY gave up: no affordable liquidity found after %d attempts over %s",
		attempt, time.Since(startTime).Round(time.Second))
	return ct.logCopyTrade(ctx, trade, tokenID, intendedUSDC, 0, 0, 0, "skipped",
		fmt.Sprintf("no affordable liquidity after %d attempts over %s", attempt, maxRetryDuration), "")
}

func (ct *CopyTrader) executeSell(ctx context.Context, trade models.TradeDetail, tokenID string, negRisk bool) error {
	// Get actual position from Polymarket API - this is the source of truth
	var sellSize float64
	actualPositions, err := ct.client.GetOpenPositions(ctx, ct.myAddress)
	if err != nil {
		log.Printf("[CopyTrader] SELL: Warning: failed to fetch actual positions: %v, falling back to local tracking", err)
	} else {
		// Find matching position by tokenID
		for _, pos := range actualPositions {
			if pos.Asset == tokenID && pos.Size.Float64() > 0 {
				sellSize = pos.Size.Float64()
				log.Printf("[CopyTrader] SELL: Found actual position from API: %.4f tokens", sellSize)
				break
			}
		}
	}

	// Fall back to local tracking if API didn't return position
	if sellSize <= 0 {
		position, err := ct.store.GetMyPosition(ctx, trade.MarketID, trade.Outcome)
		if err != nil || position.Size <= 0 {
			log.Printf("[CopyTrader] SELL: No position to sell for %s/%s (checked both API and local)", trade.Title, trade.Outcome)
			return nil
		}
		sellSize = position.Size
		log.Printf("[CopyTrader] SELL: Using local position: %.4f tokens", sellSize)
	}

	// Add token to cache for faster order book lookups
	ct.clobClient.AddTokenToCache(tokenID)

	// Calculate USDC value for market sell (sell everything at market)
	// We need to estimate the USDC we'll get - use cached order book for speed
	book, err := ct.clobClient.GetCachedOrderBook(ctx, tokenID)
	if err != nil {
		// If market doesn't exist (404), skip - it's likely closed/resolved
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "No orderbook exists") {
			log.Printf("[CopyTrader] SELL: market closed/resolved, skipping: %v", err)
			return ct.logCopyTrade(ctx, trade, tokenID, 0, 0, 0, sellSize, "skipped", "market closed/resolved", "")
		}
		errMsg := fmt.Sprintf("failed to get order book: %v", err)
		log.Printf("[CopyTrader] SELL failed: %s", errMsg)
		return ct.logCopyTrade(ctx, trade, tokenID, 0, 0, 0, sellSize, "failed", errMsg, "")
	}

	if len(book.Bids) == 0 {
		errMsg := "no bids in order book - no liquidity"
		log.Printf("[CopyTrader] SELL failed: %s", errMsg)
		return ct.logCopyTrade(ctx, trade, tokenID, 0, 0, 0, sellSize, "failed", errMsg, "")
	}

	// Get best bid for logging
	bestBidPrice := 0.0
	fmt.Sscanf(book.Bids[0].Price, "%f", &bestBidPrice)

	// Estimate USDC we'll receive from market sell
	estimatedUSDC := sellSize * bestBidPrice

	log.Printf("[CopyTrader] SELL: Market selling %.4f tokens at ~%.4f, expected ~$%.2f, Market=%s, Outcome=%s",
		sellSize, bestBidPrice, estimatedUSDC, trade.Title, trade.Outcome)

	// Place market sell order - sell everything at best available price
	resp, err := ct.clobClient.PlaceMarketOrder(ctx, tokenID, api.SideSell, estimatedUSDC, negRisk)
	if err != nil {
		errMsg := fmt.Sprintf("market sell failed: %v", err)
		log.Printf("[CopyTrader] SELL failed: %s", errMsg)
		return ct.logCopyTrade(ctx, trade, tokenID, 0, 0, bestBidPrice, sellSize, "failed", errMsg, "")
	}

	if !resp.Success {
		log.Printf("[CopyTrader] SELL rejected: %s", resp.ErrorMsg)
		return ct.logCopyTrade(ctx, trade, tokenID, 0, 0, bestBidPrice, sellSize, "failed", resp.ErrorMsg, "")
	}

	// Calculate actual fill from order book
	sizeSold, avgPrice, actualUSDC := api.CalculateOptimalFill(book, api.SideSell, estimatedUSDC)

	log.Printf("[CopyTrader] SELL success: OrderID=%s, Status=%s, Size=%.4f, AvgPrice=%.4f, USDC=$%.2f",
		resp.OrderID, resp.Status, sizeSold, avgPrice, actualUSDC)

	// Clear local position tracking
	if err := ct.store.ClearMyPosition(ctx, trade.MarketID, trade.Outcome); err != nil {
		log.Printf("[CopyTrader] Warning: failed to clear position: %v", err)
	}

	return ct.logCopyTrade(ctx, trade, tokenID, 0, actualUSDC, avgPrice, sizeSold, "executed", "", resp.OrderID)
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
	copiedPrice := trade.Price
	maxPrice := copiedPrice * 1.10 // Max 10% above copied price
	timing["2_calculation_ms"] = float64(time.Since(calcStart).Microseconds()) / 1000

	debugLog["calculation"] = map[string]interface{}{
		"copiedTradeUSDC":    trade.UsdcSize,
		"originalTargetUSDC": originalTargetUSDC,
		"finalTargetUSDC":    targetUSDC,
		"copiedPrice":        copiedPrice,
		"maxPrice":           maxPrice,
		"priceLimit":         "+10%",
	}

	log.Printf("[CopyTrader-Bot] BUY: Copied price=%.4f, maxPrice=%.4f (+10%%), targetUSDC=$%.2f, market=%s",
		copiedPrice, maxPrice, targetUSDC, trade.Title)

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
		"errorMsg": resp.ErrorMsg,
	}

	if !resp.Success {
		timing["total_ms"] = float64(time.Since(startTime).Microseconds()) / 1000
		log.Printf("[CopyTrader-Bot] BUY rejected: %s", resp.ErrorMsg)
		debugLog["decision"] = fmt.Sprintf("failed - rejected: %s", resp.ErrorMsg)
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, targetUSDC, 0, 0, 0, "failed", resp.ErrorMsg, "", storage.StrategyBot, debugLog, timing, timestamps)
	}

	log.Printf("[CopyTrader-Bot] BUY success: OrderID=%s, Size=%.4f, AvgPrice=%.4f, Cost=$%.4f",
		resp.OrderID, totalSize, avgPrice, totalCost)

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
		log.Printf("[CopyTrader-Bot] SELL: no position to sell for %s/%s", trade.Title, trade.Outcome)
		return nil
	}

	// Sell the minimum of targetTokens or ourPosition
	sellSize := targetTokens
	if sellSize > ourPosition {
		sellSize = ourPosition
		log.Printf("[CopyTrader-Bot] SELL: target %.4f > position %.4f, selling entire position", targetTokens, ourPosition)
	}

	copiedPrice := trade.Price
	minPrice := copiedPrice * 0.90 // Min 10% below copied price

	log.Printf("[CopyTrader-Bot] SELL: Copied price=%.4f, minPrice=%.4f (-10%%), sellSize=%.4f, market=%s",
		copiedPrice, minPrice, sellSize, trade.Title)

	// Add token to cache
	ct.clobClient.AddTokenToCache(tokenID)

	// Get order book
	book, err := ct.clobClient.GetOrderBook(ctx, tokenID)
	if err != nil {
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "No orderbook exists") {
			log.Printf("[CopyTrader-Bot] SELL: market closed/resolved, skipping")
			return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "skipped", "market closed/resolved", "", storage.StrategyBot, nil, nil, nil)
		}
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "failed", fmt.Sprintf("order book error: %v", err), "", storage.StrategyBot, nil, nil, nil)
	}

	if len(book.Bids) == 0 {
		log.Printf("[CopyTrader-Bot] SELL: no bids in order book")
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "skipped", "no bids in order book", "", storage.StrategyBot, nil, nil, nil)
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

		resp, err := ct.clobClient.PlaceMarketOrder(ctx, tokenID, api.SideSell, totalUSDC, negRisk)
		if err != nil {
			log.Printf("[CopyTrader-Bot] SELL failed: %v", err)
			return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "failed", fmt.Sprintf("order failed: %v", err), "", storage.StrategyBot, nil, nil, nil)
		}

		if !resp.Success {
			log.Printf("[CopyTrader-Bot] SELL rejected: %s", resp.ErrorMsg)
			return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "failed", resp.ErrorMsg, "", storage.StrategyBot, nil, nil, nil)
		}

		log.Printf("[CopyTrader-Bot] SELL success: OrderID=%s, Size=%.4f, AvgPrice=%.4f, USDC=$%.4f",
			resp.OrderID, totalSold, avgPrice, totalUSDC)

		// Clear position if we sold everything
		if remainingSize < 0.01 {
			ct.store.ClearMyPosition(ctx, trade.MarketID, trade.Outcome)
		}

		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, totalUSDC, avgPrice, totalSold, "executed", "", resp.OrderID, storage.StrategyBot, nil, nil, nil)
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
	resp, err := ct.clobClient.PlaceMarketOrder(ctx, tokenID, api.SideSell, sellSize*order3Price, negRisk)
	if err == nil && resp.Success {
		orderIDs = append(orderIDs, resp.OrderID)
		// Estimate fill
		if len(book.Bids) > 0 {
			var bestBid float64
			fmt.Sscanf(book.Bids[0].Price, "%f", &bestBid)
			totalFilled = sellSize
			totalValue = sellSize * bestBid
		}
	}

	if totalFilled > 0 {
		avgPrice := totalValue / totalFilled
		ct.store.ClearMyPosition(ctx, trade.MarketID, trade.Outcome)
		return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, totalValue, avgPrice, totalFilled, "executed", "", strings.Join(orderIDs, ","), storage.StrategyBot, nil, nil, nil)
	}

	// Failed to sell
	log.Printf("[CopyTrader-Bot] SELL: failed to fill any orders")
	return ct.logCopyTradeWithStrategy(ctx, trade, tokenID, 0, 0, 0, sellSize, "failed", "no fills achieved", "", storage.StrategyBot, nil, nil, nil)
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
	// Save to old copy_trades table for backwards compatibility
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

	if err := ct.store.SaveCopyTrade(ctx, copyTrade); err != nil {
		log.Printf("[CopyTrader] Warning: failed to save copy trade: %v", err)
	}

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

	if err := ct.store.SaveCopyTradeLog(ctx, logEntry); err != nil {
		log.Printf("[CopyTrader] Warning: failed to save copy trade log: %v", err)
	}

	return nil
}

// GetStats returns copy trading statistics
func (ct *CopyTrader) GetStats(ctx context.Context) (map[string]interface{}, error) {
	return ct.store.GetCopyTradeStats(ctx)
}
