// Package syncer provides real-time trade detection for copy trading.
package syncer

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"polymarket-analyzer/api"
	"polymarket-analyzer/models"
	"polymarket-analyzer/storage"
	"polymarket-analyzer/utils"
)

// RealtimeDetector combines multiple data sources for fastest trade detection:
// 1. CLOB API polling (PRIMARY: ~50-150ms latency - trades appear when matched off-chain)
// 2. Polygon blockchain WebSocket (BACKUP: ~10-20s from trade execution)
// 3. LiveData WebSocket for BTC 15m markets (Strategy 3: ~650ms latency)
type RealtimeDetector struct {
	apiClient  *api.Client
	clobClient *api.ClobClient      // For fast CLOB API trade detection (~50ms latency)
	store      *storage.PostgresStore
	polygonWS  *api.PolygonWSClient // Blockchain WebSocket for backup detection
	liveDataWS *api.LiveDataWSClient // LiveData WebSocket for BTC 15m (Strategy 3)

	// Followed users (Strategy 1 & 2 - polled)
	followedUsers   map[string]bool
	followedUsersMu sync.RWMutex

	// BTC 15m users (Strategy 3 - WebSocket only)
	btc15mUsers   map[string]bool
	btc15mUsersMu sync.RWMutex

	// User settings cache (refreshed every 30s to avoid DB queries during execution)
	userSettingsCache   map[string]*storage.UserCopySettings
	userSettingsCacheMu sync.RWMutex

	// Last check timestamps per user (using match_time from CLOB API)
	lastCheck   map[string]time.Time
	lastCheckMu sync.RWMutex

	// Processed trades (to avoid duplicates) - keyed by trade ID
	processedTxs   map[string]bool
	processedTxsMu sync.RWMutex

	// Trade callback
	onNewTrade func(trade models.TradeDetail)

	// Metrics
	metrics     *DetectorMetrics
	metricsMu   sync.RWMutex

	// Feature flag for CLOB detection (can fall back to Data API if needed)
	useCLOBDetection bool

	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// DetectorMetrics tracks performance metrics
type DetectorMetrics struct {
	TradesDetected      int64
	AvgDetectionLatency time.Duration
	FastestDetection    time.Duration
	SlowestDetection    time.Duration
	CLOBPolls           int64 // CLOB API polls (primary, ~50ms latency)
	DataAPIPolls        int64 // Data API polls (fallback, ~30-80s latency)
	BlockchainEvents    int64 // Events from Polygon WebSocket
	BlockchainMatches   int64 // Matched followed users on blockchain
	LiveDataWSEvents    int64 // Events from LiveData WebSocket (BTC 15m Strategy 3)
	LiveDataWSMatches   int64 // Matched followed users on LiveData WS
	LastDetectionTime   time.Time
}

// NewRealtimeDetector creates a new real-time trade detector
// clobClient: required for fast CLOB API detection (~50ms latency)
// enableBlockchainWS: if true, uses Polygon WebSocket for backup ~1s detection (heavy, use in worker only)
func NewRealtimeDetector(apiClient *api.Client, clobClient *api.ClobClient, store *storage.PostgresStore, onNewTrade func(trade models.TradeDetail), enableBlockchainWS bool) *RealtimeDetector {
	d := &RealtimeDetector{
		apiClient:         apiClient,
		clobClient:        clobClient,
		store:             store,
		followedUsers:     make(map[string]bool),
		btc15mUsers:       make(map[string]bool),
		userSettingsCache: make(map[string]*storage.UserCopySettings),
		lastCheck:         make(map[string]time.Time),
		processedTxs:      make(map[string]bool),
		onNewTrade:        onNewTrade,
		metrics:           &DetectorMetrics{},
		stopCh:            make(chan struct{}),
		useCLOBDetection:  clobClient != nil, // Enable CLOB detection if client is provided
	}

	// Create Polygon WebSocket client for backup blockchain monitoring
	// Only enable in worker to avoid heavy processing in main API app
	if enableBlockchainWS {
		d.polygonWS = api.NewPolygonWSClient(d.handleBlockchainTrade)
		log.Printf("[RealtimeDetector] Blockchain WebSocket ENABLED (backup ~10-20s detection)")
	}

	// Create LiveData WebSocket client for BTC 15m markets (Strategy 3)
	d.liveDataWS = api.NewLiveDataWSClient(d.handleLiveDataTrade)
	log.Printf("[RealtimeDetector] LiveData WebSocket ENABLED (Strategy 3: ~650ms latency)")

	if d.useCLOBDetection {
		log.Printf("[RealtimeDetector] CLOB API detection ENABLED (primary ~50ms latency)")
	} else {
		log.Printf("[RealtimeDetector] CLOB API detection DISABLED (using Data API fallback ~30-80s)")
	}

	return d
}

// Start begins real-time trade detection
func (d *RealtimeDetector) Start(ctx context.Context) error {
	if d.running {
		return fmt.Errorf("detector already running")
	}

	// Load followed users (including Strategy 3 users)
	if err := d.refreshFollowedUsers(ctx); err != nil {
		log.Printf("[RealtimeDetector] Warning: failed to load followed users: %v", err)
	}

	// Start Polygon WebSocket for backup detection (~10-20s)
	if d.polygonWS != nil {
		if err := d.polygonWS.Start(ctx); err != nil {
			log.Printf("[RealtimeDetector] Warning: Polygon WebSocket failed to start: %v", err)
			// Continue without blockchain monitoring - we'll rely on API polling
		} else {
			log.Printf("[RealtimeDetector] ✓ Polygon blockchain WebSocket started (backup detection)")
		}
	}

	// Start LiveData WebSocket for BTC 15m markets (Strategy 3)
	if d.liveDataWS != nil {
		if err := d.liveDataWS.Start(ctx); err != nil {
			log.Printf("[RealtimeDetector] Warning: LiveData WebSocket failed to start: %v", err)
		} else {
			// Subscribe to current and next BTC 15m market slugs
			current, next := api.GetBTC15mSlugs()
			if err := d.liveDataWS.UpdateSubscriptions([]string{current, next}); err != nil {
				log.Printf("[RealtimeDetector] Warning: LiveData WebSocket subscription failed: %v", err)
			} else {
				log.Printf("[RealtimeDetector] ✓ LiveData WebSocket started (BTC 15m: %s, %s)", current, next)
			}
		}
	}

	d.running = true

	// Start fast polling loop (fallback detection method)
	d.wg.Add(1)
	go d.fastPollLoop(ctx)

	// Start user refresh loop (every 30s)
	d.wg.Add(1)
	go d.userRefreshLoop(ctx)

	// Start slug rotation loop for BTC 15m markets (every 1 minute check)
	d.wg.Add(1)
	go d.slugRotationLoop(ctx)

	log.Printf("[RealtimeDetector] Started with %d polled users, %d BTC 15m users", len(d.followedUsers), len(d.btc15mUsers))
	return nil
}

// Stop gracefully shuts down the detector
func (d *RealtimeDetector) Stop() {
	if !d.running {
		return
	}

	d.running = false
	close(d.stopCh)

	if d.polygonWS != nil {
		d.polygonWS.Stop()
	}

	if d.liveDataWS != nil {
		d.liveDataWS.Stop()
	}

	d.wg.Wait()
	log.Printf("[RealtimeDetector] Stopped")
}

// GetMetrics returns current performance metrics
func (d *RealtimeDetector) GetMetrics() DetectorMetrics {
	d.metricsMu.RLock()
	defer d.metricsMu.RUnlock()
	return *d.metrics
}

// fastPollLoop polls the CLOB API for followed users' trades at high frequency
func (d *RealtimeDetector) fastPollLoop(ctx context.Context) {
	defer d.wg.Done()

	// Poll every 100ms for real-time detection via CLOB API
	// CLOB rate limit is 150 req/10s = 67ms minimum, we use 100ms for safety buffer
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.pollFollowedUsers(ctx)
		}
	}
}

// pollFollowedUsers checks each followed user for new trades
func (d *RealtimeDetector) pollFollowedUsers(ctx context.Context) {
	d.followedUsersMu.RLock()
	users := make([]string, 0, len(d.followedUsers))
	for user := range d.followedUsers {
		users = append(users, user)
	}
	d.followedUsersMu.RUnlock()

	if len(users) == 0 {
		return
	}

	// Poll users in parallel with rate limiting
	const maxConcurrent = 3
	semaphore := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup

	for _, user := range users {
		wg.Add(1)
		go func(userAddr string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			d.checkUserTrades(ctx, userAddr)
		}(user)
	}

	wg.Wait()
}

// checkUserTrades checks for new trades from a specific user
func (d *RealtimeDetector) checkUserTrades(ctx context.Context, userAddr string) {
	// Get last check time
	d.lastCheckMu.RLock()
	lastCheck := d.lastCheck[userAddr]
	d.lastCheckMu.RUnlock()

	if lastCheck.IsZero() {
		lastCheck = time.Now().Add(-1 * time.Minute) // Start from 1 minute ago
	}

	// Use CLOB API for fast detection (~50ms) or fall back to Data API (~30-80s)
	if d.useCLOBDetection {
		d.checkUserTradesCLOB(ctx, userAddr, lastCheck)
	} else {
		d.checkUserTradesDataAPI(ctx, userAddr, lastCheck)
	}
}

// checkUserTradesCLOB fetches trades from CLOB API (primary method, ~50ms latency)
func (d *RealtimeDetector) checkUserTradesCLOB(ctx context.Context, userAddr string, lastCheck time.Time) {
	// Fetch trades since last check using CLOB API
	trades, err := d.clobClient.GetCLOBTrades(ctx, api.CLOBTradeParams{
		Maker: userAddr,
		After: lastCheck.Unix(),
	})
	if err != nil {
		// Don't log every error - too noisy at 100ms polling
		// Fall back to Data API on persistent failures could be added here
		return
	}

	// Update metrics
	d.metricsMu.Lock()
	d.metrics.CLOBPolls++
	d.metricsMu.Unlock()

	// Update last check time
	d.lastCheckMu.Lock()
	d.lastCheck[userAddr] = time.Now()
	d.lastCheckMu.Unlock()

	if len(trades) == 0 {
		return
	}

	// Process new trades
	for _, trade := range trades {
		// Capture detection time immediately
		detectedAt := time.Now()

		// Convert CLOB trade to TradeDetail
		detail := d.convertCLOBTradeToDetail(ctx, trade, userAddr)
		detail.DetectedAt = detectedAt

		// Check if we've already processed this trade (use trade ID)
		d.processedTxsMu.Lock()
		if d.processedTxs[trade.ID] {
			d.processedTxsMu.Unlock()
			continue // Already processed
		}
		d.processedTxs[trade.ID] = true
		// Cleanup old entries (keep last 1000)
		if len(d.processedTxs) > 1000 {
			for k := range d.processedTxs {
				delete(d.processedTxs, k)
				if len(d.processedTxs) <= 500 {
					break
				}
			}
		}
		d.processedTxsMu.Unlock()

		// Calculate detection latency (from match_time)
		latency := time.Since(detail.Timestamp)

		// Update metrics
		d.metricsMu.Lock()
		d.metrics.TradesDetected++
		d.metrics.LastDetectionTime = time.Now()
		if d.metrics.FastestDetection == 0 || latency < d.metrics.FastestDetection {
			d.metrics.FastestDetection = latency
		}
		if latency > d.metrics.SlowestDetection {
			d.metrics.SlowestDetection = latency
		}
		// Running average
		if d.metrics.AvgDetectionLatency == 0 {
			d.metrics.AvgDetectionLatency = latency
		} else {
			d.metrics.AvgDetectionLatency = (d.metrics.AvgDetectionLatency + latency) / 2
		}
		d.metricsMu.Unlock()

		log.Printf("[RealtimeDetector] 🚀 CLOB trade detected: user=%s side=%s price=%s latency=%s",
			utils.ShortAddress(userAddr), trade.Side, trade.Price, latency.Round(time.Millisecond))

		// Save to user_trades for historical record
		if err := d.store.SaveTrades(ctx, []models.TradeDetail{detail}, false); err != nil {
			log.Printf("[RealtimeDetector] Warning: failed to save trade to user_trades: %v", err)
		}

		// Mark as detected in cache (for Data API deduplication)
		d.store.CacheTokenID(ctx, detail.ID, "processed", "true", false)

		// Notify callback
		if d.onNewTrade != nil {
			d.onNewTrade(detail)
		}
	}
}

// checkUserTradesDataAPI fetches trades from Data API (fallback method, ~30-80s latency)
func (d *RealtimeDetector) checkUserTradesDataAPI(ctx context.Context, userAddr string, lastCheck time.Time) {
	// Fetch trades since last check using data-api
	trades, err := d.apiClient.GetActivity(ctx, api.TradeQuery{
		User:  userAddr,
		Limit: 50,
		After: lastCheck.Unix(),
	})
	if err != nil {
		return
	}

	// Update metrics
	d.metricsMu.Lock()
	d.metrics.DataAPIPolls++
	d.metricsMu.Unlock()

	// Update last check time
	d.lastCheckMu.Lock()
	d.lastCheck[userAddr] = time.Now()
	d.lastCheckMu.Unlock()

	if len(trades) == 0 {
		return
	}

	// Process new trades
	for _, trade := range trades {
		detectedAt := time.Now()
		detail := d.convertDataTradeToDetail(trade, userAddr)
		detail.DetectedAt = detectedAt

		// Check if we've already processed this trade
		exists, _, _ := d.store.GetTokenIDFromCache(ctx, detail.ID, "processed")
		if exists != "" {
			continue
		}

		latency := time.Since(detail.Timestamp)

		d.metricsMu.Lock()
		d.metrics.TradesDetected++
		d.metrics.LastDetectionTime = time.Now()
		if d.metrics.FastestDetection == 0 || latency < d.metrics.FastestDetection {
			d.metrics.FastestDetection = latency
		}
		if latency > d.metrics.SlowestDetection {
			d.metrics.SlowestDetection = latency
		}
		if d.metrics.AvgDetectionLatency == 0 {
			d.metrics.AvgDetectionLatency = latency
		} else {
			d.metrics.AvgDetectionLatency = (d.metrics.AvgDetectionLatency + latency) / 2
		}
		d.metricsMu.Unlock()

		log.Printf("[RealtimeDetector] Data API trade detected: user=%s side=%s price=%.4f latency=%s",
			utils.ShortAddress(userAddr), trade.Side, trade.Price.Float64(), latency.Round(time.Millisecond))

		if err := d.store.SaveTrades(ctx, []models.TradeDetail{detail}, false); err != nil {
			log.Printf("[RealtimeDetector] Warning: failed to save trade to user_trades: %v", err)
		}

		d.store.CacheTokenID(ctx, detail.ID, "processed", "true", false)

		if d.onNewTrade != nil {
			d.onNewTrade(detail)
		}
	}
}

// handleBlockchainTrade is called when Polygon WebSocket detects a followed user's trade
// This is the BACKUP detection method - ~10-20 seconds from trade execution
func (d *RealtimeDetector) handleBlockchainTrade(event api.PolygonTradeEvent) {
	detectedAt := time.Now()

	// Update metrics
	d.metricsMu.Lock()
	d.metrics.BlockchainEvents++
	d.metrics.BlockchainMatches++
	d.metrics.TradesDetected++
	d.metrics.LastDetectionTime = detectedAt
	d.metricsMu.Unlock()

	// Check if we already processed this transaction
	d.processedTxsMu.Lock()
	if d.processedTxs[event.TxHash] {
		d.processedTxsMu.Unlock()
		return
	}
	d.processedTxs[event.TxHash] = true
	// Cleanup old entries (keep last 1000)
	if len(d.processedTxs) > 1000 {
		for k := range d.processedTxs {
			delete(d.processedTxs, k)
			if len(d.processedTxs) <= 500 {
				break
			}
		}
	}
	d.processedTxsMu.Unlock()

	// Determine which address is the followed user (maker or taker)
	userAddr := ""
	role := ""
	d.followedUsersMu.RLock()
	makerNorm := utils.NormalizeAddress(event.Maker)
	takerNorm := utils.NormalizeAddress(event.Taker)
	if d.followedUsers[makerNorm] {
		userAddr = makerNorm
		role = "MAKER"
	} else if d.followedUsers[takerNorm] {
		userAddr = takerNorm
		role = "TAKER"
	}
	d.followedUsersMu.RUnlock()

	if userAddr == "" {
		return // Neither maker nor taker is followed (shouldn't happen)
	}

	log.Printf("[RealtimeDetector] 🚀 BLOCKCHAIN TRADE DETECTED: user=%s role=%s tx=%s",
		utils.ShortAddress(userAddr), role, event.TxHash[:16])

	// Determine token ID and side based on role
	// In CTF Exchange:
	// - MakerAssetID = what the maker gave (outcome token if they're selling)
	// - TakerAssetID = what the taker gave (USDC if they're buying)
	//
	// For copy trading, we primarily care about BUY trades (TAKER receiving outcome tokens)
	// - TAKER with makerAssetId != 0: they're BUYING (receiving outcome tokens)
	// - TAKER with makerAssetId = 0: skip (they're selling, received USDC)
	// - MAKER: skip for now (limit order fills, less relevant for copy trading)

	// Skip MAKER trades (they placed limit orders, not active trading)
	if role == "MAKER" {
		log.Printf("[RealtimeDetector] Skipping MAKER trade (limit order fill): %s", event.TxHash[:16])
		return
	}

	tokenID := event.MakerAssetID
	side := "BUY"

	// Check if makerAssetId is USDC (0 or very small) - that means taker SOLD
	if event.MakerAssetID == "0x0000000000000000000000000000000000000000000000000000000000000000" {
		// Taker received USDC (makerAssetId=0), meaning they SOLD their tokens
		// Use takerAssetId (what they gave = the outcome token they sold)
		tokenID = event.TakerAssetID
		side = "SELL"
		log.Printf("[RealtimeDetector] Detected SELL trade (taker sold tokens)")
	}

	// Clean up token ID (remove leading zeros if needed)
	if len(tokenID) == 66 && tokenID[:2] == "0x" {
		// Token IDs are 32 bytes (64 hex + 0x prefix)
		// Convert to decimal string for compatibility
		tokenID = tokenID // Keep as hex for now, copy_trader will handle it
	}

	// Convert to TradeDetail
	detail := models.TradeDetail{
		ID:              event.TxHash,
		UserID:          userAddr,
		MarketID:        tokenID, // Token ID from blockchain
		Type:            "TRADE",
		Side:            side,
		Role:            role,
		TransactionHash: event.TxHash,
		Timestamp:       event.Timestamp,
		DetectedAt:      detectedAt,
		DetectionSource: "polygon_ws", // Detected via Polygon blockchain WebSocket
		// Size, Price will be determined by copy_trader from orderbook
	}

	// Notify callback - copy trader will handle the rest
	if d.onNewTrade != nil {
		d.onNewTrade(detail)
	}
}

// userRefreshLoop periodically refreshes the list of followed users
func (d *RealtimeDetector) userRefreshLoop(ctx context.Context) {
	defer d.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		case <-ticker.C:
			if err := d.refreshFollowedUsers(ctx); err != nil {
				log.Printf("[RealtimeDetector] Warning: failed to refresh followed users: %v", err)
			}
		}
	}
}

// slugRotationLoop updates BTC 15m market subscriptions every minute
func (d *RealtimeDetector) slugRotationLoop(ctx context.Context) {
	defer d.wg.Done()

	// Check every minute for slug changes
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	var lastCurrent, lastNext string

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		case <-ticker.C:
			if d.liveDataWS == nil {
				continue
			}

			current, next := api.GetBTC15mSlugs()

			// Only update if slugs changed
			if current != lastCurrent || next != lastNext {
				log.Printf("[RealtimeDetector] BTC 15m slug rotation: %s → %s", current, next)
				if err := d.liveDataWS.UpdateSubscriptions([]string{current, next}); err != nil {
					log.Printf("[RealtimeDetector] Warning: slug rotation failed: %v", err)
				}
				lastCurrent = current
				lastNext = next
			}
		}
	}
}

// handleLiveDataTrade is called when LiveData WebSocket detects a trade on BTC 15m markets
// This is the PRIMARY detection method for Strategy 3 users (~650ms latency)
func (d *RealtimeDetector) handleLiveDataTrade(event api.LiveDataTradeEvent) {
	detectedAt := time.Now()

	// Update metrics for all events
	d.metricsMu.Lock()
	d.metrics.LiveDataWSEvents++
	d.metricsMu.Unlock()

	// Check if the trader (proxyWallet) is a followed BTC 15m user
	d.btc15mUsersMu.RLock()
	isFollowed := d.btc15mUsers[utils.NormalizeAddress(event.ProxyWallet)]
	d.btc15mUsersMu.RUnlock()

	if !isFollowed {
		return // Not a followed user
	}

	// Check if we've already processed this trade
	d.processedTxsMu.Lock()
	if d.processedTxs[event.TransactionHash] {
		d.processedTxsMu.Unlock()
		return // Already processed
	}
	d.processedTxs[event.TransactionHash] = true
	// Cleanup old entries (keep last 1000)
	if len(d.processedTxs) > 1000 {
		for k := range d.processedTxs {
			delete(d.processedTxs, k)
			if len(d.processedTxs) <= 500 {
				break
			}
		}
	}
	d.processedTxsMu.Unlock()

	// Calculate detection latency
	tradeTime := time.Unix(event.Timestamp, 0)
	latency := detectedAt.Sub(tradeTime)

	// Update metrics
	d.metricsMu.Lock()
	d.metrics.LiveDataWSMatches++
	d.metrics.TradesDetected++
	d.metrics.LastDetectionTime = detectedAt
	if d.metrics.FastestDetection == 0 || latency < d.metrics.FastestDetection {
		d.metrics.FastestDetection = latency
	}
	if latency > d.metrics.SlowestDetection {
		d.metrics.SlowestDetection = latency
	}
	if d.metrics.AvgDetectionLatency == 0 {
		d.metrics.AvgDetectionLatency = latency
	} else {
		d.metrics.AvgDetectionLatency = (d.metrics.AvgDetectionLatency + latency) / 2
	}
	d.metricsMu.Unlock()

	log.Printf("[RealtimeDetector] 🚀 LIVE DATA TRADE DETECTED: user=%s side=%s size=%.2f price=%.4f outcome=%s latency=%s",
		event.Name, event.Side, event.Size, event.Price, event.Outcome, latency.Round(time.Millisecond))

	// Convert to TradeDetail
	usdcSize := event.Size * event.Price
	detail := models.TradeDetail{
		ID:              event.TransactionHash,
		UserID:          utils.NormalizeAddress(event.ProxyWallet),
		MarketID:        event.ConditionID, // Use conditionId as market identifier
		Type:            "TRADE",
		Side:            event.Side,
		Size:            event.Size,
		UsdcSize:        usdcSize,
		Price:           event.Price,
		Outcome:         event.Outcome,
		Slug:            event.EventSlug,
		TransactionHash: event.TransactionHash,
		Name:            event.Name,
		Pseudonym:       event.Pseudonym,
		Timestamp:       tradeTime,
		DetectedAt:      detectedAt,
		DetectionSource: "live_ws", // Detected via LiveData WebSocket
	}

	// Save to user_trades for historical record
	ctx := context.Background()
	if err := d.store.SaveTrades(ctx, []models.TradeDetail{detail}, false); err != nil {
		log.Printf("[RealtimeDetector] Warning: failed to save LiveData trade: %v", err)
	}

	// Mark as processed
	d.store.CacheTokenID(ctx, detail.ID, "processed", "true", false)

	// Notify callback - copy trader will handle the rest
	if d.onNewTrade != nil {
		d.onNewTrade(detail)
	}
}

// refreshFollowedUsers reloads the list of users to monitor and their settings
func (d *RealtimeDetector) refreshFollowedUsers(ctx context.Context) error {
	// Get all followed user addresses
	allUsers, err := d.store.GetFollowedUserAddresses(ctx)
	if err != nil {
		return err
	}

	// Get Strategy 3 (BTC 15m) users separately
	btc15mUsers, err := d.store.GetFollowedUsersByStrategy(ctx, storage.StrategyBTC15m)
	if err != nil {
		log.Printf("[RealtimeDetector] Warning: failed to get BTC 15m users: %v", err)
		btc15mUsers = []string{}
	}

	// Build set of BTC 15m users for quick lookup
	btc15mSet := make(map[string]bool, len(btc15mUsers))
	for _, user := range btc15mUsers {
		btc15mSet[utils.NormalizeAddress(user)] = true
	}

	// Load settings for all followed users into cache (avoid DB queries during execution)
	newSettingsCache := make(map[string]*storage.UserCopySettings, len(allUsers))
	for _, user := range allUsers {
		normalized := utils.NormalizeAddress(user)
		settings, err := d.store.GetUserCopySettings(ctx, user)
		if err != nil {
			log.Printf("[RealtimeDetector] Warning: failed to get settings for %s: %v", normalized[:10], err)
			continue
		}
		if settings != nil {
			newSettingsCache[normalized] = settings
		}
	}

	// Update settings cache
	d.userSettingsCacheMu.Lock()
	d.userSettingsCache = newSettingsCache
	d.userSettingsCacheMu.Unlock()

	// Update BTC 15m users (Strategy 3)
	d.btc15mUsersMu.Lock()
	d.btc15mUsers = btc15mSet
	d.btc15mUsersMu.Unlock()

	// Build followed users list, EXCLUDING Strategy 3 users (they use WebSocket only)
	d.followedUsersMu.Lock()
	d.followedUsers = make(map[string]bool, len(allUsers))
	for _, user := range allUsers {
		normalized := utils.NormalizeAddress(user)
		if !btc15mSet[normalized] {
			d.followedUsers[normalized] = true
		}
	}
	d.followedUsersMu.Unlock()

	// Update Polygon WebSocket with followed addresses for blockchain monitoring
	// Include all users (both polled and BTC 15m) for backup detection
	if d.polygonWS != nil {
		d.polygonWS.SetFollowedAddresses(allUsers)
	}

	if len(btc15mUsers) > 0 {
		log.Printf("[RealtimeDetector] Refreshed: %d polled, %d BTC 15m, %d settings cached",
			len(d.followedUsers), len(btc15mUsers), len(newSettingsCache))
	}

	return nil
}

// GetCachedUserSettings returns cached user settings (nil if not found)
func (d *RealtimeDetector) GetCachedUserSettings(userAddr string) *storage.UserCopySettings {
	d.userSettingsCacheMu.RLock()
	defer d.userSettingsCacheMu.RUnlock()
	return d.userSettingsCache[utils.NormalizeAddress(userAddr)]
}

// AddFollowedUser adds a user to the monitored list
func (d *RealtimeDetector) AddFollowedUser(userAddr string) {
	d.followedUsersMu.Lock()
	d.followedUsers[utils.NormalizeAddress(userAddr)] = true
	d.followedUsersMu.Unlock()
}

// RemoveFollowedUser removes a user from the monitored list
func (d *RealtimeDetector) RemoveFollowedUser(userAddr string) {
	d.followedUsersMu.Lock()
	delete(d.followedUsers, utils.NormalizeAddress(userAddr))
	d.followedUsersMu.Unlock()
}

// convertCLOBTradeToDetail converts a CLOB API trade to a TradeDetail
// CLOB trades have different field names and string types that need parsing
func (d *RealtimeDetector) convertCLOBTradeToDetail(ctx context.Context, trade api.CLOBTrade, userAddr string) models.TradeDetail {
	// Parse numeric values from strings
	size, _ := strconv.ParseFloat(trade.Size, 64)
	price, _ := strconv.ParseFloat(trade.Price, 64)
	usdcSize := size * price // Calculate USDC value

	// Parse match_time timestamp
	matchTime, _ := strconv.ParseInt(trade.MatchTime, 10, 64)
	timestamp := time.Unix(matchTime, 0).UTC()

	// Use trade ID as the unique identifier
	tradeID := trade.ID
	if tradeID == "" && trade.TransactionHash != "" {
		tradeID = trade.TransactionHash
	}

	// Determine role (maker = placed limit order, taker = took from book)
	role := "TAKER" // Default assumption for copy trading
	if trade.MakerAddress == userAddr {
		role = "MAKER"
	}

	// Try to get title/slug/outcome from token cache
	title := ""
	slug := ""
	outcome := trade.Outcome // CLOB might provide this
	if trade.AssetID != "" {
		tokenInfo, err := d.store.GetTokenInfo(ctx, trade.AssetID)
		if err == nil && tokenInfo != nil {
			title = tokenInfo.Title
			slug = tokenInfo.Slug
			if outcome == "" {
				outcome = tokenInfo.Outcome
			}
		}
	}

	return models.TradeDetail{
		ID:              tradeID,
		UserID:          utils.NormalizeAddress(userAddr),
		MarketID:        trade.AssetID, // Token ID
		Type:            "TRADE",
		Side:            trade.Side,
		Role:            role,
		Size:            size,
		UsdcSize:        usdcSize,
		Price:           price,
		Outcome:         outcome,
		Title:           title,
		Slug:            slug,
		TransactionHash: trade.TransactionHash,
		Timestamp:       timestamp,
		DetectionSource: "clob", // Detected via CLOB API
	}
}

// convertDataTradeToDetail converts a Data API trade to a TradeDetail
func (d *RealtimeDetector) convertDataTradeToDetail(trade api.DataTrade, userAddr string) models.TradeDetail {
	tradeID := trade.TransactionHash
	if tradeID == "" {
		tradeID = fmt.Sprintf("%s-%d-%s", trade.ProxyWallet, trade.Timestamp, trade.Asset)
	}

	role := "TAKER"
	if trade.IsMaker {
		role = "MAKER"
	}

	tradeType := trade.Type
	if tradeType == "" {
		tradeType = "TRADE"
	}

	return models.TradeDetail{
		ID:              tradeID,
		UserID:          utils.NormalizeAddress(userAddr),
		MarketID:        trade.Asset,
		Type:            tradeType,
		Side:            trade.Side,
		Role:            role,
		Size:            trade.Size.Float64(),
		UsdcSize:        trade.UsdcSize.Float64(),
		Price:           trade.Price.Float64(),
		Outcome:         trade.Outcome,
		Title:           trade.Title,
		Slug:            trade.Slug,
		TransactionHash: trade.TransactionHash,
		Name:            trade.Name,
		Pseudonym:       trade.Pseudonym,
		Timestamp:       time.Unix(trade.Timestamp, 0).UTC(),
		DetectionSource: "data_api", // Detected via Data API (fallback)
	}
}
