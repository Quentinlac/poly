// Package syncer provides real-time trade detection for copy trading.
package syncer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"polymarket-analyzer/api"
	"polymarket-analyzer/models"
	"polymarket-analyzer/storage"
	"polymarket-analyzer/utils"
)

// RealtimeDetector combines multiple data sources for fastest trade detection:
// 1. Polygon blockchain WebSocket (FASTEST: ~1-2s from trade execution)
// 2. Direct data-api polling (fallback: ~30-80s lag due to indexing)
// 3. WebSocket for market activity (instant notification but no user addresses)
type RealtimeDetector struct {
	apiClient   *api.Client
	store       *storage.PostgresStore
	wsClient    *api.WSClient
	polygonWS   *api.PolygonWSClient // NEW: Blockchain WebSocket for instant detection

	// Followed users
	followedUsers   map[string]bool
	followedUsersMu sync.RWMutex

	// Last check timestamps per user
	lastCheck   map[string]time.Time
	lastCheckMu sync.RWMutex

	// Processed blockchain trades (to avoid duplicates)
	processedTxs   map[string]bool
	processedTxsMu sync.RWMutex

	// Trade callback
	onNewTrade func(trade models.TradeDetail)

	// Metrics
	metrics     *DetectorMetrics
	metricsMu   sync.RWMutex

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
	APIPolls            int64
	WebSocketEvents     int64
	BlockchainEvents    int64 // NEW: Events from Polygon WebSocket
	BlockchainMatches   int64 // NEW: Matched followed users on blockchain
	LastDetectionTime   time.Time
}

// NewRealtimeDetector creates a new real-time trade detector
func NewRealtimeDetector(apiClient *api.Client, store *storage.PostgresStore, onNewTrade func(trade models.TradeDetail)) *RealtimeDetector {
	d := &RealtimeDetector{
		apiClient:     apiClient,
		store:         store,
		followedUsers: make(map[string]bool),
		lastCheck:     make(map[string]time.Time),
		processedTxs:  make(map[string]bool),
		onNewTrade:    onNewTrade,
		metrics:       &DetectorMetrics{},
		stopCh:        make(chan struct{}),
	}

	// Create Polygon WebSocket client for real-time blockchain monitoring
	d.polygonWS = api.NewPolygonWSClient(d.handleBlockchainTrade)

	return d
}

// Start begins real-time trade detection
func (d *RealtimeDetector) Start(ctx context.Context) error {
	if d.running {
		return fmt.Errorf("detector already running")
	}

	// Load followed users
	if err := d.refreshFollowedUsers(ctx); err != nil {
		log.Printf("[RealtimeDetector] Warning: failed to load followed users: %v", err)
	}

	// Start Polygon WebSocket for FASTEST detection (~1-2s)
	if d.polygonWS != nil {
		if err := d.polygonWS.Start(ctx); err != nil {
			log.Printf("[RealtimeDetector] Warning: Polygon WebSocket failed to start: %v", err)
			// Continue without blockchain monitoring - we'll rely on API polling
		} else {
			log.Printf("[RealtimeDetector] ✓ Polygon blockchain WebSocket started (instant detection)")
		}
	}

	// Start Polymarket WebSocket client for market events (supplementary)
	d.wsClient = api.NewWSClient(d.handleWSTradeEvent)
	if err := d.wsClient.Start(ctx); err != nil {
		log.Printf("[RealtimeDetector] Warning: Polymarket WebSocket failed to start: %v", err)
		// Continue without WebSocket - we'll rely on polling
	}

	d.running = true

	// Start fast polling loop (fallback detection method)
	d.wg.Add(1)
	go d.fastPollLoop(ctx)

	// Start user refresh loop (every 30s)
	d.wg.Add(1)
	go d.userRefreshLoop(ctx)

	log.Printf("[RealtimeDetector] Started with %d followed users", len(d.followedUsers))
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

	if d.wsClient != nil {
		d.wsClient.Stop()
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

// fastPollLoop polls the data-api for followed users' trades at high frequency
func (d *RealtimeDetector) fastPollLoop(ctx context.Context) {
	defer d.wg.Done()

	// Poll every 200ms for near real-time detection
	ticker := time.NewTicker(200 * time.Millisecond)
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

	d.metricsMu.Lock()
	d.metrics.APIPolls++
	d.metricsMu.Unlock()
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

	// Fetch trades since last check using data-api (faster than subgraph)
	trades, err := d.apiClient.GetActivity(ctx, api.TradeQuery{
		User:  userAddr,
		Limit: 50,
		After: lastCheck.Unix(),
	})
	if err != nil {
		// Don't log every error - too noisy at 200ms polling
		return
	}

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

		// Convert to TradeDetail
		detail := d.convertToTradeDetail(trade, userAddr)
		detail.DetectedAt = detectedAt // Set when we detected this trade

		// Check if we've already processed this trade
		exists, _, _ := d.store.GetTokenIDFromCache(ctx, detail.ID, "processed")
		if exists != "" {
			continue // Already processed
		}

		// Calculate detection latency
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

		log.Printf("[RealtimeDetector] New trade detected: user=%s side=%s price=%.4f latency=%s",
			utils.ShortAddress(userAddr), trade.Side, trade.Price.Float64(), latency.Round(time.Millisecond))

		// Save to user_trades for historical record
		if err := d.store.SaveTrades(ctx, []models.TradeDetail{detail}, false); err != nil {
			log.Printf("[RealtimeDetector] Warning: failed to save trade to user_trades: %v", err)
		}

		// Mark as detected (to avoid duplicates)
		d.store.CacheTokenID(ctx, detail.ID, "processed", "true", false)

		// Notify callback
		if d.onNewTrade != nil {
			d.onNewTrade(detail)
		}
	}
}

// handleWSTradeEvent is called when WebSocket detects a trade
func (d *RealtimeDetector) handleWSTradeEvent(event api.WSTradeEvent) {
	d.metricsMu.Lock()
	d.metrics.WebSocketEvents++
	d.metricsMu.Unlock()

	// WebSocket events tell us a trade happened on a market
	// This can trigger immediate polling of followed users who have positions in that market
	log.Printf("[RealtimeDetector] WebSocket trade event: asset=%s price=%.4f",
		event.AssetID[:16], event.Price)
}

// handleBlockchainTrade is called when Polygon WebSocket detects a followed user's trade
// This is the FASTEST detection method - ~1-2 seconds from trade execution
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

	log.Printf("[RealtimeDetector] 🚀 BLOCKCHAIN TRADE DETECTED in ~1s! user=%s role=%s tx=%s",
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

// refreshFollowedUsers reloads the list of users to monitor
func (d *RealtimeDetector) refreshFollowedUsers(ctx context.Context) error {
	users, err := d.store.GetFollowedUserAddresses(ctx)
	if err != nil {
		return err
	}

	d.followedUsersMu.Lock()
	d.followedUsers = make(map[string]bool, len(users))
	for _, user := range users {
		d.followedUsers[utils.NormalizeAddress(user)] = true
	}
	d.followedUsersMu.Unlock()

	// Update Polygon WebSocket with followed addresses for blockchain monitoring
	if d.polygonWS != nil {
		d.polygonWS.SetFollowedAddresses(users)
	}

	return nil
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

// convertToTradeDetail converts an API trade to a TradeDetail
func (d *RealtimeDetector) convertToTradeDetail(trade api.DataTrade, userAddr string) models.TradeDetail {
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
	}
}
