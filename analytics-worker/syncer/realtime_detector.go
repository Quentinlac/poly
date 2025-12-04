// Package syncer provides real-time trade detection for copy trading.
//
// =============================================================================
// REALTIME DETECTOR - ENTRY POINT FOR TRADE DETECTION
// =============================================================================
//
// This is the MAIN COORDINATOR for detecting trades from followed users.
// It monitors the Polygon blockchain via WebSocket and notifies the CopyTrader
// when a followed user executes a trade.
//
// DETECTION FLOW OVERVIEW:
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │  1. Polygon Blockchain emits "OrderFilled" event (~1-2s after block)       │
// │     ↓                                                                       │
// │  2. PolygonWSClient receives event via WebSocket subscription              │
// │     ↓                                                                       │
// │  3. RealtimeDetector.handleBlockchainTrade() processes the event           │
// │     ↓                                                                       │
// │  4. Check if maker/taker is a followed user (in-memory cache lookup)       │
// │     ↓                                                                       │
// │  5. Decode trade details (tokenID, side, price, size) from blockchain data │
// │     ↓                                                                       │
// │  6. Look up market title/outcome from token cache (or fetch sync from API) │
// │     ↓                                                                       │
// │  7. Call onNewTrade callback → CopyTrader.handleRealtimeTrade()            │
// │     ↓                                                                       │
// │  8. CopyTrader fetches order book and places copy order                    │
// └─────────────────────────────────────────────────────────────────────────────┘
//
// KEY CONCEPTS:
// - Followed Users: Addresses we monitor (stored in user_copy_settings table)
// - Deduplication: Uses TxHash:LogIndex to prevent processing same trade twice
// - Settings Cache: Refreshes every 30s to avoid DB queries on critical path
// - Detection Latency: ~1-2 seconds after the block containing the trade
//
// =============================================================================
package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"polymarket-analyzer/api"
	"polymarket-analyzer/models"
	"polymarket-analyzer/storage"
	"polymarket-analyzer/utils"
)

// RealtimeDetector uses multiple WebSocket sources for trade detection.
// Detection methods (first one wins, others are deduplicated):
//   - Mempool WS: ~3-5s BEFORE block (pending transactions)
//   - Polygon WS: ~1-2s AFTER block (confirmed OrderFilled events)
type RealtimeDetector struct {
	apiClient *api.Client
	store     *storage.PostgresStore
	polygonWS *api.PolygonWSClient  // Blockchain WebSocket for trade detection (~1-2s after block)
	mempoolWS *api.MempoolWSClient  // Mempool WebSocket for early detection (~3-5s before block)

	// Our own address (to track blockchain confirmations of our trades)
	myAddress string

	// All followed users (both Strategy 1/2 and Strategy 3)
	followedUsers   map[string]bool
	followedUsersMu sync.RWMutex

	// BTC 15m users (Strategy 3) - kept for settings lookup
	btc15mUsers   map[string]bool
	btc15mUsersMu sync.RWMutex

	// User settings cache (refreshed every 30s to avoid DB queries during execution)
	userSettingsCache   map[string]*storage.UserCopySettings
	userSettingsCacheMu sync.RWMutex

	// Processed trades (to avoid duplicates) - keyed by TxHash:LogIndex
	processedTxs   map[string]bool
	processedTxsMu sync.RWMutex

	// Trade callback
	onNewTrade func(trade models.TradeDetail)

	// Metrics
	metrics   *DetectorMetrics
	metricsMu sync.RWMutex

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
	BlockchainEvents    int64 // Events from Polygon WebSocket
	BlockchainMatches   int64 // Matched followed users on blockchain
	MempoolEvents       int64 // Events from Mempool WebSocket
	MempoolMatches      int64 // Matched followed users in mempool
	LastDetectionTime   time.Time
}

// NewRealtimeDetector creates a new real-time trade detector using Polygon WebSocket only.
// myAddressOverride: if provided, use this address for blockchain confirmation tracking (overrides env var)
func NewRealtimeDetector(apiClient *api.Client, clobClient *api.ClobClient, store *storage.PostgresStore, onNewTrade func(trade models.TradeDetail), enableBlockchainWS bool, myAddressOverride string) *RealtimeDetector {
	// Get our own address for tracking blockchain confirmations
	myAddress := strings.TrimSpace(myAddressOverride)
	if myAddress == "" {
		myAddress = strings.TrimSpace(os.Getenv("POLYMARKET_FUNDER_ADDRESS"))
	}
	if myAddress != "" {
		myAddress = utils.NormalizeAddress(myAddress)
		log.Printf("[RealtimeDetector] Will track blockchain confirmations for our address: %s", utils.ShortAddress(myAddress))
	} else {
		log.Printf("[RealtimeDetector] WARNING: No address configured for blockchain confirmation tracking")
	}

	d := &RealtimeDetector{
		apiClient:         apiClient,
		store:             store,
		myAddress:         myAddress,
		followedUsers:     make(map[string]bool),
		btc15mUsers:       make(map[string]bool),
		userSettingsCache: make(map[string]*storage.UserCopySettings),
		processedTxs:      make(map[string]bool),
		onNewTrade:        onNewTrade,
		metrics:           &DetectorMetrics{},
		stopCh:            make(chan struct{}),
	}

	// Create Polygon WebSocket client for blockchain events (~1-2s after block)
	d.polygonWS = api.NewPolygonWSClient(d.handleBlockchainTrade)
	log.Printf("[RealtimeDetector] Polygon WebSocket detection ENABLED (~1-2s after block)")

	// Create Mempool WebSocket client for early detection (~3-5s before block)
	d.mempoolWS = api.NewMempoolWSClient(d.handleMempoolTrade)
	log.Printf("[RealtimeDetector] Mempool WebSocket detection ENABLED (~3-5s before block)")

	return d
}

// Start begins real-time trade detection via multiple WebSocket sources
func (d *RealtimeDetector) Start(ctx context.Context) error {
	if d.running {
		return fmt.Errorf("detector already running")
	}

	// Load followed users
	if err := d.refreshFollowedUsers(ctx); err != nil {
		log.Printf("[RealtimeDetector] Warning: failed to load followed users: %v", err)
	}

	// Sync followed addresses to mempool client
	d.syncFollowedAddressesToMempool()

	// Start Mempool WebSocket for early detection (~3-5s before block)
	if err := d.mempoolWS.Start(ctx); err != nil {
		// Mempool is optional - log warning but continue
		log.Printf("[RealtimeDetector] ⚠ Mempool WebSocket failed to start: %v (continuing without)", err)
	} else {
		log.Printf("[RealtimeDetector] ✓ Mempool WebSocket started (early detection ~3-5s before block)")
	}

	// Start Polygon WebSocket for trade detection (~1-2s after block)
	if err := d.polygonWS.Start(ctx); err != nil {
		return fmt.Errorf("failed to start Polygon WebSocket: %w", err)
	}
	log.Printf("[RealtimeDetector] ✓ Polygon blockchain WebSocket started (~1-2s after block)")

	d.running = true

	// Start user refresh loop (every 30s)
	d.wg.Add(1)
	go d.userRefreshLoop(ctx)

	log.Printf("[RealtimeDetector] Started with %d followed users (Mempool + Polygon WS)", len(d.followedUsers))
	return nil
}

// Stop gracefully shuts down the detector
func (d *RealtimeDetector) Stop() {
	if !d.running {
		return
	}

	d.running = false
	close(d.stopCh)

	if d.mempoolWS != nil {
		d.mempoolWS.Stop()
	}

	if d.polygonWS != nil {
		d.polygonWS.Stop()
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

// =============================================================================
// handleBlockchainTrade - PRIMARY TRADE DETECTION HANDLER
// =============================================================================
//
// Called by PolygonWSClient when an OrderFilled event is received.
// This is the CRITICAL PATH - every millisecond counts for copy trading!
//
// INPUT: PolygonTradeEvent containing:
//   - TxHash, LogIndex: Unique identifier for this specific fill
//   - Maker, Taker: Addresses involved in the trade
//   - MakerAssetID, TakerAssetID: Which asset each party gave
//   - MakerAmount, TakerAmount: How much each party gave (in wei, 6 decimals)
//   - BlockNumber: Which block this trade was confirmed in
//
// PROCESSING STEPS:
//   1. Deduplication check (skip if TxHash:LogIndex already processed)
//   2. Identify if maker or taker is a followed user
//   3. Determine trade direction (BUY vs SELL) based on asset types
//   4. Convert hex tokenID to decimal format (APIs expect decimal)
//   5. Calculate price and size from blockchain amounts
//   6. Look up market title/outcome (from cache or Gamma API)
//   7. Build TradeDetail model and notify callback
//
// LATENCY: ~1-2 seconds from trade execution to detection
// OUTPUT: Calls onNewTrade callback with populated TradeDetail
// =============================================================================

// =============================================================================
// handleMempoolTrade - EARLY DETECTION FROM PENDING TRANSACTIONS
// =============================================================================
//
// Called by MempoolWSClient when a pending trade from a followed user is detected.
// This is ~3-5 seconds FASTER than blockchain detection because we see the
// transaction while it's still in the mempool (before block confirmation).
//
// IMPORTANT: Mempool trades may have decoded data that needs verification.
// The copy_trader.go handles Alchemy verification for accurate data.
//
// DEDUP: Uses TxHash as key - if this trade is later detected via Polygon WS,
// the copy_trader will deduplicate using TransactionHash.
// =============================================================================
func (d *RealtimeDetector) handleMempoolTrade(event api.MempoolTradeEvent) {
	detectedAt := time.Now()

	// Update metrics
	d.metricsMu.Lock()
	d.metrics.MempoolEvents++
	d.metricsMu.Unlock()

	// Dedup check using TxHash
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

	// Check if this is OUR OWN trade (copy trade settlement appearing in mempool)
	fromNorm := utils.NormalizeAddress(event.From)
	if d.myAddress != "" && fromNorm == d.myAddress {
		// This is our own copy trade hitting the mempool!
		txRef := event.TxHash
		if len(txRef) > 16 {
			txRef = txRef[:16]
		}
		log.Printf("[OUR_TX] 📡 OUR COPY TRADE IN MEMPOOL! tx=%s side=%s size=%.4f price=%.4f token=%s",
			txRef, event.Side, event.Size, event.Price, event.TokenID[:16])
		// Don't process further - this is just for monitoring
		return
	}

	// Update metrics for matched trades (from followed users only)
	d.metricsMu.Lock()
	d.metrics.MempoolMatches++
	d.metrics.TradesDetected++
	d.metrics.LastDetectionTime = detectedAt
	d.metricsMu.Unlock()

	log.Printf("[RealtimeDetector] ⚡ MEMPOOL TRADE DETECTED! from=%s side=%s size=%.4f price=%.4f tx=%s",
		utils.ShortAddress(event.From), event.Side, event.Size, event.Price, event.TxHash[:16])

	// Convert to TradeDetail
	// Note: Mempool decoded data may be inaccurate - copy_trader will verify via Alchemy
	detail := models.TradeDetail{
		ID:              event.TxHash, // Use TxHash as ID (matches dedup key)
		UserID:          fromNorm,
		MarketID:        event.TokenID, // May need verification in copy_trader
		Type:            "TRADE",
		Side:            event.Side,
		Role:            event.Role,
		Size:            event.Size,
		Price:           event.Price,
		TransactionHash: event.TxHash,
		Timestamp:       event.DetectedAt,
		DetectedAt:      detectedAt,
		DetectionSource: "mempool", // Detected via mempool monitoring
	}

	// Notify callback - copy trader will verify and execute
	if d.onNewTrade != nil {
		d.onNewTrade(detail)
	}
}

func (d *RealtimeDetector) handleBlockchainTrade(event api.PolygonTradeEvent) {
	detectedAt := time.Now()

	// Update metrics
	d.metricsMu.Lock()
	d.metrics.BlockchainEvents++
	d.metricsMu.Unlock()

	// Check if we already processed this transaction via mempool
	// Mempool uses TxHash only as key, so check that first to avoid duplicate processing
	d.processedTxsMu.Lock()
	if d.processedTxs[event.TxHash] {
		d.processedTxsMu.Unlock()
		log.Printf("[RealtimeDetector] Skipping polygon tx %s (already processed via mempool)", event.TxHash[:16])
		return
	}

	// Check if we already processed this specific log event
	// Use TxHash+LogIndex as key because one tx can have MULTIPLE OrderFilled events
	// (e.g., market order filling against multiple limit orders)
	eventKey := event.TxHash + ":" + event.LogIndex
	if d.processedTxs[eventKey] {
		d.processedTxsMu.Unlock()
		return
	}
	d.processedTxs[eventKey] = true
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
	isOurTrade := false
	d.followedUsersMu.RLock()
	makerNorm := utils.NormalizeAddress(event.Maker)
	takerNorm := utils.NormalizeAddress(event.Taker)

	// Check if this is our own trade (for blockchain confirmation tracking)
	if d.myAddress != "" && (makerNorm == d.myAddress || takerNorm == d.myAddress) {
		isOurTrade = true
		if makerNorm == d.myAddress {
			userAddr = makerNorm
			role = "MAKER"
		} else {
			userAddr = takerNorm
			role = "TAKER"
		}
	} else if d.followedUsers[makerNorm] {
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

	// Update metrics for matched trades
	d.metricsMu.Lock()
	d.metrics.BlockchainMatches++
	d.metrics.TradesDetected++
	d.metrics.LastDetectionTime = detectedAt
	d.metricsMu.Unlock()

	if isOurTrade {
		log.Printf("[RealtimeDetector] 📍 OUR TRADE CONFIRMED ON BLOCKCHAIN! role=%s block=%d tx=%s",
			role, event.BlockNumber, event.TxHash[:16])
		// Handle our own trade confirmation asynchronously
		go d.handleOurBlockchainTrade(event, role, detectedAt)
		return
	}

	log.Printf("[RealtimeDetector] 🚀 BLOCKCHAIN TRADE DETECTED! user=%s role=%s block=%d tx=%s detected=%s",
		utils.ShortAddress(userAddr), role, event.BlockNumber, event.TxHash[:16], detectedAt.Format("15:04:05.000"))

	// Log actual latency asynchronously (doesn't block detection path)
	go logBlockchainLatency(event.BlockNumber, detectedAt, event.TxHash)

	// =========================================================================
	// TRADE DIRECTION DECODING - Understanding BUY vs SELL
	// =========================================================================
	//
	// In Polymarket's CTF Exchange OrderFilled events:
	//   - MakerAssetID = the asset the MAKER gave away
	//   - TakerAssetID = the asset the TAKER gave away
	//   - Zero asset (0x000...000) = USDC (collateral)
	//   - Non-zero asset = outcome token (the prediction market position)
	//
	// Logic to determine trade direction:
	//   IF MakerAssetID = 0 (USDC):
	//       → Maker gave USDC to receive tokens → Maker BOUGHT
	//       → Taker gave tokens to receive USDC → Taker SOLD
	//   IF MakerAssetID = token (non-zero):
	//       → Maker gave tokens to receive USDC → Maker SOLD
	//       → Taker gave USDC to receive tokens → Taker BOUGHT
	//
	// The token ID is always the NON-ZERO asset ID (the actual market token)
	// =========================================================================

	var tokenID string
	var side string
	zeroAsset := "0x0000000000000000000000000000000000000000000000000000000000000000"

	if event.MakerAssetID == zeroAsset {
		// Maker gave USDC → Maker BOUGHT, Taker SOLD
		tokenID = event.TakerAssetID
		if role == "MAKER" {
			side = "BUY"
		} else {
			side = "SELL"
		}
	} else {
		// Maker gave tokens → Maker SOLD, Taker BOUGHT
		tokenID = event.MakerAssetID
		if role == "MAKER" {
			side = "SELL"
		} else {
			side = "BUY"
		}
	}

	// Convert hex token ID to decimal string (APIs expect decimal format)
	if len(tokenID) >= 2 && tokenID[:2] == "0x" {
		hexVal := strings.TrimPrefix(tokenID, "0x")
		// Remove leading zeros
		hexVal = strings.TrimLeft(hexVal, "0")
		if hexVal == "" {
			hexVal = "0"
		}
		// Convert to big.Int then to decimal string
		bigInt := new(big.Int)
		bigInt.SetString(hexVal, 16)
		tokenID = bigInt.String()
	}

	// Calculate price and size from blockchain amounts
	// Amounts are in wei (10^6 decimals for both USDC and shares on Polymarket)
	var price, size, usdcSize float64
	decimals := new(big.Float).SetFloat64(1e6)

	if event.MakerAssetID == zeroAsset {
		// Maker gave USDC, Taker gave tokens
		// MakerAmount = USDC, TakerAmount = tokens
		makerAmt := new(big.Float).SetInt(event.MakerAmount)
		takerAmt := new(big.Float).SetInt(event.TakerAmount)

		usdcFloat := new(big.Float).Quo(makerAmt, decimals)
		tokenFloat := new(big.Float).Quo(takerAmt, decimals)

		usdcSize, _ = usdcFloat.Float64()
		size, _ = tokenFloat.Float64()
		if size > 0 {
			price = usdcSize / size
		}
	} else {
		// Maker gave tokens, Taker gave USDC
		// MakerAmount = tokens, TakerAmount = USDC
		makerAmt := new(big.Float).SetInt(event.MakerAmount)
		takerAmt := new(big.Float).SetInt(event.TakerAmount)

		tokenFloat := new(big.Float).Quo(makerAmt, decimals)
		usdcFloat := new(big.Float).Quo(takerAmt, decimals)

		size, _ = tokenFloat.Float64()
		usdcSize, _ = usdcFloat.Float64()
		if size > 0 {
			price = usdcSize / size
		}
	}

	log.Printf("[RealtimeDetector] Trade decoded: role=%s side=%s tokenID=%s price=%.4f size=%.4f usdc=$%.2f",
		role, side, tokenID, price, size, usdcSize)

	// Look up market title and outcome from our token cache
	var title, outcome string
	tokenInfo, err := d.store.GetTokenInfo(context.Background(), tokenID)
	if err == nil && tokenInfo != nil {
		title = tokenInfo.Title
		outcome = tokenInfo.Outcome
		log.Printf("[RealtimeDetector] Token info from cache: title=%s outcome=%s", title, outcome)
	} else {
		// Not in cache - fetch SYNCHRONOUSLY from Gamma API (short timeout)
		// This ensures title/outcome are populated for copy_trade_log
		log.Printf("[RealtimeDetector] Token info not in cache, fetching sync from Gamma API")
		title, outcome = d.fetchTokenInfoSync(tokenID)
	}

	// Convert to TradeDetail with all the data from blockchain
	// Use TxHash:LogIndex as ID to ensure uniqueness for multiple fills in same tx
	tradeID := event.TxHash
	if event.LogIndex != "" {
		tradeID = event.TxHash + ":" + event.LogIndex
	}
	detail := models.TradeDetail{
		ID:              tradeID,
		UserID:          userAddr,
		MarketID:        tokenID, // Token ID from blockchain
		Type:            "TRADE",
		Side:            side,
		Role:            role,
		Size:            size,
		UsdcSize:        usdcSize,
		Price:           price,
		Title:           title,
		Outcome:         outcome,
		TransactionHash: event.TxHash,
		Timestamp:       event.Timestamp,
		DetectedAt:      detectedAt,
		DetectionSource: "polygon_ws", // Detected via Polygon blockchain WebSocket
	}

	// Save trade to user_trades table asynchronously (don't block critical path)
	go func() {
		bgCtx := context.Background()
		if err := d.store.SaveTrades(bgCtx, []models.TradeDetail{detail}, false); err != nil {
			log.Printf("[RealtimeDetector] Warning: async save trade failed: %v", err)
		}
	}()

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

// refreshFollowedUsers reloads the list of users to monitor and their settings
func (d *RealtimeDetector) refreshFollowedUsers(ctx context.Context) error {
	// Get all followed user addresses
	allUsers, err := d.store.GetFollowedUserAddresses(ctx)
	if err != nil {
		return err
	}

	// Get Strategy 3 (BTC 15m) users separately (for settings lookup)
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

	// Update BTC 15m users (for settings lookup)
	d.btc15mUsersMu.Lock()
	d.btc15mUsers = btc15mSet
	d.btc15mUsersMu.Unlock()

	// Build followed users list - ALL users now use Polygon WS
	d.followedUsersMu.Lock()
	d.followedUsers = make(map[string]bool, len(allUsers))
	for _, user := range allUsers {
		normalized := utils.NormalizeAddress(user)
		d.followedUsers[normalized] = true
	}
	d.followedUsersMu.Unlock()

	// Update Polygon WebSocket with followed addresses
	// Include our own address to track blockchain confirmations
	allAddrs := make([]string, 0, len(allUsers)+1)
	allAddrs = append(allAddrs, allUsers...)
	if d.myAddress != "" {
		allAddrs = append(allAddrs, d.myAddress)
	}
	d.polygonWS.SetFollowedAddresses(allAddrs)

	// Also sync to Mempool WebSocket
	d.syncFollowedAddressesToMempool()

	log.Printf("[RealtimeDetector] Refreshed: %d followed users, %d settings cached",
		len(d.followedUsers), len(newSettingsCache))

	return nil
}

// syncFollowedAddressesToMempool updates the mempool client with current followed addresses
func (d *RealtimeDetector) syncFollowedAddressesToMempool() {
	if d.mempoolWS == nil {
		return
	}

	d.followedUsersMu.RLock()
	addrs := make([]string, 0, len(d.followedUsers)+1)
	for addr := range d.followedUsers {
		addrs = append(addrs, addr)
	}
	d.followedUsersMu.RUnlock()

	// Also add our own address to detect when our copy trades hit mempool
	if d.myAddress != "" {
		addrs = append(addrs, d.myAddress)
	}

	d.mempoolWS.SetFollowedAddresses(addrs)
}

// GetCachedUserSettings returns cached user settings (nil if not found)
func (d *RealtimeDetector) GetCachedUserSettings(userAddr string) *storage.UserCopySettings {
	d.userSettingsCacheMu.RLock()
	defer d.userSettingsCacheMu.RUnlock()
	return d.userSettingsCache[utils.NormalizeAddress(userAddr)]
}

// SetCachedUserSettings adds/updates user settings in the cache
func (d *RealtimeDetector) SetCachedUserSettings(userAddr string, settings *storage.UserCopySettings) {
	d.userSettingsCacheMu.Lock()
	defer d.userSettingsCacheMu.Unlock()
	d.userSettingsCache[utils.NormalizeAddress(userAddr)] = settings
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

// getBlockTimestamp fetches the timestamp of a block from Polygon RPC
func getBlockTimestamp(blockNum uint64) (time.Time, error) {
	blockHex := fmt.Sprintf("0x%x", blockNum)
	payload := fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["%s",false],"id":1}`, blockHex)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post("https://polygon-bor-rpc.publicnode.com", "application/json", strings.NewReader(payload))
	if err != nil {
		return time.Time{}, err
	}
	defer resp.Body.Close()

	var result struct {
		Result struct {
			Timestamp string `json:"timestamp"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return time.Time{}, err
	}

	ts, _ := strconv.ParseInt(strings.TrimPrefix(result.Result.Timestamp, "0x"), 16, 64)
	return time.Unix(ts, 0), nil
}

// logBlockchainLatency fetches block timestamp and logs actual detection latency
func logBlockchainLatency(blockNum uint64, detectedAt time.Time, txHash string) {
	blockTime, err := getBlockTimestamp(blockNum)
	if err != nil {
		log.Printf("[RealtimeDetector] ⏱️ LATENCY: block=%d (failed to fetch timestamp: %v)", blockNum, err)
		return
	}
	latency := detectedAt.Sub(blockTime)
	log.Printf("[RealtimeDetector] ⏱️ LATENCY: %dms (block=%d @ %s, detected=%s) tx=%s",
		latency.Milliseconds(), blockNum, blockTime.Format("15:04:05"), detectedAt.Format("15:04:05.000"), txHash[:16])
}

// fetchTokenInfoSync fetches token info synchronously with a short timeout.
// Returns title and outcome, or empty strings if fetch fails.
// This ensures copy_trade_log entries always have market title/outcome populated.
func (d *RealtimeDetector) fetchTokenInfoSync(tokenID string) (title, outcome string) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Fetch from Gamma API /markets endpoint (the /tokens endpoint doesn't exist)
	url := fmt.Sprintf("https://gamma-api.polymarket.com/markets?clob_token_ids=%s", tokenID)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("[RealtimeDetector] fetchTokenInfoSync: failed to create request: %v", err)
		return "", ""
	}

	client := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[RealtimeDetector] fetchTokenInfoSync: API request failed: %v", err)
		return "", ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("[RealtimeDetector] fetchTokenInfoSync: Gamma API returned %d", resp.StatusCode)
		return "", ""
	}

	var markets []struct {
		Question     string `json:"question"`
		ConditionID  string `json:"conditionId"`
		Slug         string `json:"slug"`
		Outcomes     string `json:"outcomes"`     // JSON string: ["Up", "Down"]
		ClobTokenIds string `json:"clobTokenIds"` // JSON string: ["token1", "token2"]
	}
	if err := json.NewDecoder(resp.Body).Decode(&markets); err != nil {
		log.Printf("[RealtimeDetector] fetchTokenInfoSync: failed to decode: %v", err)
		return "", ""
	}

	if len(markets) == 0 {
		log.Printf("[RealtimeDetector] fetchTokenInfoSync: no market found for token: %s", tokenID[:20])
		return "", ""
	}

	market := markets[0]

	// Parse the JSON string arrays to determine outcome for this token
	var outcomes []string
	var tokenIds []string
	json.Unmarshal([]byte(market.Outcomes), &outcomes)
	json.Unmarshal([]byte(market.ClobTokenIds), &tokenIds)

	// Find which outcome corresponds to our token ID
	for i, tid := range tokenIds {
		if tid == tokenID && i < len(outcomes) {
			outcome = outcomes[i]
			break
		}
	}

	title = market.Question

	// Cache it for future trades (async - don't block)
	go func() {
		bgCtx := context.Background()
		if err := d.store.SaveTokenInfo(bgCtx, tokenID, market.ConditionID, outcome, title, market.Slug); err != nil {
			log.Printf("[RealtimeDetector] fetchTokenInfoSync: failed to cache: %v", err)
		}
	}()

	log.Printf("[RealtimeDetector] fetchTokenInfoSync: got %s - %s", title, outcome)
	return title, outcome
}

// handleOurBlockchainTrade updates copy_trade_log with blockchain confirmation timestamp
func (d *RealtimeDetector) handleOurBlockchainTrade(event api.PolygonTradeEvent, role string, detectedAt time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Determine token ID and side based on role
	var tokenID string
	var side string
	zeroAsset := "0x0000000000000000000000000000000000000000000000000000000000000000"

	if event.MakerAssetID == zeroAsset {
		// Maker gave USDC → Maker BOUGHT, Taker SOLD
		tokenID = event.TakerAssetID
		if role == "MAKER" {
			side = "BUY"
		} else {
			side = "SELL"
		}
	} else {
		// Maker gave tokens → Maker SOLD, Taker BOUGHT
		tokenID = event.MakerAssetID
		if role == "MAKER" {
			side = "SELL"
		} else {
			side = "BUY"
		}
	}

	// Convert hex token ID to decimal string
	if len(tokenID) >= 2 && tokenID[:2] == "0x" {
		hexVal := strings.TrimPrefix(tokenID, "0x")
		hexVal = strings.TrimLeft(hexVal, "0")
		if hexVal == "" {
			hexVal = "0"
		}
		bigInt := new(big.Int)
		bigInt.SetString(hexVal, 16)
		tokenID = bigInt.String()
	}

	// Get actual block timestamp for more accurate timing
	blockTime, err := getBlockTimestamp(event.BlockNumber)
	if err != nil {
		// Fall back to detection time if we can't get block time
		blockTime = detectedAt
		log.Printf("[RealtimeDetector] Using detection time as blockchain time (failed to get block: %v)", err)
	}

	// Update the copy_trade_log with blockchain confirmation time
	if err := d.store.UpdateBlockchainConfirmedAt(ctx, tokenID, side, blockTime, event.TxHash); err != nil {
		log.Printf("[RealtimeDetector] Failed to update blockchain_confirmed_at: %v", err)
		return
	}

	// Calculate and log the settlement latency
	log.Printf("[RealtimeDetector] ⏱️ OUR TRADE: side=%s token=%s block_time=%s tx=%s",
		side, tokenID[:20], blockTime.Format("15:04:05"), event.TxHash[:16])
}
