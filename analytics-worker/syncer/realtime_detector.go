// Package syncer provides real-time trade detection for copy trading.
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

// RealtimeDetector uses Polygon blockchain WebSocket for trade detection.
// This is the most reliable method - detects OrderFilled events ~1-2s after block confirmation.
type RealtimeDetector struct {
	apiClient *api.Client
	store     *storage.PostgresStore
	polygonWS *api.PolygonWSClient // Blockchain WebSocket for trade detection

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

	// Create Polygon WebSocket client - this is the ONLY detection method
	d.polygonWS = api.NewPolygonWSClient(d.handleBlockchainTrade)
	log.Printf("[RealtimeDetector] Polygon WebSocket detection ENABLED (primary ~1-2s after block)")

	return d
}

// Start begins real-time trade detection via Polygon WebSocket
func (d *RealtimeDetector) Start(ctx context.Context) error {
	if d.running {
		return fmt.Errorf("detector already running")
	}

	// Load followed users
	if err := d.refreshFollowedUsers(ctx); err != nil {
		log.Printf("[RealtimeDetector] Warning: failed to load followed users: %v", err)
	}

	// Start Polygon WebSocket for trade detection
	if err := d.polygonWS.Start(ctx); err != nil {
		return fmt.Errorf("failed to start Polygon WebSocket: %w", err)
	}
	log.Printf("[RealtimeDetector] ✓ Polygon blockchain WebSocket started (primary detection)")

	d.running = true

	// Start user refresh loop (every 30s)
	d.wg.Add(1)
	go d.userRefreshLoop(ctx)

	log.Printf("[RealtimeDetector] Started with %d followed users (Polygon WS only)", len(d.followedUsers))
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

	d.wg.Wait()
	log.Printf("[RealtimeDetector] Stopped")
}

// GetMetrics returns current performance metrics
func (d *RealtimeDetector) GetMetrics() DetectorMetrics {
	d.metricsMu.RLock()
	defer d.metricsMu.RUnlock()
	return *d.metrics
}

// handleBlockchainTrade is called when Polygon WebSocket detects a followed user's trade
// This is the PRIMARY detection method - ~1-2 seconds after block confirmation
func (d *RealtimeDetector) handleBlockchainTrade(event api.PolygonTradeEvent) {
	detectedAt := time.Now()

	// Update metrics
	d.metricsMu.Lock()
	d.metrics.BlockchainEvents++
	d.metricsMu.Unlock()

	// Check if we already processed this specific log event
	// Use TxHash+LogIndex as key because one tx can have MULTIPLE OrderFilled events
	// (e.g., market order filling against multiple limit orders)
	eventKey := event.TxHash + ":" + event.LogIndex
	d.processedTxsMu.Lock()
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

	// Determine token ID and side based on role
	// In CTF Exchange OrderFilled:
	// - MakerAssetID = what the maker gave
	// - TakerAssetID = what the taker gave
	//
	// Trade direction logic:
	// - MakerAssetID = token (non-zero): Maker SOLD tokens, Taker BOUGHT tokens
	// - MakerAssetID = 0 (USDC): Maker BOUGHT tokens, Taker SOLD tokens

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

	log.Printf("[RealtimeDetector] Refreshed: %d followed users, %d settings cached",
		len(d.followedUsers), len(newSettingsCache))

	return nil
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
