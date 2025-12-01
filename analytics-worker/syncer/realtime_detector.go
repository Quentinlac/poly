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

// RealtimeDetector combines multiple data sources for fastest trade detection:
// 1. CLOB API polling (PRIMARY: ~50-150ms latency - trades appear when matched off-chain)
// 2. Polygon blockchain WebSocket (BACKUP: ~10-20s from trade execution)
// 3. Polygon Mempool monitoring for Strategy 3 users (~2-3s latency from user click)
// 4. LiveData WebSocket for BTC 15m markets (FALLBACK for Strategy 3: ~6s latency)
type RealtimeDetector struct {
	apiClient   *api.Client
	clobClient  *api.ClobClient       // For fast CLOB API trade detection (~50ms latency)
	store       *storage.PostgresStore
	wsClient    *api.WSClient
	polygonWS   *api.PolygonWSClient  // Blockchain WebSocket for backup detection
	mempoolWS   *api.MempoolWSClient  // Mempool monitoring for Strategy 3 (~2-3s from click)
	liveDataWS  *api.LiveDataWSClient // LiveData WebSocket for BTC 15m (Strategy 3 fallback)

	// Our own address (to track blockchain confirmations of our trades)
	myAddress string

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

	// Mempool pre-detections (txHash -> detected time) for measuring time saved vs LiveData
	mempoolPreDetections   map[string]time.Time
	mempoolPreDetectionsMu sync.RWMutex

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
	WebSocketEvents     int64
	BlockchainEvents    int64 // Events from Polygon WebSocket
	BlockchainMatches   int64 // Matched followed users on blockchain
	MempoolEvents       int64 // Pending transactions seen in mempool
	MempoolMatches      int64 // Matched followed users in mempool (Strategy 3)
	LiveDataWSEvents    int64 // Events from LiveData WebSocket (BTC 15m Strategy 3 fallback)
	LiveDataWSMatches   int64 // Matched followed users on LiveData WS
	LastDetectionTime   time.Time
}

// NewRealtimeDetector creates a new real-time trade detector
// clobClient: required for fast CLOB API detection (~50ms latency)
// enableBlockchainWS: if true, uses Polygon WebSocket for backup ~1s detection (heavy, use in worker only)
// myAddressOverride: if provided, use this address for blockchain confirmation tracking (overrides env var)
func NewRealtimeDetector(apiClient *api.Client, clobClient *api.ClobClient, store *storage.PostgresStore, onNewTrade func(trade models.TradeDetail), enableBlockchainWS bool, myAddressOverride string) *RealtimeDetector {
	// Get our own address for tracking blockchain confirmations
	// Priority: 1) myAddressOverride parameter, 2) POLYMARKET_FUNDER_ADDRESS env var
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
		apiClient:            apiClient,
		clobClient:           clobClient,
		store:                store,
		myAddress:            myAddress,
		followedUsers:        make(map[string]bool),
		btc15mUsers:          make(map[string]bool),
		userSettingsCache:    make(map[string]*storage.UserCopySettings),
		lastCheck:            make(map[string]time.Time),
		processedTxs:         make(map[string]bool),
		mempoolPreDetections: make(map[string]time.Time),
		onNewTrade:           onNewTrade,
		metrics:              &DetectorMetrics{},
		stopCh:               make(chan struct{}),
		useCLOBDetection:     clobClient != nil, // Enable CLOB detection if client is provided
	}

	// Create Polygon WebSocket client for backup blockchain monitoring
	// Only enable in worker to avoid heavy processing in main API app
	if enableBlockchainWS {
		d.polygonWS = api.NewPolygonWSClient(d.handleBlockchainTrade)
		log.Printf("[RealtimeDetector] Blockchain WebSocket ENABLED (backup ~10-20s detection)")
	}

	// Create Mempool WebSocket client for Strategy 3 (fastest detection ~2-3s from user click)
	d.mempoolWS = api.NewMempoolWSClient(d.handleMempoolTrade)
	log.Printf("[RealtimeDetector] Mempool WebSocket ENABLED (Strategy 3: ~2-3s from user click)")

	// Create LiveData WebSocket client for BTC 15m markets (Strategy 3 fallback ~6s)
	d.liveDataWS = api.NewLiveDataWSClient(d.handleLiveDataTrade)
	log.Printf("[RealtimeDetector] LiveData WebSocket ENABLED (Strategy 3 fallback: ~6s latency)")

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

	// Load followed users
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

	// Start Mempool WebSocket for Strategy 3 users (PRIMARY - fastest ~2-3s from click)
	if d.mempoolWS != nil {
		// Set the addresses to monitor
		btc15mAddrs := make([]string, 0, len(d.btc15mUsers))
		d.btc15mUsersMu.RLock()
		for addr := range d.btc15mUsers {
			btc15mAddrs = append(btc15mAddrs, addr)
		}
		d.btc15mUsersMu.RUnlock()

		if len(btc15mAddrs) > 0 {
			d.mempoolWS.SetFollowedAddresses(btc15mAddrs)
			if err := d.mempoolWS.Start(ctx); err != nil {
				log.Printf("[RealtimeDetector] Warning: Mempool WebSocket failed to start: %v", err)
			} else {
				log.Printf("[RealtimeDetector] ✓ Mempool WebSocket started (Strategy 3: %d users, ~2-3s detection)", len(btc15mAddrs))
			}
		} else {
			log.Printf("[RealtimeDetector] Mempool WebSocket NOT started (no Strategy 3 users)")
		}
	}

	// Start LiveData WebSocket for Strategy 3 users (FALLBACK - ~6s from click)
	// Subscribe to ALL trades and filter locally by proxyWallet
	if d.liveDataWS != nil {
		if err := d.liveDataWS.Start(ctx); err != nil {
			log.Printf("[RealtimeDetector] Warning: LiveData WebSocket failed to start: %v", err)
		} else {
			if err := d.liveDataWS.SubscribeToAllTrades(); err != nil {
				log.Printf("[RealtimeDetector] Warning: LiveData WebSocket subscription failed: %v", err)
			} else {
				log.Printf("[RealtimeDetector] ✓ LiveData WebSocket started (Strategy 3 fallback)")
			}
		}
	}

	// Start Polymarket WebSocket client for market events (supplementary)
	// Only useful when blockchain WS is NOT enabled (it only logs market activity, not user trades)
	if d.polygonWS == nil {
		d.wsClient = api.NewWSClient(d.handleWSTradeEvent)
		if err := d.wsClient.Start(ctx); err != nil {
			log.Printf("[RealtimeDetector] Warning: Polymarket WebSocket failed to start: %v", err)
		}
	}

	d.running = true

	// Start fast polling loop when CLOB detection is enabled
	// CLOB API gives ~50ms detection, faster than blockchain WS (~1s)
	if d.useCLOBDetection {
		d.wg.Add(1)
		go d.fastPollLoop(ctx)
		log.Printf("[RealtimeDetector] CLOB API polling enabled (100ms interval, ~50ms detection)")
	} else if d.polygonWS == nil {
		// Fallback to Data API polling if no CLOB and no blockchain WS
		d.wg.Add(1)
		go d.fastPollLoop(ctx)
		log.Printf("[RealtimeDetector] Data API polling enabled (fallback mode, ~30-80s detection)")
	} else {
		log.Printf("[RealtimeDetector] API polling DISABLED (using blockchain WebSocket only)")
	}

	// Start user refresh loop (every 30s)
	d.wg.Add(1)
	go d.userRefreshLoop(ctx)

	log.Printf("[RealtimeDetector] Started with %d polled users, %d Strategy 3 users", len(d.followedUsers), len(d.btc15mUsers))
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

	if d.mempoolWS != nil {
		d.mempoolWS.Stop()
	}

	if d.liveDataWS != nil {
		d.liveDataWS.Stop()
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

// handleMempoolTrade is called when a pending transaction is detected in mempool
// This is the FASTEST detection method for Strategy 3 users (~2-3s from user click)
// If trade details are decoded, execute copy trade immediately
// Otherwise, record pre-detection and wait for LiveData to provide details
func (d *RealtimeDetector) handleMempoolTrade(event api.MempoolTradeEvent) {
	detectedAt := time.Now()

	// Update metrics
	d.metricsMu.Lock()
	d.metrics.MempoolEvents++
	d.metrics.MempoolMatches++
	d.metricsMu.Unlock()

	// Normalize the tx hash for consistent lookup
	txHash := strings.ToLower(event.TxHash)

	// Check if we already processed this trade
	d.processedTxsMu.Lock()
	if d.processedTxs[txHash] {
		d.processedTxsMu.Unlock()
		log.Printf("[MempoolWS] Trade %s already processed, skipping", txHash[:16])
		return
	}
	// Mark as processed immediately to prevent duplicate execution
	d.processedTxs[txHash] = true
	d.processedTxsMu.Unlock()

	// If we have decoded trade details, execute immediately!
	if event.Decoded && event.TokenID != "" && event.Side != "" {
		log.Printf("[MempoolWS] 🚀 EXECUTING FROM MEMPOOL! from=%s side=%s size=%.4f price=%.4f tx=%s",
			utils.ShortAddress(event.From), event.Side, event.Size, event.Price, txHash[:16])

		// Create TradeDetail from decoded mempool data
		usdcSize := event.Size * event.Price
		detail := models.TradeDetail{
			ID:              txHash,
			UserID:          utils.NormalizeAddress(event.From),
			MarketID:        event.TokenID, // Token ID from decoded input
			Type:            "TRADE",
			Side:            event.Side,
			Size:            event.Size,
			UsdcSize:        usdcSize,
			Price:           event.Price,
			TransactionHash: txHash,
			Timestamp:       detectedAt, // Use detection time as trade time
			DetectedAt:      detectedAt,
			DetectionSource: "mempool", // Detected via mempool (~3s faster than LiveData)
		}

		// Update metrics
		d.metricsMu.Lock()
		d.metrics.TradesDetected++
		d.metrics.LastDetectionTime = detectedAt
		d.metricsMu.Unlock()

		// Save trade asynchronously
		go func() {
			bgCtx := context.Background()
			if err := d.store.SaveTrades(bgCtx, []models.TradeDetail{detail}, false); err != nil {
				log.Printf("[MempoolWS] Warning: failed to save mempool trade: %v", err)
			}
		}()

		// Execute copy trade immediately!
		if d.onNewTrade != nil {
			d.onNewTrade(detail)
		}
		return
	}

	// Fallback: Record pre-detection for when LiveData arrives
	d.mempoolPreDetectionsMu.Lock()
	d.mempoolPreDetections[txHash] = detectedAt
	// Cleanup old entries (keep last 100)
	if len(d.mempoolPreDetections) > 100 {
		cutoff := time.Now().Add(-5 * time.Minute)
		for k, t := range d.mempoolPreDetections {
			if t.Before(cutoff) {
				delete(d.mempoolPreDetections, k)
			}
		}
	}
	d.mempoolPreDetectionsMu.Unlock()

	// Unmark as processed so LiveData can handle it
	d.processedTxsMu.Lock()
	delete(d.processedTxs, txHash)
	d.processedTxsMu.Unlock()

	log.Printf("[MempoolWS] ⏳ PENDING (not decoded): from=%s contract=%s tx=%s - waiting for LiveData",
		utils.ShortAddress(event.From), event.ContractName, txHash[:16])
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

// handleLiveDataTrade is called when LiveData WebSocket detects a trade
// This is the PRIMARY detection method for Strategy 3 users (~650ms latency)
func (d *RealtimeDetector) handleLiveDataTrade(event api.LiveDataTradeEvent) {
	detectedAt := time.Now()

	// Update metrics for all events
	d.metricsMu.Lock()
	d.metrics.LiveDataWSEvents++
	eventCount := d.metrics.LiveDataWSEvents
	d.metricsMu.Unlock()

	// Log every 10th trade to show WebSocket is receiving data
	if eventCount%10 == 1 {
		log.Printf("[LiveDataWS] Received trade #%d: %s %s %.0f @ %.2f on %s",
			eventCount, event.Name, event.Side, event.Size, event.Price, event.EventSlug)
	}

	// Check if the trader (proxyWallet) is a followed BTC 15m user
	normalizedWallet := utils.NormalizeAddress(event.ProxyWallet)
	d.btc15mUsersMu.RLock()
	isFollowed := d.btc15mUsers[normalizedWallet]
	numFollowed := len(d.btc15mUsers)
	d.btc15mUsersMu.RUnlock()

	// Debug: Log when checking a trade from a potential match
	if len(event.ProxyWallet) > 10 {
		shortWallet := event.ProxyWallet[:10]
		// Check if this wallet starts with our followed user's prefix
		for addr := range d.btc15mUsers {
			if len(addr) > 10 && addr[:10] == normalizedWallet[:10] {
				log.Printf("[LiveDataWS] DEBUG: Potential match! wallet=%s followed=%v (tracking %d users)", shortWallet, isFollowed, numFollowed)
				break
			}
		}
	}

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

	// Check if we had a mempool pre-detection for this trade
	txHashLower := strings.ToLower(event.TransactionHash)
	d.mempoolPreDetectionsMu.RLock()
	mempoolTime, wasPreDetected := d.mempoolPreDetections[txHashLower]
	d.mempoolPreDetectionsMu.RUnlock()

	detectionSource := "live_ws"
	if wasPreDetected {
		timeSaved := detectedAt.Sub(mempoolTime)
		log.Printf("[RealtimeDetector] ⚡ MEMPOOL PRE-DETECTION! tx=%s detected %s earlier via mempool",
			txHashLower[:16], timeSaved.Round(time.Millisecond))
		detectionSource = "mempool" // Credit to mempool for the detection

		// Clean up the pre-detection entry
		d.mempoolPreDetectionsMu.Lock()
		delete(d.mempoolPreDetections, txHashLower)
		d.mempoolPreDetectionsMu.Unlock()
	}

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
		DetectionSource: detectionSource, // "mempool" if pre-detected, otherwise "live_ws"
	}

	// Save to user_trades for historical record
	bgCtx := context.Background()
	if err := d.store.SaveTrades(bgCtx, []models.TradeDetail{detail}, false); err != nil {
		log.Printf("[RealtimeDetector] Warning: failed to save LiveData trade: %v", err)
	}

	// Mark as processed
	d.store.CacheTokenID(bgCtx, detail.ID, "processed", "true", false)

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

	// Update mempool monitoring addresses for Strategy 3 users
	if d.mempoolWS != nil && len(btc15mUsers) > 0 {
		d.mempoolWS.SetFollowedAddresses(btc15mUsers)
	}

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
	// Also add our own address to track blockchain confirmations
	if d.polygonWS != nil {
		allAddrs := make([]string, 0, len(allUsers)+1)
		allAddrs = append(allAddrs, allUsers...)
		if d.myAddress != "" {
			allAddrs = append(allAddrs, d.myAddress)
		}
		d.polygonWS.SetFollowedAddresses(allAddrs)
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
		} else {
			// Not in cache - fetch SYNCHRONOUSLY from Gamma API (short timeout)
			// This ensures title/outcome are populated for copy_trade_log
			log.Printf("[RealtimeDetector] CLOB: Token info not in cache for %s, fetching from Gamma API", trade.AssetID[:20])
			title, outcome = d.fetchTokenInfoSync(trade.AssetID)
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

// fetchAndCacheTokenInfo fetches token info from Gamma API and caches it
func (d *RealtimeDetector) fetchAndCacheTokenInfo(tokenID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Fetch from Gamma API /markets endpoint (the /tokens endpoint doesn't exist)
	url := fmt.Sprintf("https://gamma-api.polymarket.com/markets?clob_token_ids=%s", tokenID)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("[RealtimeDetector] Failed to create request: %v", err)
		return
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[RealtimeDetector] Failed to fetch from Gamma: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("[RealtimeDetector] Gamma API returned %d", resp.StatusCode)
		return
	}

	var markets []struct {
		Question     string `json:"question"`
		ConditionID  string `json:"conditionId"`
		Slug         string `json:"slug"`
		Outcomes     string `json:"outcomes"`     // JSON string: ["Up", "Down"]
		ClobTokenIds string `json:"clobTokenIds"` // JSON string: ["token1", "token2"]
	}
	if err := json.NewDecoder(resp.Body).Decode(&markets); err != nil {
		log.Printf("[RealtimeDetector] Failed to decode Gamma response: %v", err)
		return
	}

	if len(markets) == 0 {
		log.Printf("[RealtimeDetector] No market found in Gamma for token: %s", tokenID[:20])
		return
	}

	market := markets[0]

	// Parse the JSON string arrays to determine outcome for this token
	var outcomes []string
	var tokenIds []string
	json.Unmarshal([]byte(market.Outcomes), &outcomes)
	json.Unmarshal([]byte(market.ClobTokenIds), &tokenIds)

	// Find which outcome corresponds to our token ID
	var outcome string
	for i, tid := range tokenIds {
		if tid == tokenID && i < len(outcomes) {
			outcome = outcomes[i]
			break
		}
	}

	// Cache it for future trades
	if err := d.store.SaveTokenInfo(ctx, tokenID, market.ConditionID, outcome, market.Question, market.Slug); err != nil {
		log.Printf("[RealtimeDetector] Failed to cache token info: %v", err)
		return
	}

	log.Printf("[RealtimeDetector] Cached token info: %s - %s", market.Question, outcome)
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
