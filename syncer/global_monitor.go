package syncer

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"polymarket-analyzer/api"
	"polymarket-analyzer/models"
	"polymarket-analyzer/storage"
)

// GlobalTradeMonitor polls for ALL platform trades and redemptions.
// This captures every trade happening on Polymarket for later analysis.
type GlobalTradeMonitor struct {
	subgraph *api.SubgraphClient
	theGraph *api.TheGraphClient
	store    storage.DataStore

	lastTradeTimestamp      int64
	lastRedemptionTimestamp int64

	stop chan struct{}
	wg   sync.WaitGroup
}

// NewGlobalTradeMonitor creates a new global trade monitor.
func NewGlobalTradeMonitor(subgraph *api.SubgraphClient, theGraph *api.TheGraphClient, store storage.DataStore) *GlobalTradeMonitor {
	return &GlobalTradeMonitor{
		subgraph: subgraph,
		theGraph: theGraph,
		store:    store,
		stop:     make(chan struct{}),
	}
}

// Start launches the global trade monitoring loop.
func (m *GlobalTradeMonitor) Start() {
	tradeInterval := 2 * time.Second
	redemptionInterval := 5 * time.Minute
	log.Printf("[global-monitor] starting trades with %v interval, redemptions with %v interval", tradeInterval, redemptionInterval)

	// Initialize timestamps to now (only fetch new trades/redemptions from startup)
	m.lastTradeTimestamp = time.Now().Unix()
	m.lastRedemptionTimestamp = time.Now().Unix()

	// Trade monitoring loop (every 2 seconds)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()

		ticker := time.NewTicker(tradeInterval)
		defer ticker.Stop()

		for {
			select {
			case <-m.stop:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				if err := m.tickTrades(ctx); err != nil {
					log.Printf("[global-monitor] trades tick error: %v", err)
				}
				cancel()
			}
		}
	}()

	// Redemption monitoring loop (every 5 minutes) - uses The Graph API
	if m.theGraph != nil {
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()

			// Fetch redemptions immediately on startup
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			if err := m.tickRedemptions(ctx); err != nil {
				log.Printf("[global-monitor] initial redemptions fetch error: %v", err)
			}
			cancel()

			ticker := time.NewTicker(redemptionInterval)
			defer ticker.Stop()

			for {
				select {
				case <-m.stop:
					return
				case <-ticker.C:
					ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
					if err := m.tickRedemptions(ctx); err != nil {
						log.Printf("[global-monitor] redemptions tick error: %v", err)
					}
					cancel()
				}
			}
		}()
	} else {
		log.Printf("[global-monitor] The Graph client not configured - redemptions disabled")
	}
}

// Stop gracefully shuts down the monitor.
func (m *GlobalTradeMonitor) Stop() {
	close(m.stop)
	m.wg.Wait()
}

// tickTrades performs one polling cycle for global trades only.
func (m *GlobalTradeMonitor) tickTrades(ctx context.Context) error {
	trades, err := m.subgraph.GetGlobalTradesSince(ctx, m.lastTradeTimestamp)
	if err != nil {
		return fmt.Errorf("fetch global trades: %w", err)
	}

	if len(trades) == 0 {
		return nil
	}

	tradeDetails := m.convertTradesToDetails(trades)

	// Enrich trades with token metadata (title, outcome, slug)
	tradeDetails = m.enrichTradesWithTokenInfo(ctx, tradeDetails)

	if err := m.store.SaveGlobalTrades(ctx, tradeDetails); err != nil {
		log.Printf("[global-monitor] warning: failed to save trades: %v", err)
	} else {
		log.Printf("[global-monitor] saved %d trades", len(tradeDetails))
	}

	// Update timestamp to latest trade
	for _, t := range trades {
		ts, _ := strconv.ParseInt(t.Timestamp, 10, 64)
		if ts > m.lastTradeTimestamp {
			m.lastTradeTimestamp = ts
		}
	}

	return nil
}

// tickRedemptions performs one polling cycle for global redemptions using The Graph.
func (m *GlobalTradeMonitor) tickRedemptions(ctx context.Context) error {
	if m.theGraph == nil {
		return nil
	}

	redemptions, err := m.theGraph.GetGlobalRedemptionsSince(ctx, m.lastRedemptionTimestamp)
	if err != nil {
		return fmt.Errorf("fetch global redemptions: %w", err)
	}

	if len(redemptions) == 0 {
		return nil
	}

	redemptionDetails := m.convertRedemptionsToDetails(redemptions)

	// Enrich redemptions with market metadata (title, slug) from conditionId
	redemptionDetails = m.enrichRedemptionsWithMarketInfo(ctx, redemptionDetails)

	if err := m.store.SaveGlobalTrades(ctx, redemptionDetails); err != nil {
		log.Printf("[global-monitor] warning: failed to save redemptions: %v", err)
	} else {
		log.Printf("[global-monitor] saved %d redemptions from The Graph", len(redemptionDetails))
	}

	// Update timestamp to latest redemption
	for _, r := range redemptions {
		ts, _ := strconv.ParseInt(r.Timestamp, 10, 64)
		if ts > m.lastRedemptionTimestamp {
			m.lastRedemptionTimestamp = ts
		}
	}

	return nil
}

// enrichRedemptionsWithMarketInfo looks up market info from conditionId and enriches redemption details.
func (m *GlobalTradeMonitor) enrichRedemptionsWithMarketInfo(ctx context.Context, redemptions []models.TradeDetail) []models.TradeDetail {
	if len(redemptions) == 0 {
		return redemptions
	}

	// Collect unique condition IDs (stored in MarketID field)
	conditionIDs := make(map[string]bool)
	for _, r := range redemptions {
		if r.MarketID != "" {
			conditionIDs[r.MarketID] = true
		}
	}

	// Batch lookup all conditions
	conditionInfoMap := make(map[string]*storage.TokenInfo)
	for conditionID := range conditionIDs {
		info, err := m.store.GetTokenByCondition(ctx, conditionID)
		if err == nil && info != nil {
			conditionInfoMap[conditionID] = info
		}
	}

	// Enrich redemptions with market metadata
	for i := range redemptions {
		if info, ok := conditionInfoMap[redemptions[i].MarketID]; ok {
			redemptions[i].Title = info.Title
			redemptions[i].Slug = info.Slug
			// For redemptions, don't set specific outcome since user redeems both
			redemptions[i].Outcome = "Redeemed"
		}
	}

	log.Printf("[global-monitor] enriched %d/%d redemptions with market info", len(conditionInfoMap), len(conditionIDs))
	return redemptions
}

// convertTradesToDetails converts subgraph events to TradeDetail format.
// Each trade event creates TWO trade details: one for maker, one for taker.
// Uses proxy wallet addresses (maker/taker) which are the user identifiers in Polymarket.
func (m *GlobalTradeMonitor) convertTradesToDetails(events []api.OrderFilledEvent) []models.TradeDetail {
	var details []models.TradeDetail

	for _, e := range events {
		timestamp, _ := strconv.ParseInt(e.Timestamp, 10, 64)

		// Determine side based on asset IDs
		makerSide := "SELL"
		assetID := e.MakerAssetID
		if e.MakerAssetID == "0" {
			makerSide = "BUY"
			assetID = e.TakerAssetID
		}

		// Calculate price and size
		makerAmt, _ := strconv.ParseFloat(e.MakerAmountFilled, 64)
		takerAmt, _ := strconv.ParseFloat(e.TakerAmountFilled, 64)

		var price, size float64
		if makerSide == "BUY" {
			if takerAmt > 0 {
				price = makerAmt / takerAmt
				size = takerAmt / 1e6
			}
		} else {
			if makerAmt > 0 {
				price = takerAmt / makerAmt
				size = makerAmt / 1e6
			}
		}

		// Calculate USDC value
		usdcSize := size * price

		// Create maker trade (using maker's proxy wallet address)
		makerTrade := models.TradeDetail{
			ID:              e.ID + "-maker",
			UserID:          e.Maker,
			TransactionHash: e.TransactionHash,
			MarketID:        assetID,
			Side:            makerSide,
			Type:            "TRADE",
			Role:            "MAKER",
			Size:            size,
			UsdcSize:        usdcSize,
			Price:           price,
			Timestamp:       time.Unix(timestamp, 0),
		}
		details = append(details, makerTrade)

		// Create taker trade (opposite side, using taker's proxy wallet address)
		takerSide := "BUY"
		if makerSide == "BUY" {
			takerSide = "SELL"
		}

		takerTrade := models.TradeDetail{
			ID:              e.ID + "-taker",
			UserID:          e.Taker,
			TransactionHash: e.TransactionHash,
			MarketID:        assetID,
			Side:            takerSide,
			Type:            "TRADE",
			Role:            "TAKER",
			Size:            size,
			UsdcSize:        usdcSize,
			Price:           price,
			Timestamp:       time.Unix(timestamp, 0),
		}
		details = append(details, takerTrade)
	}

	return details
}

// convertRedemptionsToDetails converts redemption events to TradeDetail format.
func (m *GlobalTradeMonitor) convertRedemptionsToDetails(events []api.RedemptionEvent) []models.TradeDetail {
	var details []models.TradeDetail

	for _, e := range events {
		timestamp, _ := strconv.ParseInt(e.Timestamp, 10, 64)
		payout, _ := strconv.ParseFloat(e.Payout, 64)

		// Extract tx_hash from the id (format: txHash_logIndex)
		txHash := e.ID
		if idx := strings.Index(e.ID, "_"); idx > 0 {
			txHash = e.ID[:idx]
		}

		// Store conditionId in MarketID field for later lookup
		detail := models.TradeDetail{
			ID:              e.ID,
			UserID:          e.Redeemer,
			TransactionHash: txHash,
			MarketID:        e.Condition, // Store conditionId for market lookup
			Type:            "REDEEM",
			Role:            "REDEEM",
			UsdcSize:        payout / 1e6, // Convert from base units
			Size:            payout / 1e6,
			Price:           1.0, // Redemptions are always at $1
			Side:            "SELL",
			Timestamp:       time.Unix(timestamp, 0),
		}
		details = append(details, detail)
	}

	return details
}

// enrichTradesWithTokenInfo looks up token metadata from the database and enriches trade details.
// For missing tokens, it fetches them from the Gamma API and caches them.
func (m *GlobalTradeMonitor) enrichTradesWithTokenInfo(ctx context.Context, trades []models.TradeDetail) []models.TradeDetail {
	if len(trades) == 0 {
		return trades
	}

	// Collect unique token IDs
	tokenIDs := make(map[string]bool)
	for _, t := range trades {
		if t.MarketID != "" {
			tokenIDs[t.MarketID] = true
		}
	}

	// Batch lookup all tokens from database
	tokenInfoMap := make(map[string]*storage.TokenInfo)
	missingTokens := []string{}

	for tokenID := range tokenIDs {
		info, err := m.store.GetTokenInfo(ctx, tokenID)
		if err == nil && info != nil {
			tokenInfoMap[tokenID] = info
		} else {
			missingTokens = append(missingTokens, tokenID)
		}
	}

	// Fetch missing tokens from Gamma API
	if len(missingTokens) > 0 {
		log.Printf("[global-monitor] Fetching %d missing tokens from Gamma API", len(missingTokens))

		apiTokenMap, err := m.subgraph.BuildTokenMap(ctx, missingTokens)
		if err != nil {
			log.Printf("[global-monitor] Warning: failed to fetch tokens from API: %v", err)
		} else {
			// Convert api.TokenInfo to storage.TokenInfo and save to cache
			toCache := make(map[string]storage.TokenInfo)
			for tokenID, apiInfo := range apiTokenMap {
				storageInfo := storage.TokenInfo{
					TokenID:     apiInfo.TokenID,
					ConditionID: apiInfo.ConditionID,
					Outcome:     apiInfo.Outcome,
					Title:       apiInfo.Title,
					Slug:        apiInfo.Slug,
				}
				toCache[tokenID] = storageInfo
				tokenInfoMap[tokenID] = &storageInfo
			}

			// Save to cache for future use
			if len(toCache) > 0 {
				if err := m.store.SaveTokenCache(ctx, toCache); err != nil {
					log.Printf("[global-monitor] Warning: failed to save token cache: %v", err)
				} else {
					log.Printf("[global-monitor] Cached %d new tokens", len(toCache))
				}
			}
		}
	}

	// Enrich trades with token metadata
	enrichedCount := 0
	for i := range trades {
		if info, ok := tokenInfoMap[trades[i].MarketID]; ok {
			trades[i].Title = info.Title
			trades[i].Outcome = info.Outcome
			trades[i].Slug = info.Slug
			enrichedCount++
		}
	}

	log.Printf("[global-monitor] Enriched %d/%d trades with token metadata", enrichedCount, len(trades))
	return trades
}
