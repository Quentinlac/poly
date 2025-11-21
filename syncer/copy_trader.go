package syncer

import (
	"context"
	"fmt"
	"log"
	"strings"
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
}

// CopyTraderConfig holds configuration for copy trading
type CopyTraderConfig struct {
	Enabled          bool
	Multiplier       float64 // 0.05 = 1/20th
	MinOrderUSDC     float64 // Minimum order size
	CheckIntervalSec int     // Poll frequency
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

	// Set defaults
	if config.Multiplier == 0 {
		config.Multiplier = 0.05 // 1/20th
	}
	if config.MinOrderUSDC == 0 {
		config.MinOrderUSDC = 1.0 // $1 minimum
	}
	if config.CheckIntervalSec == 0 {
		config.CheckIntervalSec = 2 // 2 seconds
	}

	return &CopyTrader{
		store:      store,
		client:     client,
		clobClient: clobClient,
		config:     config,
		stopCh:     make(chan struct{}),
	}, nil
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

	ct.running = true
	go ct.run(ctx)

	log.Printf("[CopyTrader] Started with multiplier=%.2f, minOrder=$%.2f, interval=%ds",
		ct.config.Multiplier, ct.config.MinOrderUSDC, ct.config.CheckIntervalSec)

	return nil
}

// Stop halts the copy trader
func (ct *CopyTrader) Stop() {
	if ct.running {
		close(ct.stopCh)
		ct.running = false
		log.Printf("[CopyTrader] Stopped")
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
	// Get unprocessed trades from user_trades
	trades, err := ct.store.GetUnprocessedTrades(ctx, 100)
	if err != nil {
		return fmt.Errorf("failed to get unprocessed trades: %w", err)
	}

	if len(trades) == 0 {
		return nil
	}

	log.Printf("[CopyTrader] Processing %d new trades", len(trades))

	for _, trade := range trades {
		if err := ct.processTrade(ctx, trade); err != nil {
			log.Printf("[CopyTrader] Error processing trade %s: %v", trade.ID, err)
		}

		// Mark as processed regardless of success/failure
		if err := ct.store.MarkTradeProcessed(ctx, trade.ID); err != nil {
			log.Printf("[CopyTrader] Error marking trade processed %s: %v", trade.ID, err)
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

	// Get token ID from market
	tokenID, negRisk, err := ct.getTokenIDForTrade(ctx, trade)
	if err != nil {
		return ct.logCopyTrade(ctx, trade, "", 0, 0, 0, 0, "failed", fmt.Sprintf("failed to get token ID: %v", err), "")
	}

	if trade.Side == "BUY" {
		return ct.executeBuy(ctx, trade, tokenID, negRisk)
	} else if trade.Side == "SELL" {
		return ct.executeSell(ctx, trade, tokenID, negRisk)
	}

	return nil
}

func (ct *CopyTrader) getTokenIDForTrade(ctx context.Context, trade models.TradeDetail) (string, bool, error) {
	// First check token_map_cache
	tokenID, negRisk, err := ct.store.GetTokenIDFromCache(ctx, trade.MarketID, trade.Outcome)
	if err == nil && tokenID != "" {
		return tokenID, negRisk, nil
	}

	// If not cached, try to get from CLOB market info using condition ID
	// The MarketID field might be the condition ID or token ID
	market, err := ct.clobClient.GetMarket(ctx, trade.MarketID)
	if err != nil {
		// Try using MarketID as token ID directly
		return trade.MarketID, false, nil
	}

	// Find matching token by outcome
	for _, token := range market.Tokens {
		if strings.EqualFold(token.Outcome, trade.Outcome) {
			// Cache it for next time
			ct.store.CacheTokenID(ctx, trade.MarketID, trade.Outcome, token.TokenID, market.NegRisk)
			return token.TokenID, market.NegRisk, nil
		}
	}

	// Fallback: use MarketID as token ID
	return trade.MarketID, false, nil
}

func (ct *CopyTrader) executeBuy(ctx context.Context, trade models.TradeDetail, tokenID string, negRisk bool) error {
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
		log.Printf("[CopyTrader] Using custom settings for %s: multiplier=%.4f, minUSDC=$%.2f",
			trade.UserID, multiplier, minUSDC)
	}

	// Calculate amount to buy
	intendedUSDC := trade.UsdcSize * multiplier

	// Ensure minimum order
	if intendedUSDC < minUSDC {
		intendedUSDC = minUSDC
		log.Printf("[CopyTrader] BUY amount below minimum, using $%.2f", intendedUSDC)
	}

	log.Printf("[CopyTrader] BUY: Original=$%.2f, Copy=$%.4f, Market=%s, Outcome=%s",
		trade.UsdcSize, intendedUSDC, trade.Title, trade.Outcome)

	// Place market order
	resp, err := ct.clobClient.PlaceMarketOrder(ctx, tokenID, api.SideBuy, intendedUSDC, negRisk)
	if err != nil {
		errMsg := fmt.Sprintf("order failed: %v", err)
		log.Printf("[CopyTrader] BUY failed: %s", errMsg)
		return ct.logCopyTrade(ctx, trade, tokenID, intendedUSDC, 0, 0, 0, "failed", errMsg, "")
	}

	if !resp.Success {
		log.Printf("[CopyTrader] BUY rejected: %s", resp.ErrorMsg)
		return ct.logCopyTrade(ctx, trade, tokenID, intendedUSDC, 0, 0, 0, "failed", resp.ErrorMsg, "")
	}

	// Get order book to estimate what we got
	book, _ := ct.clobClient.GetOrderBook(ctx, tokenID)
	var avgPrice, sizeBought, actualUSDC float64
	if book != nil {
		sizeBought, avgPrice, actualUSDC = api.CalculateOptimalFill(book, api.SideBuy, intendedUSDC)
	}

	log.Printf("[CopyTrader] BUY success: OrderID=%s, Status=%s, Size=%.4f, AvgPrice=%.4f",
		resp.OrderID, resp.Status, sizeBought, avgPrice)

	// Update position
	if err := ct.store.UpdateMyPosition(ctx, MyPosition{
		MarketID:  trade.MarketID,
		TokenID:   tokenID,
		Outcome:   trade.Outcome,
		Title:     trade.Title,
		Size:      sizeBought,
		AvgPrice:  avgPrice,
		TotalCost: actualUSDC,
	}); err != nil {
		log.Printf("[CopyTrader] Warning: failed to update position: %v", err)
	}

	return ct.logCopyTrade(ctx, trade, tokenID, intendedUSDC, actualUSDC, avgPrice, sizeBought, "executed", "", resp.OrderID)
}

func (ct *CopyTrader) executeSell(ctx context.Context, trade models.TradeDetail, tokenID string, negRisk bool) error {
	// Get our current position for this market/outcome
	position, err := ct.store.GetMyPosition(ctx, trade.MarketID, trade.Outcome)
	if err != nil || position.Size <= 0 {
		log.Printf("[CopyTrader] SELL: No position to sell for %s/%s", trade.Title, trade.Outcome)
		return nil
	}

	log.Printf("[CopyTrader] SELL: Our position=%.4f tokens, Market=%s, Outcome=%s",
		position.Size, trade.Title, trade.Outcome)

	// Get order book to estimate price
	book, err := ct.clobClient.GetOrderBook(ctx, tokenID)
	if err != nil {
		errMsg := fmt.Sprintf("failed to get order book: %v", err)
		return ct.logCopyTrade(ctx, trade, tokenID, 0, 0, 0, 0, "failed", errMsg, "")
	}

	// Calculate expected USDC from selling our position
	_, avgPrice, expectedUSDC := api.CalculateOptimalFill(book, api.SideSell, position.Size*1.0) // size * avgPrice estimate

	// Place sell order for entire position
	// For sell, we need to convert to limit order at best bid
	if len(book.Bids) == 0 {
		errMsg := "no bids in order book"
		return ct.logCopyTrade(ctx, trade, tokenID, 0, 0, 0, 0, "failed", errMsg, "")
	}

	var resp *api.OrderResponse

	// If user sold at high price (>= 0.96), use limit order at same price
	// to avoid selling cheaper in a market that's about to resolve
	if trade.Price >= 0.96 {
		log.Printf("[CopyTrader] SELL: High price detected (%.4f >= 0.96), using limit order at same price",
			trade.Price)
		resp, err = ct.clobClient.PlaceLimitOrder(ctx, tokenID, api.SideSell, position.Size, trade.Price, negRisk)
		if err != nil {
			errMsg := fmt.Sprintf("limit sell order failed: %v", err)
			log.Printf("[CopyTrader] SELL failed: %s", errMsg)
			return ct.logCopyTrade(ctx, trade, tokenID, 0, 0, trade.Price, position.Size, "failed", errMsg, "")
		}
		expectedUSDC = position.Size * trade.Price
		avgPrice = trade.Price
	} else {
		// Use market order to sell everything
		resp, err = ct.clobClient.PlaceMarketOrder(ctx, tokenID, api.SideSell, position.Size*avgPrice, negRisk)
		if err != nil {
			errMsg := fmt.Sprintf("sell order failed: %v", err)
			log.Printf("[CopyTrader] SELL failed: %s", errMsg)
			return ct.logCopyTrade(ctx, trade, tokenID, 0, 0, 0, position.Size, "failed", errMsg, "")
		}
	}

	if !resp.Success {
		log.Printf("[CopyTrader] SELL rejected: %s", resp.ErrorMsg)
		return ct.logCopyTrade(ctx, trade, tokenID, 0, 0, avgPrice, position.Size, "failed", resp.ErrorMsg, "")
	}

	orderType := "market"
	if trade.Price >= 0.96 {
		orderType = "limit"
	}
	log.Printf("[CopyTrader] SELL success (%s): OrderID=%s, Status=%s, Size=%.4f, Price=%.4f, ExpectedUSDC=%.4f",
		orderType, resp.OrderID, resp.Status, position.Size, avgPrice, expectedUSDC)

	// Clear position
	if err := ct.store.ClearMyPosition(ctx, trade.MarketID, trade.Outcome); err != nil {
		log.Printf("[CopyTrader] Warning: failed to clear position: %v", err)
	}

	return ct.logCopyTrade(ctx, trade, tokenID, 0, expectedUSDC, avgPrice, position.Size, "executed", "", resp.OrderID)
}

func (ct *CopyTrader) logCopyTrade(ctx context.Context, trade models.TradeDetail, tokenID string, intended, actual, price, size float64, status, errReason, orderID string) error {
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
	}

	return ct.store.SaveCopyTrade(ctx, copyTrade)
}

// GetStats returns copy trading statistics
func (ct *CopyTrader) GetStats(ctx context.Context) (map[string]interface{}, error) {
	return ct.store.GetCopyTradeStats(ctx)
}
