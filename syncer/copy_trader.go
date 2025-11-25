package syncer

import (
	"context"
	"fmt"
	"log"
	"os"
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
	myAddress  string // Our wallet address for position lookups
}

// CopyTraderConfig holds configuration for copy trading
type CopyTraderConfig struct {
	Enabled          bool
	Multiplier       float64 // 0.05 = 1/20th
	MinOrderUSDC     float64 // Minimum order size
	MaxPriceSlippage float64 // Max price above trader's price (0.20 = 20%)
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

	// Configure for Magic/Email wallet if funder address is set
	funderAddress := os.Getenv("POLYMARKET_FUNDER_ADDRESS")
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
		config.CheckIntervalSec = 2 // 2 seconds
	}

	// Determine our wallet address for position lookups
	myAddress := funderAddress
	if myAddress == "" {
		myAddress = auth.GetAddress().Hex()
	}

	return &CopyTrader{
		store:      store,
		client:     client,
		clobClient: clobClient,
		config:     config,
		stopCh:     make(chan struct{}),
		myAddress:  myAddress,
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

	// Get order book FIRST to check price before buying
	book, err := ct.clobClient.GetOrderBook(ctx, tokenID)
	if err != nil {
		errMsg := fmt.Sprintf("failed to get order book: %v", err)
		log.Printf("[CopyTrader] BUY failed: %s", errMsg)
		return ct.logCopyTrade(ctx, trade, tokenID, intendedUSDC, 0, 0, 0, "failed", errMsg, "")
	}

	if len(book.Asks) == 0 {
		errMsg := "no asks in order book - no liquidity"
		log.Printf("[CopyTrader] BUY failed: %s", errMsg)
		return ct.logCopyTrade(ctx, trade, tokenID, intendedUSDC, 0, 0, 0, "failed", errMsg, "")
	}

	// Check max price protection - don't pay more than X% above trader's price
	bestAskPrice := 0.0
	if len(book.Asks) > 0 {
		fmt.Sscanf(book.Asks[0].Price, "%f", &bestAskPrice)
	}
	maxAllowedPrice := trade.Price * (1 + ct.config.MaxPriceSlippage)

	if bestAskPrice > maxAllowedPrice {
		errMsg := fmt.Sprintf("price too high: best ask %.4f > max allowed %.4f (trader paid %.4f + %.0f%% slippage)",
			bestAskPrice, maxAllowedPrice, trade.Price, ct.config.MaxPriceSlippage*100)
		log.Printf("[CopyTrader] BUY skipped: %s", errMsg)
		return ct.logCopyTrade(ctx, trade, tokenID, intendedUSDC, 0, bestAskPrice, 0, "skipped", errMsg, "")
	}

	log.Printf("[CopyTrader] BUY: Original=$%.2f@%.4f, Copy=$%.4f, CurrentAsk=%.4f, MaxPrice=%.4f, Market=%s, Outcome=%s",
		trade.UsdcSize, trade.Price, intendedUSDC, bestAskPrice, maxAllowedPrice, trade.Title, trade.Outcome)

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

	// Estimate what we got from the order book
	var avgPrice, sizeBought, actualUSDC float64
	sizeBought, avgPrice, actualUSDC = api.CalculateOptimalFill(book, api.SideBuy, intendedUSDC)

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
	// Try to get actual position from Polymarket API first
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

	log.Printf("[CopyTrader] SELL: Selling %.4f tokens, Market=%s, Outcome=%s",
		sellSize, trade.Title, trade.Outcome)

	// Get order book to find best prices
	book, err := ct.clobClient.GetOrderBook(ctx, tokenID)
	if err != nil {
		errMsg := fmt.Sprintf("failed to get order book: %v", err)
		return ct.logCopyTrade(ctx, trade, tokenID, 0, 0, 0, 0, "failed", errMsg, "")
	}

	if len(book.Bids) == 0 {
		errMsg := "no bids in order book - no liquidity"
		return ct.logCopyTrade(ctx, trade, tokenID, 0, 0, 0, 0, "failed", errMsg, "")
	}

	// Get best bid price
	bestBidPrice := 0.0
	fmt.Sscanf(book.Bids[0].Price, "%f", &bestBidPrice)

	// Retry logic: try different price levels
	// 1. First try at trader's price (if high enough)
	// 2. Then try at best bid
	// 3. Finally try at 95% of best bid (more aggressive)
	priceLevels := []struct {
		price float64
		desc  string
	}{
		{trade.Price, "trader's price"},
		{bestBidPrice, "best bid"},
		{bestBidPrice * 0.95, "aggressive (95% of best bid)"},
	}

	var resp *api.OrderResponse
	var successPrice float64
	var lastErr error

	for _, level := range priceLevels {
		// Skip if price is too low (< 0.01) or would result in tiny USDC
		if level.price < 0.01 || sellSize*level.price < 0.5 {
			log.Printf("[CopyTrader] SELL: Skipping %s (price=%.4f too low)", level.desc, level.price)
			continue
		}

		log.Printf("[CopyTrader] SELL: Trying %s at %.4f for %.4f tokens", level.desc, level.price, sellSize)

		resp, lastErr = ct.clobClient.PlaceLimitOrder(ctx, tokenID, api.SideSell, sellSize, level.price, negRisk)
		if lastErr != nil {
			log.Printf("[CopyTrader] SELL: %s failed: %v", level.desc, lastErr)
			continue
		}

		if resp.Success {
			successPrice = level.price
			log.Printf("[CopyTrader] SELL success (%s): OrderID=%s, Status=%s, Size=%.4f, Price=%.4f",
				level.desc, resp.OrderID, resp.Status, sellSize, level.price)
			break
		}

		log.Printf("[CopyTrader] SELL: %s rejected: %s", level.desc, resp.ErrorMsg)
		lastErr = fmt.Errorf(resp.ErrorMsg)
	}

	// All attempts failed
	if resp == nil || !resp.Success {
		errMsg := "all sell attempts failed"
		if lastErr != nil {
			errMsg = fmt.Sprintf("all sell attempts failed: %v", lastErr)
		}
		log.Printf("[CopyTrader] SELL failed: %s", errMsg)
		return ct.logCopyTrade(ctx, trade, tokenID, 0, 0, bestBidPrice, sellSize, "failed", errMsg, "")
	}

	expectedUSDC := sellSize * successPrice

	// Clear local position tracking
	if err := ct.store.ClearMyPosition(ctx, trade.MarketID, trade.Outcome); err != nil {
		log.Printf("[CopyTrader] Warning: failed to clear position: %v", err)
	}

	return ct.logCopyTrade(ctx, trade, tokenID, 0, expectedUSDC, successPrice, sellSize, "executed", "", resp.OrderID)
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
