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

	if trade.Side == "BUY" {
		return ct.executeBuy(ctx, trade, tokenID, negRisk)
	} else if trade.Side == "SELL" {
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
		// Found the token - check if the outcome matches
		if tokenInfo.Outcome != trade.Outcome {
			log.Printf("[CopyTrader] DEBUG getTokenID: OUTCOME MISMATCH! Token is %s but trade says %s",
				tokenInfo.Outcome, trade.Outcome)
			// The trade.MarketID is the wrong token - we need to find the correct one
			// Look up the sibling token with the correct outcome
			siblingToken, err := ct.store.GetTokenByConditionAndOutcome(ctx, tokenInfo.ConditionID, trade.Outcome)
			if err == nil && siblingToken != nil {
				log.Printf("[CopyTrader] DEBUG getTokenID: Found correct token %s for outcome %s",
					siblingToken.TokenID, trade.Outcome)
				return siblingToken.TokenID, trade.Outcome, false, nil
			}
			// Couldn't find sibling, use the token we have but return its actual outcome
			log.Printf("[CopyTrader] DEBUG getTokenID: Using original token %s with corrected outcome %s",
				trade.MarketID, tokenInfo.Outcome)
			return trade.MarketID, tokenInfo.Outcome, false, nil
		}
		// Outcome matches, use as-is
		log.Printf("[CopyTrader] DEBUG getTokenID: Token verified - outcome=%s matches", tokenInfo.Outcome)
		return trade.MarketID, trade.Outcome, false, nil
	}

	// Token not in cache - try CLOB API
	market, err := ct.clobClient.GetMarket(ctx, trade.MarketID)
	if err != nil {
		// GetMarket failed (trade.MarketID is likely a token ID, not condition ID)
		log.Printf("[CopyTrader] DEBUG getTokenID: GetMarket failed (%v), using trade.MarketID directly", err)
		// We don't know the actual outcome, so return empty to use trade.Outcome
		return trade.MarketID, "", false, nil
	}

	// Find matching token by outcome
	log.Printf("[CopyTrader] DEBUG getTokenID: GetMarket SUCCESS, market has %d tokens", len(market.Tokens))
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
	return trade.MarketID, "", false, nil
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

	// Calculate max allowed price based on tiered slippage
	maxSlippage := getMaxSlippage(trade.Price)
	maxAllowedPrice := trade.Price * (1 + maxSlippage)

	log.Printf("[CopyTrader] DEBUG executeBuy: trade.MarketID=%s, tokenID=%s", trade.MarketID, tokenID)
	log.Printf("[CopyTrader] DEBUG executeBuy: trade.Price=%.4f, maxSlippage=%.0f%%, maxAllowedPrice=%.4f",
		trade.Price, maxSlippage*100, maxAllowedPrice)

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

		// Get fresh order book
		book, err := ct.clobClient.GetOrderBook(ctx, tokenID)
		if err != nil {
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

	// Calculate USDC value for market sell (sell everything at market)
	// We need to estimate the USDC we'll get - use order book
	book, err := ct.clobClient.GetOrderBook(ctx, tokenID)
	if err != nil {
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
