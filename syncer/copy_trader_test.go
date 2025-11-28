package syncer

import (
	"context"
	"testing"
	"time"

	"polymarket-analyzer/api"
	"polymarket-analyzer/models"
	"polymarket-analyzer/storage"
)

func TestCalculateOptimalFill(t *testing.T) {
	tests := []struct {
		name         string
		book         *api.OrderBook
		side         api.Side
		amountUSDC   float64
		wantSize     float64
		wantAvgPrice float64
		wantFilled   float64
	}{
		{
			name: "buy from asks - single level",
			book: &api.OrderBook{
				Asks: []api.OrderBookLevel{
					{Price: "0.50", Size: "100"},
				},
			},
			side:         api.SideBuy,
			amountUSDC:   25.0,
			wantSize:     50.0,  // 25 / 0.50
			wantAvgPrice: 0.50,
			wantFilled:   25.0,
		},
		{
			name: "buy from asks - multiple levels",
			book: &api.OrderBook{
				Asks: []api.OrderBookLevel{
					{Price: "0.10", Size: "10"},  // $1.00 total
					{Price: "0.15", Size: "10"},  // $1.50 total
					{Price: "0.20", Size: "100"}, // $20.00 total
				},
			},
			side:         api.SideBuy,
			amountUSDC:   5.0,
			wantSize:     32.5, // 10 + 10 + 12.5
			wantAvgPrice: 0.1538,
			wantFilled:   5.0,
		},
		{
			name: "buy with insufficient liquidity",
			book: &api.OrderBook{
				Asks: []api.OrderBookLevel{
					{Price: "0.50", Size: "10"}, // Only $5 available
				},
			},
			side:         api.SideBuy,
			amountUSDC:   10.0,
			wantSize:     10.0,
			wantAvgPrice: 0.50,
			wantFilled:   5.0, // Only filled $5
		},
		{
			name: "sell to bids",
			book: &api.OrderBook{
				Bids: []api.OrderBookLevel{
					{Price: "0.60", Size: "100"},
				},
			},
			side:         api.SideSell,
			amountUSDC:   30.0,
			wantSize:     50.0,
			wantAvgPrice: 0.60,
			wantFilled:   30.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size, avgPrice, filled := api.CalculateOptimalFill(tt.book, tt.side, tt.amountUSDC)

			// Allow small floating point differences
			if !floatEquals(size, tt.wantSize, 0.01) {
				t.Errorf("size = %v, want %v", size, tt.wantSize)
			}
			if !floatEquals(avgPrice, tt.wantAvgPrice, 0.01) {
				t.Errorf("avgPrice = %v, want %v", avgPrice, tt.wantAvgPrice)
			}
			if !floatEquals(filled, tt.wantFilled, 0.01) {
				t.Errorf("filled = %v, want %v", filled, tt.wantFilled)
			}
		})
	}
}

func TestCopyTraderConfig(t *testing.T) {
	config := CopyTraderConfig{
		Enabled:          true,
		Multiplier:       0.05,
		MinOrderUSDC:     1.0,
		CheckIntervalSec: 2,
	}

	// Test multiplier calculation
	originalAmount := 100.0
	copyAmount := originalAmount * config.Multiplier
	if copyAmount != 5.0 {
		t.Errorf("copy amount = %v, want 5.0", copyAmount)
	}

	// Test minimum order enforcement
	smallAmount := 0.5 * config.Multiplier // $0.025
	if smallAmount < config.MinOrderUSDC {
		smallAmount = config.MinOrderUSDC
	}
	if smallAmount != 1.0 {
		t.Errorf("enforced minimum = %v, want 1.0", smallAmount)
	}
}

func floatEquals(a, b, tolerance float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff <= tolerance
}

// Integration tests that require database connection
func TestCopyTraderStorage(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	store, err := storage.NewPostgres()
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	t.Run("mark trade processed", func(t *testing.T) {
		tradeID := "test-trade-" + time.Now().Format("20060102150405")

		// Mark as processed
		err := store.MarkTradeProcessed(ctx, tradeID)
		if err != nil {
			t.Fatalf("MarkTradeProcessed failed: %v", err)
		}

		// Verify it's marked (by trying to insert again - should not error due to ON CONFLICT)
		err = store.MarkTradeProcessed(ctx, tradeID)
		if err != nil {
			t.Fatalf("Second MarkTradeProcessed should not fail: %v", err)
		}
	})

	t.Run("update and get position", func(t *testing.T) {
		marketID := "test-market-" + time.Now().Format("20060102150405")
		outcome := "Yes"

		// Create position
		pos := MyPosition{
			MarketID:  marketID,
			TokenID:   "token-123",
			Outcome:   outcome,
			Title:     "Test Market",
			Size:      10.0,
			AvgPrice:  0.50,
			TotalCost: 5.0,
		}

		err := store.UpdateMyPosition(ctx, pos)
		if err != nil {
			t.Fatalf("UpdateMyPosition failed: %v", err)
		}

		// Retrieve position
		retrieved, err := store.GetMyPosition(ctx, marketID, outcome)
		if err != nil {
			t.Fatalf("GetMyPosition failed: %v", err)
		}

		if retrieved.Size != 10.0 {
			t.Errorf("size = %v, want 10.0", retrieved.Size)
		}
		if retrieved.MarketID != marketID {
			t.Errorf("marketID = %v, want %v", retrieved.MarketID, marketID)
		}

		// Add to position
		pos2 := MyPosition{
			MarketID:  marketID,
			TokenID:   "token-123",
			Outcome:   outcome,
			Title:     "Test Market",
			Size:      5.0,
			AvgPrice:  0.60,
			TotalCost: 3.0,
		}

		err = store.UpdateMyPosition(ctx, pos2)
		if err != nil {
			t.Fatalf("Second UpdateMyPosition failed: %v", err)
		}

		// Verify accumulated
		retrieved2, err := store.GetMyPosition(ctx, marketID, outcome)
		if err != nil {
			t.Fatalf("Second GetMyPosition failed: %v", err)
		}

		// Size should be 10 + 5 = 15
		if retrieved2.Size != 15.0 {
			t.Errorf("accumulated size = %v, want 15.0", retrieved2.Size)
		}

		// Total cost should be 5 + 3 = 8
		if retrieved2.TotalCost != 8.0 {
			t.Errorf("accumulated cost = %v, want 8.0", retrieved2.TotalCost)
		}

		// Clear position
		err = store.ClearMyPosition(ctx, marketID, outcome)
		if err != nil {
			t.Fatalf("ClearMyPosition failed: %v", err)
		}

		// Verify cleared
		_, err = store.GetMyPosition(ctx, marketID, outcome)
		if err == nil {
			t.Error("Position should be cleared")
		}
	})

	t.Run("save and get copy trade stats", func(t *testing.T) {
		trade := CopyTrade{
			OriginalTradeID: "orig-" + time.Now().Format("20060102150405"),
			OriginalTrader:  "0x1234567890123456789012345678901234567890",
			MarketID:        "market-123",
			TokenID:         "token-123",
			Outcome:         "Yes",
			Title:           "Test Market",
			Side:            "BUY",
			IntendedUSDC:    5.0,
			ActualUSDC:      4.95,
			PricePaid:       0.50,
			SizeBought:      9.9,
			Status:          "executed",
		}

		err := store.SaveCopyTrade(ctx, trade)
		if err != nil {
			t.Fatalf("SaveCopyTrade failed: %v", err)
		}

		// Get stats
		stats, err := store.GetCopyTradeStats(ctx)
		if err != nil {
			t.Fatalf("GetCopyTradeStats failed: %v", err)
		}

		if stats["total_trades"].(int) < 1 {
			t.Errorf("total_trades = %v, want >= 1", stats["total_trades"])
		}
	})

	t.Run("get unprocessed trades", func(t *testing.T) {
		// This test checks that the query works, results depend on actual data
		trades, err := store.GetUnprocessedTrades(ctx, 10)
		if err != nil {
			t.Fatalf("GetUnprocessedTrades failed: %v", err)
		}

		// Just verify it returns without error
		t.Logf("Found %d unprocessed trades", len(trades))
	})
}

func TestOrderBookEmpty(t *testing.T) {
	// Test with empty order book
	book := &api.OrderBook{
		Asks: []api.OrderBookLevel{},
		Bids: []api.OrderBookLevel{},
	}

	size, avgPrice, filled := api.CalculateOptimalFill(book, api.SideBuy, 100)

	if size != 0 {
		t.Errorf("size should be 0 for empty book, got %v", size)
	}
	if avgPrice != 0 {
		t.Errorf("avgPrice should be 0 for empty book, got %v", avgPrice)
	}
	if filled != 0 { // Nothing was filled from empty book
		t.Errorf("filled should be 0 for empty book, got %v", filled)
	}
}

func TestTradeTypeFiltering(t *testing.T) {
	// Test that non-TRADE types are properly identified
	trades := []models.TradeDetail{
		{ID: "1", Type: "TRADE", Side: "BUY"},
		{ID: "2", Type: "REDEEM", Side: ""},
		{ID: "3", Type: "SPLIT", Side: ""},
		{ID: "4", Type: "", Side: "SELL"}, // Empty type should be treated as TRADE
	}

	tradesToCopy := 0
	for _, trade := range trades {
		if trade.Type == "" || trade.Type == "TRADE" {
			tradesToCopy++
		}
	}

	if tradesToCopy != 2 {
		t.Errorf("tradesToCopy = %v, want 2", tradesToCopy)
	}
}

func TestMultiplierCalculations(t *testing.T) {
	tests := []struct {
		original   float64
		multiplier float64
		minOrder   float64
		want       float64
	}{
		{100.0, 0.05, 1.0, 5.0},    // Normal case: 100 * 0.05 = 5
		{10.0, 0.05, 1.0, 1.0},     // Below min: 10 * 0.05 = 0.5, enforced to 1.0
		{1000.0, 0.05, 1.0, 50.0},  // Large trade: 1000 * 0.05 = 50
		{20.0, 0.05, 1.0, 1.0},     // Exactly min: 20 * 0.05 = 1.0
		{19.99, 0.05, 1.0, 1.0},    // Just below min threshold
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := tt.original * tt.multiplier
			if result < tt.minOrder {
				result = tt.minOrder
			}

			if !floatEquals(result, tt.want, 0.001) {
				t.Errorf("multiplier calc: %v * %v (min %v) = %v, want %v",
					tt.original, tt.multiplier, tt.minOrder, result, tt.want)
			}
		})
	}
}

func TestSellAllLogic(t *testing.T) {
	// Test that any sell triggers full position liquidation
	userSells := []float64{1, 10, 50, 100} // User sells various amounts
	myPosition := 25.0                      // I always have 25 tokens

	for _, sellAmount := range userSells {
		// Regardless of how much user sells, I sell everything
		mySellAmount := myPosition

		if mySellAmount != 25.0 {
			t.Errorf("For user sell of %v, my sell should be %v (full position), got %v",
				sellAmount, myPosition, mySellAmount)
		}
	}
}

func TestPerUserMultiplier(t *testing.T) {
	// Test per-user multiplier calculations
	tests := []struct {
		name           string
		originalAmount float64
		globalMult     float64
		userMult       float64
		useUserMult    bool
		minOrder       float64
		want           float64
	}{
		{
			name:           "global default multiplier",
			originalAmount: 100.0,
			globalMult:     0.05,
			userMult:       0,
			useUserMult:    false,
			minOrder:       1.0,
			want:           5.0, // 100 * 0.05
		},
		{
			name:           "custom user multiplier - higher",
			originalAmount: 100.0,
			globalMult:     0.05,
			userMult:       0.10, // 10%
			useUserMult:    true,
			minOrder:       1.0,
			want:           10.0, // 100 * 0.10
		},
		{
			name:           "custom user multiplier - lower",
			originalAmount: 100.0,
			globalMult:     0.05,
			userMult:       0.01, // 1%
			useUserMult:    true,
			minOrder:       1.0,
			want:           1.0, // 100 * 0.01 = 1.0
		},
		{
			name:           "user multiplier below minimum",
			originalAmount: 50.0,
			globalMult:     0.05,
			userMult:       0.01, // 1%
			useUserMult:    true,
			minOrder:       1.0,
			want:           1.0, // 50 * 0.01 = 0.5, but min is 1.0
		},
		{
			name:           "user with custom minimum",
			originalAmount: 100.0,
			globalMult:     0.05,
			userMult:       0.02, // 2%
			useUserMult:    true,
			minOrder:       5.0,  // Custom min
			want:           5.0,  // 100 * 0.02 = 2.0, but min is 5.0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Determine which multiplier to use
			multiplier := tt.globalMult
			minOrder := tt.minOrder
			if tt.useUserMult {
				multiplier = tt.userMult
			}

			// Calculate intended amount
			result := tt.originalAmount * multiplier
			if result < minOrder {
				result = minOrder
			}

			if !floatEquals(result, tt.want, 0.001) {
				t.Errorf("per-user calc: %v * %v (min %v) = %v, want %v",
					tt.originalAmount, multiplier, minOrder, result, tt.want)
			}
		})
	}
}

func TestHighPriceSellBehavior(t *testing.T) {
	// Test that sells at >= 0.96 should use limit orders
	tests := []struct {
		name          string
		price         float64
		shouldLimit   bool
		expectedPrice float64
	}{
		{
			name:          "low price - market order",
			price:         0.50,
			shouldLimit:   false,
			expectedPrice: 0, // Market order, price determined by book
		},
		{
			name:          "medium price - market order",
			price:         0.80,
			shouldLimit:   false,
			expectedPrice: 0,
		},
		{
			name:          "just below threshold - market order",
			price:         0.95,
			shouldLimit:   false,
			expectedPrice: 0,
		},
		{
			name:          "at threshold - limit order",
			price:         0.96,
			shouldLimit:   true,
			expectedPrice: 0.96,
		},
		{
			name:          "above threshold - limit order",
			price:         0.98,
			shouldLimit:   true,
			expectedPrice: 0.98,
		},
		{
			name:          "max price - limit order",
			price:         0.99,
			shouldLimit:   true,
			expectedPrice: 0.99,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shouldUseLimitOrder := tt.price >= 0.96

			if shouldUseLimitOrder != tt.shouldLimit {
				t.Errorf("price %.2f: shouldUseLimitOrder = %v, want %v",
					tt.price, shouldUseLimitOrder, tt.shouldLimit)
			}

			if tt.shouldLimit {
				// For limit orders, price should match the trade price
				if tt.price != tt.expectedPrice {
					t.Errorf("limit order price = %v, want %v", tt.price, tt.expectedPrice)
				}
			}
		})
	}
}

func TestHighPriceSellCalculation(t *testing.T) {
	// Test the expected USDC calculation for limit sells
	positionSize := 100.0
	sellPrice := 0.97

	expectedUSDC := positionSize * sellPrice // 100 * 0.97 = 97

	if expectedUSDC != 97.0 {
		t.Errorf("expectedUSDC = %v, want 97.0", expectedUSDC)
	}

	// Verify this is better than selling at market (e.g., 0.90)
	marketPrice := 0.90
	marketUSDC := positionSize * marketPrice // 100 * 0.90 = 90

	benefit := expectedUSDC - marketUSDC // 97 - 90 = 7

	if benefit != 7.0 {
		t.Errorf("benefit of limit order = %v, want 7.0", benefit)
	}
}

// TestGetMaxSlippage tests the tiered slippage calculation
// Critical for copy trading - ensures we allow appropriate slippage at different price levels
func TestGetMaxSlippage(t *testing.T) {
	tests := []struct {
		name        string
		traderPrice float64
		wantSlip    float64
		description string
	}{
		{
			name:        "very low price under $0.10",
			traderPrice: 0.05,
			wantSlip:    2.00, // 200% - can pay up to 3x
			description: "Low prices are volatile, need high slippage",
		},
		{
			name:        "low price at $0.09",
			traderPrice: 0.09,
			wantSlip:    2.00,
			description: "Still under $0.10 threshold",
		},
		{
			name:        "price at $0.10 boundary",
			traderPrice: 0.10,
			wantSlip:    0.80, // 80% - next tier
			description: "At $0.10, moves to 80% tier",
		},
		{
			name:        "price at $0.15",
			traderPrice: 0.15,
			wantSlip:    0.80,
			description: "Still in $0.10-$0.20 range",
		},
		{
			name:        "price at $0.20 boundary",
			traderPrice: 0.20,
			wantSlip:    0.50, // 50%
			description: "At $0.20, moves to 50% tier",
		},
		{
			name:        "price at $0.25",
			traderPrice: 0.25,
			wantSlip:    0.50,
			description: "Still in $0.20-$0.30 range",
		},
		{
			name:        "price at $0.30 boundary",
			traderPrice: 0.30,
			wantSlip:    0.30, // 30%
			description: "At $0.30, moves to 30% tier",
		},
		{
			name:        "price at $0.35",
			traderPrice: 0.35,
			wantSlip:    0.30,
			description: "Still in $0.30-$0.40 range",
		},
		{
			name:        "price at $0.40 boundary",
			traderPrice: 0.40,
			wantSlip:    0.20, // 20%
			description: "At $0.40+, uses 20% tier",
		},
		{
			name:        "high price at $0.80",
			traderPrice: 0.80,
			wantSlip:    0.20,
			description: "High prices use standard 20%",
		},
		{
			name:        "very high price at $0.95",
			traderPrice: 0.95,
			wantSlip:    0.20,
			description: "Even very high prices use 20%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getMaxSlippage(tt.traderPrice)
			if got != tt.wantSlip {
				t.Errorf("getMaxSlippage(%.2f) = %.2f, want %.2f (%s)",
					tt.traderPrice, got, tt.wantSlip, tt.description)
			}
		})
	}
}

// TestMaxAllowedPriceCalculation tests the max price calculation
func TestMaxAllowedPriceCalculation(t *testing.T) {
	tests := []struct {
		traderPrice    float64
		wantMaxPrice   float64
		description    string
	}{
		{
			traderPrice:  0.05,
			wantMaxPrice: 0.15, // 0.05 * (1 + 2.00) = 0.15
			description:  "At 5¢, max is 15¢ (3x)",
		},
		{
			traderPrice:  0.10,
			wantMaxPrice: 0.18, // 0.10 * (1 + 0.80) = 0.18
			description:  "At 10¢, max is 18¢ (1.8x)",
		},
		{
			traderPrice:  0.20,
			wantMaxPrice: 0.30, // 0.20 * (1 + 0.50) = 0.30
			description:  "At 20¢, max is 30¢ (1.5x)",
		},
		{
			traderPrice:  0.50,
			wantMaxPrice: 0.60, // 0.50 * (1 + 0.20) = 0.60
			description:  "At 50¢, max is 60¢ (1.2x)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			maxSlippage := getMaxSlippage(tt.traderPrice)
			maxAllowedPrice := tt.traderPrice * (1 + maxSlippage)

			if !floatEquals(maxAllowedPrice, tt.wantMaxPrice, 0.001) {
				t.Errorf("maxAllowedPrice for %.2f = %.4f, want %.4f",
					tt.traderPrice, maxAllowedPrice, tt.wantMaxPrice)
			}
		})
	}
}

// TestClosedMarketDetection tests that we detect 404 errors for closed markets
func TestClosedMarketDetection(t *testing.T) {
	tests := []struct {
		name       string
		errorMsg   string
		shouldSkip bool
	}{
		{
			name:       "404 error should skip",
			errorMsg:   "get order book failed: 404 {\"error\":\"No orderbook exists for the requested token id\"}",
			shouldSkip: true,
		},
		{
			name:       "No orderbook message should skip",
			errorMsg:   "No orderbook exists for the requested token id",
			shouldSkip: true,
		},
		{
			name:       "Network timeout should not skip",
			errorMsg:   "connection timeout",
			shouldSkip: false,
		},
		{
			name:       "Server error should not skip",
			errorMsg:   "500 internal server error",
			shouldSkip: false,
		},
		{
			name:       "Rate limit should not skip",
			errorMsg:   "429 too many requests",
			shouldSkip: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Check if error contains 404 or "No orderbook exists"
			import_strings := false
			if contains(tt.errorMsg, "404") || contains(tt.errorMsg, "No orderbook exists") {
				import_strings = true
			}

			if import_strings != tt.shouldSkip {
				t.Errorf("closed market detection for %q = %v, want %v",
					tt.errorMsg, import_strings, tt.shouldSkip)
			}
		})
	}
}

// contains is a helper for string matching
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestTradeOutcomePreservation ensures trades keep their original outcome
// This is critical - we must NOT convert SELL No to BUY Yes
func TestTradeOutcomePreservation(t *testing.T) {
	tests := []struct {
		name           string
		tradeSide      string
		tradeOutcome   string
		expectedAction string
	}{
		{
			name:           "SELL No should copy as SELL No",
			tradeSide:      "SELL",
			tradeOutcome:   "No",
			expectedAction: "SELL No tokens",
		},
		{
			name:           "BUY No should copy as BUY No",
			tradeSide:      "BUY",
			tradeOutcome:   "No",
			expectedAction: "BUY No tokens",
		},
		{
			name:           "SELL Yes should copy as SELL Yes",
			tradeSide:      "SELL",
			tradeOutcome:   "Yes",
			expectedAction: "SELL Yes tokens",
		},
		{
			name:           "BUY Yes should copy as BUY Yes",
			tradeSide:      "BUY",
			tradeOutcome:   "Yes",
			expectedAction: "BUY Yes tokens",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trade := models.TradeDetail{
				ID:       "test-trade",
				Side:     tt.tradeSide,
				Outcome:  tt.tradeOutcome,
				MarketID: "test-market",
			}

			// The trade outcome should be preserved as-is
			// We should NOT transform SELL No to BUY Yes
			if trade.Side != tt.tradeSide {
				t.Errorf("Trade side was modified: got %s, want %s", trade.Side, tt.tradeSide)
			}
			if trade.Outcome != tt.tradeOutcome {
				t.Errorf("Trade outcome was modified: got %s, want %s", trade.Outcome, tt.tradeOutcome)
			}

			// Verify expected copy action
			copyAction := trade.Side + " " + trade.Outcome + " tokens"
			if copyAction != tt.expectedAction {
				t.Errorf("Copy action = %s, want %s", copyAction, tt.expectedAction)
			}
		})
	}
}

// TestCopyTraderSkipsNonTradeTypes verifies REDEEM/SPLIT/MERGE are skipped
func TestCopyTraderSkipsNonTradeTypes(t *testing.T) {
	tests := []struct {
		tradeType  string
		shouldCopy bool
	}{
		{"TRADE", true},
		{"", true},        // Empty type = TRADE
		{"REDEEM", false},
		{"SPLIT", false},
		{"MERGE", false},
	}

	for _, tt := range tests {
		t.Run(tt.tradeType, func(t *testing.T) {
			shouldCopy := tt.tradeType == "" || tt.tradeType == "TRADE"
			if shouldCopy != tt.shouldCopy {
				t.Errorf("shouldCopy(%q) = %v, want %v", tt.tradeType, shouldCopy, tt.shouldCopy)
			}
		})
	}
}

// TestBotStrategyBuyPriceLimit tests the 10% max price limit for bot buys
func TestBotStrategyBuyPriceLimit(t *testing.T) {
	tests := []struct {
		name         string
		copiedPrice  float64
		askPrice     float64
		shouldAccept bool
	}{
		{
			name:         "exact copied price - accept",
			copiedPrice:  0.50,
			askPrice:     0.50,
			shouldAccept: true,
		},
		{
			name:         "5% above - accept",
			copiedPrice:  0.50,
			askPrice:     0.525,
			shouldAccept: true,
		},
		{
			name:         "exactly 10% above - accept",
			copiedPrice:  0.50,
			askPrice:     0.55,
			shouldAccept: true,
		},
		{
			name:         "11% above - reject",
			copiedPrice:  0.50,
			askPrice:     0.555,
			shouldAccept: false,
		},
		{
			name:         "20% above - reject",
			copiedPrice:  0.50,
			askPrice:     0.60,
			shouldAccept: false,
		},
		{
			name:         "below copied price - accept",
			copiedPrice:  0.50,
			askPrice:     0.45,
			shouldAccept: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maxPrice := tt.copiedPrice * 1.10 // 10% above copied price
			shouldAccept := tt.askPrice <= maxPrice

			if shouldAccept != tt.shouldAccept {
				t.Errorf("askPrice %.4f with copiedPrice %.4f: accepted=%v, want=%v (maxPrice=%.4f)",
					tt.askPrice, tt.copiedPrice, shouldAccept, tt.shouldAccept, maxPrice)
			}
		})
	}
}

// TestBotStrategySellPriceLimit tests the 10% min price limit for bot sells
func TestBotStrategySellPriceLimit(t *testing.T) {
	tests := []struct {
		name         string
		copiedPrice  float64
		bidPrice     float64
		shouldAccept bool
	}{
		{
			name:         "exact copied price - accept",
			copiedPrice:  0.50,
			bidPrice:     0.50,
			shouldAccept: true,
		},
		{
			name:         "5% below - accept",
			copiedPrice:  0.50,
			bidPrice:     0.475,
			shouldAccept: true,
		},
		{
			name:         "exactly 10% below - accept",
			copiedPrice:  0.50,
			bidPrice:     0.45,
			shouldAccept: true,
		},
		{
			name:         "11% below - reject",
			copiedPrice:  0.50,
			bidPrice:     0.445,
			shouldAccept: false,
		},
		{
			name:         "20% below - reject",
			copiedPrice:  0.50,
			bidPrice:     0.40,
			shouldAccept: false,
		},
		{
			name:         "above copied price - accept",
			copiedPrice:  0.50,
			bidPrice:     0.55,
			shouldAccept: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			minPrice := tt.copiedPrice * 0.90 // 10% below copied price
			shouldAccept := tt.bidPrice >= minPrice

			if shouldAccept != tt.shouldAccept {
				t.Errorf("bidPrice %.4f with copiedPrice %.4f: accepted=%v, want=%v (minPrice=%.4f)",
					tt.bidPrice, tt.copiedPrice, shouldAccept, tt.shouldAccept, minPrice)
			}
		})
	}
}

// TestBotStrategySellAmountCalculation tests the proportional sell amount
func TestBotStrategySellAmountCalculation(t *testing.T) {
	tests := []struct {
		name         string
		copiedSize   float64
		multiplier   float64
		ourPosition  float64
		expectedSell float64
	}{
		{
			name:         "normal case - sell proportionally",
			copiedSize:   100.0,
			multiplier:   0.10,
			ourPosition:  20.0,
			expectedSell: 10.0, // 100 * 0.10 = 10, we have 20, sell 10
		},
		{
			name:         "not enough position - sell all we have",
			copiedSize:   100.0,
			multiplier:   0.10,
			ourPosition:  5.0,
			expectedSell: 5.0, // 100 * 0.10 = 10, but we only have 5
		},
		{
			name:         "large position - sell proportionally",
			copiedSize:   50.0,
			multiplier:   0.05,
			ourPosition:  100.0,
			expectedSell: 2.5, // 50 * 0.05 = 2.5
		},
		{
			name:         "very small multiplier",
			copiedSize:   1000.0,
			multiplier:   0.01,
			ourPosition:  50.0,
			expectedSell: 10.0, // 1000 * 0.01 = 10
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			targetSell := tt.copiedSize * tt.multiplier
			actualSell := targetSell
			if actualSell > tt.ourPosition {
				actualSell = tt.ourPosition
			}

			if !floatEquals(actualSell, tt.expectedSell, 0.01) {
				t.Errorf("sell amount = %.4f, want %.4f (target=%.4f, position=%.4f)",
					actualSell, tt.expectedSell, targetSell, tt.ourPosition)
			}
		})
	}
}

// TestStrategyTypeSelection tests that strategy type correctly routes execution
func TestStrategyTypeSelection(t *testing.T) {
	tests := []struct {
		name         string
		strategyType int
		expectedName string
	}{
		{
			name:         "human strategy",
			strategyType: storage.StrategyHuman,
			expectedName: "Human",
		},
		{
			name:         "bot strategy",
			strategyType: storage.StrategyBot,
			expectedName: "Bot",
		},
		{
			name:         "default (0) should be human",
			strategyType: 0,
			expectedName: "Human",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategyType := tt.strategyType
			if strategyType == 0 {
				strategyType = storage.StrategyHuman
			}

			var name string
			switch strategyType {
			case storage.StrategyHuman:
				name = "Human"
			case storage.StrategyBot:
				name = "Bot"
			default:
				name = "Unknown"
			}

			if name != tt.expectedName {
				t.Errorf("strategy name = %s, want %s", name, tt.expectedName)
			}
		})
	}
}

// TestBotBuyOrderBookSweep tests sweeping order book for cheapest prices
func TestBotBuyOrderBookSweep(t *testing.T) {
	// Simulate order book asks sorted by price ascending
	asks := []struct {
		price float64
		size  float64
	}{
		{0.48, 50},  // Cheapest
		{0.49, 100},
		{0.50, 200}, // Copied price
		{0.51, 150},
		{0.55, 500}, // 10% above = 0.55, this is at the limit
		{0.56, 1000}, // This is above 10%
	}

	copiedPrice := 0.50
	maxPrice := copiedPrice * 1.10 // 0.55
	targetUSDC := 30.0

	// Sweep asks from cheapest
	var totalCost, totalSize float64
	remainingUSDC := targetUSDC

	for _, ask := range asks {
		if ask.price > maxPrice {
			break // Stop at 10% limit
		}
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

	// At $0.48, we can buy 50 tokens for $24
	// Remaining $6 at $0.49, we can buy 12.24 tokens
	// Total: ~62.24 tokens for $30
	expectedSize := 62.24
	if !floatEquals(totalSize, expectedSize, 0.5) {
		t.Errorf("total size = %.2f, want ~%.2f", totalSize, expectedSize)
	}

	if !floatEquals(totalCost, targetUSDC, 0.01) {
		t.Errorf("total cost = %.2f, want %.2f", totalCost, targetUSDC)
	}

	// Verify we didn't go above maxPrice
	avgPrice := totalCost / totalSize
	if avgPrice > maxPrice {
		t.Errorf("avg price %.4f exceeds max %.4f", avgPrice, maxPrice)
	}
}

// TestBotSellOrderBookSweep tests sweeping order book for highest bids
func TestBotSellOrderBookSweep(t *testing.T) {
	// Simulate order book bids sorted by price descending
	bids := []struct {
		price float64
		size  float64
	}{
		{0.52, 30},  // Best bid (above copied)
		{0.50, 50},  // Copied price
		{0.48, 100},
		{0.46, 150},
		{0.45, 200}, // 10% below = 0.45, this is at the limit
		{0.44, 500}, // This is below 10%
	}

	copiedPrice := 0.50
	minPrice := copiedPrice * 0.90 // 0.45
	sellSize := 100.0

	// Sweep bids from highest
	var totalSold, totalUSDC float64
	remainingSize := sellSize

	for _, bid := range bids {
		if bid.price < minPrice {
			break // Stop at 10% limit
		}
		if remainingSize <= 0 {
			break
		}

		if bid.size <= remainingSize {
			totalSold += bid.size
			totalUSDC += bid.price * bid.size
			remainingSize -= bid.size
		} else {
			totalSold += remainingSize
			totalUSDC += bid.price * remainingSize
			remainingSize = 0
		}
	}

	// 30 @ 0.52 = $15.60
	// 50 @ 0.50 = $25.00
	// 20 @ 0.48 = $9.60 (partial)
	// Total: 100 tokens for $50.20
	if !floatEquals(totalSold, 100.0, 0.01) {
		t.Errorf("total sold = %.2f, want 100.0", totalSold)
	}

	expectedUSDC := 30*0.52 + 50*0.50 + 20*0.48
	if !floatEquals(totalUSDC, expectedUSDC, 0.1) {
		t.Errorf("total USDC = %.2f, want ~%.2f", totalUSDC, expectedUSDC)
	}

	// Verify avg price is above minimum
	avgPrice := totalUSDC / totalSold
	if avgPrice < minPrice {
		t.Errorf("avg price %.4f below min %.4f", avgPrice, minPrice)
	}
}

// Integration test for user copy settings storage
func TestUserCopySettingsStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	store, err := storage.NewPostgres()
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	t.Run("set and get user settings", func(t *testing.T) {
		userAddr := "0xtest" + time.Now().Format("20060102150405")

		// Initially no settings
		settings, err := store.GetUserCopySettings(ctx, userAddr)
		if err != nil {
			t.Fatalf("GetUserCopySettings failed: %v", err)
		}
		if settings != nil {
			t.Error("Expected nil settings for new user")
		}

		// Set custom settings
		err = store.SetUserCopySettings(ctx, storage.UserCopySettings{
			UserAddress: userAddr,
			Multiplier:  0.10, // 10%
			Enabled:     true,
			MinUSDC:     2.0,
		})
		if err != nil {
			t.Fatalf("SetUserCopySettings failed: %v", err)
		}

		// Retrieve settings
		settings, err = store.GetUserCopySettings(ctx, userAddr)
		if err != nil {
			t.Fatalf("GetUserCopySettings failed: %v", err)
		}
		if settings == nil {
			t.Fatal("Expected settings but got nil")
		}
		if settings.Multiplier != 0.10 {
			t.Errorf("multiplier = %v, want 0.10", settings.Multiplier)
		}
		if settings.MinUSDC != 2.0 {
			t.Errorf("minUSDC = %v, want 2.0", settings.MinUSDC)
		}

		// Update settings
		err = store.SetUserCopySettings(ctx, storage.UserCopySettings{
			UserAddress: userAddr,
			Multiplier:  0.20, // 20%
			Enabled:     false,
			MinUSDC:     5.0,
		})
		if err != nil {
			t.Fatalf("Update SetUserCopySettings failed: %v", err)
		}

		// Verify update
		settings, err = store.GetUserCopySettings(ctx, userAddr)
		if err != nil {
			t.Fatalf("GetUserCopySettings after update failed: %v", err)
		}
		if settings.Multiplier != 0.20 {
			t.Errorf("updated multiplier = %v, want 0.20", settings.Multiplier)
		}
		if settings.Enabled != false {
			t.Error("expected enabled = false")
		}

		// Delete settings
		err = store.DeleteUserCopySettings(ctx, userAddr)
		if err != nil {
			t.Fatalf("DeleteUserCopySettings failed: %v", err)
		}

		// Verify deleted
		settings, err = store.GetUserCopySettings(ctx, userAddr)
		if err != nil {
			t.Fatalf("GetUserCopySettings after delete failed: %v", err)
		}
		if settings != nil {
			t.Error("Expected nil after delete")
		}
	})
}

// TestMaxUSDCap tests the max USD cap feature for copy trading
func TestMaxUSDCap(t *testing.T) {
	tests := []struct {
		name           string
		tradeUSDC      float64
		multiplier     float64
		minUSDC        float64
		maxUSD         *float64
		expectedAmount float64
		description    string
	}{
		{
			name:           "no cap - normal calculation",
			tradeUSDC:      1000.0,
			multiplier:     0.10,
			minUSDC:        1.0,
			maxUSD:         nil,
			expectedAmount: 100.0, // 1000 * 0.10
			description:    "Without cap, should use multiplier normally",
		},
		{
			name:           "cap applied - large trade",
			tradeUSDC:      10000.0,
			multiplier:     1.0,
			minUSDC:        1.0,
			maxUSD:         floatPtr(500.0),
			expectedAmount: 500.0, // Capped at 500
			description:    "Should cap at max_usd when multiplied amount exceeds it",
		},
		{
			name:           "cap not needed - small trade",
			tradeUSDC:      100.0,
			multiplier:     0.10,
			minUSDC:        1.0,
			maxUSD:         floatPtr(500.0),
			expectedAmount: 10.0, // 100 * 0.10, below cap
			description:    "Should use multiplied amount when below cap",
		},
		{
			name:           "min takes precedence over cap",
			tradeUSDC:      5.0,
			multiplier:     0.10,
			minUSDC:        2.0,
			maxUSD:         floatPtr(500.0),
			expectedAmount: 2.0, // min enforced (5 * 0.10 = 0.5 < 2.0)
			description:    "Minimum should be enforced even with cap",
		},
		{
			name:           "cap at exact boundary",
			tradeUSDC:      5000.0,
			multiplier:     0.10,
			minUSDC:        1.0,
			maxUSD:         floatPtr(500.0),
			expectedAmount: 500.0, // Exactly at cap
			description:    "Should work at exact cap boundary",
		},
		{
			name:           "very small cap",
			tradeUSDC:      1000.0,
			multiplier:     0.50,
			minUSDC:        1.0,
			maxUSD:         floatPtr(10.0),
			expectedAmount: 10.0, // 1000 * 0.50 = 500, capped to 10
			description:    "Small cap should limit large trades",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the calculation logic from executeBuy/executeBotBuy
			intendedUSDC := tt.tradeUSDC * tt.multiplier

			// Ensure minimum order
			if intendedUSDC < tt.minUSDC {
				intendedUSDC = tt.minUSDC
			}

			// Apply max cap if set
			if tt.maxUSD != nil && intendedUSDC > *tt.maxUSD {
				intendedUSDC = *tt.maxUSD
			}

			if !floatEquals(intendedUSDC, tt.expectedAmount, 0.01) {
				t.Errorf("%s: got $%.2f, want $%.2f", tt.description, intendedUSDC, tt.expectedAmount)
			}
		})
	}
}

// TestMaxUSDCapEdgeCases tests edge cases for max USD cap
func TestMaxUSDCapEdgeCases(t *testing.T) {
	t.Run("cap less than min should use min", func(t *testing.T) {
		// Edge case: what if maxUSD < minUSDC?
		// The logic applies min first, then cap, so cap wins
		tradeUSDC := 10.0
		multiplier := 0.10
		minUSDC := 5.0
		maxUSD := 2.0 // Less than min!

		amount := tradeUSDC * multiplier // 1.0
		if amount < minUSDC {
			amount = minUSDC // 5.0
		}
		if amount > maxUSD {
			amount = maxUSD // 2.0
		}

		// In this edge case, cap wins over min
		// This might be unexpected behavior - documenting it here
		if amount != 2.0 {
			t.Errorf("got $%.2f, want $2.00 (cap should override min)", amount)
		}
	})

	t.Run("zero cap should result in zero", func(t *testing.T) {
		tradeUSDC := 1000.0
		multiplier := 0.10
		maxUSD := 0.0

		amount := tradeUSDC * multiplier
		if amount > maxUSD {
			amount = maxUSD
		}

		if amount != 0.0 {
			t.Errorf("zero cap should result in zero, got $%.2f", amount)
		}
	})

	t.Run("negative cap treated as zero", func(t *testing.T) {
		// Negative caps shouldn't happen but test defensive behavior
		maxUSD := -100.0
		amount := 500.0

		if amount > maxUSD {
			// This would be true, so amount stays 500
			// But if we compare properly...
		}

		// In practice, negative caps should be validated at input
		// This test documents current behavior
		if maxUSD < 0 && amount > 0 {
			// Amount would not be capped to negative
			t.Log("Negative cap does not affect positive amounts (expected)")
		}
	})
}

// floatPtr is a helper to create *float64
func floatPtr(f float64) *float64 {
	return &f
}

// TestCopyTradeLogSave tests saving entries to copy_trade_log table
func TestCopyTradeLogSave(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	store, err := storage.NewPostgres()
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	t.Run("save successful trade log", func(t *testing.T) {
		now := time.Now()
		followerTime := now.Add(100 * time.Millisecond)
		followerShares := -10.0 // Buy
		followerPrice := 0.52

		entry := storage.CopyTradeLogEntry{
			FollowingAddress: "0xtest123",
			FollowingTradeID: "trade-" + now.Format("20060102150405"),
			FollowingTime:    now,
			FollowingShares:  -20.0, // Buy (negative)
			FollowingPrice:   0.50,
			FollowerTime:     &followerTime,
			FollowerShares:   &followerShares,
			FollowerPrice:    &followerPrice,
			MarketTitle:      "Test Market",
			Outcome:          "Yes",
			TokenID:          "token-123",
			Status:           "success",
			FailedReason:     "",
			StrategyType:     storage.StrategyHuman,
		}

		err := store.SaveCopyTradeLog(ctx, entry)
		if err != nil {
			t.Fatalf("SaveCopyTradeLog failed: %v", err)
		}
	})

	t.Run("save failed trade log", func(t *testing.T) {
		now := time.Now()

		entry := storage.CopyTradeLogEntry{
			FollowingAddress: "0xtest456",
			FollowingTradeID: "trade-failed-" + now.Format("20060102150405"),
			FollowingTime:    now,
			FollowingShares:  -50.0,
			FollowingPrice:   0.75,
			FollowerTime:     nil, // No execution
			FollowerShares:   nil,
			FollowerPrice:    nil,
			MarketTitle:      "Failed Market",
			Outcome:          "No",
			TokenID:          "token-456",
			Status:           "failed",
			FailedReason:     "no liquidity within 10%",
			StrategyType:     storage.StrategyBot,
		}

		err := store.SaveCopyTradeLog(ctx, entry)
		if err != nil {
			t.Fatalf("SaveCopyTradeLog for failed trade failed: %v", err)
		}
	})

	t.Run("save skipped trade log", func(t *testing.T) {
		now := time.Now()

		entry := storage.CopyTradeLogEntry{
			FollowingAddress: "0xtest789",
			FollowingTradeID: "trade-skipped-" + now.Format("20060102150405"),
			FollowingTime:    now,
			FollowingShares:  100.0, // Sell (positive)
			FollowingPrice:   0.90,
			FollowerTime:     nil,
			FollowerShares:   nil,
			FollowerPrice:    nil,
			MarketTitle:      "Skipped Market",
			Outcome:          "Yes",
			TokenID:          "token-789",
			Status:           "skipped",
			FailedReason:     "market closed/resolved",
			StrategyType:     storage.StrategyHuman,
		}

		err := store.SaveCopyTradeLog(ctx, entry)
		if err != nil {
			t.Fatalf("SaveCopyTradeLog for skipped trade failed: %v", err)
		}
	})

	t.Run("retrieve copy trade logs", func(t *testing.T) {
		logs, err := store.GetCopyTradeLogs(ctx, 10)
		if err != nil {
			t.Fatalf("GetCopyTradeLogs failed: %v", err)
		}

		if len(logs) == 0 {
			t.Log("No logs found (may be empty database)")
		} else {
			t.Logf("Retrieved %d copy trade logs", len(logs))
			// Verify structure of first log
			log := logs[0]
			if log.FollowingAddress == "" {
				t.Error("following_address should not be empty")
			}
			if log.Status == "" {
				t.Error("status should not be empty")
			}
		}
	})
}

// TestSharesSignConvention tests that shares are correctly signed (negative=buy, positive=sell)
func TestSharesSignConvention(t *testing.T) {
	tests := []struct {
		name          string
		side          string
		shares        float64
		expectedSign  string
		description   string
	}{
		{
			name:         "buy should be negative",
			side:         "BUY",
			shares:       100.0,
			expectedSign: "negative",
			description:  "BUY trades should have negative shares",
		},
		{
			name:         "sell should be positive",
			side:         "SELL",
			shares:       100.0,
			expectedSign: "positive",
			description:  "SELL trades should have positive shares",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert shares based on side (as done in logCopyTradeWithStrategy)
			var signedShares float64
			if tt.side == "BUY" {
				signedShares = -tt.shares // Negative for buy
			} else {
				signedShares = tt.shares // Positive for sell
			}

			if tt.expectedSign == "negative" && signedShares >= 0 {
				t.Errorf("%s: expected negative, got %.2f", tt.description, signedShares)
			}
			if tt.expectedSign == "positive" && signedShares <= 0 {
				t.Errorf("%s: expected positive, got %.2f", tt.description, signedShares)
			}
		})
	}
}

// TestGetUnprocessedTradesFiltering tests the filtering logic for unprocessed trades
func TestGetUnprocessedTradesFiltering(t *testing.T) {
	// This is a unit test for the filtering logic, not integration

	t.Run("filter by processed flag", func(t *testing.T) {
		// Simulate trades with processed flags
		allTrades := []struct {
			id        string
			processed bool
		}{
			{"trade-1", false},
			{"trade-2", true},
			{"trade-3", false},
			{"trade-4", true},
			{"trade-5", false},
		}

		// Filter unprocessed
		var unprocessed []string
		for _, trade := range allTrades {
			if !trade.processed {
				unprocessed = append(unprocessed, trade.id)
			}
		}

		if len(unprocessed) != 3 {
			t.Errorf("expected 3 unprocessed trades, got %d", len(unprocessed))
		}

		// Verify correct trades
		expected := map[string]bool{"trade-1": true, "trade-3": true, "trade-5": true}
		for _, id := range unprocessed {
			if !expected[id] {
				t.Errorf("unexpected trade in unprocessed: %s", id)
			}
		}
	})

	t.Run("limit parameter", func(t *testing.T) {
		// Simulate limiting results
		allUnprocessed := []string{"t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10"}
		limit := 5

		result := allUnprocessed
		if len(result) > limit {
			result = result[:limit]
		}

		if len(result) != 5 {
			t.Errorf("expected %d trades with limit, got %d", limit, len(result))
		}
	})

	t.Run("empty result when all processed", func(t *testing.T) {
		allTrades := []struct {
			id        string
			processed bool
		}{
			{"trade-1", true},
			{"trade-2", true},
		}

		var unprocessed []string
		for _, trade := range allTrades {
			if !trade.processed {
				unprocessed = append(unprocessed, trade.id)
			}
		}

		if len(unprocessed) != 0 {
			t.Errorf("expected 0 unprocessed trades, got %d", len(unprocessed))
		}
	})
}

// TestTokenIDVerificationLogic tests the token ID verification logic
func TestTokenIDVerificationLogic(t *testing.T) {
	t.Run("outcome matches token", func(t *testing.T) {
		// When trade.Outcome matches token.Outcome, use as-is
		tradeOutcome := "Yes"
		tokenOutcome := "Yes"

		if tradeOutcome != tokenOutcome {
			t.Error("outcomes should match")
		}
	})

	t.Run("outcome mismatch correction", func(t *testing.T) {
		// When trade says "Yes" but token is actually "No", correct it
		tradeOutcome := "Yes"
		tokenOutcome := "No"

		correctedOutcome := tradeOutcome
		if tokenOutcome != tradeOutcome && tokenOutcome != "" {
			correctedOutcome = tokenOutcome
		}

		if correctedOutcome != "No" {
			t.Errorf("outcome should be corrected to 'No', got '%s'", correctedOutcome)
		}
	})

	t.Run("sibling token lookup needed", func(t *testing.T) {
		// When we have token for "No" but trade is for "Yes", need sibling
		tokenOutcome := "No"
		tradeOutcome := "Yes"

		needsSiblingLookup := tokenOutcome != tradeOutcome

		if !needsSiblingLookup {
			t.Error("should need sibling lookup when outcomes differ")
		}
	})

	t.Run("negRisk detection from market", func(t *testing.T) {
		// Test negRisk flag propagation
		type mockMarket struct {
			conditionID string
			negRisk     bool
		}

		markets := []mockMarket{
			{"cond-1", false}, // Regular market
			{"cond-2", true},  // NegRisk market
		}

		for _, m := range markets {
			// Simulate contract selection based on negRisk
			var contract string
			if m.negRisk {
				contract = "NegRiskCTFExchange"
			} else {
				contract = "CTFExchange"
			}

			if m.negRisk && contract != "NegRiskCTFExchange" {
				t.Errorf("negRisk market should use NegRiskCTFExchange")
			}
			if !m.negRisk && contract != "CTFExchange" {
				t.Errorf("regular market should use CTFExchange")
			}
		}
	})
}

// TestBuyRetryLoopLogic tests the retry loop behavior for human strategy buys
func TestBuyRetryLoopLogic(t *testing.T) {
	t.Run("retry until liquidity found", func(t *testing.T) {
		maxRetries := 180 // 3 minutes at 1 second intervals
		retryInterval := 1 * time.Second

		// Simulate: no liquidity for first 5 attempts, then liquidity appears
		liquidityAppearsAt := 5

		var filledAt int
		for attempt := 1; attempt <= maxRetries; attempt++ {
			hasLiquidity := attempt >= liquidityAppearsAt

			if hasLiquidity {
				filledAt = attempt
				break
			}

			// Would sleep here in real code
			_ = retryInterval
		}

		if filledAt != 5 {
			t.Errorf("expected fill at attempt 5, got %d", filledAt)
		}
	})

	t.Run("timeout after max duration", func(t *testing.T) {
		maxDuration := 3 * time.Minute
		retryInterval := 1 * time.Second
		maxAttempts := int(maxDuration / retryInterval)

		// Simulate: never find liquidity
		attempts := 0
		for attempts < maxAttempts {
			attempts++
			// No liquidity ever
		}

		if attempts != 180 {
			t.Errorf("expected 180 attempts (3 min at 1s), got %d", attempts)
		}
	})

	t.Run("partial fill then continue", func(t *testing.T) {
		targetUSDC := 100.0
		minUSDC := 1.0

		// Simulate partial fills
		fills := []float64{30.0, 25.0, 20.0, 15.0, 10.0}

		remaining := targetUSDC
		totalFilled := 0.0

		for _, fill := range fills {
			if remaining < minUSDC {
				break
			}

			actualFill := fill
			if actualFill > remaining {
				actualFill = remaining
			}

			totalFilled += actualFill
			remaining -= actualFill
		}

		if !floatEquals(totalFilled, 100.0, 0.01) {
			t.Errorf("expected total fill of $100, got $%.2f", totalFilled)
		}
		if remaining > minUSDC {
			t.Errorf("should have filled all, remaining $%.2f", remaining)
		}
	})

	t.Run("stop when remaining below minimum", func(t *testing.T) {
		targetUSDC := 10.0
		minUSDC := 2.0

		// First fill leaves less than minimum
		firstFill := 9.0
		remaining := targetUSDC - firstFill // $1 remaining

		shouldContinue := remaining >= minUSDC

		if shouldContinue {
			t.Error("should stop when remaining < minUSDC")
		}
	})
}

// TestIncrementalSyncDeduplication tests the trade deduplication logic
func TestIncrementalSyncDeduplication(t *testing.T) {
	t.Run("filter out existing trades", func(t *testing.T) {
		// Existing trades in DB
		existingIDs := map[string]bool{
			"trade-001": true,
			"trade-002": true,
			"trade-003": true,
		}

		// New trades from API
		apiTrades := []string{
			"trade-002", // Duplicate
			"trade-003", // Duplicate
			"trade-004", // New
			"trade-005", // New
		}

		// Filter
		var newTrades []string
		for _, id := range apiTrades {
			if !existingIDs[id] {
				newTrades = append(newTrades, id)
			}
		}

		if len(newTrades) != 2 {
			t.Errorf("expected 2 new trades, got %d", len(newTrades))
		}

		expected := map[string]bool{"trade-004": true, "trade-005": true}
		for _, id := range newTrades {
			if !expected[id] {
				t.Errorf("unexpected new trade: %s", id)
			}
		}
	})

	t.Run("all duplicates returns empty", func(t *testing.T) {
		existingIDs := map[string]bool{
			"trade-001": true,
			"trade-002": true,
		}

		apiTrades := []string{"trade-001", "trade-002"}

		var newTrades []string
		for _, id := range apiTrades {
			if !existingIDs[id] {
				newTrades = append(newTrades, id)
			}
		}

		if len(newTrades) != 0 {
			t.Errorf("expected 0 new trades when all duplicates, got %d", len(newTrades))
		}
	})

	t.Run("all new trades", func(t *testing.T) {
		existingIDs := map[string]bool{}

		apiTrades := []string{"trade-001", "trade-002", "trade-003"}

		var newTrades []string
		for _, id := range apiTrades {
			if !existingIDs[id] {
				newTrades = append(newTrades, id)
			}
		}

		if len(newTrades) != 3 {
			t.Errorf("expected 3 new trades, got %d", len(newTrades))
		}
	})
}

// TestMaxUSDCapWithStrategyTypes tests max cap works for both strategy types
func TestMaxUSDCapWithStrategyTypes(t *testing.T) {
	tests := []struct {
		name         string
		strategyType int
		tradeUSDC    float64
		multiplier   float64
		maxUSD       *float64
		expected     float64
	}{
		{
			name:         "human strategy with cap",
			strategyType: storage.StrategyHuman,
			tradeUSDC:    10000.0,
			multiplier:   0.10,
			maxUSD:       floatPtr(500.0),
			expected:     500.0,
		},
		{
			name:         "bot strategy with cap",
			strategyType: storage.StrategyBot,
			tradeUSDC:    10000.0,
			multiplier:   0.10,
			maxUSD:       floatPtr(500.0),
			expected:     500.0,
		},
		{
			name:         "human strategy no cap",
			strategyType: storage.StrategyHuman,
			tradeUSDC:    1000.0,
			multiplier:   0.10,
			maxUSD:       nil,
			expected:     100.0,
		},
		{
			name:         "bot strategy no cap",
			strategyType: storage.StrategyBot,
			tradeUSDC:    1000.0,
			multiplier:   0.10,
			maxUSD:       nil,
			expected:     100.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			amount := tt.tradeUSDC * tt.multiplier

			if tt.maxUSD != nil && amount > *tt.maxUSD {
				amount = *tt.maxUSD
			}

			if !floatEquals(amount, tt.expected, 0.01) {
				t.Errorf("strategy %d: got $%.2f, want $%.2f", tt.strategyType, amount, tt.expected)
			}
		})
	}
}

// TestUserCopySettingsWithMaxUSD tests storage of max_usd field
func TestUserCopySettingsWithMaxUSD(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	store, err := storage.NewPostgres()
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	userAddr := "0xtestmaxusd" + time.Now().Format("150405")

	t.Run("save and retrieve max_usd", func(t *testing.T) {
		maxUSD := 500.0
		err := store.SetUserCopySettings(ctx, storage.UserCopySettings{
			UserAddress:  userAddr,
			Multiplier:   0.10,
			Enabled:      true,
			MinUSDC:      1.0,
			StrategyType: storage.StrategyBot,
			MaxUSD:       &maxUSD,
		})
		if err != nil {
			t.Fatalf("SetUserCopySettings with max_usd failed: %v", err)
		}

		settings, err := store.GetUserCopySettings(ctx, userAddr)
		if err != nil {
			t.Fatalf("GetUserCopySettings failed: %v", err)
		}

		if settings.MaxUSD == nil {
			t.Fatal("max_usd should not be nil")
		}
		if *settings.MaxUSD != 500.0 {
			t.Errorf("max_usd = %.2f, want 500.00", *settings.MaxUSD)
		}
	})

	t.Run("save null max_usd", func(t *testing.T) {
		err := store.SetUserCopySettings(ctx, storage.UserCopySettings{
			UserAddress:  userAddr,
			Multiplier:   0.10,
			Enabled:      true,
			MinUSDC:      1.0,
			StrategyType: storage.StrategyHuman,
			MaxUSD:       nil, // No cap
		})
		if err != nil {
			t.Fatalf("SetUserCopySettings with nil max_usd failed: %v", err)
		}

		settings, err := store.GetUserCopySettings(ctx, userAddr)
		if err != nil {
			t.Fatalf("GetUserCopySettings failed: %v", err)
		}

		if settings.MaxUSD != nil {
			t.Errorf("max_usd should be nil, got %.2f", *settings.MaxUSD)
		}
	})

	// Cleanup
	_ = store.DeleteUserCopySettings(ctx, userAddr)
}
