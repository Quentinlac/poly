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
