package syncer

import (
	"testing"

	"polymarket-analyzer/api"
)

func TestAnalyzeBotBuy(t *testing.T) {
	tests := []struct {
		name           string
		book           *api.OrderBook
		copiedPrice    float64
		targetUSDC     float64
		wantCanExecute bool
		wantTotalSize  float64
		wantTotalCost  float64
		wantSkipReason string
	}{
		{
			name: "normal execution - single ask level",
			book: &api.OrderBook{
				Asks: []api.OrderBookLevel{
					{Price: "0.50", Size: "100"},
				},
			},
			copiedPrice:    0.50,
			targetUSDC:     25.0,
			wantCanExecute: true,
			wantTotalSize:  50.0, // 25 / 0.50
			wantTotalCost:  25.0,
		},
		{
			name: "normal execution - multiple ask levels",
			book: &api.OrderBook{
				Asks: []api.OrderBookLevel{
					{Price: "0.48", Size: "50"},  // $24 total
					{Price: "0.50", Size: "100"}, // $50 total
					{Price: "0.52", Size: "200"}, // $104 total - within 10%
				},
			},
			copiedPrice:    0.50,
			targetUSDC:     30.0,
			wantCanExecute: true,
			wantTotalSize:  62.0,                          // 50 + 12 (partial at 0.50)
			wantTotalCost:  30.0,                          // 24 + 6
			wantSkipReason: "",
		},
		{
			name: "best ask above 10% limit - should skip",
			book: &api.OrderBook{
				Asks: []api.OrderBookLevel{
					{Price: "0.60", Size: "100"}, // 20% above 0.50
				},
			},
			copiedPrice:    0.50,
			targetUSDC:     25.0,
			wantCanExecute: false,
			wantSkipReason: "no liquidity within 10%",
		},
		{
			name: "asks within 10% limit - 0.55 is exactly 10%",
			book: &api.OrderBook{
				Asks: []api.OrderBookLevel{
					{Price: "0.55", Size: "100"}, // Exactly 10% above
				},
			},
			copiedPrice:    0.50,
			targetUSDC:     27.5,
			wantCanExecute: true,
			wantTotalSize:  50.0,
			wantTotalCost:  27.5,
		},
		{
			name: "asks just above 10% limit - 0.551 should be rejected",
			book: &api.OrderBook{
				Asks: []api.OrderBookLevel{
					{Price: "0.551", Size: "100"}, // Just above 10%
				},
			},
			copiedPrice:    0.50,
			targetUSDC:     25.0,
			wantCanExecute: false,
			wantSkipReason: "no liquidity within 10%",
		},
		{
			name: "empty order book",
			book: &api.OrderBook{
				Asks: []api.OrderBookLevel{},
			},
			copiedPrice:    0.50,
			targetUSDC:     25.0,
			wantCanExecute: false,
			wantSkipReason: "empty order book",
		},
		{
			name:           "nil order book",
			book:           nil,
			copiedPrice:    0.50,
			targetUSDC:     25.0,
			wantCanExecute: false,
			wantSkipReason: "empty order book",
		},
		{
			name: "partial liquidity - not enough to fill target",
			book: &api.OrderBook{
				Asks: []api.OrderBookLevel{
					{Price: "0.50", Size: "10"}, // Only $5 available
				},
			},
			copiedPrice:    0.50,
			targetUSDC:     25.0,
			wantCanExecute: true,
			wantTotalSize:  10.0,
			wantTotalCost:  5.0,
		},
		{
			name: "very small liquidity - should skip",
			book: &api.OrderBook{
				Asks: []api.OrderBookLevel{
					{Price: "0.50", Size: "0.001"},
				},
			},
			copiedPrice:    0.50,
			targetUSDC:     25.0,
			wantCanExecute: false,
			wantSkipReason: "insufficient affordable liquidity",
		},
		{
			name: "sweep multiple levels within 10%",
			book: &api.OrderBook{
				Asks: []api.OrderBookLevel{
					{Price: "0.48", Size: "50"},  // $24 total
					{Price: "0.49", Size: "50"},  // $24.50 total
					{Price: "0.50", Size: "50"},  // $25 total
					{Price: "0.55", Size: "50"},  // At 10% limit, $27.50 total
					{Price: "0.56", Size: "1000"}, // Above limit - should be ignored
				},
			},
			copiedPrice:    0.50,
			targetUSDC:     100.0,
			wantCanExecute: true,
			// 50@0.48=$24 + 50@0.49=$24.50 + 50@0.50=$25 + (100-73.50)/0.55=48.18@0.55
			// Total: 50+50+50+48.18 = 198.18 tokens
			wantTotalSize:  198.18,
			wantTotalCost:  100.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AnalyzeBotBuy(tt.book, tt.copiedPrice, tt.targetUSDC)

			if result.CanExecute != tt.wantCanExecute {
				t.Errorf("CanExecute = %v, want %v", result.CanExecute, tt.wantCanExecute)
			}

			if tt.wantCanExecute {
				if !floatEquals(result.TotalSize, tt.wantTotalSize, 0.5) {
					t.Errorf("TotalSize = %.4f, want %.4f", result.TotalSize, tt.wantTotalSize)
				}
				if !floatEquals(result.TotalCost, tt.wantTotalCost, 0.1) {
					t.Errorf("TotalCost = %.4f, want %.4f", result.TotalCost, tt.wantTotalCost)
				}
			} else {
				if tt.wantSkipReason != "" && !contains(result.SkipReason, tt.wantSkipReason) {
					t.Errorf("SkipReason = %q, want to contain %q", result.SkipReason, tt.wantSkipReason)
				}
			}
		})
	}
}

func TestAnalyzeBotSell(t *testing.T) {
	tests := []struct {
		name              string
		book              *api.OrderBook
		copiedPrice       float64
		sellSize          float64
		wantCanExecute    bool
		wantTotalSold     float64
		wantTotalUSDC     float64
		wantNeedsLimit    bool
		wantRemainingSize float64
		wantSkipReason    string
	}{
		{
			name: "normal execution - single bid level",
			book: &api.OrderBook{
				Bids: []api.OrderBookLevel{
					{Price: "0.50", Size: "100"},
				},
			},
			copiedPrice:    0.50,
			sellSize:       50.0,
			wantCanExecute: true,
			wantTotalSold:  50.0,
			wantTotalUSDC:  25.0, // 50 * 0.50
		},
		{
			name: "sweep multiple bid levels",
			book: &api.OrderBook{
				Bids: []api.OrderBookLevel{
					{Price: "0.52", Size: "30"},  // Best bid - above copied
					{Price: "0.50", Size: "50"},  // At copied price
					{Price: "0.48", Size: "100"}, // Below copied but within 10%
				},
			},
			copiedPrice:    0.50,
			sellSize:       100.0,
			wantCanExecute: true,
			wantTotalSold:  100.0,
			wantTotalUSDC:  50.2, // 30*0.52 + 50*0.50 + 20*0.48
		},
		{
			name: "bids below 10% limit - should skip",
			book: &api.OrderBook{
				Bids: []api.OrderBookLevel{
					{Price: "0.40", Size: "100"}, // 20% below
				},
			},
			copiedPrice:    0.50,
			sellSize:       50.0,
			wantCanExecute: false,
			wantNeedsLimit: true,
			wantSkipReason: "no bids within 10%",
		},
		{
			name: "bid exactly at 10% limit - 0.45 is exactly 10% below",
			book: &api.OrderBook{
				Bids: []api.OrderBookLevel{
					{Price: "0.45", Size: "100"}, // Exactly 10% below
				},
			},
			copiedPrice:    0.50,
			sellSize:       50.0,
			wantCanExecute: true,
			wantTotalSold:  50.0,
			wantTotalUSDC:  22.5,
		},
		{
			name: "bid just below 10% limit - 0.449 should be rejected",
			book: &api.OrderBook{
				Bids: []api.OrderBookLevel{
					{Price: "0.449", Size: "100"}, // Just below 10%
				},
			},
			copiedPrice:    0.50,
			sellSize:       50.0,
			wantCanExecute: false,
			wantNeedsLimit: true,
		},
		{
			name: "empty order book",
			book: &api.OrderBook{
				Bids: []api.OrderBookLevel{},
			},
			copiedPrice:    0.50,
			sellSize:       50.0,
			wantCanExecute: false,
			wantNeedsLimit: true,
			wantSkipReason: "no bids in order book",
		},
		{
			name:           "nil order book",
			book:           nil,
			copiedPrice:    0.50,
			sellSize:       50.0,
			wantCanExecute: false,
			wantNeedsLimit: true,
			wantSkipReason: "no bids in order book",
		},
		{
			name: "partial liquidity - remaining size",
			book: &api.OrderBook{
				Bids: []api.OrderBookLevel{
					{Price: "0.50", Size: "30"},
				},
			},
			copiedPrice:       0.50,
			sellSize:          100.0,
			wantCanExecute:    true,
			wantTotalSold:     30.0,
			wantTotalUSDC:     15.0,
			wantRemainingSize: 70.0,
		},
		{
			name: "mixed acceptable and unacceptable bids",
			book: &api.OrderBook{
				Bids: []api.OrderBookLevel{
					{Price: "0.50", Size: "50"},
					{Price: "0.45", Size: "50"},  // At 10% limit
					{Price: "0.40", Size: "500"}, // Below limit - ignored
				},
			},
			copiedPrice:       0.50,
			sellSize:          200.0,
			wantCanExecute:    true,
			wantTotalSold:     100.0,
			wantTotalUSDC:     47.5, // 50*0.50 + 50*0.45
			wantRemainingSize: 100.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AnalyzeBotSell(tt.book, tt.copiedPrice, tt.sellSize)

			if result.CanExecute != tt.wantCanExecute {
				t.Errorf("CanExecute = %v, want %v", result.CanExecute, tt.wantCanExecute)
			}

			if result.NeedsLimitOrder != tt.wantNeedsLimit {
				t.Errorf("NeedsLimitOrder = %v, want %v", result.NeedsLimitOrder, tt.wantNeedsLimit)
			}

			if tt.wantCanExecute {
				if !floatEquals(result.TotalSold, tt.wantTotalSold, 0.1) {
					t.Errorf("TotalSold = %.4f, want %.4f", result.TotalSold, tt.wantTotalSold)
				}
				if !floatEquals(result.TotalUSDC, tt.wantTotalUSDC, 0.1) {
					t.Errorf("TotalUSDC = %.4f, want %.4f", result.TotalUSDC, tt.wantTotalUSDC)
				}
			}

			if tt.wantRemainingSize > 0 {
				if !floatEquals(result.RemainingSize, tt.wantRemainingSize, 0.1) {
					t.Errorf("RemainingSize = %.4f, want %.4f", result.RemainingSize, tt.wantRemainingSize)
				}
			}

			if tt.wantSkipReason != "" && !contains(result.SkipReason, tt.wantSkipReason) {
				t.Errorf("SkipReason = %q, want to contain %q", result.SkipReason, tt.wantSkipReason)
			}
		})
	}
}

func TestPlanLimitOrderFallback(t *testing.T) {
	tests := []struct {
		name        string
		sellSize    float64
		copiedPrice float64
	}{
		{
			name:        "standard case",
			sellSize:    100.0,
			copiedPrice: 0.50,
		},
		{
			name:        "small order",
			sellSize:    10.0,
			copiedPrice: 0.80,
		},
		{
			name:        "large order",
			sellSize:    10000.0,
			copiedPrice: 0.25,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := PlanLimitOrderFallback(tt.sellSize, tt.copiedPrice)

			if len(plan.Orders) != 3 {
				t.Fatalf("expected 3 orders, got %d", len(plan.Orders))
			}

			// Verify percentages
			if plan.Orders[0].Percentage != 20 {
				t.Errorf("order 1 percentage = %.0f, want 20", plan.Orders[0].Percentage)
			}
			if plan.Orders[1].Percentage != 40 {
				t.Errorf("order 2 percentage = %.0f, want 40", plan.Orders[1].Percentage)
			}
			if plan.Orders[2].Percentage != 40 {
				t.Errorf("order 3 percentage = %.0f, want 40", plan.Orders[2].Percentage)
			}

			// Verify sizes
			expectedSize1 := tt.sellSize * 0.20
			expectedSize2 := tt.sellSize * 0.40
			expectedSize3 := tt.sellSize * 0.40

			if !floatEquals(plan.Orders[0].Size, expectedSize1, 0.01) {
				t.Errorf("order 1 size = %.4f, want %.4f", plan.Orders[0].Size, expectedSize1)
			}
			if !floatEquals(plan.Orders[1].Size, expectedSize2, 0.01) {
				t.Errorf("order 2 size = %.4f, want %.4f", plan.Orders[1].Size, expectedSize2)
			}
			if !floatEquals(plan.Orders[2].Size, expectedSize3, 0.01) {
				t.Errorf("order 3 size = %.4f, want %.4f", plan.Orders[2].Size, expectedSize3)
			}

			// Verify prices
			expectedPrice1 := tt.copiedPrice
			expectedPrice2 := tt.copiedPrice * 0.97
			expectedPrice3 := tt.copiedPrice * 0.95

			if !floatEquals(plan.Orders[0].Price, expectedPrice1, 0.0001) {
				t.Errorf("order 1 price = %.4f, want %.4f", plan.Orders[0].Price, expectedPrice1)
			}
			if !floatEquals(plan.Orders[1].Price, expectedPrice2, 0.0001) {
				t.Errorf("order 2 price = %.4f, want %.4f", plan.Orders[1].Price, expectedPrice2)
			}
			if !floatEquals(plan.Orders[2].Price, expectedPrice3, 0.0001) {
				t.Errorf("order 3 price = %.4f, want %.4f", plan.Orders[2].Price, expectedPrice3)
			}

			// Verify total size equals original sell size
			totalSize := plan.Orders[0].Size + plan.Orders[1].Size + plan.Orders[2].Size
			if !floatEquals(totalSize, tt.sellSize, 0.01) {
				t.Errorf("total size = %.4f, want %.4f", totalSize, tt.sellSize)
			}
		})
	}
}

func TestCalculateBotBuyAmount(t *testing.T) {
	tests := []struct {
		name       string
		tradeUSDC  float64
		multiplier float64
		minUSDC    float64
		maxUSD     *float64
		want       float64
	}{
		{
			name:       "normal case - no cap",
			tradeUSDC:  1000.0,
			multiplier: 0.10,
			minUSDC:    1.0,
			maxUSD:     nil,
			want:       100.0,
		},
		{
			name:       "below minimum - bumped up",
			tradeUSDC:  10.0,
			multiplier: 0.05,
			minUSDC:    1.0,
			maxUSD:     nil,
			want:       1.0, // 10 * 0.05 = 0.5 < 1.0
		},
		{
			name:       "with cap - capped",
			tradeUSDC:  10000.0,
			multiplier: 0.10,
			minUSDC:    1.0,
			maxUSD:     floatPtr(500.0),
			want:       500.0, // 10000 * 0.10 = 1000 > 500
		},
		{
			name:       "with cap - below cap",
			tradeUSDC:  100.0,
			multiplier: 0.10,
			minUSDC:    1.0,
			maxUSD:     floatPtr(500.0),
			want:       10.0, // 100 * 0.10 = 10 < 500
		},
		{
			name:       "min takes effect before cap",
			tradeUSDC:  5.0,
			multiplier: 0.10,
			minUSDC:    2.0,
			maxUSD:     floatPtr(500.0),
			want:       2.0, // 5 * 0.10 = 0.5, min 2.0, cap 500
		},
		{
			name:       "cap smaller than min - cap wins",
			tradeUSDC:  100.0,
			multiplier: 0.10,
			minUSDC:    5.0,
			maxUSD:     floatPtr(2.0),
			want:       2.0, // 100 * 0.10 = 10, min 5, cap 2 -> cap wins
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateBotBuyAmount(tt.tradeUSDC, tt.multiplier, tt.minUSDC, tt.maxUSD)
			if !floatEquals(got, tt.want, 0.01) {
				t.Errorf("CalculateBotBuyAmount() = %.4f, want %.4f", got, tt.want)
			}
		})
	}
}

func TestCalculateBotSellAmount(t *testing.T) {
	tests := []struct {
		name        string
		tradeSize   float64
		multiplier  float64
		ourPosition float64
		want        float64
	}{
		{
			name:        "normal case - have enough",
			tradeSize:   100.0,
			multiplier:  0.10,
			ourPosition: 20.0,
			want:        10.0, // 100 * 0.10 = 10
		},
		{
			name:        "not enough position - sell all",
			tradeSize:   100.0,
			multiplier:  0.10,
			ourPosition: 5.0,
			want:        5.0, // Want 10, have 5
		},
		{
			name:        "exact position",
			tradeSize:   100.0,
			multiplier:  0.10,
			ourPosition: 10.0,
			want:        10.0,
		},
		{
			name:        "large position",
			tradeSize:   1000.0,
			multiplier:  0.05,
			ourPosition: 100.0,
			want:        50.0, // 1000 * 0.05 = 50
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculateBotSellAmount(tt.tradeSize, tt.multiplier, tt.ourPosition)
			if !floatEquals(got, tt.want, 0.01) {
				t.Errorf("CalculateBotSellAmount() = %.4f, want %.4f", got, tt.want)
			}
		})
	}
}

func TestIsWithinBuyPriceLimit(t *testing.T) {
	tests := []struct {
		name        string
		askPrice    float64
		copiedPrice float64
		want        bool
	}{
		{"exact price", 0.50, 0.50, true},
		{"5% above", 0.525, 0.50, true},
		{"exactly 10% above", 0.55, 0.50, true},
		{"11% above", 0.555, 0.50, false},
		{"20% above", 0.60, 0.50, false},
		{"below copied price", 0.45, 0.50, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsWithinBuyPriceLimit(tt.askPrice, tt.copiedPrice)
			if got != tt.want {
				t.Errorf("IsWithinBuyPriceLimit(%.4f, %.4f) = %v, want %v", tt.askPrice, tt.copiedPrice, got, tt.want)
			}
		})
	}
}

func TestIsWithinSellPriceLimit(t *testing.T) {
	tests := []struct {
		name        string
		bidPrice    float64
		copiedPrice float64
		want        bool
	}{
		{"exact price", 0.50, 0.50, true},
		{"5% below", 0.475, 0.50, true},
		{"exactly 10% below", 0.45, 0.50, true},
		{"11% below", 0.445, 0.50, false},
		{"20% below", 0.40, 0.50, false},
		{"above copied price", 0.55, 0.50, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsWithinSellPriceLimit(tt.bidPrice, tt.copiedPrice)
			if got != tt.want {
				t.Errorf("IsWithinSellPriceLimit(%.4f, %.4f) = %v, want %v", tt.bidPrice, tt.copiedPrice, got, tt.want)
			}
		})
	}
}

// Test for BUG: verify that the actual executeBotBuy/Sell logic matches these helper functions
func TestBotBuyPriceLimitBoundary(t *testing.T) {
	// Critical boundary test: exactly at 10% should be accepted
	book := &api.OrderBook{
		Asks: []api.OrderBookLevel{
			{Price: "0.550", Size: "100"}, // Exactly 10% above 0.50
		},
	}

	result := AnalyzeBotBuy(book, 0.50, 27.5)
	if !result.CanExecute {
		t.Error("BUG: asks at exactly 10% above should be accepted")
	}

	// Just above 10% should be rejected
	book2 := &api.OrderBook{
		Asks: []api.OrderBookLevel{
			{Price: "0.5501", Size: "100"}, // Just above 10%
		},
	}

	result2 := AnalyzeBotBuy(book2, 0.50, 27.5)
	if result2.CanExecute {
		t.Error("BUG: asks above 10% should be rejected")
	}
}

func TestBotSellPriceLimitBoundary(t *testing.T) {
	// Critical boundary test: exactly at 10% below should be accepted
	book := &api.OrderBook{
		Bids: []api.OrderBookLevel{
			{Price: "0.450", Size: "100"}, // Exactly 10% below 0.50
		},
	}

	result := AnalyzeBotSell(book, 0.50, 50.0)
	if !result.CanExecute {
		t.Error("BUG: bids at exactly 10% below should be accepted")
	}

	// Just below 10% should be rejected
	book2 := &api.OrderBook{
		Bids: []api.OrderBookLevel{
			{Price: "0.4499", Size: "100"}, // Just below 10%
		},
	}

	result2 := AnalyzeBotSell(book2, 0.50, 50.0)
	if result2.CanExecute {
		t.Error("BUG: bids below 10% should be rejected")
	}
}
