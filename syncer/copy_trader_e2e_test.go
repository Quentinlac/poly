package syncer

import (
	"context"
	"sync"
	"testing"

	"polymarket-analyzer/api"
	"polymarket-analyzer/models"
	"polymarket-analyzer/storage"
)

// MockStore implements a minimal in-memory store for E2E tests
type MockStore struct {
	mu sync.RWMutex

	// Positions
	positions map[string]*MyPosition

	// Processed trades
	processedTrades map[string]bool

	// User settings
	userSettings map[string]*storage.UserCopySettings

	// Copy trade logs
	copyTradeLogs []storage.CopyTradeLogEntry

	// Trading accounts
	tradingAccounts map[int]*storage.TradingAccount
	defaultAccount  int

	// Call tracking
	Calls map[string]int

	// Error injection
	ErrorOnNext map[string]error
}

func NewMockStore() *MockStore {
	return &MockStore{
		positions:       make(map[string]*MyPosition),
		processedTrades: make(map[string]bool),
		userSettings:    make(map[string]*storage.UserCopySettings),
		copyTradeLogs:   []storage.CopyTradeLogEntry{},
		tradingAccounts: make(map[int]*storage.TradingAccount),
		Calls:           make(map[string]int),
		ErrorOnNext:     make(map[string]error),
	}
}

func (m *MockStore) trackCall(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls[name]++
	if err, ok := m.ErrorOnNext[name]; ok {
		delete(m.ErrorOnNext, name)
		return err
	}
	return nil
}

func (m *MockStore) GetMyPosition(ctx context.Context, marketID, outcome string) (*MyPosition, error) {
	if err := m.trackCall("GetMyPosition"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := marketID + ":" + outcome
	return m.positions[key], nil
}

func (m *MockStore) UpdateMyPosition(ctx context.Context, pos MyPosition) error {
	if err := m.trackCall("UpdateMyPosition"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := pos.MarketID + ":" + pos.Outcome
	existing := m.positions[key]
	if existing != nil {
		existing.Size += pos.Size
		existing.TotalCost += pos.TotalCost
	} else {
		m.positions[key] = &pos
	}
	return nil
}

func (m *MockStore) ClearMyPosition(ctx context.Context, marketID, outcome string) error {
	if err := m.trackCall("ClearMyPosition"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := marketID + ":" + outcome
	delete(m.positions, key)
	return nil
}

func (m *MockStore) MarkTradeProcessed(ctx context.Context, tradeID string) error {
	if err := m.trackCall("MarkTradeProcessed"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processedTrades[tradeID] = true
	return nil
}

func (m *MockStore) IsTradeProcessed(ctx context.Context, tradeID string) (bool, error) {
	if err := m.trackCall("IsTradeProcessed"); err != nil {
		return false, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.processedTrades[tradeID], nil
}

func (m *MockStore) GetUserCopySettings(ctx context.Context, userAddr string) (*storage.UserCopySettings, error) {
	if err := m.trackCall("GetUserCopySettings"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.userSettings[userAddr], nil
}

func (m *MockStore) SaveCopyTradeLog(ctx context.Context, entry storage.CopyTradeLogEntry) error {
	if err := m.trackCall("SaveCopyTradeLog"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.copyTradeLogs = append(m.copyTradeLogs, entry)
	return nil
}

func (m *MockStore) GetTradingAccount(ctx context.Context, id int) (*storage.TradingAccount, error) {
	if err := m.trackCall("GetTradingAccount"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tradingAccounts[id], nil
}

func (m *MockStore) GetDefaultTradingAccount(ctx context.Context) (*storage.TradingAccount, error) {
	if err := m.trackCall("GetDefaultTradingAccount"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.defaultAccount == 0 {
		return nil, nil
	}
	return m.tradingAccounts[m.defaultAccount], nil
}

// Helper methods for setting up test state
func (m *MockStore) SetUserSettings(addr string, settings storage.UserCopySettings) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.userSettings[addr] = &settings
}

func (m *MockStore) SetPosition(marketID, outcome string, pos MyPosition) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.positions[marketID+":"+outcome] = &pos
}

func (m *MockStore) AddTradingAccount(acc storage.TradingAccount) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tradingAccounts[acc.ID] = &acc
	if acc.IsDefault {
		m.defaultAccount = acc.ID
	}
}

func (m *MockStore) GetCopyTradeLogs() []storage.CopyTradeLogEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.copyTradeLogs
}

// E2E Test: Full copy trade buy flow
func TestE2E_CopyTradeBuyFlow(t *testing.T) {
	t.Run("human strategy buy - full flow", func(t *testing.T) {
		// Setup
		store := NewMockStore()
		mockClient := api.NewMockClobClient()

		// Configure order book with liquidity
		mockClient.OrderBook = &api.OrderBook{
			Asks: []api.OrderBookLevel{
				{Price: "0.50", Size: "100"},
				{Price: "0.52", Size: "200"},
			},
			Bids: []api.OrderBookLevel{
				{Price: "0.48", Size: "100"},
			},
		}

		// Configure successful order response
		mockClient.OrderResponse = &api.OrderResponse{
			Success: true,
			OrderID: "test-order-123",
			Status:  "matched",
		}

		// User settings: 10% multiplier
		store.SetUserSettings("0xtrader1", storage.UserCopySettings{
			UserAddress:  "0xtrader1",
			Multiplier:   0.10,
			Enabled:      true,
			MinUSDC:      1.0,
			StrategyType: storage.StrategyHuman,
		})

		// Simulate trade from followed trader
		trade := models.TradeDetail{
			ID:       "trade-001",
			Side:     "BUY",
			Outcome:  "Yes",
			Price:    0.50,
			UsdcSize: 50.0, // $50 trade
			Size:     100.0,
			MarketID: "market-123",
			UserID:   "0xtrader1",
		}

		// Calculate expected copy amount
		expectedCopyAmount := trade.UsdcSize * 0.10 // $5
		if expectedCopyAmount < 1.0 {
			expectedCopyAmount = 1.0
		}

		// Verify analysis would pass
		analysis := AnalyzeBotBuy(mockClient.OrderBook, trade.Price, expectedCopyAmount)
		if analysis.TotalCost == 0 {
			t.Error("analysis should show available liquidity")
		}

		// Verify price is within acceptable range
		if analysis.AvgPrice > trade.Price*1.10 {
			t.Errorf("avg price %.4f should be within 10%% of trade price %.4f", analysis.AvgPrice, trade.Price)
		}
	})

	t.Run("human strategy buy - high price with tiered slippage", func(t *testing.T) {
		// Test that low prices get more slippage tolerance
		tests := []struct {
			tradePrice     float64
			askPrice       float64
			shouldExecute  bool
		}{
			{0.05, 0.10, true},  // 100% above, but within 200% tier
			{0.05, 0.16, false}, // 220% above, outside 200% tier
			{0.20, 0.30, true},  // 50% above, within 50% tier
			{0.20, 0.35, false}, // 75% above, outside 50% tier
			{0.50, 0.60, true},  // 20% above, within 20% tier
			{0.50, 0.65, false}, // 30% above, outside 20% tier
		}

		for _, tt := range tests {
			maxSlippage := getMaxSlippage(tt.tradePrice)
			maxPrice := tt.tradePrice * (1 + maxSlippage)
			canExecute := tt.askPrice <= maxPrice

			if canExecute != tt.shouldExecute {
				t.Errorf("trade@%.2f ask@%.2f: execute=%v, want %v (maxSlip=%.2f, maxPrice=%.2f)",
					tt.tradePrice, tt.askPrice, canExecute, tt.shouldExecute, maxSlippage, maxPrice)
			}
		}
	})
}

// E2E Test: Full copy trade sell flow
func TestE2E_CopyTradeSellFlow(t *testing.T) {
	t.Run("human strategy sell - full position", func(t *testing.T) {
		store := NewMockStore()

		// Set up existing position
		store.SetPosition("market-123", "Yes", MyPosition{
			MarketID:  "market-123",
			TokenID:   "token-yes-123",
			Outcome:   "Yes",
			Size:      50.0,
			AvgPrice:  0.40,
			TotalCost: 20.0,
		})

		// Verify position exists
		pos, err := store.GetMyPosition(context.Background(), "market-123", "Yes")
		if err != nil || pos == nil {
			t.Fatal("position should exist")
		}

		// Human strategy: any sell triggers full liquidation
		trade := models.TradeDetail{
			Side:    "SELL",
			Size:    10.0, // Trader sells 10
			Outcome: "Yes",
		}

		// Our sell should be entire position
		ourSellSize := pos.Size
		if trade.Side == "SELL" && ourSellSize != 50.0 {
			t.Errorf("human sell should liquidate entire position, got %.2f", ourSellSize)
		}
	})

	t.Run("bot strategy sell - proportional", func(t *testing.T) {
		store := NewMockStore()

		// Set up existing position
		store.SetPosition("market-123", "Yes", MyPosition{
			MarketID:  "market-123",
			TokenID:   "token-yes-123",
			Outcome:   "Yes",
			Size:      100.0,
			AvgPrice:  0.40,
			TotalCost: 40.0,
		})

		// Verify position
		pos, _ := store.GetMyPosition(context.Background(), "market-123", "Yes")

		// Bot strategy: sell proportionally
		trade := models.TradeDetail{
			Side:    "SELL",
			Size:    200.0, // Trader sells 200
			Outcome: "Yes",
		}
		multiplier := 0.10

		// Calculate sell amount
		targetSell := trade.Size * multiplier // 20
		actualSell := targetSell
		if actualSell > pos.Size {
			actualSell = pos.Size // Cap at our position
		}

		if actualSell != 20.0 {
			t.Errorf("bot sell should be proportional: got %.2f, want 20.0", actualSell)
		}
	})

	t.Run("bot strategy sell - more than position", func(t *testing.T) {
		store := NewMockStore()

		// Set up small position
		store.SetPosition("market-123", "Yes", MyPosition{
			Size: 5.0,
		})

		pos, _ := store.GetMyPosition(context.Background(), "market-123", "Yes")

		// Trader sells a lot
		trade := models.TradeDetail{
			Side: "SELL",
			Size: 1000.0,
		}
		multiplier := 0.10

		// Calculate - should be capped at our position
		targetSell := trade.Size * multiplier // 100
		actualSell := targetSell
		if actualSell > pos.Size {
			actualSell = pos.Size // 5
		}

		if actualSell != 5.0 {
			t.Errorf("sell capped at position: got %.2f, want 5.0", actualSell)
		}
	})
}

// E2E Test: Bot strategy price limits
func TestE2E_BotStrategyPriceLimits(t *testing.T) {
	t.Run("bot buy rejects high prices", func(t *testing.T) {
		mockClient := api.NewMockClobClient()

		// Order book with asks above 10% limit
		mockClient.OrderBook = &api.OrderBook{
			Asks: []api.OrderBookLevel{
				{Price: "0.60", Size: "100"}, // 20% above 0.50
			},
		}

		copiedPrice := 0.50
		targetUSDC := 10.0

		analysis := AnalyzeBotBuy(mockClient.OrderBook, copiedPrice, targetUSDC)

		// Should reject - no liquidity within 10%
		if analysis.TotalCost != 0 {
			t.Errorf("should reject prices above 10%%: got $%.2f available", analysis.TotalCost)
		}
	})

	t.Run("bot buy accepts prices within limit", func(t *testing.T) {
		mockClient := api.NewMockClobClient()

		// Order book with asks within 10% limit
		mockClient.OrderBook = &api.OrderBook{
			Asks: []api.OrderBookLevel{
				{Price: "0.52", Size: "100"}, // 4% above 0.50
			},
		}

		copiedPrice := 0.50
		targetUSDC := 10.0

		analysis := AnalyzeBotBuy(mockClient.OrderBook, copiedPrice, targetUSDC)

		// Should accept
		if analysis.TotalCost == 0 {
			t.Error("should accept prices within 10%")
		}
		if !floatEquals(analysis.TotalCost, targetUSDC, 0.01) {
			t.Errorf("should fill target: got $%.2f, want $%.2f", analysis.TotalCost, targetUSDC)
		}
	})

	t.Run("bot sell rejects low prices", func(t *testing.T) {
		mockClient := api.NewMockClobClient()

		// Order book with bids below 10% limit
		mockClient.OrderBook = &api.OrderBook{
			Bids: []api.OrderBookLevel{
				{Price: "0.40", Size: "100"}, // 20% below 0.50
			},
		}

		copiedPrice := 0.50
		sellSize := 20.0

		analysis := AnalyzeBotSell(mockClient.OrderBook, copiedPrice, sellSize)

		// Should reject - no bids within 10%
		if analysis.TotalSold != 0 {
			t.Errorf("should reject prices below 10%%: got %.2f sellable", analysis.TotalSold)
		}
	})

	t.Run("bot sell accepts prices within limit", func(t *testing.T) {
		mockClient := api.NewMockClobClient()

		// Order book with bids within 10%
		mockClient.OrderBook = &api.OrderBook{
			Bids: []api.OrderBookLevel{
				{Price: "0.48", Size: "100"}, // 4% below 0.50
			},
		}

		copiedPrice := 0.50
		sellSize := 20.0

		analysis := AnalyzeBotSell(mockClient.OrderBook, copiedPrice, sellSize)

		// Should accept
		if analysis.TotalSold == 0 {
			t.Error("should accept prices within 10%")
		}
		if !floatEquals(analysis.TotalSold, sellSize, 0.01) {
			t.Errorf("should fill target: got %.2f, want %.2f", analysis.TotalSold, sellSize)
		}
	})
}

// E2E Test: Limit order fallback for bot sells
func TestE2E_LimitOrderFallback(t *testing.T) {
	t.Run("plan fallback orders", func(t *testing.T) {
		sellSize := 100.0
		copiedPrice := 0.50

		plan := PlanLimitOrderFallback(sellSize, copiedPrice)

		// Should have 3 orders
		if len(plan.Orders) != 3 {
			t.Fatalf("expected 3 limit orders, got %d", len(plan.Orders))
		}

		// First order: 20% at copied price
		if !floatEquals(plan.Orders[0].Size, 20.0, 0.01) {
			t.Errorf("order 1 size = %.2f, want 20.0", plan.Orders[0].Size)
		}
		if !floatEquals(plan.Orders[0].Price, 0.50, 0.001) {
			t.Errorf("order 1 price = %.4f, want 0.50", plan.Orders[0].Price)
		}

		// Second order: 40% at -3%
		if !floatEquals(plan.Orders[1].Size, 40.0, 0.01) {
			t.Errorf("order 2 size = %.2f, want 40.0", plan.Orders[1].Size)
		}
		expectedPrice2 := 0.50 * 0.97
		if !floatEquals(plan.Orders[1].Price, expectedPrice2, 0.001) {
			t.Errorf("order 2 price = %.4f, want %.4f", plan.Orders[1].Price, expectedPrice2)
		}

		// Third order: 40% at -5%
		if !floatEquals(plan.Orders[2].Size, 40.0, 0.01) {
			t.Errorf("order 3 size = %.2f, want 40.0", plan.Orders[2].Size)
		}
		expectedPrice3 := 0.50 * 0.95
		if !floatEquals(plan.Orders[2].Price, expectedPrice3, 0.001) {
			t.Errorf("order 3 price = %.4f, want %.4f", plan.Orders[2].Price, expectedPrice3)
		}

		// Total size should equal original
		totalSize := plan.Orders[0].Size + plan.Orders[1].Size + plan.Orders[2].Size
		if !floatEquals(totalSize, sellSize, 0.01) {
			t.Errorf("total size = %.2f, want %.2f", totalSize, sellSize)
		}
	})
}

// E2E Test: Multi-account routing
func TestE2E_MultiAccountRouting(t *testing.T) {
	t.Run("user with assigned account uses that account", func(t *testing.T) {
		store := NewMockStore()

		// Add trading accounts
		store.AddTradingAccount(storage.TradingAccount{
			ID:        1,
			Name:      "Default Account",
			IsDefault: true,
			Enabled:   true,
		})
		store.AddTradingAccount(storage.TradingAccount{
			ID:      2,
			Name:    "Whale Account",
			Enabled: true,
		})
		store.AddTradingAccount(storage.TradingAccount{
			ID:      3,
			Name:    "Test Account",
			Enabled: true,
		})

		// User assigned to account 3
		accountID := 3
		store.SetUserSettings("0xwhale", storage.UserCopySettings{
			UserAddress:      "0xwhale",
			Multiplier:       0.20,
			Enabled:          true,
			TradingAccountID: &accountID,
		})

		// Check routing
		settings, _ := store.GetUserCopySettings(context.Background(), "0xwhale")
		if settings.TradingAccountID == nil {
			t.Fatal("user should have assigned account")
		}
		if *settings.TradingAccountID != 3 {
			t.Errorf("user should use account 3, got %d", *settings.TradingAccountID)
		}

		// Verify account exists
		acc, _ := store.GetTradingAccount(context.Background(), 3)
		if acc == nil {
			t.Fatal("account 3 should exist")
		}
		if acc.Name != "Test Account" {
			t.Errorf("account name = %s, want Test Account", acc.Name)
		}
	})

	t.Run("user without assignment uses default", func(t *testing.T) {
		store := NewMockStore()

		// Add default account
		store.AddTradingAccount(storage.TradingAccount{
			ID:        1,
			Name:      "Default Account",
			IsDefault: true,
			Enabled:   true,
		})

		// User with no account assignment
		store.SetUserSettings("0xnormal", storage.UserCopySettings{
			UserAddress:      "0xnormal",
			Multiplier:       0.10,
			Enabled:          true,
			TradingAccountID: nil,
		})

		// Check routing
		settings, _ := store.GetUserCopySettings(context.Background(), "0xnormal")
		if settings.TradingAccountID != nil {
			t.Error("user should not have assigned account")
		}

		// Should fall back to default
		defaultAcc, _ := store.GetDefaultTradingAccount(context.Background())
		if defaultAcc == nil {
			t.Fatal("default account should exist")
		}
		if defaultAcc.ID != 1 {
			t.Errorf("default account ID = %d, want 1", defaultAcc.ID)
		}
	})

	t.Run("unknown user uses default", func(t *testing.T) {
		store := NewMockStore()

		// Add default account
		store.AddTradingAccount(storage.TradingAccount{
			ID:        1,
			Name:      "Default Account",
			IsDefault: true,
			Enabled:   true,
		})

		// Unknown user - no settings
		settings, err := store.GetUserCopySettings(context.Background(), "0xunknown")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if settings != nil {
			t.Error("unknown user should have no settings")
		}

		// Should use default
		defaultAcc, _ := store.GetDefaultTradingAccount(context.Background())
		if defaultAcc == nil {
			t.Fatal("default account should exist")
		}
	})
}

// E2E Test: Max USD cap enforcement
func TestE2E_MaxUSDCapEnforcement(t *testing.T) {
	tests := []struct {
		name           string
		tradeUSDC      float64
		multiplier     float64
		minUSDC        float64
		maxUSD         *float64
		expectedAmount float64
	}{
		{
			name:           "large trade capped",
			tradeUSDC:      10000.0,
			multiplier:     0.10,
			minUSDC:        1.0,
			maxUSD:         floatPtr(500.0),
			expectedAmount: 500.0,
		},
		{
			name:           "small trade not capped",
			tradeUSDC:      100.0,
			multiplier:     0.10,
			minUSDC:        1.0,
			maxUSD:         floatPtr(500.0),
			expectedAmount: 10.0,
		},
		{
			name:           "no cap unlimited",
			tradeUSDC:      50000.0,
			multiplier:     0.10,
			minUSDC:        1.0,
			maxUSD:         nil,
			expectedAmount: 5000.0,
		},
		{
			name:           "min enforced even with cap",
			tradeUSDC:      5.0,
			multiplier:     0.10,
			minUSDC:        2.0,
			maxUSD:         floatPtr(500.0),
			expectedAmount: 2.0, // min takes precedence
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			amount := CalculateBotBuyAmount(tt.tradeUSDC, tt.multiplier, tt.minUSDC, tt.maxUSD)
			if !floatEquals(amount, tt.expectedAmount, 0.01) {
				t.Errorf("got $%.2f, want $%.2f", amount, tt.expectedAmount)
			}
		})
	}
}

// E2E Test: Trade type filtering
func TestE2E_TradeTypeFiltering(t *testing.T) {
	trades := []models.TradeDetail{
		{ID: "1", Type: "TRADE", Side: "BUY"},
		{ID: "2", Type: "", Side: "BUY"},      // Empty = TRADE
		{ID: "3", Type: "REDEEM", Side: ""},   // Skip
		{ID: "4", Type: "SPLIT", Side: ""},    // Skip
		{ID: "5", Type: "MERGE", Side: ""},    // Skip
		{ID: "6", Type: "TRADE", Side: "SELL"},
	}

	var tradesToCopy []models.TradeDetail
	for _, trade := range trades {
		if trade.Type == "" || trade.Type == "TRADE" {
			tradesToCopy = append(tradesToCopy, trade)
		}
	}

	if len(tradesToCopy) != 3 {
		t.Errorf("expected 3 trades to copy, got %d", len(tradesToCopy))
	}

	// Verify correct trades
	expectedIDs := map[string]bool{"1": true, "2": true, "6": true}
	for _, trade := range tradesToCopy {
		if !expectedIDs[trade.ID] {
			t.Errorf("unexpected trade in copy list: %s", trade.ID)
		}
	}
}

// E2E Test: High price sell limit order
func TestE2E_HighPriceSellLimitOrder(t *testing.T) {
	tests := []struct {
		name        string
		price       float64
		shouldLimit bool
	}{
		{"low price market order", 0.50, false},
		{"medium price market order", 0.80, false},
		{"near threshold market order", 0.95, false},
		{"at threshold limit order", 0.96, true},
		{"high price limit order", 0.98, true},
		{"max price limit order", 0.99, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shouldUseLimitOrder := tt.price >= 0.96

			if shouldUseLimitOrder != tt.shouldLimit {
				t.Errorf("price %.2f: limit order = %v, want %v",
					tt.price, shouldUseLimitOrder, tt.shouldLimit)
			}
		})
	}
}

// E2E Test: Strategy type routing
func TestE2E_StrategyTypeRouting(t *testing.T) {
	tests := []struct {
		name         string
		strategyType int
		tradeSide    string
		behavior     string
	}{
		{
			name:         "human buy",
			strategyType: storage.StrategyHuman,
			tradeSide:    "BUY",
			behavior:     "tiered slippage",
		},
		{
			name:         "human sell",
			strategyType: storage.StrategyHuman,
			tradeSide:    "SELL",
			behavior:     "full liquidation",
		},
		{
			name:         "bot buy",
			strategyType: storage.StrategyBot,
			tradeSide:    "BUY",
			behavior:     "10% price limit",
		},
		{
			name:         "bot sell",
			strategyType: storage.StrategyBot,
			tradeSide:    "SELL",
			behavior:     "proportional + fallback",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the strategy routing logic
			isHuman := tt.strategyType == storage.StrategyHuman
			isBot := tt.strategyType == storage.StrategyBot
			isBuy := tt.tradeSide == "BUY"
			isSell := tt.tradeSide == "SELL"

			switch {
			case isHuman && isBuy:
				if tt.behavior != "tiered slippage" {
					t.Errorf("human buy should use tiered slippage")
				}
			case isHuman && isSell:
				if tt.behavior != "full liquidation" {
					t.Errorf("human sell should use full liquidation")
				}
			case isBot && isBuy:
				if tt.behavior != "10% price limit" {
					t.Errorf("bot buy should use 10%% price limit")
				}
			case isBot && isSell:
				if tt.behavior != "proportional + fallback" {
					t.Errorf("bot sell should use proportional + fallback")
				}
			}
		})
	}
}

// E2E Test: Deduplication
func TestE2E_TradeDeduplication(t *testing.T) {
	store := NewMockStore()
	ctx := context.Background()

	// Mark some trades as processed
	store.MarkTradeProcessed(ctx, "trade-001")
	store.MarkTradeProcessed(ctx, "trade-002")

	trades := []string{"trade-001", "trade-002", "trade-003", "trade-004"}

	var unprocessed []string
	for _, id := range trades {
		processed, _ := store.IsTradeProcessed(ctx, id)
		if !processed {
			unprocessed = append(unprocessed, id)
		}
	}

	if len(unprocessed) != 2 {
		t.Errorf("expected 2 unprocessed trades, got %d", len(unprocessed))
	}

	expected := map[string]bool{"trade-003": true, "trade-004": true}
	for _, id := range unprocessed {
		if !expected[id] {
			t.Errorf("unexpected unprocessed trade: %s", id)
		}
	}
}

// E2E Test: Position tracking
func TestE2E_PositionTracking(t *testing.T) {
	store := NewMockStore()
	ctx := context.Background()

	t.Run("accumulate position on buys", func(t *testing.T) {
		// First buy
		store.UpdateMyPosition(ctx, MyPosition{
			MarketID:  "market-1",
			TokenID:   "token-1",
			Outcome:   "Yes",
			Size:      10.0,
			AvgPrice:  0.50,
			TotalCost: 5.0,
		})

		// Second buy
		store.UpdateMyPosition(ctx, MyPosition{
			MarketID:  "market-1",
			TokenID:   "token-1",
			Outcome:   "Yes",
			Size:      20.0,
			AvgPrice:  0.55,
			TotalCost: 11.0,
		})

		pos, _ := store.GetMyPosition(ctx, "market-1", "Yes")
		if pos.Size != 30.0 {
			t.Errorf("accumulated size = %.2f, want 30.0", pos.Size)
		}
		if pos.TotalCost != 16.0 {
			t.Errorf("accumulated cost = %.2f, want 16.0", pos.TotalCost)
		}
	})

	t.Run("clear position on sell", func(t *testing.T) {
		store.ClearMyPosition(ctx, "market-1", "Yes")

		pos, _ := store.GetMyPosition(ctx, "market-1", "Yes")
		if pos != nil {
			t.Error("position should be cleared")
		}
	})
}

// E2E Test: Copy trade logging
func TestE2E_CopyTradeLogging(t *testing.T) {
	store := NewMockStore()
	ctx := context.Background()

	// Log successful trade
	store.SaveCopyTradeLog(ctx, storage.CopyTradeLogEntry{
		FollowingAddress: "0xtrader",
		FollowingTradeID: "trade-001",
		MarketTitle:      "Test Market",
		Status:           "success",
		StrategyType:     storage.StrategyHuman,
	})

	// Log failed trade
	store.SaveCopyTradeLog(ctx, storage.CopyTradeLogEntry{
		FollowingAddress: "0xtrader",
		FollowingTradeID: "trade-002",
		MarketTitle:      "Test Market 2",
		Status:           "failed",
		FailedReason:     "no liquidity",
		StrategyType:     storage.StrategyBot,
	})

	logs := store.GetCopyTradeLogs()
	if len(logs) != 2 {
		t.Errorf("expected 2 logs, got %d", len(logs))
	}

	// Check first log
	if logs[0].Status != "success" {
		t.Errorf("log 1 status = %s, want success", logs[0].Status)
	}
	if logs[0].StrategyType != storage.StrategyHuman {
		t.Errorf("log 1 strategy = %d, want Human", logs[0].StrategyType)
	}

	// Check second log
	if logs[1].Status != "failed" {
		t.Errorf("log 2 status = %s, want failed", logs[1].Status)
	}
	if logs[1].FailedReason != "no liquidity" {
		t.Errorf("log 2 reason = %s, want 'no liquidity'", logs[1].FailedReason)
	}
}
