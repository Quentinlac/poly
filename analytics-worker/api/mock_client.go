package api

import (
	"context"
	"sync"
)

// MockHTTPClient is a mock HTTP client for testing
type MockHTTPClient struct {
	mu sync.RWMutex

	// Response data
	ActivityResponse    []DataTrade
	OrderBookResponse   *OrderBook
	MarketResponse      *MarketInfo
	TokenInfoResponse   *GammaTokenInfo
	LastTradePriceResp  float64

	// Call tracking
	Calls map[string]int

	// Error injection
	ErrorOnNext map[string]error
}

// NewMockHTTPClient creates a new mock HTTP client
func NewMockHTTPClient() *MockHTTPClient {
	return &MockHTTPClient{
		Calls:       make(map[string]int),
		ErrorOnNext: make(map[string]error),
	}
}

func (m *MockHTTPClient) trackCall(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls[name]++
	if err, ok := m.ErrorOnNext[name]; ok {
		delete(m.ErrorOnNext, name)
		return err
	}
	return nil
}

// MockClobClient is a mock CLOB client for testing
type MockClobClient struct {
	mu sync.RWMutex

	// Response data
	OrderBook     *OrderBook
	MarketInfo    *MarketInfo
	TokenInfo     *GammaTokenInfo
	OrderResponse *OrderResponse
	APICreds      *APICreds

	// Call tracking
	Calls map[string]int

	// Error injection
	ErrorOnNext map[string]error
}

// NewMockClobClient creates a new mock CLOB client
func NewMockClobClient() *MockClobClient {
	return &MockClobClient{
		Calls:       make(map[string]int),
		ErrorOnNext: make(map[string]error),
		APICreds: &APICreds{
			APIKey:        "test-api-key",
			APISecret:     "test-api-secret",
			APIPassphrase: "test-passphrase",
		},
	}
}

func (m *MockClobClient) trackCall(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls[name]++
	if err, ok := m.ErrorOnNext[name]; ok {
		delete(m.ErrorOnNext, name)
		return err
	}
	return nil
}

func (m *MockClobClient) GetOrderBook(ctx context.Context, tokenID string) (*OrderBook, error) {
	if err := m.trackCall("GetOrderBook"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.OrderBook != nil {
		return m.OrderBook, nil
	}
	// Return default order book
	return &OrderBook{
		Asks: []OrderBookLevel{
			{Price: "0.50", Size: "100"},
		},
		Bids: []OrderBookLevel{
			{Price: "0.49", Size: "100"},
		},
	}, nil
}

func (m *MockClobClient) GetMarket(ctx context.Context, conditionID string) (*MarketInfo, error) {
	if err := m.trackCall("GetMarket"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.MarketInfo != nil {
		return m.MarketInfo, nil
	}
	return &MarketInfo{
		ConditionID: conditionID,
		Description: "Test Market",
		NegRisk:     false,
	}, nil
}

func (m *MockClobClient) GetTokenInfoByID(ctx context.Context, tokenID string) (*GammaTokenInfo, error) {
	if err := m.trackCall("GetTokenInfoByID"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.TokenInfo != nil {
		return m.TokenInfo, nil
	}
	return &GammaTokenInfo{
		TokenID:     tokenID,
		ConditionID: "test-condition-id",
		Outcome:     "Yes",
		Title:       "Test Token",
	}, nil
}

func (m *MockClobClient) DeriveAPICreds(ctx context.Context) (*APICreds, error) {
	if err := m.trackCall("DeriveAPICreds"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.APICreds, nil
}

func (m *MockClobClient) PlaceOrder(ctx context.Context, order OrderRequest) (*OrderResponse, error) {
	if err := m.trackCall("PlaceOrder"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.OrderResponse != nil {
		return m.OrderResponse, nil
	}
	return &OrderResponse{
		OrderID: "mock-order-id",
		Status:  "MATCHED",
	}, nil
}

func (m *MockClobClient) SetFunder(address string) {
	m.trackCall("SetFunder")
}

func (m *MockClobClient) SetSignatureType(sigType int) {
	m.trackCall("SetSignatureType")
}

// MockWSClient is a mock WebSocket client for testing
type MockWSClient struct {
	mu sync.RWMutex

	// State
	Connected     bool
	Subscriptions []string

	// Call tracking
	Calls map[string]int

	// Error injection
	ErrorOnNext map[string]error

	// Event handler
	TradeHandler TradeHandler
}

// NewMockWSClient creates a new mock WebSocket client
func NewMockWSClient(handler TradeHandler) *MockWSClient {
	return &MockWSClient{
		Calls:        make(map[string]int),
		ErrorOnNext:  make(map[string]error),
		TradeHandler: handler,
	}
}

func (m *MockWSClient) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls["Start"]++
	if err, ok := m.ErrorOnNext["Start"]; ok {
		delete(m.ErrorOnNext, "Start")
		return err
	}
	m.Connected = true
	return nil
}

func (m *MockWSClient) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls["Stop"]++
	m.Connected = false
}

func (m *MockWSClient) Subscribe(assetIDs ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls["Subscribe"]++
	if err, ok := m.ErrorOnNext["Subscribe"]; ok {
		delete(m.ErrorOnNext, "Subscribe")
		return err
	}
	m.Subscriptions = append(m.Subscriptions, assetIDs...)
	return nil
}

func (m *MockWSClient) SimulateTradeEvent(event WSTradeEvent) {
	if m.TradeHandler != nil {
		m.TradeHandler(event)
	}
}

// MockPolygonWSClient is a mock Polygon WebSocket client for testing
type MockPolygonWSClient struct {
	mu sync.RWMutex

	// State
	Connected       bool
	FollowedAddrs   map[string]bool
	EventsReceived  int64
	TradesMatched   int64

	// Call tracking
	Calls map[string]int

	// Error injection
	ErrorOnNext map[string]error

	// Trade handler
	OnTrade func(event PolygonTradeEvent)
}

// NewMockPolygonWSClient creates a new mock Polygon WebSocket client
func NewMockPolygonWSClient(onTrade func(event PolygonTradeEvent)) *MockPolygonWSClient {
	return &MockPolygonWSClient{
		FollowedAddrs: make(map[string]bool),
		Calls:         make(map[string]int),
		ErrorOnNext:   make(map[string]error),
		OnTrade:       onTrade,
	}
}

func (m *MockPolygonWSClient) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls["Start"]++
	if err, ok := m.ErrorOnNext["Start"]; ok {
		delete(m.ErrorOnNext, "Start")
		return err
	}
	m.Connected = true
	return nil
}

func (m *MockPolygonWSClient) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls["Stop"]++
	m.Connected = false
}

func (m *MockPolygonWSClient) SetFollowedAddresses(addrs []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls["SetFollowedAddresses"]++
	m.FollowedAddrs = make(map[string]bool)
	for _, addr := range addrs {
		m.FollowedAddrs[addr] = true
	}
}

func (m *MockPolygonWSClient) AddFollowedAddress(addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls["AddFollowedAddress"]++
	m.FollowedAddrs[addr] = true
}

func (m *MockPolygonWSClient) GetStats() (eventsReceived, tradesMatched int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.EventsReceived, m.TradesMatched
}

func (m *MockPolygonWSClient) SimulateTradeEvent(event PolygonTradeEvent) {
	m.mu.Lock()
	m.EventsReceived++
	m.TradesMatched++
	m.mu.Unlock()

	if m.OnTrade != nil {
		m.OnTrade(event)
	}
}
