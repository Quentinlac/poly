package api

import (
	"context"
	"sync"
)

// ClobClientInterface defines the methods needed from a CLOB client.
// This interface enables dependency injection for testing.
type ClobClientInterface interface {
	// Configuration
	SetFunder(address string)
	SetSignatureType(sigType int)
	DeriveAPICreds(ctx context.Context) (*APICreds, error)

	// Order Book
	GetOrderBook(ctx context.Context, tokenID string) (*OrderBook, error)
	GetCachedOrderBook(ctx context.Context, tokenID string) (*OrderBook, error)
	AddTokenToCache(tokenID string)
	RemoveTokenFromCache(tokenID string)
	StartOrderBookCaching()
	StopOrderBookCaching()

	// Market Info
	GetMarket(ctx context.Context, conditionID string) (*MarketInfo, error)
	GetTokenInfoByID(ctx context.Context, tokenID string) (*GammaTokenInfo, error)

	// Order Placement
	PlaceMarketOrder(ctx context.Context, tokenID string, side Side, amountUSDC float64, negRisk bool) (*OrderResponse, error)
	PlaceLimitOrder(ctx context.Context, tokenID string, side Side, size float64, price float64, negRisk bool) (*OrderResponse, error)

	// Order Management
	GetOrderStatus(ctx context.Context, orderID string) (*OrderStatus, error)
	CancelOrder(ctx context.Context, orderID string) error
	CancelOrders(ctx context.Context, orderIDs []string) error
}

// Ensure ClobClient implements ClobClientInterface
var _ ClobClientInterface = (*ClobClient)(nil)

// Ensure MockClobClient implements ClobClientInterface
var _ ClobClientInterface = (*MockClobClient)(nil)

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

	// Detailed call tracking for verification
	PlaceMarketOrderCalls []PlaceMarketOrderCall
	PlaceLimitOrderCalls  []PlaceLimitOrderCall
	CancelOrderCalls      []string

	// Token cache tracking
	CachedTokens map[string]bool
}

// PlaceMarketOrderCall records a call to PlaceMarketOrder
type PlaceMarketOrderCall struct {
	TokenID    string
	Side       Side
	AmountUSDC float64
	NegRisk    bool
}

// PlaceLimitOrderCall records a call to PlaceLimitOrder
type PlaceLimitOrderCall struct {
	TokenID string
	Side    Side
	Size    float64
	Price   float64
	NegRisk bool
}

// NewMockClobClient creates a new mock CLOB client
func NewMockClobClient() *MockClobClient {
	return &MockClobClient{
		Calls:                 make(map[string]int),
		ErrorOnNext:           make(map[string]error),
		PlaceMarketOrderCalls: []PlaceMarketOrderCall{},
		PlaceLimitOrderCalls:  []PlaceLimitOrderCall{},
		CancelOrderCalls:      []string{},
		CachedTokens:          make(map[string]bool),
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

func (m *MockClobClient) StartOrderBookCaching() {
	m.trackCall("StartOrderBookCaching")
}

func (m *MockClobClient) StopOrderBookCaching() {
	m.trackCall("StopOrderBookCaching")
}

func (m *MockClobClient) SetFunder(address string) {
	m.trackCall("SetFunder")
}

func (m *MockClobClient) SetSignatureType(sigType int) {
	m.trackCall("SetSignatureType")
}

// PlaceMarketOrder mocks placing a market order
func (m *MockClobClient) PlaceMarketOrder(ctx context.Context, tokenID string, side Side, amountUSDC float64, negRisk bool) (*OrderResponse, error) {
	if err := m.trackCall("PlaceMarketOrder"); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.PlaceMarketOrderCalls = append(m.PlaceMarketOrderCalls, PlaceMarketOrderCall{
		TokenID:    tokenID,
		Side:       side,
		AmountUSDC: amountUSDC,
		NegRisk:    negRisk,
	})
	m.mu.Unlock()

	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.OrderResponse != nil {
		return m.OrderResponse, nil
	}
	return &OrderResponse{
		Success: true,
		OrderID: "mock-order-id",
		Status:  "matched",
	}, nil
}

// PlaceLimitOrder mocks placing a limit order
func (m *MockClobClient) PlaceLimitOrder(ctx context.Context, tokenID string, side Side, size float64, price float64, negRisk bool) (*OrderResponse, error) {
	if err := m.trackCall("PlaceLimitOrder"); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.PlaceLimitOrderCalls = append(m.PlaceLimitOrderCalls, PlaceLimitOrderCall{
		TokenID: tokenID,
		Side:    side,
		Size:    size,
		Price:   price,
		NegRisk: negRisk,
	})
	m.mu.Unlock()

	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.OrderResponse != nil {
		return m.OrderResponse, nil
	}
	return &OrderResponse{
		Success: true,
		OrderID: "mock-limit-order-id",
		Status:  "live",
	}, nil
}

// CancelOrder mocks canceling an order
func (m *MockClobClient) CancelOrder(ctx context.Context, orderID string) error {
	if err := m.trackCall("CancelOrder"); err != nil {
		return err
	}
	m.mu.Lock()
	m.CancelOrderCalls = append(m.CancelOrderCalls, orderID)
	m.mu.Unlock()
	return nil
}

// AddTokenToCache mocks adding a token to cache
func (m *MockClobClient) AddTokenToCache(tokenID string) {
	m.trackCall("AddTokenToCache")
	m.mu.Lock()
	m.CachedTokens[tokenID] = true
	m.mu.Unlock()
}

// RemoveTokenFromCache mocks removing a token from cache
func (m *MockClobClient) RemoveTokenFromCache(tokenID string) {
	m.trackCall("RemoveTokenFromCache")
	m.mu.Lock()
	delete(m.CachedTokens, tokenID)
	m.mu.Unlock()
}

// GetCachedOrderBook mocks getting a cached order book
func (m *MockClobClient) GetCachedOrderBook(ctx context.Context, tokenID string) (*OrderBook, error) {
	if err := m.trackCall("GetCachedOrderBook"); err != nil {
		return nil, err
	}
	// Falls back to regular GetOrderBook
	return m.GetOrderBook(ctx, tokenID)
}

// GetOrderStatus mocks getting order status
func (m *MockClobClient) GetOrderStatus(ctx context.Context, orderID string) (*OrderStatus, error) {
	if err := m.trackCall("GetOrderStatus"); err != nil {
		return nil, err
	}
	// Return a default "MATCHED" status
	return &OrderStatus{
		OrderID:      orderID,
		Status:       "MATCHED",
		SizeMatched:  "100",
		OriginalSize: "100",
	}, nil
}

// CancelOrders mocks canceling multiple orders
func (m *MockClobClient) CancelOrders(ctx context.Context, orderIDs []string) error {
	if err := m.trackCall("CancelOrders"); err != nil {
		return err
	}
	m.mu.Lock()
	m.CancelOrderCalls = append(m.CancelOrderCalls, orderIDs...)
	m.mu.Unlock()
	return nil
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
