// Package api provides WebSocket client for real-time Polymarket data.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// WebSocket endpoints
	wsMarketURL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
	wsUserURL   = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

	// Reconnection settings
	reconnectDelay    = 2 * time.Second
	maxReconnectDelay = 30 * time.Second
	pingInterval      = 30 * time.Second
	pongTimeout       = 10 * time.Second
)

// WSEventType represents the type of WebSocket event
type WSEventType string

const (
	WSEventBook           WSEventType = "book"
	WSEventPriceChange    WSEventType = "price_change"
	WSEventLastTradePrice WSEventType = "last_trade_price"
	WSEventTickSizeChange WSEventType = "tick_size_change"
)

// WSMessage represents a WebSocket message from Polymarket
type WSMessage struct {
	EventType WSEventType     `json:"event_type"`
	AssetID   string          `json:"asset_id"`
	Market    string          `json:"market"`
	Timestamp int64           `json:"timestamp"`
	Price     string          `json:"price,omitempty"`
	Hash      string          `json:"hash,omitempty"`
	Changes   json.RawMessage `json:"changes,omitempty"`
}

// WSBookChange represents an order book change
type WSBookChange struct {
	Price string `json:"price"`
	Size  string `json:"size"`
	Side  string `json:"side"` // "buy" or "sell"
}

// WSTradeEvent represents a detected trade from last_trade_price events
type WSTradeEvent struct {
	AssetID   string
	Price     float64
	Timestamp time.Time
}

// TradeHandler is called when a new trade is detected
type TradeHandler func(event WSTradeEvent)

// WSClient manages WebSocket connections to Polymarket
type WSClient struct {
	conn          *websocket.Conn
	connMu        sync.Mutex
	subscriptions map[string]bool // asset_id -> subscribed
	subMu         sync.RWMutex

	tradeHandler TradeHandler
	running      bool
	stopCh       chan struct{}
	doneCh       chan struct{}

	// Reconnection state
	reconnectAttempts int
	lastPong          time.Time
}

// NewWSClient creates a new WebSocket client
func NewWSClient(handler TradeHandler) *WSClient {
	return &WSClient{
		subscriptions: make(map[string]bool),
		tradeHandler:  handler,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
}

// Start connects to the WebSocket and begins listening
func (c *WSClient) Start(ctx context.Context) error {
	if c.running {
		return fmt.Errorf("WebSocket client already running")
	}

	if err := c.connect(); err != nil {
		return fmt.Errorf("initial connection failed: %w", err)
	}

	c.running = true
	go c.readLoop(ctx)
	go c.pingLoop(ctx)

	log.Printf("[WebSocket] Started connection to %s", wsMarketURL)
	return nil
}

// Stop gracefully closes the WebSocket connection
func (c *WSClient) Stop() {
	if !c.running {
		return
	}

	c.running = false
	close(c.stopCh)

	c.connMu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.connMu.Unlock()

	// Wait for goroutines to finish
	select {
	case <-c.doneCh:
	case <-time.After(5 * time.Second):
		log.Printf("[WebSocket] Shutdown timeout")
	}

	log.Printf("[WebSocket] Stopped")
}

// Subscribe adds asset IDs to monitor for trade events
func (c *WSClient) Subscribe(assetIDs ...string) error {
	c.subMu.Lock()
	newSubs := make([]string, 0)
	for _, id := range assetIDs {
		if !c.subscriptions[id] {
			c.subscriptions[id] = true
			newSubs = append(newSubs, id)
		}
	}
	c.subMu.Unlock()

	if len(newSubs) == 0 {
		return nil
	}

	return c.sendSubscription(newSubs)
}

// Unsubscribe removes asset IDs from monitoring
func (c *WSClient) Unsubscribe(assetIDs ...string) error {
	c.subMu.Lock()
	toRemove := make([]string, 0)
	for _, id := range assetIDs {
		if c.subscriptions[id] {
			delete(c.subscriptions, id)
			toRemove = append(toRemove, id)
		}
	}
	c.subMu.Unlock()

	if len(toRemove) == 0 {
		return nil
	}

	// Send unsubscribe message
	msg := map[string]interface{}{
		"type":       "unsubscribe",
		"assets_ids": toRemove,
	}

	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("not connected")
	}

	return c.conn.WriteJSON(msg)
}

// SubscriptionCount returns the number of active subscriptions
func (c *WSClient) SubscriptionCount() int {
	c.subMu.RLock()
	defer c.subMu.RUnlock()
	return len(c.subscriptions)
}

func (c *WSClient) connect() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn != nil {
		c.conn.Close()
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(wsMarketURL, nil)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}

	c.conn = conn
	c.lastPong = time.Now()
	c.reconnectAttempts = 0

	// Set up pong handler
	conn.SetPongHandler(func(string) error {
		c.lastPong = time.Now()
		return nil
	})

	return nil
}

func (c *WSClient) sendSubscription(assetIDs []string) error {
	if len(assetIDs) == 0 {
		return nil
	}

	// Polymarket accepts up to 100 assets per subscription message
	const batchSize = 100

	for i := 0; i < len(assetIDs); i += batchSize {
		end := i + batchSize
		if end > len(assetIDs) {
			end = len(assetIDs)
		}

		msg := map[string]interface{}{
			"type":       "subscribe",
			"assets_ids": assetIDs[i:end],
		}

		c.connMu.Lock()
		if c.conn == nil {
			c.connMu.Unlock()
			return fmt.Errorf("not connected")
		}
		err := c.conn.WriteJSON(msg)
		c.connMu.Unlock()

		if err != nil {
			return fmt.Errorf("send subscription failed: %w", err)
		}

		log.Printf("[WebSocket] Subscribed to %d assets (batch %d-%d)", end-i, i, end)
	}

	return nil
}

func (c *WSClient) resubscribe() error {
	c.subMu.RLock()
	assetIDs := make([]string, 0, len(c.subscriptions))
	for id := range c.subscriptions {
		assetIDs = append(assetIDs, id)
	}
	c.subMu.RUnlock()

	if len(assetIDs) == 0 {
		return nil
	}

	return c.sendSubscription(assetIDs)
}

func (c *WSClient) readLoop(ctx context.Context) {
	defer close(c.doneCh)

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		c.connMu.Lock()
		conn := c.conn
		c.connMu.Unlock()

		if conn == nil {
			c.reconnect(ctx)
			continue
		}

		// Set read deadline
		conn.SetReadDeadline(time.Now().Add(pongTimeout + pingInterval))

		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Printf("[WebSocket] Connection closed normally")
				return
			}
			log.Printf("[WebSocket] Read error: %v, reconnecting...", err)
			c.reconnect(ctx)
			continue
		}

		c.handleMessage(message)
	}
}

func (c *WSClient) pingLoop(ctx context.Context) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.connMu.Lock()
			conn := c.conn
			c.connMu.Unlock()

			if conn == nil {
				continue
			}

			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("[WebSocket] Ping failed: %v", err)
			}

			// Check if we haven't received a pong in too long
			if time.Since(c.lastPong) > pongTimeout+pingInterval {
				log.Printf("[WebSocket] Pong timeout, reconnecting...")
				c.reconnect(ctx)
			}
		}
	}
}

func (c *WSClient) reconnect(ctx context.Context) {
	c.reconnectAttempts++
	delay := reconnectDelay * time.Duration(c.reconnectAttempts)
	if delay > maxReconnectDelay {
		delay = maxReconnectDelay
	}

	log.Printf("[WebSocket] Reconnecting in %v (attempt %d)...", delay, c.reconnectAttempts)

	select {
	case <-ctx.Done():
		return
	case <-c.stopCh:
		return
	case <-time.After(delay):
	}

	if err := c.connect(); err != nil {
		log.Printf("[WebSocket] Reconnection failed: %v", err)
		return
	}

	// Resubscribe to all assets
	if err := c.resubscribe(); err != nil {
		log.Printf("[WebSocket] Resubscription failed: %v", err)
	}
}

func (c *WSClient) handleMessage(data []byte) {
	var msg WSMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		// Try parsing as array (some messages come as arrays)
		var msgs []WSMessage
		if err := json.Unmarshal(data, &msgs); err != nil {
			return
		}
		for _, m := range msgs {
			c.processMessage(m)
		}
		return
	}

	c.processMessage(msg)
}

func (c *WSClient) processMessage(msg WSMessage) {
	switch msg.EventType {
	case WSEventLastTradePrice:
		// This indicates a trade just happened
		if c.tradeHandler != nil && msg.Price != "" {
			var price float64
			fmt.Sscanf(msg.Price, "%f", &price)

			event := WSTradeEvent{
				AssetID:   msg.AssetID,
				Price:     price,
				Timestamp: time.Now(), // Use current time as trade just happened
			}
			c.tradeHandler(event)
		}

	case WSEventPriceChange:
		// Price changed - could indicate trade activity
		// We primarily use last_trade_price for trade detection

	case WSEventBook:
		// Order book update - useful for liquidity monitoring
		// but not directly indicating a trade

	case WSEventTickSizeChange:
		// Tick size change - not relevant for trade detection
	}
}

// WSUserClient manages WebSocket connections for user-specific data (authenticated)
type WSUserClient struct {
	conn     *websocket.Conn
	connMu   sync.Mutex
	apiCreds *APICreds
	markets  map[string]bool // condition_id -> subscribed
	subMu    sync.RWMutex

	tradeHandler func(trade DataTrade)
	running      bool
	stopCh       chan struct{}
	doneCh       chan struct{}
}

// NewWSUserClient creates a new authenticated WebSocket client for user data
func NewWSUserClient(creds *APICreds, handler func(trade DataTrade)) *WSUserClient {
	return &WSUserClient{
		apiCreds:     creds,
		markets:      make(map[string]bool),
		tradeHandler: handler,
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}
}

// Start connects to the user WebSocket and begins listening
func (c *WSUserClient) Start(ctx context.Context) error {
	if c.running {
		return fmt.Errorf("WebSocket user client already running")
	}

	if err := c.connect(); err != nil {
		return fmt.Errorf("initial connection failed: %w", err)
	}

	c.running = true
	go c.readLoop(ctx)

	log.Printf("[WSUser] Started connection to %s", wsUserURL)
	return nil
}

// Stop gracefully closes the WebSocket connection
func (c *WSUserClient) Stop() {
	if !c.running {
		return
	}

	c.running = false
	close(c.stopCh)

	c.connMu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.connMu.Unlock()

	<-c.doneCh
	log.Printf("[WSUser] Stopped")
}

func (c *WSUserClient) connect() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn != nil {
		c.conn.Close()
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(wsUserURL, nil)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}

	c.conn = conn

	// Send authentication message
	authMsg := map[string]interface{}{
		"auth": map[string]string{
			"apiKey":     c.apiCreds.APIKey,
			"secret":     c.apiCreds.APISecret,
			"passphrase": c.apiCreds.APIPassphrase,
		},
		"type": "USER",
	}

	if err := conn.WriteJSON(authMsg); err != nil {
		return fmt.Errorf("auth failed: %w", err)
	}

	return nil
}

// SubscribeMarkets subscribes to user activity on specific markets
func (c *WSUserClient) SubscribeMarkets(conditionIDs ...string) error {
	c.subMu.Lock()
	for _, id := range conditionIDs {
		c.markets[id] = true
	}
	c.subMu.Unlock()

	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("not connected")
	}

	msg := map[string]interface{}{
		"auth": map[string]string{
			"apiKey":     c.apiCreds.APIKey,
			"secret":     c.apiCreds.APISecret,
			"passphrase": c.apiCreds.APIPassphrase,
		},
		"markets": conditionIDs,
		"type":    "USER",
	}

	return c.conn.WriteJSON(msg)
}

func (c *WSUserClient) readLoop(ctx context.Context) {
	defer close(c.doneCh)

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		c.connMu.Lock()
		conn := c.conn
		c.connMu.Unlock()

		if conn == nil {
			time.Sleep(reconnectDelay)
			c.connect()
			continue
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[WSUser] Read error: %v", err)
			time.Sleep(reconnectDelay)
			c.connect()
			continue
		}

		c.handleUserMessage(message)
	}
}

func (c *WSUserClient) handleUserMessage(data []byte) {
	// User channel messages contain trade/fill information
	var msg map[string]interface{}
	if err := json.Unmarshal(data, &msg); err != nil {
		return
	}

	// Process trade events and call handler
	if c.tradeHandler != nil {
		// Parse trade data and call handler
		// The exact format depends on Polymarket's API
		// This is a placeholder for the actual implementation
	}
}
