// Package api provides WebSocket client for real-time Polymarket live data.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// LiveData WebSocket endpoint
	liveDataWSURL = "wss://ws-live-data.polymarket.com/"

	// Reconnection settings for live data
	liveDataReconnectDelay    = 2 * time.Second
	liveDataMaxReconnectDelay = 30 * time.Second
	liveDataPingInterval      = 30 * time.Second
	liveDataReadTimeout       = 90 * time.Second // Allow longer idle periods between trades
)

// LiveDataTradeEvent represents a trade from the orders_matched WebSocket
type LiveDataTradeEvent struct {
	Name            string  `json:"name"`
	ProxyWallet     string  `json:"proxyWallet"`
	Pseudonym       string  `json:"pseudonym"`
	Side            string  `json:"side"`
	Size            float64 `json:"size"`
	Price           float64 `json:"price"`
	Outcome         string  `json:"outcome"`
	OutcomeIndex    int     `json:"outcomeIndex"`
	EventSlug       string  `json:"eventSlug"`
	ConditionID     string  `json:"conditionId"`
	TransactionHash string  `json:"transactionHash"`
	Timestamp       int64   `json:"timestamp"`
}

// LiveDataMessage represents a WebSocket message from ws-live-data
type LiveDataMessage struct {
	ConnectionID string             `json:"connection_id"`
	Topic        string             `json:"topic"`
	Type         string             `json:"type"`
	Timestamp    int64              `json:"timestamp"`
	Payload      LiveDataTradeEvent `json:"payload"`
}

// LiveDataTradeHandler is called when a new trade is detected
type LiveDataTradeHandler func(event LiveDataTradeEvent)

// LiveDataWSClient manages WebSocket connection to ws-live-data.polymarket.com
type LiveDataWSClient struct {
	conn   *websocket.Conn
	connMu sync.Mutex

	// Current subscriptions (event slugs)
	subscriptions   []string
	subscriptionsMu sync.RWMutex

	tradeHandler LiveDataTradeHandler
	running      bool
	stopCh       chan struct{}
	doneCh       chan struct{}

	// Reconnection state
	reconnectAttempts int
	lastPong          time.Time
}

// NewLiveDataWSClient creates a new WebSocket client for live data
func NewLiveDataWSClient(handler LiveDataTradeHandler) *LiveDataWSClient {
	return &LiveDataWSClient{
		subscriptions: make([]string, 0),
		tradeHandler:  handler,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
}

// Start connects to the WebSocket and begins listening
func (c *LiveDataWSClient) Start(ctx context.Context) error {
	if c.running {
		return fmt.Errorf("LiveData WebSocket client already running")
	}

	if err := c.connect(); err != nil {
		return fmt.Errorf("initial connection failed: %w", err)
	}

	c.running = true
	go c.readLoop(ctx)
	go c.pingLoop(ctx)

	log.Printf("[LiveDataWS] Started connection to %s", liveDataWSURL)
	return nil
}

// Stop gracefully closes the WebSocket connection
func (c *LiveDataWSClient) Stop() {
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
		log.Printf("[LiveDataWS] Shutdown timeout")
	}

	log.Printf("[LiveDataWS] Stopped")
}

// UpdateSubscriptions updates the event slugs to subscribe to
func (c *LiveDataWSClient) UpdateSubscriptions(eventSlugs []string) error {
	c.subscriptionsMu.Lock()
	c.subscriptions = eventSlugs
	c.subscriptionsMu.Unlock()

	return c.sendSubscription(eventSlugs)
}

// SubscribeToAllTrades subscribes to all orders_matched events without filtering
// This is simpler and more reliable than filtering by event_slug
func (c *LiveDataWSClient) SubscribeToAllTrades() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("not connected")
	}

	msg := map[string]interface{}{
		"action": "subscribe",
		"subscriptions": []map[string]interface{}{
			{
				"topic": "activity",
				"type":  "orders_matched",
				// No filters - receive ALL trades, filter locally by proxyWallet
			},
		},
	}

	if err := c.conn.WriteJSON(msg); err != nil {
		return fmt.Errorf("send subscription failed: %w", err)
	}

	log.Printf("[LiveDataWS] Subscribed to ALL orders_matched (no filter)")
	return nil
}

func (c *LiveDataWSClient) connect() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn != nil {
		c.conn.Close()
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	// Required Origin header
	headers := http.Header{}
	headers.Set("Origin", "https://polymarket.com")

	conn, _, err := dialer.Dial(liveDataWSURL, headers)
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

func (c *LiveDataWSClient) sendSubscription(eventSlugs []string) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("not connected")
	}

	// Build subscription message with filters for each slug
	subscriptions := make([]map[string]interface{}, 0, len(eventSlugs))
	for _, slug := range eventSlugs {
		subscriptions = append(subscriptions, map[string]interface{}{
			"topic":   "activity",
			"type":    "orders_matched",
			"filters": fmt.Sprintf(`{"event_slug":"%s"}`, slug),
		})
	}

	msg := map[string]interface{}{
		"action":        "subscribe",
		"subscriptions": subscriptions,
	}

	if err := c.conn.WriteJSON(msg); err != nil {
		return fmt.Errorf("send subscription failed: %w", err)
	}

	log.Printf("[LiveDataWS] Subscribed to %d event slugs: %v", len(eventSlugs), eventSlugs)
	return nil
}

func (c *LiveDataWSClient) resubscribe() error {
	// Always subscribe to all trades (no filter)
	return c.SubscribeToAllTrades()
}

func (c *LiveDataWSClient) readLoop(ctx context.Context) {
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

		// Set read deadline - allow longer idle periods between trades
		conn.SetReadDeadline(time.Now().Add(liveDataReadTimeout))

		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Printf("[LiveDataWS] Connection closed normally")
				return
			}
			// Timeouts are expected when no trades happen - just reconnect quietly
			if c.reconnectAttempts == 0 {
				log.Printf("[LiveDataWS] Idle timeout, reconnecting...")
			}
			c.reconnect(ctx)
			continue
		}

		c.handleMessage(message)
	}
}

func (c *LiveDataWSClient) pingLoop(ctx context.Context) {
	ticker := time.NewTicker(liveDataPingInterval)
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
				log.Printf("[LiveDataWS] Ping failed: %v", err)
			}

			// Check if we haven't received a pong in too long
			if time.Since(c.lastPong) > liveDataReadTimeout {
				log.Printf("[LiveDataWS] Pong timeout (no response in %v), reconnecting...", liveDataReadTimeout)
				c.reconnect(ctx)
			}
		}
	}
}

func (c *LiveDataWSClient) reconnect(ctx context.Context) {
	c.reconnectAttempts++
	delay := liveDataReconnectDelay * time.Duration(c.reconnectAttempts)
	if delay > liveDataMaxReconnectDelay {
		delay = liveDataMaxReconnectDelay
	}

	log.Printf("[LiveDataWS] Reconnecting in %v (attempt %d)...", delay, c.reconnectAttempts)

	select {
	case <-ctx.Done():
		return
	case <-c.stopCh:
		return
	case <-time.After(delay):
	}

	if err := c.connect(); err != nil {
		log.Printf("[LiveDataWS] Reconnection failed: %v", err)
		return
	}

	// Resubscribe to all slugs
	if err := c.resubscribe(); err != nil {
		log.Printf("[LiveDataWS] Resubscription failed: %v", err)
	}
}

func (c *LiveDataWSClient) handleMessage(data []byte) {
	var msg LiveDataMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		// Ignore non-JSON messages (like connection confirmations)
		return
	}

	// Only process orders_matched events
	if msg.Type != "orders_matched" {
		return
	}

	// Call the trade handler
	if c.tradeHandler != nil {
		c.tradeHandler(msg.Payload)
	}
}
