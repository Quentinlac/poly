// Package api provides Polygon blockchain WebSocket client for real-time trade detection.
package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// Polygon WebSocket RPC endpoints (public)
	polygonWSURL        = "wss://polygon-bor-rpc.publicnode.com"
	polygonWSURLBackup  = "wss://polygon.drpc.org"

	// CTF Exchange contract address on Polygon
	CTFExchangeAddress = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"

	// OrderFilled event signature: OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)
	// keccak256("OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)")
	OrderFilledTopic = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
)

// PolygonTradeEvent represents a decoded trade from the blockchain
type PolygonTradeEvent struct {
	TxHash       string
	LogIndex     string // unique within transaction - needed to distinguish multiple fills
	BlockNumber  uint64
	Maker        string // maker address (lowercase, 0x-prefixed)
	Taker        string // taker address (lowercase, 0x-prefixed)
	MakerAssetID string // token ID
	TakerAssetID string
	MakerAmount  *big.Int
	TakerAmount  *big.Int
	Fee          *big.Int
	Timestamp    time.Time
}

// PolygonWSClient monitors Polygon blockchain for CTF Exchange trades
type PolygonWSClient struct {
	conn   *websocket.Conn
	connMu sync.Mutex

	// Subscription ID from eth_subscribe
	subID string

	// Callback when trade detected
	onTrade func(event PolygonTradeEvent)

	// Followed addresses (lowercase, no 0x prefix for faster lookup)
	followedAddrs   map[string]bool
	followedAddrsMu sync.RWMutex

	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}

	// Stats
	eventsReceived int64
	tradesMatched  int64
	statsMu        sync.RWMutex
}

// NewPolygonWSClient creates a new Polygon blockchain WebSocket monitor
func NewPolygonWSClient(onTrade func(event PolygonTradeEvent)) *PolygonWSClient {
	return &PolygonWSClient{
		onTrade:       onTrade,
		followedAddrs: make(map[string]bool),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
}

// SetFollowedAddresses updates the list of addresses to monitor
func (c *PolygonWSClient) SetFollowedAddresses(addrs []string) {
	c.followedAddrsMu.Lock()
	defer c.followedAddrsMu.Unlock()

	c.followedAddrs = make(map[string]bool, len(addrs))
	for _, addr := range addrs {
		// Store lowercase without 0x prefix for fast matching against topics
		normalized := strings.ToLower(strings.TrimPrefix(addr, "0x"))
		c.followedAddrs[normalized] = true
	}
	log.Printf("[PolygonWS] Monitoring %d addresses", len(c.followedAddrs))
}

// AddFollowedAddress adds an address to monitor
func (c *PolygonWSClient) AddFollowedAddress(addr string) {
	c.followedAddrsMu.Lock()
	defer c.followedAddrsMu.Unlock()
	normalized := strings.ToLower(strings.TrimPrefix(addr, "0x"))
	c.followedAddrs[normalized] = true
}

// Start connects to Polygon WebSocket and subscribes to CTF Exchange events
func (c *PolygonWSClient) Start(ctx context.Context) error {
	if c.running {
		return fmt.Errorf("PolygonWS client already running")
	}

	if err := c.connect(); err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}

	if err := c.subscribe(); err != nil {
		c.conn.Close()
		return fmt.Errorf("subscription failed: %w", err)
	}

	c.running = true
	go c.readLoop(ctx)

	log.Printf("[PolygonWS] Started - monitoring CTF Exchange at %s", CTFExchangeAddress)
	return nil
}

// Stop gracefully shuts down the client
func (c *PolygonWSClient) Stop() {
	if !c.running {
		return
	}

	c.running = false
	close(c.stopCh)

	c.connMu.Lock()
	if c.conn != nil {
		// Unsubscribe
		if c.subID != "" {
			unsubMsg := map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "eth_unsubscribe",
				"params":  []string{c.subID},
				"id":      2,
			}
			c.conn.WriteJSON(unsubMsg)
		}
		c.conn.Close()
	}
	c.connMu.Unlock()

	select {
	case <-c.doneCh:
	case <-time.After(5 * time.Second):
		log.Printf("[PolygonWS] Shutdown timeout")
	}

	log.Printf("[PolygonWS] Stopped")
}

// GetStats returns monitoring statistics
func (c *PolygonWSClient) GetStats() (eventsReceived, tradesMatched int64) {
	c.statsMu.RLock()
	defer c.statsMu.RUnlock()
	return c.eventsReceived, c.tradesMatched
}

func (c *PolygonWSClient) connect() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	// Try primary endpoint
	conn, _, err := dialer.Dial(polygonWSURL, nil)
	if err != nil {
		// Try backup
		log.Printf("[PolygonWS] Primary endpoint failed, trying backup...")
		conn, _, err = dialer.Dial(polygonWSURLBackup, nil)
		if err != nil {
			return fmt.Errorf("all endpoints failed: %w", err)
		}
	}

	c.conn = conn
	log.Printf("[PolygonWS] Connected to Polygon RPC")
	return nil
}

func (c *PolygonWSClient) subscribe() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("not connected")
	}

	// Subscribe to logs from CTF Exchange with OrderFilled topic
	subMsg := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_subscribe",
		"params": []interface{}{
			"logs",
			map[string]interface{}{
				"address": CTFExchangeAddress,
				"topics":  []string{OrderFilledTopic},
			},
		},
		"id": 1,
	}

	if err := c.conn.WriteJSON(subMsg); err != nil {
		return fmt.Errorf("subscribe write failed: %w", err)
	}

	// Read subscription response
	c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, msg, err := c.conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("subscribe read failed: %w", err)
	}
	c.conn.SetReadDeadline(time.Time{}) // Reset deadline

	var resp struct {
		Result string `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(msg, &resp); err != nil {
		return fmt.Errorf("subscribe parse failed: %w", err)
	}

	if resp.Error != nil {
		return fmt.Errorf("subscribe error: %s", resp.Error.Message)
	}

	c.subID = resp.Result
	log.Printf("[PolygonWS] Subscribed to OrderFilled events (sub_id=%s)", c.subID)
	return nil
}

func (c *PolygonWSClient) readLoop(ctx context.Context) {
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

		_, msg, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return
			}
			log.Printf("[PolygonWS] Read error: %v, reconnecting...", err)
			c.reconnect(ctx)
			continue
		}

		c.handleMessage(msg)
	}
}

func (c *PolygonWSClient) reconnect(ctx context.Context) {
	log.Printf("[PolygonWS] Reconnecting in 2s...")

	select {
	case <-ctx.Done():
		return
	case <-c.stopCh:
		return
	case <-time.After(2 * time.Second):
	}

	if err := c.connect(); err != nil {
		log.Printf("[PolygonWS] Reconnection failed: %v", err)
		return
	}

	if err := c.subscribe(); err != nil {
		log.Printf("[PolygonWS] Resubscription failed: %v", err)
	}
}

func (c *PolygonWSClient) handleMessage(data []byte) {
	// Parse subscription notification
	var notif struct {
		Method string `json:"method"`
		Params struct {
			Subscription string          `json:"subscription"`
			Result       json.RawMessage `json:"result"`
		} `json:"params"`
	}

	if err := json.Unmarshal(data, &notif); err != nil {
		return
	}

	if notif.Method != "eth_subscription" || notif.Params.Subscription != c.subID {
		return
	}

	// Parse log entry
	var logEntry struct {
		Address          string   `json:"address"`
		Topics           []string `json:"topics"`
		Data             string   `json:"data"`
		BlockNumber      string   `json:"blockNumber"`
		TransactionHash  string   `json:"transactionHash"`
		TransactionIndex string   `json:"transactionIndex"`
		BlockHash        string   `json:"blockHash"`
		LogIndex         string   `json:"logIndex"`
		Removed          bool     `json:"removed"`
	}

	if err := json.Unmarshal(notif.Params.Result, &logEntry); err != nil {
		log.Printf("[PolygonWS] Failed to parse log: %v", err)
		return
	}

	c.statsMu.Lock()
	c.eventsReceived++
	c.statsMu.Unlock()

	// Parse the event
	event, err := c.decodeOrderFilledEvent(logEntry.Topics, logEntry.Data, logEntry.TransactionHash, logEntry.BlockNumber, logEntry.LogIndex)
	if err != nil {
		log.Printf("[PolygonWS] Failed to decode event: %v", err)
		return
	}

	// Check if maker or taker is a followed address
	c.followedAddrsMu.RLock()
	makerNorm := strings.ToLower(strings.TrimPrefix(event.Maker, "0x"))
	takerNorm := strings.ToLower(strings.TrimPrefix(event.Taker, "0x"))
	isFollowed := c.followedAddrs[makerNorm] || c.followedAddrs[takerNorm]
	c.followedAddrsMu.RUnlock()

	if isFollowed {
		c.statsMu.Lock()
		c.tradesMatched++
		c.statsMu.Unlock()

		log.Printf("[PolygonWS] 🚨 FOLLOWED USER TRADE: maker=%s taker=%s tx=%s",
			event.Maker[:10], event.Taker[:10], event.TxHash[:10])

		if c.onTrade != nil {
			c.onTrade(event)
		}
	}
}

// decodeOrderFilledEvent decodes an OrderFilled event from topics and data
func (c *PolygonWSClient) decodeOrderFilledEvent(topics []string, data string, txHash string, blockNum string, logIndex string) (PolygonTradeEvent, error) {
	event := PolygonTradeEvent{
		TxHash:    txHash,
		LogIndex:  logIndex, // unique within tx - needed to process all fills
		Timestamp: time.Now(), // Blockchain event is essentially real-time
	}

	// Parse block number
	if strings.HasPrefix(blockNum, "0x") {
		bn := new(big.Int)
		bn.SetString(blockNum[2:], 16)
		event.BlockNumber = bn.Uint64()
	}

	// Topics:
	// [0] = event signature (OrderFilledTopic)
	// [1] = orderHash (bytes32)
	// [2] = maker (address, padded to 32 bytes)
	// [3] = taker (address, padded to 32 bytes)
	if len(topics) < 4 {
		return event, fmt.Errorf("expected 4 topics, got %d", len(topics))
	}

	// Extract maker address from topic[2] (last 40 chars = 20 bytes = address)
	maker := topics[2]
	if len(maker) >= 42 {
		event.Maker = "0x" + maker[len(maker)-40:]
	}

	// Extract taker address from topic[3]
	taker := topics[3]
	if len(taker) >= 42 {
		event.Taker = "0x" + taker[len(taker)-40:]
	}

	// Data contains: makerAssetId, takerAssetId, makerAmountFilled, takerAmountFilled, fee
	// Each is 32 bytes (64 hex chars)
	dataHex := strings.TrimPrefix(data, "0x")
	if len(dataHex) >= 320 { // 5 * 64 = 320
		// makerAssetId (32 bytes)
		event.MakerAssetID = "0x" + dataHex[0:64]

		// takerAssetId (32 bytes)
		event.TakerAssetID = "0x" + dataHex[64:128]

		// makerAmountFilled (32 bytes)
		makerAmtHex := dataHex[128:192]
		event.MakerAmount = new(big.Int)
		event.MakerAmount.SetString(makerAmtHex, 16)

		// takerAmountFilled (32 bytes)
		takerAmtHex := dataHex[192:256]
		event.TakerAmount = new(big.Int)
		event.TakerAmount.SetString(takerAmtHex, 16)

		// fee (32 bytes)
		feeHex := dataHex[256:320]
		event.Fee = new(big.Int)
		event.Fee.SetString(feeHex, 16)
	}

	return event, nil
}

// Helper to convert hex string to address
func hexToAddress(h string) string {
	h = strings.TrimPrefix(h, "0x")
	if len(h) < 40 {
		return ""
	}
	// Take last 40 characters (address is 20 bytes = 40 hex chars)
	return "0x" + strings.ToLower(h[len(h)-40:])
}

// Helper to decode hex data
func decodeHexData(h string) ([]byte, error) {
	h = strings.TrimPrefix(h, "0x")
	return hex.DecodeString(h)
}
