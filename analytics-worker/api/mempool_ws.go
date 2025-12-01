// Package api provides Polygon mempool WebSocket client for faster trade detection.
// This monitors pending transactions (~3-4s faster than LiveData WebSocket)
package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// Polygon WebSocket RPC endpoints for mempool monitoring
	mempoolWSURL       = "wss://polygon-bor-rpc.publicnode.com"
	mempoolWSURLBackup = "wss://polygon.drpc.org"

	// HTTP RPC for fetching full transaction data
	// IMPORTANT: Must use SAME provider as WebSocket to find pending TXs!
	polygonHTTPRPC       = "https://polygon-bor-rpc.publicnode.com"
	polygonHTTPRPCBackup = "https://polygon.drpc.org"
)

// Polymarket contract addresses (lowercase)
var polymarketContracts = map[string]string{
	"0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e": "CTFExchange",
	"0xc5d563a36ae78145c45a50134d48a1215220f80a": "NegRiskCTFExchange",
	"0xe3f18acc55091e2c48d883fc8c8413319d4ab7b0": "NegRiskAdapter",
}

// MempoolTradeEvent represents a pending trade detected in mempool
type MempoolTradeEvent struct {
	TxHash       string
	From         string    // User's proxy wallet address
	To           string    // Polymarket contract
	ContractName string    // "CTFExchange", "NegRiskCTFExchange", or "NegRiskAdapter"
	Input        string    // Encoded function call
	DetectedAt   time.Time // When we saw it in mempool
	GasPrice     *big.Int
	Nonce        uint64

	// Decoded trade details (if available)
	Decoded      bool
	Side         string // "BUY" or "SELL"
	TokenID      string // Market token ID
	MakerAmount  *big.Int
	TakerAmount  *big.Int
	Size         float64 // Decoded size
	Price        float64 // Decoded price
}

// MempoolTradeHandler is called when a pending trade is detected from a followed user
type MempoolTradeHandler func(event MempoolTradeEvent)

// MempoolWSClient monitors Polygon mempool for pending trades
type MempoolWSClient struct {
	conn   *websocket.Conn
	connMu sync.Mutex

	// Subscription ID
	subID string

	// HTTP client for fetching transaction details
	httpClient *http.Client

	// Callback when trade detected
	onTrade MempoolTradeHandler

	// Followed addresses (lowercase, with 0x prefix)
	followedAddrs   map[string]bool
	followedAddrsMu sync.RWMutex

	// Cache of Polymarket transactions seen in mempool: tx_hash -> timestamp
	// Used to check if a trade was pre-detected when LiveData reports it
	mempoolCache   map[string]time.Time
	mempoolCacheMu sync.RWMutex

	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}

	// Stats
	pendingTxSeen        int64
	polymarketTxSeen     int64
	tradesDetected       int64
	statsMu              sync.RWMutex

	// Rate limiting for RPC calls
	lastRPCCall time.Time
	rpcMu       sync.Mutex
}

// NewMempoolWSClient creates a new mempool monitor
func NewMempoolWSClient(onTrade MempoolTradeHandler) *MempoolWSClient {
	return &MempoolWSClient{
		onTrade:       onTrade,
		followedAddrs: make(map[string]bool),
		mempoolCache:  make(map[string]time.Time),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// GetMempoolTime returns when a transaction was first seen in the mempool
// Returns zero time if not found
func (c *MempoolWSClient) GetMempoolTime(txHash string) (time.Time, bool) {
	c.mempoolCacheMu.RLock()
	defer c.mempoolCacheMu.RUnlock()
	t, ok := c.mempoolCache[strings.ToLower(txHash)]
	return t, ok
}

// GetPolymarketTxCount returns the number of Polymarket transactions seen in mempool
func (c *MempoolWSClient) GetPolymarketTxCount() int64 {
	c.statsMu.RLock()
	defer c.statsMu.RUnlock()
	return c.polymarketTxSeen
}

// SetFollowedAddresses updates the list of addresses to monitor
func (c *MempoolWSClient) SetFollowedAddresses(addrs []string) {
	c.followedAddrsMu.Lock()
	defer c.followedAddrsMu.Unlock()

	c.followedAddrs = make(map[string]bool, len(addrs))
	for _, addr := range addrs {
		normalized := strings.ToLower(addr)
		if !strings.HasPrefix(normalized, "0x") {
			normalized = "0x" + normalized
		}
		c.followedAddrs[normalized] = true
	}
	log.Printf("[MempoolWS] Monitoring %d addresses for pending transactions", len(c.followedAddrs))
}

// AddFollowedAddress adds an address to monitor
func (c *MempoolWSClient) AddFollowedAddress(addr string) {
	c.followedAddrsMu.Lock()
	defer c.followedAddrsMu.Unlock()
	normalized := strings.ToLower(addr)
	if !strings.HasPrefix(normalized, "0x") {
		normalized = "0x" + normalized
	}
	c.followedAddrs[normalized] = true
}

// Start connects to Polygon WebSocket and subscribes to pending transactions
func (c *MempoolWSClient) Start(ctx context.Context) error {
	if c.running {
		return fmt.Errorf("MempoolWS client already running")
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

	log.Printf("[MempoolWS] Started - monitoring pending transactions to CTF Exchange")
	return nil
}

// Stop gracefully shuts down the client
func (c *MempoolWSClient) Stop() {
	if !c.running {
		return
	}

	c.running = false
	close(c.stopCh)

	c.connMu.Lock()
	if c.conn != nil {
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
		log.Printf("[MempoolWS] Shutdown timeout")
	}

	log.Printf("[MempoolWS] Stopped")
}

// GetStats returns monitoring statistics
func (c *MempoolWSClient) GetStats() (pendingTxSeen, polymarketTxSeen, tradesDetected int64) {
	c.statsMu.RLock()
	defer c.statsMu.RUnlock()
	return c.pendingTxSeen, c.polymarketTxSeen, c.tradesDetected
}

func (c *MempoolWSClient) connect() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	// Try primary endpoint
	conn, _, err := dialer.Dial(mempoolWSURL, nil)
	if err != nil {
		log.Printf("[MempoolWS] Primary endpoint failed, trying backup...")
		conn, _, err = dialer.Dial(mempoolWSURLBackup, nil)
		if err != nil {
			return fmt.Errorf("all endpoints failed: %w", err)
		}
	}

	c.conn = conn
	log.Printf("[MempoolWS] Connected to Polygon RPC")
	return nil
}

func (c *MempoolWSClient) subscribe() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("not connected")
	}

	// Subscribe to all pending transactions
	subMsg := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_subscribe",
		"params":  []interface{}{"newPendingTransactions"},
		"id":      1,
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
	c.conn.SetReadDeadline(time.Time{})

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
	log.Printf("[MempoolWS] Subscribed to newPendingTransactions (sub_id=%s)", c.subID)
	return nil
}

func (c *MempoolWSClient) readLoop(ctx context.Context) {
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
			log.Printf("[MempoolWS] Read error: %v, reconnecting...", err)
			c.reconnect(ctx)
			continue
		}

		c.handleMessage(ctx, msg)
	}
}

func (c *MempoolWSClient) reconnect(ctx context.Context) {
	log.Printf("[MempoolWS] Reconnecting in 2s...")

	select {
	case <-ctx.Done():
		return
	case <-c.stopCh:
		return
	case <-time.After(2 * time.Second):
	}

	if err := c.connect(); err != nil {
		log.Printf("[MempoolWS] Reconnection failed: %v", err)
		return
	}

	if err := c.subscribe(); err != nil {
		log.Printf("[MempoolWS] Resubscription failed: %v", err)
	}
}

func (c *MempoolWSClient) handleMessage(ctx context.Context, data []byte) {
	// Parse subscription notification
	var notif struct {
		Method string `json:"method"`
		Params struct {
			Subscription string `json:"subscription"`
			Result       string `json:"result"` // This is the tx hash
		} `json:"params"`
	}

	if err := json.Unmarshal(data, &notif); err != nil {
		return
	}

	if notif.Method != "eth_subscription" || notif.Params.Subscription != c.subID {
		return
	}

	txHash := notif.Params.Result
	if txHash == "" {
		return
	}

	now := time.Now()

	c.statsMu.Lock()
	c.pendingTxSeen++
	count := c.pendingTxSeen
	c.statsMu.Unlock()

	// Cache ALL transaction hashes immediately (no HTTP call needed)
	// When LiveData reports a trade, we'll check if we saw its TX hash here
	c.mempoolCacheMu.Lock()
	if _, exists := c.mempoolCache[strings.ToLower(txHash)]; !exists {
		c.mempoolCache[strings.ToLower(txHash)] = now
		c.statsMu.Lock()
		c.polymarketTxSeen++ // Count all cached (might be Polymarket or not)
		c.statsMu.Unlock()
	}
	// Cleanup old entries to prevent memory growth (keep last 5 minutes)
	if count%10000 == 0 {
		cutoff := now.Add(-5 * time.Minute)
		for k, t := range c.mempoolCache {
			if t.Before(cutoff) {
				delete(c.mempoolCache, k)
			}
		}
	}
	c.mempoolCacheMu.Unlock()

	// Log progress periodically
	if count%5000 == 0 {
		c.mempoolCacheMu.RLock()
		cacheSize := len(c.mempoolCache)
		c.mempoolCacheMu.RUnlock()
		log.Printf("[MempoolWS] Seen %d pending transactions, cache size: %d", count, cacheSize)
	}

	// Still try to check if it's a followed user (async, best effort)
	// This enables direct mempool execution if we can decode the trade
	go c.checkTransaction(ctx, txHash)
}

// extractMakerAddresses extracts maker/signer addresses from Polymarket order input data
// Order struct fields (each 32 bytes):
// 0: salt, 1: maker, 2: signer, 3: taker, 4: tokenId, 5: makerAmount, 6: takerAmount...
func extractMakerAddresses(input string) []string {
	if len(input) < 10 {
		return nil
	}

	data := strings.TrimPrefix(input, "0x")
	if len(data) < 8 {
		return nil
	}

	// Skip function selector (4 bytes = 8 hex chars)
	data = data[8:]

	// Decode hex to bytes
	dataBytes, err := hex.DecodeString(data)
	if err != nil || len(dataBytes) < 256 {
		return nil
	}

	var addresses []string
	seen := make(map[string]bool)

	// Scan through the data looking for potential addresses
	// Addresses are 20 bytes, stored in 32-byte fields (12 zero bytes + 20 address bytes)
	// Order struct: salt(32), maker(32), signer(32), taker(32), tokenId(32)...
	// We scan at 32-byte boundaries looking for address patterns

	for offset := 0; offset+32 <= len(dataBytes); offset += 32 {
		// Check if this looks like an address field (first 12 bytes should be zeros)
		isAddressField := true
		for i := 0; i < 12; i++ {
			if dataBytes[offset+i] != 0 {
				isAddressField = false
				break
			}
		}
		if !isAddressField {
			continue
		}

		// Extract the 20-byte address
		addrBytes := dataBytes[offset+12 : offset+32]

		// Skip zero addresses and small values that aren't real addresses
		// Real addresses should have non-zero bytes in the first ~10 bytes
		// (addresses like 0x05c1... have non-zero early bytes)
		nonZeroCount := 0
		for i := 0; i < 10; i++ {
			if addrBytes[i] != 0 {
				nonZeroCount++
			}
		}
		// Require at least 3 non-zero bytes in first 10 bytes to be a real address
		if nonZeroCount < 3 {
			continue
		}

		addr := fmt.Sprintf("0x%x", addrBytes)
		if !seen[addr] {
			seen[addr] = true
			addresses = append(addresses, addr)
		}
	}

	return addresses
}

func (c *MempoolWSClient) checkTransaction(ctx context.Context, txHash string) {
	// Rate limit RPC calls slightly
	c.rpcMu.Lock()
	if time.Since(c.lastRPCCall) < 10*time.Millisecond {
		time.Sleep(10 * time.Millisecond)
	}
	c.lastRPCCall = time.Now()
	c.rpcMu.Unlock()

	tx, err := c.getTransaction(txHash)
	if err != nil {
		// Transaction might not be available yet or already mined
		return
	}

	// Check if transaction is to a Polymarket contract
	toAddr := strings.ToLower(tx.To)
	contractName, isPolymarket := polymarketContracts[toAddr]
	if !isPolymarket {
		return
	}

	now := time.Now()

	// Cache ALL Polymarket transactions - used for pre-detection lookup
	// When LiveData reports a trade, we can check if we saw it in mempool first
	c.mempoolCacheMu.Lock()
	if _, exists := c.mempoolCache[strings.ToLower(txHash)]; !exists {
		c.mempoolCache[strings.ToLower(txHash)] = now
		c.statsMu.Lock()
		c.polymarketTxSeen++
		c.statsMu.Unlock()
	}
	c.mempoolCacheMu.Unlock()

	// Extract maker/signer addresses from the order input data
	// The whale's address is INSIDE the order struct, not tx.From (which is the operator)
	makerAddrs := extractMakerAddresses(tx.Input)

	// Check if any maker/signer is a followed address
	var followedMaker string
	c.followedAddrsMu.RLock()
	for _, addr := range makerAddrs {
		if c.followedAddrs[strings.ToLower(addr)] {
			followedMaker = addr
			break
		}
	}
	c.followedAddrsMu.RUnlock()

	// If no followed address found, still return - we already cached the TX
	if followedMaker == "" {
		return
	}

	// Use the followed maker as the "from" address for the event
	fromAddr := strings.ToLower(followedMaker)

	// We found a pending trade from a followed user!
	c.statsMu.Lock()
	c.tradesDetected++
	c.statsMu.Unlock()

	event := MempoolTradeEvent{
		TxHash:       txHash,
		From:         fromAddr, // User's proxy wallet (NOT tx.From which is the operator)
		To:           tx.To,
		ContractName: contractName,
		Input:        tx.Input,
		DetectedAt:   time.Now(),
		GasPrice:     tx.GasPrice,
		Nonce:        tx.Nonce,
	}

	// Try to decode the trade details from input
	if decoded, side, tokenID, makerAmt, takerAmt := DecodeTradeInput(tx.Input); decoded {
		event.Decoded = true
		event.Side = side
		event.TokenID = tokenID
		event.MakerAmount = makerAmt
		event.TakerAmount = takerAmt

		// Calculate size and price
		// For BUY: makerAmount=USDC paid, takerAmount=tokens received
		// For SELL: makerAmount=tokens sold, takerAmount=USDC received
		if makerAmt != nil && takerAmt != nil {
			makerF := new(big.Float).SetInt(makerAmt)
			takerF := new(big.Float).SetInt(takerAmt)

			if side == "BUY" {
				// Size = tokens received (takerAmount / 1e6)
				sizeF := new(big.Float).Quo(takerF, big.NewFloat(1e6))
				event.Size, _ = sizeF.Float64()
				// Price = USDC paid / tokens received
				if takerAmt.Sign() > 0 {
					priceF := new(big.Float).Quo(makerF, takerF)
					event.Price, _ = priceF.Float64()
				}
			} else {
				// SELL: Size = tokens sold (makerAmount / 1e6)
				sizeF := new(big.Float).Quo(makerF, big.NewFloat(1e6))
				event.Size, _ = sizeF.Float64()
				// Price = USDC received / tokens sold
				if makerAmt.Sign() > 0 {
					priceF := new(big.Float).Quo(takerF, makerF)
					event.Price, _ = priceF.Float64()
				}
			}
		}

		log.Printf("[MempoolWS] 🚀 PENDING TRADE DECODED: from=%s side=%s size=%.4f price=%.4f token=%s tx=%s",
			fromAddr[:16], side, event.Size, event.Price, tokenID[:16], txHash[:16])
	} else {
		log.Printf("[MempoolWS] 🚀 PENDING TRADE DETECTED (not decoded): from=%s contract=%s tx=%s",
			fromAddr[:16], contractName, txHash[:16])
	}

	if c.onTrade != nil {
		c.onTrade(event)
	}
}

type rpcTransaction struct {
	From     string   `json:"from"`
	To       string   `json:"to"`
	Input    string   `json:"input"`
	Value    string   `json:"value"`
	GasPrice *big.Int `json:"gasPrice"`
	Nonce    uint64   `json:"nonce"`
}

func (c *MempoolWSClient) getTransaction(txHash string) (*rpcTransaction, error) {
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_getTransactionByHash",
		"params":  []string{txHash},
		"id":      1,
	}

	jsonBody, _ := json.Marshal(reqBody)

	resp, err := c.httpClient.Post(polygonHTTPRPC, "application/json", strings.NewReader(string(jsonBody)))
	if err != nil {
		// Try backup
		resp, err = c.httpClient.Post(polygonHTTPRPCBackup, "application/json", strings.NewReader(string(jsonBody)))
		if err != nil {
			return nil, err
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result struct {
		Result *struct {
			From     string `json:"from"`
			To       string `json:"to"`
			Input    string `json:"input"`
			Value    string `json:"value"`
			GasPrice string `json:"gasPrice"`
			Nonce    string `json:"nonce"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	if result.Error != nil {
		return nil, fmt.Errorf("rpc error: %s", result.Error.Message)
	}

	if result.Result == nil {
		return nil, fmt.Errorf("transaction not found")
	}

	tx := &rpcTransaction{
		From:  result.Result.From,
		To:    result.Result.To,
		Input: result.Result.Input,
	}

	// Parse gas price
	if result.Result.GasPrice != "" {
		tx.GasPrice = new(big.Int)
		tx.GasPrice.SetString(strings.TrimPrefix(result.Result.GasPrice, "0x"), 16)
	}

	// Parse nonce
	if result.Result.Nonce != "" {
		nonceInt := new(big.Int)
		nonceInt.SetString(strings.TrimPrefix(result.Result.Nonce, "0x"), 16)
		tx.Nonce = nonceInt.Uint64()
	}

	return tx, nil
}

// DecodeTradeInput attempts to decode trade details from transaction input
// Works with NegRiskAdapter (0x2287e350) and CTFExchange fillOrder functions
// Returns: decoded, side, tokenID, makerAmount, takerAmount
func DecodeTradeInput(input string) (decoded bool, side string, tokenID string, makerAmount *big.Int, takerAmount *big.Int) {
	if len(input) < 10 {
		return false, "", "", nil, nil
	}

	// Remove 0x prefix
	data := strings.TrimPrefix(input, "0x")

	// First 4 bytes (8 hex chars) are function selector
	if len(data) < 8 {
		return false, "", "", nil, nil
	}

	selector := data[:8]

	// Known function selectors for Polymarket
	// 0x2287e350 = fillOrders on NegRiskAdapter
	// 0xe20b2304 = fillOrder on CTFExchange
	// 0xa4a6c5a5 = matchOrders

	// For NegRiskAdapter fillOrders (0x2287e350), the structure is complex
	// but we can extract the Order struct from known offsets

	// Decode the hex data
	bytes, err := hex.DecodeString(data[8:]) // Skip function selector
	if err != nil || len(bytes) < 448 {      // Need at least 14 * 32 bytes for one order
		return false, "", "", nil, nil
	}

	// The input for fillOrders has:
	// - Offset to orders array
	// - Offset to fill amounts array
	// - ... other params
	// Then the actual Order structs

	// For a simpler approach, scan for the Order struct pattern
	// Order struct fields (each 32 bytes):
	// 0: salt
	// 1: maker
	// 2: signer
	// 3: taker
	// 4: tokenId (32 bytes - this is what we want!)
	// 5: makerAmount (32 bytes)
	// 6: takerAmount (32 bytes)
	// 7: expiration
	// 8: nonce
	// 9: feeRateBps
	// 10: side (0=BUY, 1=SELL)
	// 11: signatureType
	// 12-13: signature data

	// Try to find an Order struct by looking for reasonable patterns
	// We'll look for potential tokenId (should be a large number ~76 digits)
	// followed by reasonable amounts

	// Skip the first few offsets (usually around 64-128 bytes of header)
	for offset := 64; offset+384 <= len(bytes); offset += 32 {
		// Check if this could be a tokenId position
		// TokenId should be at offset+128 from start of Order struct
		// Let's try reading from here as if it's the start of an Order

		// Try to read what might be tokenId at this position
		potentialTokenID := new(big.Int).SetBytes(bytes[offset : offset+32])

		// TokenIDs are typically very large numbers (256-bit)
		// Skip if it's 0 or too small
		if potentialTokenID.Sign() == 0 {
			continue
		}

		// Check if the next 32 bytes could be makerAmount (reasonable size, not huge offset)
		potentialMakerAmt := new(big.Int).SetBytes(bytes[offset+32 : offset+64])
		potentialTakerAmt := new(big.Int).SetBytes(bytes[offset+64 : offset+96])

		// Sanity check: amounts should be reasonable (not zero, not absurdly large)
		// USDC has 6 decimals, so 1 USDC = 1_000_000
		// A reasonable trade would be 0.1 USDC to 10_000 USDC
		minAmt := big.NewInt(100_000)       // 0.1 USDC
		maxAmt := big.NewInt(10_000_000_000) // 10,000 USDC

		if potentialMakerAmt.Cmp(minAmt) >= 0 && potentialMakerAmt.Cmp(maxAmt) <= 0 &&
			potentialTakerAmt.Cmp(minAmt) >= 0 && potentialTakerAmt.Cmp(maxAmt) <= 0 {

			// Check for side value (should be at offset+192 from tokenId, value 0 or 1)
			sideOffset := offset + 192
			if sideOffset+32 <= len(bytes) {
				sideValue := bytes[sideOffset+31] // Last byte of 32-byte field
				if sideValue == 0 {
					side = "BUY"
				} else if sideValue == 1 {
					side = "SELL"
				} else {
					continue // Not a valid side value
				}

				// We found a likely Order struct!
				tokenID = potentialTokenID.String()
				makerAmount = potentialMakerAmt
				takerAmount = potentialTakerAmt
				return true, side, tokenID, makerAmount, takerAmount
			}
		}
	}

	// Fallback: just detect it's a trade but can't decode details
	if selector == "2287e350" || selector == "e20b2304" || selector == "a4a6c5a5" {
		return false, "", "", nil, nil // Known trade function but couldn't decode
	}

	return false, "", "", nil, nil
}
