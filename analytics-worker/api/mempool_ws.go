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
	From         string    // User's proxy wallet address (the followed user)
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
	Role         string  // "MAKER" or "TAKER" - indicates if user is maker or taker in the order

	// Additional decoded fields for analysis
	TxSender       string   // Who sent the TX (the relayer/operator, NOT the trader)
	MakerAddress   string   // Maker address from Order struct
	TakerAddress   string   // Taker address from Order struct (often 0x0 for open orders)
	SignerAddress  string   // Who signed the order (may differ from maker for proxy wallets)
	Expiration     uint64   // Order expiration timestamp
	OrderNonce     *big.Int // Order nonce (sequence number)
	FeeRateBps     uint64   // Fee rate in basis points
	FillAmount     *big.Int // Actual fill amount (may be less than full order)
	OrderCount     int      // Number of orders in multi-fill TX
	FunctionSig    string   // Function signature (e.g., "fillOrders", "matchOrders")
	InputDataLen   int      // Length of input data in bytes
	SignatureType  uint8    // 0=EOA, 1=POLY_PROXY, 2=POLY_GNOSIS_SAFE
	SaltHex        string   // Salt as hex string (unique order ID)

	// TX-level fields
	Gas            uint64   // Gas limit for this TX
	MaxFeePerGas   *big.Int // EIP-1559 max fee per gas (nil if legacy TX)
	MaxPriorityFee *big.Int // EIP-1559 max priority fee (nil if legacy TX)
	TxValue        *big.Int // TX value (should be 0 for trades)
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

// AddressWithRole contains an address and its role in the Order struct
type AddressWithRole struct {
	Address string
	Role    string // "MAKER" or "TAKER"
}

// extractMakerAddresses extracts maker/signer/taker addresses from Polymarket order input data
// Order struct fields (each 32 bytes):
// 0: salt, 1: maker, 2: signer, 3: taker, 4: tokenId, 5: makerAmount, 6: takerAmount...
// Returns addresses with their role (MAKER or TAKER) in the order
func extractMakerAddresses(input string) []string {
	results := extractAddressesWithRole(input)
	addresses := make([]string, 0, len(results))
	for _, r := range results {
		addresses = append(addresses, r.Address)
	}
	return addresses
}

// extractAddressesWithRole extracts addresses and their role from order calldata
// The Order struct layout is:
//   - Offset 0x00: salt (32 bytes)
//   - Offset 0x20: maker (32 bytes) <-- MAKER address
//   - Offset 0x40: signer (32 bytes)
//   - Offset 0x60: taker (32 bytes) <-- TAKER address
//   - Offset 0x80: tokenId (32 bytes)
//   - ...
func extractAddressesWithRole(input string) []AddressWithRole {
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

	var results []AddressWithRole
	seen := make(map[string]bool)

	// Order struct size is approximately 13-14 * 32 bytes = 416-448 bytes
	// We look for Order structs by scanning for valid address patterns
	// at maker (offset +0x20) and taker (offset +0x60) positions

	// First pass: find potential Order struct starts by looking for salt patterns
	// Salt is typically a large random number, followed by maker address
	for orderStart := 0; orderStart+448 <= len(dataBytes); orderStart += 32 {
		// Check if this could be the start of an Order struct
		// Try to extract maker at orderStart+32 and taker at orderStart+96

		makerOffset := orderStart + 32  // 0x20
		takerOffset := orderStart + 96  // 0x60

		// Check maker field
		if makerOffset+32 <= len(dataBytes) {
			if isValidAddressField(dataBytes[makerOffset : makerOffset+32]) {
				addrBytes := dataBytes[makerOffset+12 : makerOffset+32]
				addr := fmt.Sprintf("0x%x", addrBytes)
				key := addr + ":MAKER"
				if !seen[key] {
					seen[key] = true
					results = append(results, AddressWithRole{Address: addr, Role: "MAKER"})
				}
			}
		}

		// Check taker field
		if takerOffset+32 <= len(dataBytes) {
			if isValidAddressField(dataBytes[takerOffset : takerOffset+32]) {
				addrBytes := dataBytes[takerOffset+12 : takerOffset+32]
				addr := fmt.Sprintf("0x%x", addrBytes)
				key := addr + ":TAKER"
				if !seen[key] {
					seen[key] = true
					results = append(results, AddressWithRole{Address: addr, Role: "TAKER"})
				}
			}
		}
	}

	return results
}

// isValidAddressField checks if a 32-byte field contains a valid address
// (first 12 bytes should be zeros, remaining 20 bytes should look like an address)
func isValidAddressField(field []byte) bool {
	if len(field) != 32 {
		return false
	}

	// First 12 bytes should be zeros
	for i := 0; i < 12; i++ {
		if field[i] != 0 {
			return false
		}
	}

	// Check if the address part looks real
	// Real addresses should have non-zero bytes distributed across the address
	addrBytes := field[12:32]
	nonZeroCount := 0
	for i := 0; i < 10; i++ {
		if addrBytes[i] != 0 {
			nonZeroCount++
		}
	}

	// Require at least 3 non-zero bytes in first 10 bytes to be a real address
	return nonZeroCount >= 3
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

	// Extract maker/signer/taker addresses from the order input data with their roles
	// The whale's address is INSIDE the order struct, not tx.From (which is the operator)
	addressesWithRoles := extractAddressesWithRole(tx.Input)

	// Check if any address is a followed user and get their role
	var followedAddr string
	var userRole string
	c.followedAddrsMu.RLock()
	for _, ar := range addressesWithRoles {
		if c.followedAddrs[strings.ToLower(ar.Address)] {
			followedAddr = ar.Address
			userRole = ar.Role
			break
		}
	}
	c.followedAddrsMu.RUnlock()

	// If no followed address found, still return - we already cached the TX
	if followedAddr == "" {
		return
	}

	// Use the followed address as the "from" address for the event
	fromAddr := strings.ToLower(followedAddr)

	// We found a pending trade from a followed user!
	c.statsMu.Lock()
	c.tradesDetected++
	c.statsMu.Unlock()

	// Use DecodeTradeInputForTarget for comprehensive decoding with per-order fill amounts
	decodedOrder, functionSig, orderCount := DecodeTradeInputForTarget(tx.Input, followedAddr)

	event := MempoolTradeEvent{
		TxHash:       txHash,
		From:         fromAddr, // User's proxy wallet (NOT tx.From which is the operator)
		To:           tx.To,
		ContractName: contractName,
		Input:        tx.Input,
		DetectedAt:   time.Now(),
		GasPrice:     tx.GasPrice,
		Nonce:        tx.Nonce,
		Role:         userRole, // "MAKER" or "TAKER" based on position in Order struct

		// Additional analysis fields
		TxSender:       tx.From,                 // The actual TX sender (relayer/operator)
		FunctionSig:    functionSig,             // e.g., "fillOrders", "matchOrders"
		InputDataLen:   (len(tx.Input) - 2) / 2, // Length in bytes (subtract 0x prefix, divide by 2)
		OrderCount:     orderCount,              // Number of orders in this TX
		Gas:            tx.Gas,                  // Gas limit
		MaxFeePerGas:   tx.MaxFeePerGas,         // EIP-1559 max fee (nil if legacy)
		MaxPriorityFee: tx.MaxPriorityFee,       // EIP-1559 priority fee (nil if legacy)
		TxValue:        tx.Value,                // TX value (should be 0)
	}

	// Populate fields from decoded order
	if decodedOrder.Decoded {
		event.Decoded = true
		event.Side = decodedOrder.Side
		event.TokenID = decodedOrder.TokenID
		event.MakerAmount = decodedOrder.MakerAmount
		event.TakerAmount = decodedOrder.TakerAmount
		event.MakerAddress = decodedOrder.MakerAddress
		event.TakerAddress = decodedOrder.TakerAddress
		event.SignerAddress = decodedOrder.SignerAddress
		event.Expiration = decodedOrder.Expiration
		event.OrderNonce = decodedOrder.OrderNonce
		event.FeeRateBps = decodedOrder.FeeRateBps
		event.FillAmount = decodedOrder.FillAmount
		event.SignatureType = decodedOrder.SignatureType
		if decodedOrder.Salt != nil {
			event.SaltHex = fmt.Sprintf("0x%x", decodedOrder.Salt)
		}

		// Calculate size and price
		// FillAmount is now the per-order fill from fillAmounts[orderIndex], NOT Word 3 (total)
		// For price calculation, we use the Order's makerAmount/takerAmount ratio
		makerAmt := decodedOrder.MakerAmount
		takerAmt := decodedOrder.TakerAmount

		// Size comes from FillAmount (takerFillAmountShares), NOT from Order amounts
		if decodedOrder.FillAmount != nil && decodedOrder.FillAmount.Sign() > 0 {
			// FillAmount is in shares (with 6 decimals like USDC)
			fillF := new(big.Float).SetInt(decodedOrder.FillAmount)
			sizeF := new(big.Float).Quo(fillF, big.NewFloat(1e6))
			event.Size, _ = sizeF.Float64()
		}

		// Price comes from Order's maker/taker amounts ratio
		if makerAmt != nil && takerAmt != nil && makerAmt.Sign() > 0 && takerAmt.Sign() > 0 {
			makerF := new(big.Float).SetInt(makerAmt)
			takerF := new(big.Float).SetInt(takerAmt)

			if decodedOrder.Side == "BUY" {
				// Price = USDC paid / tokens received = makerAmount / takerAmount
				priceF := new(big.Float).Quo(makerF, takerF)
				event.Price, _ = priceF.Float64()
			} else {
				// SELL: Price = USDC received / tokens sold = takerAmount / makerAmount
				priceF := new(big.Float).Quo(takerF, makerF)
				event.Price, _ = priceF.Float64()
			}
		}

		// Log with all decoded details
		expirationStr := "N/A"
		if event.Expiration > 0 {
			expirationStr = time.Unix(int64(event.Expiration), 0).Format("15:04:05")
		}
		log.Printf("[MempoolWS] 🚀 PENDING TRADE DECODED: from=%s role=%s side=%s size=%.4f price=%.4f token=%s tx=%s func=%s orders=%d maker=%s taker=%s exp=%s fee=%dbps",
			fromAddr[:16], userRole, decodedOrder.Side, event.Size, event.Price,
			decodedOrder.TokenID[:min(16, len(decodedOrder.TokenID))], txHash[:16],
			functionSig, orderCount,
			event.MakerAddress[:min(16, len(event.MakerAddress))],
			event.TakerAddress[:min(16, len(event.TakerAddress))],
			expirationStr, event.FeeRateBps)
	} else {
		log.Printf("[MempoolWS] 🚀 PENDING TRADE DETECTED (not decoded): from=%s role=%s contract=%s tx=%s func=%s inputLen=%d",
			fromAddr[:16], userRole, contractName, txHash[:16], functionSig, event.InputDataLen)
	}

	if c.onTrade != nil {
		c.onTrade(event)
	}
}

type rpcTransaction struct {
	From           string   `json:"from"`
	To             string   `json:"to"`
	Input          string   `json:"input"`
	Value          *big.Int `json:"value"`
	GasPrice       *big.Int `json:"gasPrice"`
	Nonce          uint64   `json:"nonce"`
	Gas            uint64   `json:"gas"`
	MaxFeePerGas   *big.Int `json:"maxFeePerGas"`   // EIP-1559
	MaxPriorityFee *big.Int `json:"maxPriorityFeePerGas"` // EIP-1559
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
			From                 string `json:"from"`
			To                   string `json:"to"`
			Input                string `json:"input"`
			Value                string `json:"value"`
			GasPrice             string `json:"gasPrice"`
			Nonce                string `json:"nonce"`
			Gas                  string `json:"gas"`
			MaxFeePerGas         string `json:"maxFeePerGas"`         // EIP-1559
			MaxPriorityFeePerGas string `json:"maxPriorityFeePerGas"` // EIP-1559
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

	// Parse gas limit
	if result.Result.Gas != "" {
		gasInt := new(big.Int)
		gasInt.SetString(strings.TrimPrefix(result.Result.Gas, "0x"), 16)
		tx.Gas = gasInt.Uint64()
	}

	// Parse value
	if result.Result.Value != "" {
		tx.Value = new(big.Int)
		tx.Value.SetString(strings.TrimPrefix(result.Result.Value, "0x"), 16)
	}

	// Parse EIP-1559 fields
	if result.Result.MaxFeePerGas != "" {
		tx.MaxFeePerGas = new(big.Int)
		tx.MaxFeePerGas.SetString(strings.TrimPrefix(result.Result.MaxFeePerGas, "0x"), 16)
	}
	if result.Result.MaxPriorityFeePerGas != "" {
		tx.MaxPriorityFee = new(big.Int)
		tx.MaxPriorityFee.SetString(strings.TrimPrefix(result.Result.MaxPriorityFeePerGas, "0x"), 16)
	}

	return tx, nil
}

// DecodedOrder contains all decoded fields from an Order struct
type DecodedOrder struct {
	Decoded       bool
	Side          string   // "BUY" or "SELL"
	TokenID       string   // Market token ID
	MakerAmount   *big.Int // Amount maker gives
	TakerAmount   *big.Int // Amount taker gives
	FillAmount    *big.Int // Actual fill amount for THIS specific order (from fillAmounts array)
	TotalFill     *big.Int // Total fill across all orders (Word 3 - for reference only)
	OrderIndex    int      // Index of this order in the orders array
	MakerAddress  string   // Maker address
	TakerAddress  string   // Taker address (often 0x0 for open orders)
	SignerAddress string   // Who signed the order
	Expiration    uint64   // Order expiration timestamp
	OrderNonce    *big.Int // Order nonce
	FeeRateBps    uint64   // Fee rate in basis points
	Salt          *big.Int // Unique order identifier
	SignatureType uint8    // 0=EOA, 1=POLY_PROXY, 2=POLY_GNOSIS_SAFE
}

// Known function selectors
var functionSelectors = map[string]string{
	"2287e350": "fillOrders",      // NegRiskAdapter
	"e20b2304": "fillOrder",       // CTFExchange single order
	"a4a6c5a5": "matchOrders",     // Match two orders
	"d798eff6": "fillOrdersNeg",   // NegRisk variant
	"4f7e43df": "cancelOrder",     // Cancel order
	"b93ea7ad": "cancelOrders",    // Cancel multiple orders
}

// DecodeTradeInputFull attempts to decode all available fields from transaction input
// Now properly extracts per-order fill amounts instead of total fill
func DecodeTradeInputFull(input string) (order DecodedOrder, functionSig string, orderCount int) {
	return DecodeTradeInputForTarget(input, "")
}

// DecodeTradeInputForTarget decodes transaction input and finds the specific order for target address
// Returns the order with correct per-order fill amount (not total fill)
// Properly parses ABI-encoded arrays at their correct offsets
func DecodeTradeInputForTarget(input string, targetAddress string) (order DecodedOrder, functionSig string, orderCount int) {
	if len(input) < 10 {
		return DecodedOrder{}, "", 0
	}

	data := strings.TrimPrefix(input, "0x")
	if len(data) < 8 {
		return DecodedOrder{}, "", 0
	}

	selector := data[:8]
	functionSig = functionSelectors[selector]
	if functionSig == "" {
		functionSig = "unknown_" + selector
	}

	// Decode hex to bytes (skip function selector)
	dataBytes, err := hex.DecodeString(data[8:])
	if err != nil || len(dataBytes) < 256 {
		return DecodedOrder{}, functionSig, 0
	}

	// Normalize target address for comparison
	targetClean := strings.ToLower(strings.TrimPrefix(targetAddress, "0x"))

	// Order struct layout (each field is 32 bytes = 384 bytes total for 12 fields):
	// 0x00: salt, 0x20: maker, 0x40: signer, 0x60: taker, 0x80: tokenId
	// 0xA0: makerAmount, 0xC0: takerAmount, 0xE0: expiration, 0x100: nonce
	// 0x120: feeRateBps, 0x140: side, 0x160: signatureType
	const orderStructSize = 384

	// Helper to parse a single order at a given byte offset
	parseOrderAt := func(offset int) (makerAddr, signerAddr, takerAddr, tokenID string, makerAmt, takerAmt *big.Int, side string, valid bool) {
		if offset+orderStructSize > len(dataBytes) {
			return
		}
		// Validate maker address field
		makerField := dataBytes[offset+32 : offset+64]
		if !isValidAddressField(makerField) {
			return
		}
		makerAddr = fmt.Sprintf("%x", dataBytes[offset+44:offset+64])
		signerAddr = fmt.Sprintf("%x", dataBytes[offset+76:offset+96])
		takerAddr = fmt.Sprintf("%x", dataBytes[offset+108:offset+128])
		tokenID = new(big.Int).SetBytes(dataBytes[offset+128 : offset+160]).String()
		makerAmt = new(big.Int).SetBytes(dataBytes[offset+160 : offset+192])
		takerAmt = new(big.Int).SetBytes(dataBytes[offset+192 : offset+224])
		sideValue := dataBytes[offset+320+31]
		if sideValue == 0 {
			side = "BUY"
		} else {
			side = "SELL"
		}
		valid = true
		return
	}

	if selector == "2287e350" { // fillOrders
		// fillOrders ABI:
		// Word 0: offset to orders[]
		// Word 1: offset to fillAmounts[]
		// Word 2: takerFillAmountUsdc (TOTAL)
		// Word 3: takerFillAmountShares (TOTAL)
		if len(dataBytes) < 128 {
			return DecodedOrder{}, functionSig, 0
		}

		ordersOffset := int(new(big.Int).SetBytes(dataBytes[0:32]).Int64())
		fillAmountsOffset := int(new(big.Int).SetBytes(dataBytes[32:64]).Int64())
		totalFill := new(big.Int).SetBytes(dataBytes[96:128])

		// Parse orders array
		if ordersOffset+32 > len(dataBytes) {
			return DecodedOrder{}, functionSig, 0
		}
		ordersLen := int(new(big.Int).SetBytes(dataBytes[ordersOffset : ordersOffset+32]).Int64())
		if ordersLen <= 0 || ordersLen > 50 {
			return DecodedOrder{}, functionSig, 0
		}
		orderCount = ordersLen

		// Parse fillAmounts array
		var fillAmounts []*big.Int
		if fillAmountsOffset+32 <= len(dataBytes) {
			fillLen := int(new(big.Int).SetBytes(dataBytes[fillAmountsOffset : fillAmountsOffset+32]).Int64())
			for i := 0; i < fillLen && i < 50; i++ {
				elemOff := fillAmountsOffset + 32 + i*32
				if elemOff+32 <= len(dataBytes) {
					fillAmounts = append(fillAmounts, new(big.Int).SetBytes(dataBytes[elemOff:elemOff+32]))
				}
			}
		}

		// Find target's order in the orders array
		for i := 0; i < ordersLen; i++ {
			orderStart := ordersOffset + 32 + i*orderStructSize
			makerAddr, signerAddr, takerAddr, tokenID, makerAmt, takerAmt, side, valid := parseOrderAt(orderStart)
			if !valid {
				continue
			}

			// Check if this order matches target
			if targetClean == "" || strings.Contains(makerAddr, targetClean) || strings.Contains(signerAddr, targetClean) || strings.Contains(takerAddr, targetClean) {
				order.Decoded = true
				order.OrderIndex = i
				order.TokenID = tokenID
				order.MakerAmount = makerAmt
				order.TakerAmount = takerAmt
				order.Side = side
				order.MakerAddress = "0x" + makerAddr
				order.SignerAddress = "0x" + signerAddr
				order.TakerAddress = "0x" + takerAddr
				order.TotalFill = totalFill

				// Get the CORRECT per-order fill amount
				if i < len(fillAmounts) {
					order.FillAmount = fillAmounts[i]
				} else if ordersLen == 1 {
					order.FillAmount = totalFill
				}
				return order, functionSig, orderCount
			}
		}

	} else if selector == "a4a6c5a5" { // matchOrders
		// matchOrders ABI:
		// Word 0: takerOrder offset
		// Word 1: makerOrders[] offset
		// Word 2: takerFillAmount (USDC)
		// Word 3: takerReceiveAmount (shares - for TAKER only)
		// Word 4: makerFillAmounts[] offset
		if len(dataBytes) < 160 {
			return DecodedOrder{}, functionSig, 0
		}

		takerOrderOffset := int(new(big.Int).SetBytes(dataBytes[0:32]).Int64())
		makerOrdersOffset := int(new(big.Int).SetBytes(dataBytes[32:64]).Int64())
		takerReceiveAmount := new(big.Int).SetBytes(dataBytes[96:128]) // Word 3 - taker's shares
		makerFillAmountsOffset := int(new(big.Int).SetBytes(dataBytes[128:160]).Int64())

		// Parse makerFillAmounts array
		var makerFillAmounts []*big.Int
		if makerFillAmountsOffset+32 <= len(dataBytes) {
			fillLen := int(new(big.Int).SetBytes(dataBytes[makerFillAmountsOffset : makerFillAmountsOffset+32]).Int64())
			for i := 0; i < fillLen && i < 50; i++ {
				elemOff := makerFillAmountsOffset + 32 + i*32
				if elemOff+32 <= len(dataBytes) {
					makerFillAmounts = append(makerFillAmounts, new(big.Int).SetBytes(dataBytes[elemOff:elemOff+32]))
				}
			}
		}

		// Check if target is the TAKER
		if takerOrderOffset+orderStructSize <= len(dataBytes) {
			makerAddr, signerAddr, takerAddr, tokenID, makerAmt, takerAmt, side, valid := parseOrderAt(takerOrderOffset)
			if valid && (targetClean == "" || strings.Contains(makerAddr, targetClean) || strings.Contains(signerAddr, targetClean)) {
				order.Decoded = true
				order.OrderIndex = -1 // Taker
				order.TokenID = tokenID
				order.MakerAmount = makerAmt
				order.TakerAmount = takerAmt
				order.Side = side
				order.MakerAddress = "0x" + makerAddr
				order.SignerAddress = "0x" + signerAddr
				order.TakerAddress = "0x" + takerAddr
				order.FillAmount = takerReceiveAmount // Taker uses Word 3
				order.TotalFill = takerReceiveAmount
				orderCount = 1 + len(makerFillAmounts)
				return order, functionSig, orderCount
			}
		}

		// Check if target is in makerOrders[]
		if makerOrdersOffset+32 <= len(dataBytes) {
			makersLen := int(new(big.Int).SetBytes(dataBytes[makerOrdersOffset : makerOrdersOffset+32]).Int64())
			orderCount = 1 + makersLen // 1 taker + N makers

			for i := 0; i < makersLen && i < 50; i++ {
				orderStart := makerOrdersOffset + 32 + i*orderStructSize
				makerAddr, signerAddr, takerAddr, tokenID, makerAmt, takerAmt, side, valid := parseOrderAt(orderStart)
				if !valid {
					continue
				}

				if strings.Contains(makerAddr, targetClean) || strings.Contains(signerAddr, targetClean) || strings.Contains(takerAddr, targetClean) {
					order.Decoded = true
					order.OrderIndex = i
					order.TokenID = tokenID
					order.MakerAmount = makerAmt
					order.TakerAmount = takerAmt
					order.Side = side
					order.MakerAddress = "0x" + makerAddr
					order.SignerAddress = "0x" + signerAddr
					order.TakerAddress = "0x" + takerAddr
					order.TotalFill = takerReceiveAmount

					// Get the CORRECT per-maker fill amount
					if i < len(makerFillAmounts) {
						order.FillAmount = makerFillAmounts[i]
					}
					return order, functionSig, orderCount
				}
			}
		}
	}

	// Fallback: use old scanning method for unknown function types
	return decodeTradeInputLegacy(dataBytes, targetClean, functionSig)
}

// decodeTradeInputLegacy is the old scanning-based decoder for unknown function types
func decodeTradeInputLegacy(dataBytes []byte, targetClean, functionSig string) (order DecodedOrder, sig string, orderCount int) {
	sig = functionSig
	var totalFill *big.Int
	if len(dataBytes) >= 128 {
		totalFill = new(big.Int).SetBytes(dataBytes[96:128])
	}

	type foundOrder struct {
		index            int
		saltOffset       int
		makerOffset      int
		signerOffset     int
		takerOffset      int
		tokenOffset      int
		makerAmtOffset   int
		takerAmtOffset   int
		expirationOffset int
		nonceOffset      int
		feeOffset        int
		sideOffset       int
		makerAddr        string
		signerAddr       string
		takerAddr        string
	}
	var orders []foundOrder

	for offset := 32; offset+384 <= len(dataBytes); offset += 32 {
		saltOffset := offset
		makerOffset := offset + 32
		signerOffset := offset + 64
		takerOffset := offset + 96
		tokenOffset := offset + 128
		makerAmtOffset := offset + 160
		takerAmtOffset := offset + 192
		expirationOffset := offset + 224
		nonceOffset := offset + 256
		feeOffset := offset + 288
		sideOffset := offset + 320

		if sideOffset+32 > len(dataBytes) {
			continue
		}
		if !isValidAddressField(dataBytes[makerOffset : makerOffset+32]) {
			continue
		}
		potentialTokenID := new(big.Int).SetBytes(dataBytes[tokenOffset : tokenOffset+32])
		if potentialTokenID.Sign() == 0 {
			continue
		}
		makerAmt := new(big.Int).SetBytes(dataBytes[makerAmtOffset : makerAmtOffset+32])
		takerAmt := new(big.Int).SetBytes(dataBytes[takerAmtOffset : takerAmtOffset+32])
		minAmt := big.NewInt(100_000)
		maxAmt := big.NewInt(100_000_000_000)
		if makerAmt.Cmp(minAmt) < 0 || makerAmt.Cmp(maxAmt) > 0 {
			continue
		}
		if takerAmt.Cmp(minAmt) < 0 || takerAmt.Cmp(maxAmt) > 0 {
			continue
		}
		sideValue := dataBytes[sideOffset+31]
		if sideValue > 1 {
			continue
		}

		makerAddr := fmt.Sprintf("%x", dataBytes[makerOffset+12:makerOffset+32])
		signerAddr := fmt.Sprintf("%x", dataBytes[signerOffset+12:signerOffset+32])
		takerAddr := fmt.Sprintf("%x", dataBytes[takerOffset+12:takerOffset+32])

		orders = append(orders, foundOrder{
			index: len(orders), saltOffset: saltOffset, makerOffset: makerOffset, signerOffset: signerOffset,
			takerOffset: takerOffset, tokenOffset: tokenOffset, makerAmtOffset: makerAmtOffset,
			takerAmtOffset: takerAmtOffset, expirationOffset: expirationOffset, nonceOffset: nonceOffset,
			feeOffset: feeOffset, sideOffset: sideOffset, makerAddr: makerAddr, signerAddr: signerAddr, takerAddr: takerAddr,
		})
	}

	orderCount = len(orders)
	if orderCount == 0 {
		return DecodedOrder{}, sig, 0
	}

	selectedIdx := 0
	if targetClean != "" {
		for _, o := range orders {
			if strings.Contains(o.makerAddr, targetClean) || strings.Contains(o.signerAddr, targetClean) || strings.Contains(o.takerAddr, targetClean) {
				selectedIdx = o.index
				break
			}
		}
	}

	if selectedIdx >= len(orders) {
		return DecodedOrder{}, sig, orderCount
	}

	o := orders[selectedIdx]
	order.Decoded = true
	order.OrderIndex = selectedIdx
	order.TokenID = new(big.Int).SetBytes(dataBytes[o.tokenOffset : o.tokenOffset+32]).String()
	order.MakerAmount = new(big.Int).SetBytes(dataBytes[o.makerAmtOffset : o.makerAmtOffset+32])
	order.TakerAmount = new(big.Int).SetBytes(dataBytes[o.takerAmtOffset : o.takerAmtOffset+32])
	order.TotalFill = totalFill
	order.FillAmount = totalFill // Legacy: use total fill as fallback

	sideValue := dataBytes[o.sideOffset+31]
	if sideValue == 0 {
		order.Side = "BUY"
	} else {
		order.Side = "SELL"
	}

	order.MakerAddress = "0x" + o.makerAddr
	order.SignerAddress = "0x" + o.signerAddr
	order.TakerAddress = "0x" + o.takerAddr
	order.Salt = new(big.Int).SetBytes(dataBytes[o.saltOffset : o.saltOffset+32])
	order.Expiration = new(big.Int).SetBytes(dataBytes[o.expirationOffset : o.expirationOffset+32]).Uint64()
	order.OrderNonce = new(big.Int).SetBytes(dataBytes[o.nonceOffset : o.nonceOffset+32])
	order.FeeRateBps = new(big.Int).SetBytes(dataBytes[o.feeOffset : o.feeOffset+32]).Uint64()

	sigTypeOffset := o.sideOffset + 32
	if sigTypeOffset+32 <= len(dataBytes) {
		order.SignatureType = dataBytes[sigTypeOffset+31]
	}

	return order, sig, orderCount
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
