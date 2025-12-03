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
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// getQuickNodeRPC returns the QuickNode RPC URL from env or default
func getQuickNodeRPC() string {
	if url := os.Getenv("QUICKNODE_RPC_URL"); url != "" {
		return url
	}
	return quickNodeRPCDefault
}

const (
	// Polygon WebSocket RPC endpoints for mempool monitoring
	mempoolWSURL       = "wss://polygon-bor-rpc.publicnode.com"
	mempoolWSURLBackup = "wss://polygon.drpc.org"

	// HTTP RPC for fetching full transaction data
	// IMPORTANT: Must use SAME provider as WebSocket to find pending TXs!
	polygonHTTPRPC       = "https://polygon-bor-rpc.publicnode.com"
	polygonHTTPRPCBackup = "https://polygon.drpc.org"

	// QuickNode RPC for trace_call (faster than Alchemy simulation)
	// Set via QUICKNODE_RPC_URL env var, fallback to default
	quickNodeRPCDefault = "https://wider-clean-scion.matic.quiknode.pro/147dd05f2eb43a0db2a87bb0a2bbeaf13780fc71/"

	// OrderFilled event signature (for fallback/legacy)
	orderFilledEventSig = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"

	// Contract addresses for trace parsing
	usdcContract = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
	ctfContract  = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"

	// Function signatures for transfer parsing
	transferSig         = "0xa9059cbb" // transfer(address,uint256)
	transferFromSig     = "0x23b872dd" // transferFrom(address,address,uint256)
	safeTransferFromSig = "0xf242432a" // safeTransferFrom(address,address,uint256,uint256,bytes)
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
	OrderIndex    int      // Index of this order in the orders array (-1 if TAKER)
	IsTaker       bool     // True if target is the TAKER of fillOrders (uses Word 3), false if MAKER
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
//
// KEY INSIGHT for fillOrders (0x2287e350):
//   - Word 8 (bytes 256-288) contains the TAKER address of the fillOrders call
//   - Word 3 (takerFillAmountShares) is ONLY valid for the TAKER
//   - For MAKERs, we must use fillAmounts[orderIndex] from the fillAmounts array
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
		// fillOrders ABI (NegRiskAdapter):
		// The NegRiskAdapter can encode orders in two different ways:
		// 1. Standard: [ordersOffset points to array length, then Order data]
		// 2. Direct: [ordersOffset points directly to Order data, no length prefix]
		//
		// Header layout:
		// Word 0: offset to orders data
		// Word 1: offset to fillAmounts[]
		// Word 2: takerFillAmountUsdc (TOTAL for TAKER)
		// Word 3: takerFillAmountShares (TOTAL for TAKER)
		// Word 4-6: other params
		// Word 7: takerNonce
		// Word 8: takerAddress <-- KEY: identifies the TAKER of this fillOrders call
		if len(dataBytes) < 288 { // Need at least 9 words
			return DecodedOrder{}, functionSig, 0
		}

		ordersOffset := int(new(big.Int).SetBytes(dataBytes[0:32]).Int64())
		fillAmountsOffsetWord1 := int(new(big.Int).SetBytes(dataBytes[32:64]).Int64())
		takerFillShares := new(big.Int).SetBytes(dataBytes[96:128]) // Word 3 - TAKER's fill
		fillAmountsOffsetWord4 := int(new(big.Int).SetBytes(dataBytes[128:160]).Int64()) // Word 4 - alternative fillAmounts offset

		// Note: fillOrders doesn't have a separate "taker address" parameter
		// Word 8 is actually Order[0].maker (orders start at offset 224 = Word 7)
		// We determine TAKER vs MAKER by checking which order position the target is in:
		// - Order[0] is typically the "active" order (taker's order being executed immediately)
		// - Order[1+] are typically "passive" orders (maker's limit orders being filled)

		// Try to determine orders array structure
		// Check if ordersOffset points to array length (small value) or Order data directly (large salt)
		var ordersLen int
		var ordersDataStart int
		useDirectOrder := false

		if ordersOffset+32 <= len(dataBytes) {
			potentialLen := new(big.Int).SetBytes(dataBytes[ordersOffset : ordersOffset+32])
			if potentialLen.Cmp(big.NewInt(50)) <= 0 && potentialLen.Sign() > 0 {
				// Standard encoding: ordersOffset points to array length
				ordersLen = int(potentialLen.Int64())
				ordersDataStart = ordersOffset + 32
			} else {
				// Direct encoding: ordersOffset points to Order data
				// Check if it looks like an Order (valid maker address at +32)
				if ordersOffset+orderStructSize <= len(dataBytes) {
					makerField := dataBytes[ordersOffset+32 : ordersOffset+64]
					if isValidAddressField(makerField) {
						useDirectOrder = true
						ordersDataStart = ordersOffset
						// Scan for orders by pattern matching
						ordersLen = 0
						for scanPos := ordersOffset; scanPos+orderStructSize <= len(dataBytes); {
							mf := dataBytes[scanPos+32 : scanPos+64]
							if !isValidAddressField(mf) {
								break
							}
							// Also validate tokenId and amounts
							tokenId := new(big.Int).SetBytes(dataBytes[scanPos+128 : scanPos+160])
							if tokenId.Sign() == 0 {
								break
							}
							ordersLen++
							// Look for next Order (may have signature data in between)
							// Try immediate next position first
							nextPos := scanPos + orderStructSize
							if nextPos+orderStructSize <= len(dataBytes) {
								nextMf := dataBytes[nextPos+32 : nextPos+64]
								if isValidAddressField(nextMf) {
									scanPos = nextPos
									continue
								}
							}
							// Scan ahead for next Order
							found := false
							for gap := orderStructSize + 32; gap < 1024 && scanPos+gap+orderStructSize <= len(dataBytes); gap += 32 {
								testPos := scanPos + gap
								testMf := dataBytes[testPos+32 : testPos+64]
								if isValidAddressField(testMf) {
									testToken := new(big.Int).SetBytes(dataBytes[testPos+128 : testPos+160])
									if testToken.Sign() > 0 {
										scanPos = testPos
										found = true
										break
									}
								}
							}
							if !found {
								break
							}
						}
					}
				}
			}
		}

		if ordersLen <= 0 || ordersLen > 50 {
			return DecodedOrder{}, functionSig, 0
		}
		orderCount = ordersLen

		// Parse fillAmounts array (these are per-MAKER fill amounts)
		// Try both Word 1 and Word 4 offsets - NegRiskAdapter uses different layouts
		var fillAmounts []*big.Int
		fillAmountsOffset := fillAmountsOffsetWord1

		// Helper to parse fillAmounts at a given offset
		parseFillAmountsAt := func(offset int) []*big.Int {
			if offset+32 > len(dataBytes) {
				return nil
			}
			fillLen := int(new(big.Int).SetBytes(dataBytes[offset : offset+32]).Int64())
			if fillLen <= 0 || fillLen > 50 {
				return nil
			}
			var amounts []*big.Int
			for i := 0; i < fillLen; i++ {
				elemOff := offset + 32 + i*32
				if elemOff+32 > len(dataBytes) {
					break
				}
				amt := new(big.Int).SetBytes(dataBytes[elemOff : elemOff+32])
				// Valid fill amounts should be reasonable (100K to 100B in raw units = 0.1 to 100K shares)
				if amt.Cmp(big.NewInt(100000)) >= 0 && amt.Cmp(big.NewInt(100000000000)) <= 0 {
					amounts = append(amounts, amt)
				} else {
					// This offset doesn't have valid fill amounts
					return nil
				}
			}
			return amounts
		}

		// Try Word 1 offset first
		fillAmounts = parseFillAmountsAt(fillAmountsOffsetWord1)

		// If that didn't work, try Word 4 offset (used in some NegRiskAdapter encodings)
		if len(fillAmounts) == 0 && fillAmountsOffsetWord4 != fillAmountsOffsetWord1 {
			fillAmounts = parseFillAmountsAt(fillAmountsOffsetWord4)
			if len(fillAmounts) > 0 {
				fillAmountsOffset = fillAmountsOffsetWord4
			}
		}
		_ = fillAmountsOffset // May be used for debugging

		// Build list of found orders with their positions
		type foundOrderInfo struct {
			pos       int
			makerAddr string
			signerAddr string
			takerAddr string
			tokenID   string
			makerAmt  *big.Int
			takerAmt  *big.Int
			side      string
		}
		var foundOrders []foundOrderInfo

		if useDirectOrder {
			// Scan for orders by pattern
			for scanPos := ordersDataStart; len(foundOrders) < ordersLen && scanPos+orderStructSize <= len(dataBytes); {
				makerAddr, signerAddr, takerAddr, tokenID, makerAmt, takerAmt, side, valid := parseOrderAt(scanPos)
				if valid {
					foundOrders = append(foundOrders, foundOrderInfo{
						pos: scanPos, makerAddr: makerAddr, signerAddr: signerAddr, takerAddr: takerAddr,
						tokenID: tokenID, makerAmt: makerAmt, takerAmt: takerAmt, side: side,
					})
					// Find next Order
					found := false
					for nextPos := scanPos + orderStructSize; nextPos+orderStructSize <= len(dataBytes) && nextPos < scanPos+1024; nextPos += 32 {
						_, _, _, _, _, _, _, valid := parseOrderAt(nextPos)
						if valid {
							scanPos = nextPos
							found = true
							break
						}
					}
					if !found {
						break
					}
				} else {
					scanPos += 32
				}
			}
		} else {
			// Standard encoding: orders are at fixed positions after length
			for i := 0; i < ordersLen; i++ {
				orderStart := ordersDataStart + i*orderStructSize
				makerAddr, signerAddr, takerAddr, tokenID, makerAmt, takerAmt, side, valid := parseOrderAt(orderStart)
				if valid {
					foundOrders = append(foundOrders, foundOrderInfo{
						pos: orderStart, makerAddr: makerAddr, signerAddr: signerAddr, takerAddr: takerAddr,
						tokenID: tokenID, makerAmt: makerAmt, takerAmt: takerAmt, side: side,
					})
				}
			}
		}


		// Determine target's role and find their order
		// In fillOrders:
		// - Order[0] is typically the "active" order (TAKER's order being matched immediately)
		// - Order[1+] are "passive" orders (MAKER's limit orders being filled against)
		// - fillAmounts[] contains fill amounts for MAKER orders only (not Order[0])

		// First, find which order(s) the target is in
		var targetOrderIdx int = -1
		var targetOrder *foundOrderInfo
		for i := range foundOrders {
			if strings.Contains(foundOrders[i].makerAddr, targetClean) ||
				strings.Contains(foundOrders[i].signerAddr, targetClean) {
				targetOrderIdx = i
				targetOrder = &foundOrders[i]
				break
			}
		}

		if targetOrder == nil {
			// Target not found in any order
			return DecodedOrder{}, functionSig, orderCount
		}

		// Determine if target is TAKER (Order[0]) or MAKER (Order[1+])
		targetIsTaker := targetOrderIdx == 0 && len(foundOrders) > 1

		if targetIsTaker {
			// Target is in Order[0] - they are the TAKER
			// Find a counter-party (MAKER) order to get correct token/side info
			var counterOrder *foundOrderInfo
			for i := 1; i < len(foundOrders); i++ {
				counterOrder = &foundOrders[i]
				break
			}
			if counterOrder == nil {
				counterOrder = targetOrder // Fallback
			}

			order.Decoded = true
			order.IsTaker = true
			order.OrderIndex = -1
			order.TokenID = targetOrder.tokenID // Use target's tokenID
			order.MakerAmount = targetOrder.makerAmt
			order.TakerAmount = targetOrder.takerAmt
			// Target's side is their order's side (what they want to do)
			order.Side = targetOrder.side
			order.MakerAddress = "0x" + targetOrder.makerAddr
			order.SignerAddress = "0x" + targetOrder.signerAddr
			order.TakerAddress = "0x" + targetOrder.takerAddr
			order.TotalFill = takerFillShares
			order.FillAmount = takerFillShares // TAKER uses Word 3
			return order, functionSig, orderCount
		}

		// Target is in Order[1+] - they are a MAKER
		order.Decoded = true
		order.IsTaker = false
		order.OrderIndex = targetOrderIdx
		order.TokenID = targetOrder.tokenID
		order.MakerAmount = targetOrder.makerAmt
		order.TakerAmount = targetOrder.takerAmt
		order.Side = targetOrder.side
		order.MakerAddress = "0x" + targetOrder.makerAddr
		order.SignerAddress = "0x" + targetOrder.signerAddr
		order.TakerAddress = "0x" + targetOrder.takerAddr
		order.TotalFill = takerFillShares

		// MAKER uses fillAmounts[] - indexed by position in MAKER orders (excluding Order[0])
		// fillAmounts[0] = Order[1]'s fill, fillAmounts[1] = Order[2]'s fill, etc.
		makerFillIdx := targetOrderIdx - 1 // Order[1] → fillAmounts[0]
		if makerFillIdx >= 0 && makerFillIdx < len(fillAmounts) {
			order.FillAmount = fillAmounts[makerFillIdx]
		} else if len(fillAmounts) > 0 {
			// Fallback: try first fillAmount
			order.FillAmount = fillAmounts[0]
		} else {
			// No fillAmounts found, estimate from total
			if ordersLen > 1 {
				// Divide total by number of maker orders
				numMakers := ordersLen - 1
				order.FillAmount = new(big.Int).Div(takerFillShares, big.NewInt(int64(numMakers)))
			} else {
				order.FillAmount = takerFillShares
			}
		}
		return order, functionSig, orderCount

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
				order.IsTaker = true   // Target is the TAKER
				order.OrderIndex = -1  // -1 indicates TAKER role
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
					order.IsTaker = false // Target is a MAKER
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

// ============================================================================
// QUICKNODE TRACE_CALL - Get accurate trade data via trace analysis
// ============================================================================

// TraceTradeResult holds the extracted trade data from QuickNode trace_call
type TraceTradeResult struct {
	Direction string  // "BUY" or "SELL"
	Tokens    float64 // Token amount (in units, not wei)
	USDC      float64 // USDC amount (in units, not wei)
	Price     float64 // USDC per token
	TokenID   string  // The token being traded (from CTF safeTransferFrom)
	Method    string  // "trace_pending" or "trace_confirmed"
	Error     string  // Error message if extraction failed
}

// ExtractViaTrace extracts accurate trade data for a target user using QuickNode trace_call
// 1. For pending TXs: traces against "latest" state
// 2. For confirmed TXs: traces against blockNumber-1 state
// Can be called with just txHash and targetAddress (will fetch TX details)
func ExtractViaTrace(txHash, from, to, input, targetAddress string) *TraceTradeResult {
	client := &http.Client{Timeout: 10 * time.Second}
	targetClean := strings.ToLower(strings.TrimPrefix(targetAddress, "0x"))

	// If we have TX details, try trace_call against "latest" (for pending TXs)
	if from != "" && to != "" && input != "" {
		result := tryQuickNodeTrace(client, from, to, input, targetClean, "latest")
		if result != nil && result.Error == "" {
			result.Method = "trace_pending"
			return result
		}
	}

	// Fetch TX details if not provided
	tx, blockNumber, err := fetchTransactionWithBlock(client, txHash)
	if err != nil {
		return &TraceTradeResult{Error: fmt.Sprintf("failed to fetch TX: %v", err)}
	}

	// If TX is confirmed (has blockNumber), trace against previous block
	if blockNumber > 0 {
		prevBlock := fmt.Sprintf("0x%x", blockNumber-1)
		result := tryQuickNodeTrace(client, tx.From, tx.To, tx.Input, targetClean, prevBlock)
		if result != nil && result.Error == "" {
			result.Method = "trace_confirmed"
			return result
		}
		return result
	}

	// TX is still pending, trace against "latest"
	result := tryQuickNodeTrace(client, tx.From, tx.To, tx.Input, targetClean, "latest")
	if result != nil && result.Error == "" {
		result.Method = "trace_pending"
		return result
	}
	return result
}

// ExtractViaTraceSimple extracts trade data using just txHash and target address
// This is the simpler API for use in copy trader
func ExtractViaTraceSimple(txHash, targetAddress string) *TraceTradeResult {
	return ExtractViaTrace(txHash, "", "", "", targetAddress)
}

// fetchTransactionWithBlock gets TX details and block number
func fetchTransactionWithBlock(client *http.Client, txHash string) (*struct{ From, To, Input string }, uint64, error) {
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_getTransactionByHash",
		"params":  []string{txHash},
		"id":      1,
	}
	jsonBody, _ := json.Marshal(reqBody)

	resp, err := client.Post(polygonHTTPRPC, "application/json", strings.NewReader(string(jsonBody)))
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	var result struct {
		Result *struct {
			From        string `json:"from"`
			To          string `json:"to"`
			Input       string `json:"input"`
			BlockNumber string `json:"blockNumber"` // null if pending
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, 0, err
	}
	if result.Result == nil {
		return nil, 0, fmt.Errorf("transaction not found")
	}

	var blockNum uint64
	if result.Result.BlockNumber != "" {
		bn := new(big.Int)
		bn.SetString(strings.TrimPrefix(result.Result.BlockNumber, "0x"), 16)
		blockNum = bn.Uint64()
	}

	return &struct{ From, To, Input string }{
		From:  result.Result.From,
		To:    result.Result.To,
		Input: result.Result.Input,
	}, blockNum, nil
}

// tryQuickNodeTrace calls trace_call and parses token transfers
func tryQuickNodeTrace(client *http.Client, from, to, input, targetClean, blockTag string) *TraceTradeResult {
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "trace_call",
		"params": []interface{}{
			map[string]string{"from": from, "to": to, "data": input},
			[]string{"trace"},
			blockTag,
		},
	}
	jsonBody, _ := json.Marshal(reqBody)

	resp, err := client.Post(getQuickNodeRPC(), "application/json", strings.NewReader(string(jsonBody)))
	if err != nil {
		return &TraceTradeResult{Error: fmt.Sprintf("trace_call request failed: %v", err)}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var traceResult struct {
		Result struct {
			Trace []struct {
				Action struct {
					To    string `json:"to"`
					Input string `json:"input"`
				} `json:"action"`
			} `json:"trace"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &traceResult); err != nil {
		return &TraceTradeResult{Error: fmt.Sprintf("trace parse failed: %v", err)}
	}

	if traceResult.Error != nil {
		return &TraceTradeResult{Error: traceResult.Error.Message}
	}

	return parseTraceForTransfers(traceResult.Result.Trace, targetClean)
}

// parseTraceForTransfers extracts USDC and CTF token transfers from trace
func parseTraceForTransfers(traces []struct {
	Action struct {
		To    string `json:"to"`
		Input string `json:"input"`
	} `json:"action"`
}, targetClean string) *TraceTradeResult {
	var usdcIn, usdcOut, tokensIn, tokensOut float64
	var tokenID string

	for _, t := range traces {
		toAddr := strings.ToLower(t.Action.To)
		inp := strings.ToLower(t.Action.Input)

		if len(inp) < 10 {
			continue
		}

		sig := inp[:10]

		// USDC transfer(address recipient, uint256 amount)
		// Data: 0xa9059cbb + recipient(32) + amount(32)
		// Layout: sig(10) + recipient(64) + amount(64) = 138 chars total
		// Address is last 40 chars of 32-byte field (padded with zeros)
		if toAddr == usdcContract && sig == transferSig && len(inp) >= 138 {
			recipient := inp[34:74] // Last 20 bytes of 32-byte field (chars 10+24 to 10+64)
			amountHex := inp[74:138]
			amount := parseHexToFloat(amountHex) / 1e6

			if strings.Contains(recipient, targetClean) {
				usdcIn += amount
			}
		}

		// USDC transferFrom(address from, address to, uint256 amount)
		// Data: 0x23b872dd + from(32) + to(32) + amount(32)
		// Layout: sig(10) + from(64) + to(64) + amount(64) = 202 chars total
		if toAddr == usdcContract && sig == transferFromSig && len(inp) >= 202 {
			fromAddr := inp[34:74] // Last 20 bytes of first 32-byte field
			amountHex := inp[138:202]
			amount := parseHexToFloat(amountHex) / 1e6

			if strings.Contains(fromAddr, targetClean) {
				usdcOut += amount
			}
		}

		// CTF safeTransferFrom(address from, address to, uint256 id, uint256 amount, bytes data)
		// Data: 0xf242432a + from(32) + to(32) + id(32) + amount(32) + ...
		// Layout: sig(10) + from(64) + to(64) + id(64) + amount(64) + ... = 266+ chars
		if toAddr == ctfContract && sig == safeTransferFromSig && len(inp) >= 266 {
			sender := inp[34:74]    // Last 20 bytes of first 32-byte field
			recipient := inp[98:138] // Last 20 bytes of second 32-byte field
			tokenIDHex := inp[138:202]
			amountHex := inp[202:266]
			amount := parseHexToFloat(amountHex) / 1e6

			if strings.Contains(sender, targetClean) {
				tokensOut += amount
				if tokenID == "" {
					// Convert hex to decimal
					tokenIDBig := new(big.Int)
					tokenIDBig.SetString(tokenIDHex, 16)
					tokenID = tokenIDBig.String()
				}
			}
			if strings.Contains(recipient, targetClean) {
				tokensIn += amount
				if tokenID == "" {
					tokenIDBig := new(big.Int)
					tokenIDBig.SetString(tokenIDHex, 16)
					tokenID = tokenIDBig.String()
				}
			}
		}
	}

	// Determine direction based on token flow
	var direction string
	var tokens, usdc float64

	if tokensOut > 0 && usdcIn > 0 {
		// User sent tokens, received USDC → SELL
		direction = "SELL"
		tokens = tokensOut
		usdc = usdcIn
	} else if tokensIn > 0 && usdcOut > 0 {
		// User sent USDC, received tokens → BUY
		direction = "BUY"
		tokens = tokensIn
		usdc = usdcOut
	} else if tokensIn > 0 {
		// Only received tokens (might be partial data)
		direction = "BUY"
		tokens = tokensIn
		usdc = usdcOut
	} else if tokensOut > 0 {
		// Only sent tokens
		direction = "SELL"
		tokens = tokensOut
		usdc = usdcIn
	} else {
		return &TraceTradeResult{Error: "no token transfers found for target user"}
	}

	price := float64(0)
	if tokens > 0 {
		price = usdc / tokens
	}

	return &TraceTradeResult{
		Direction: direction,
		Tokens:    tokens,
		USDC:      usdc,
		Price:     price,
		TokenID:   tokenID,
	}
}

func parseHexToFloat(hexStr string) float64 {
	val, ok := new(big.Int).SetString(hexStr, 16)
	if !ok {
		return 0
	}
	f, _ := new(big.Float).SetInt(val).Float64()
	return f
}

// Legacy alias for backward compatibility
type AlchemyTradeResult = TraceTradeResult

// ExtractViaAlchemy is now an alias for ExtractViaTrace
func ExtractViaAlchemy(txHash, from, to, input, targetAddress string) *TraceTradeResult {
	return ExtractViaTrace(txHash, from, to, input, targetAddress)
}

// ExtractViaAlchemySimple is now an alias for ExtractViaTraceSimple
func ExtractViaAlchemySimple(txHash, targetAddress string) *TraceTradeResult {
	return ExtractViaTraceSimple(txHash, targetAddress)
}
