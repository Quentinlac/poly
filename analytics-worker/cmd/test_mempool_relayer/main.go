// =============================================================================
// MEMPOOL RELAYER MONITOR - Test Script for Polymarket Trade Detection
// =============================================================================
//
// PURPOSE:
// This script monitors the Polygon mempool for pending Polymarket trades.
// It watches transactions going to the Relayer contract and extracts trade details.
//
// KEY DISCOVERY:
// - Polymarket trades do NOT go directly to CTF Exchange
// - They go to Relayer: 0xe3f18acc55091e2c48d883fc8c8413319d4ab7b0
// - The Polymarket Operator (0x7210dd22a1461e6f44701d3d97f4a9f452b144e1) submits ALL trades
// - The actual user's address is embedded in the calldata as the "maker" field
//
// TIMING ADVANTAGE:
// - Mempool detection: ~2.4s after user clicks
// - LiveData detection: ~6s after user clicks
// - Advantage: ~3.6s faster than LiveData WebSocket
//
// USAGE:
//   go run cmd/test_mempool_relayer/main.go                    # Monitor all trades
//   go run cmd/test_mempool_relayer/main.go 0x05c188...        # Monitor specific user
//
// TO IMPLEMENT IN MAIN APP:
// 1. Copy the MempoolRelayerClient struct and methods
// 2. Set followed addresses via SetFollowedAddresses()
// 3. Handle trades via the callback function
// 4. Call Start() to begin monitoring
//
// =============================================================================

package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// =============================================================================
// CONSTANTS - Key addresses and configuration
// =============================================================================

const (
	// Polygon WebSocket RPC for mempool subscription
	PolygonWSRPC = "wss://polygon-bor-rpc.publicnode.com"

	// Polygon HTTP RPC for fetching transaction details
	PolygonHTTPRPC = "https://polygon-bor-rpc.publicnode.com"

	// IMPORTANT: Trades go to Relayer, NOT CTF Exchange!
	// This is the key discovery - we must watch this address
	RelayerAddress = "0xe3f18acc55091e2c48d883fc8c8413319d4ab7b0"

	// CTF Exchange - only receives redemptions, not trades
	CTFExchangeAddress = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"

	// Polymarket Operator - submits all trades on behalf of users
	PolymarketOperator = "0x7210dd22a1461e6f44701d3d97f4a9f452b144e1"
)

// Function selectors for trade functions
// These are the first 4 bytes of keccak256(function signature)
var TradeFunctionSelectors = map[string]string{
	"fe729aaf": "fillOrder",       // Single order fill
	"2287e350": "fillOrders",      // Multiple orders fill (most common)
	"e20b2304": "matchOrders",     // Order matching
	"a4a6c5a5": "operatorFillOrders", // Operator batch fill
}

// =============================================================================
// DATA STRUCTURES
// =============================================================================

// MempoolTrade represents a decoded trade from the mempool
type MempoolTrade struct {
	// Timing information
	DetectedAt    time.Time `json:"detected_at"`    // When we saw it in mempool
	TxHash        string    `json:"tx_hash"`        // Transaction hash

	// Transaction details
	From          string    `json:"from"`           // Always Polymarket Operator
	To            string    `json:"to"`             // Always Relayer
	Nonce         uint64    `json:"nonce"`          // TX nonce
	GasPrice      *big.Int  `json:"gas_price"`      // Gas price in wei

	// Decoded trade details
	Decoded       bool      `json:"decoded"`        // Whether we successfully decoded
	FunctionName  string    `json:"function_name"`  // fillOrder, fillOrders, etc.
	UserAddress   string    `json:"user_address"`   // The actual trader (maker)
	TokenID       string    `json:"token_id"`       // Outcome token ID
	Side          string    `json:"side"`           // BUY or SELL
	MakerAmount   *big.Int  `json:"maker_amount"`   // Amount maker provides
	TakerAmount   *big.Int  `json:"taker_amount"`   // Amount taker provides
	Price         float64   `json:"price"`          // Calculated price
	Size          float64   `json:"size"`           // Calculated size in tokens

	// Raw data for debugging
	RawInput      string    `json:"raw_input"`      // Full calldata (hex)
	InputLength   int       `json:"input_length"`   // Length of calldata
}

// MempoolRelayerClient monitors the mempool for Polymarket trades
type MempoolRelayerClient struct {
	// Configuration
	followedAddresses map[string]bool // Addresses to watch (lowercase, no 0x)

	// WebSocket connection
	conn     *websocket.Conn
	connLock sync.Mutex

	// HTTP client for fetching TX details
	httpClient *http.Client

	// Callback for detected trades
	onTrade func(MempoolTrade)

	// Statistics
	stats struct {
		TotalTxSeen      int64
		RelayerTxSeen    int64
		TradesDecoded    int64
		FollowedTrades   int64
		StartTime        time.Time
	}
	statsLock sync.Mutex

	// Control
	ctx    context.Context
	cancel context.CancelFunc
}

// =============================================================================
// CLIENT IMPLEMENTATION
// =============================================================================

// NewMempoolRelayerClient creates a new mempool monitor
func NewMempoolRelayerClient(onTrade func(MempoolTrade)) *MempoolRelayerClient {
	return &MempoolRelayerClient{
		followedAddresses: make(map[string]bool),
		httpClient:        &http.Client{Timeout: 3 * time.Second},
		onTrade:           onTrade,
	}
}

// SetFollowedAddresses sets the addresses to watch
// Pass empty slice to watch ALL trades
func (c *MempoolRelayerClient) SetFollowedAddresses(addresses []string) {
	c.followedAddresses = make(map[string]bool)
	for _, addr := range addresses {
		// Store lowercase without 0x prefix for easy matching in calldata
		clean := strings.ToLower(strings.TrimPrefix(addr, "0x"))
		c.followedAddresses[clean] = true
	}
}

// Start begins monitoring the mempool
func (c *MempoolRelayerClient) Start(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.stats.StartTime = time.Now()

	go c.monitorLoop()
	return nil
}

// Stop stops the monitor
func (c *MempoolRelayerClient) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.connLock.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.connLock.Unlock()
}

// GetStats returns current statistics
func (c *MempoolRelayerClient) GetStats() (totalTx, relayerTx, decoded, followed int64, uptime time.Duration) {
	c.statsLock.Lock()
	defer c.statsLock.Unlock()
	return c.stats.TotalTxSeen, c.stats.RelayerTxSeen, c.stats.TradesDecoded, c.stats.FollowedTrades, time.Since(c.stats.StartTime)
}

// monitorLoop is the main monitoring loop
func (c *MempoolRelayerClient) monitorLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			if err := c.connectAndMonitor(); err != nil {
				log.Printf("[Mempool] Connection error: %v, reconnecting in 5s...", err)
				time.Sleep(5 * time.Second)
			}
		}
	}
}

// connectAndMonitor connects to WebSocket and processes transactions
func (c *MempoolRelayerClient) connectAndMonitor() error {
	log.Printf("[Mempool] Connecting to %s", PolygonWSRPC)

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.Dial(PolygonWSRPC, nil)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}

	c.connLock.Lock()
	c.conn = conn
	c.connLock.Unlock()

	defer func() {
		c.connLock.Lock()
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
		c.connLock.Unlock()
	}()

	log.Printf("[Mempool] Connected! Subscribing to pending transactions...")

	// Subscribe to pending transactions
	subReq := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_subscribe",
		"params":  []string{"newPendingTransactions"},
		"id":      1,
	}
	if err := conn.WriteJSON(subReq); err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	var subResp struct {
		Result string `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := conn.ReadJSON(&subResp); err != nil {
		return fmt.Errorf("read subscribe response failed: %w", err)
	}
	if subResp.Error != nil {
		return fmt.Errorf("subscribe error: %s", subResp.Error.Message)
	}

	log.Printf("[Mempool] Subscribed (ID: %s)", subResp.Result)
	log.Printf("[Mempool] Watching Relayer: %s", RelayerAddress)
	if len(c.followedAddresses) > 0 {
		log.Printf("[Mempool] Filtering for %d followed addresses", len(c.followedAddresses))
	} else {
		log.Printf("[Mempool] Watching ALL trades (no address filter)")
	}

	// Process incoming transactions
	for {
		select {
		case <-c.ctx.Done():
			return nil
		default:
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))

			var msg struct {
				Params struct {
					Result string `json:"result"`
				} `json:"params"`
			}
			if err := conn.ReadJSON(&msg); err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
					return nil
				}
				return fmt.Errorf("read failed: %w", err)
			}

			txHash := msg.Params.Result
			if txHash == "" {
				continue
			}

			c.statsLock.Lock()
			c.stats.TotalTxSeen++
			c.statsLock.Unlock()

			// Process in background to not block WebSocket
			go c.processPendingTx(txHash)
		}
	}
}

// processPendingTx fetches and processes a pending transaction
func (c *MempoolRelayerClient) processPendingTx(txHash string) {
	detectedAt := time.Now()

	// Fetch transaction details
	tx, err := c.getTransaction(txHash)
	if err != nil || tx == nil {
		return
	}

	// Check if it's going to the Relayer
	if strings.ToLower(tx.To) != RelayerAddress {
		return
	}

	c.statsLock.Lock()
	c.stats.RelayerTxSeen++
	c.statsLock.Unlock()

	// IMPORTANT: Check if ANY followed address appears in the calldata
	// This is more reliable than trying to decode the struct perfectly
	if len(c.followedAddresses) > 0 {
		inputLower := strings.ToLower(tx.Input)
		found := false
		var matchedAddr string
		for addr := range c.followedAddresses {
			if strings.Contains(inputLower, addr) {
				found = true
				matchedAddr = addr
				break
			}
		}
		if !found {
			return // No followed address in this TX
		}
		c.statsLock.Lock()
		c.stats.FollowedTrades++
		c.statsLock.Unlock()

		// Store matched address for later
		_ = matchedAddr
	}

	// Try to decode the trade
	trade := c.decodeTrade(tx, txHash, detectedAt)

	if trade.Decoded {
		c.statsLock.Lock()
		c.stats.TradesDecoded++
		c.statsLock.Unlock()
	}

	// Call the handler
	if c.onTrade != nil {
		c.onTrade(trade)
	}
}

// rpcTransaction represents a transaction from JSON-RPC
type rpcTransaction struct {
	From     string `json:"from"`
	To       string `json:"to"`
	Input    string `json:"input"`
	Nonce    string `json:"nonce"`
	GasPrice string `json:"gasPrice"`
	Gas      string `json:"gas"`
}

// getTransaction fetches transaction details via HTTP RPC
func (c *MempoolRelayerClient) getTransaction(txHash string) (*rpcTransaction, error) {
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_getTransactionByHash",
		"params":  []string{txHash},
		"id":      1,
	}
	jsonBody, _ := json.Marshal(reqBody)

	resp, err := c.httpClient.Post(PolygonHTTPRPC, "application/json", strings.NewReader(string(jsonBody)))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Result *rpcTransaction `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result.Result, nil
}

// decodeTrade extracts trade information from transaction calldata
func (c *MempoolRelayerClient) decodeTrade(tx *rpcTransaction, txHash string, detectedAt time.Time) MempoolTrade {
	trade := MempoolTrade{
		DetectedAt:  detectedAt,
		TxHash:      txHash,
		From:        tx.From,
		To:          tx.To,
		RawInput:    tx.Input,
		InputLength: len(tx.Input),
	}

	// Parse nonce
	if tx.Nonce != "" {
		nonce, _ := hexToBigInt(tx.Nonce)
		if nonce != nil {
			trade.Nonce = nonce.Uint64()
		}
	}

	// Parse gas price
	if tx.GasPrice != "" {
		trade.GasPrice, _ = hexToBigInt(tx.GasPrice)
	}

	// Need at least function selector (4 bytes = 8 hex chars + "0x")
	if len(tx.Input) < 10 {
		return trade
	}

	// Get function selector
	selector := tx.Input[2:10]
	funcName, ok := TradeFunctionSelectors[selector]
	if !ok {
		return trade // Not a trade function
	}
	trade.FunctionName = funcName

	// Decode the calldata
	decoded, userAddr, tokenID, side, makerAmt, takerAmt := decodeTradeCalldata(tx.Input)
	if !decoded {
		return trade
	}

	trade.Decoded = true
	trade.UserAddress = userAddr
	trade.TokenID = tokenID
	trade.Side = side
	trade.MakerAmount = makerAmt
	trade.TakerAmount = takerAmt

	// Calculate price and size
	// For BUY: user pays USDC (makerAmount), receives tokens (takerAmount)
	// For SELL: user pays tokens (makerAmount), receives USDC (takerAmount)
	if makerAmt != nil && takerAmt != nil {
		makerF := new(big.Float).SetInt(makerAmt)
		takerF := new(big.Float).SetInt(takerAmt)

		if side == "BUY" {
			// Size = takerAmount (tokens received) / 1e6
			trade.Size, _ = new(big.Float).Quo(takerF, big.NewFloat(1e6)).Float64()
			// Price = makerAmount / takerAmount
			if takerAmt.Sign() > 0 {
				trade.Price, _ = new(big.Float).Quo(makerF, takerF).Float64()
			}
		} else {
			// Size = makerAmount (tokens sold) / 1e6
			trade.Size, _ = new(big.Float).Quo(makerF, big.NewFloat(1e6)).Float64()
			// Price = takerAmount / makerAmount
			if makerAmt.Sign() > 0 {
				trade.Price, _ = new(big.Float).Quo(takerF, makerF).Float64()
			}
		}
	}

	return trade
}

// =============================================================================
// CALLDATA DECODING
// =============================================================================

// decodeTradeCalldata extracts trade parameters from ABI-encoded calldata
// This handles the complex nested struct encoding of Polymarket orders
//
// Order struct layout (simplified):
// - salt (32 bytes)
// - maker (20 bytes, padded to 32)
// - signer (20 bytes, padded to 32)
// - taker (20 bytes, padded to 32)
// - tokenId (32 bytes)
// - makerAmount (32 bytes)
// - takerAmount (32 bytes)
// - expiration (32 bytes)
// - nonce (32 bytes)
// - feeRateBps (32 bytes)
// - side (32 bytes) - 0=BUY, 1=SELL
// - signatureType (32 bytes)
// - signature (dynamic)
func decodeTradeCalldata(input string) (decoded bool, userAddr, tokenID, side string, makerAmount, takerAmount *big.Int) {
	if len(input) < 10 {
		return false, "", "", "", nil, nil
	}

	// Remove 0x prefix and function selector
	data := input[10:]

	// Decode hex to bytes
	bytes, err := hex.DecodeString(data)
	if err != nil {
		return false, "", "", "", nil, nil
	}

	// We need to find the Order struct within the calldata
	// The structure varies by function, so we use heuristics

	// Strategy: Look for patterns that match Order struct
	// - TokenID is a large number (256 bits)
	// - MakerAmount/TakerAmount are reasonable USDC amounts (100000 to 10000000000)
	// - Side is 0 or 1
	// - Maker address is 20 bytes (but padded to 32)

	// First, try to find a maker address (20 bytes in a 32-byte slot)
	// Addresses are left-padded with zeros
	for offset := 0; offset+384 <= len(bytes); offset += 32 {
		// Check if this could be an address (12 zero bytes followed by 20 address bytes)
		if isAddress(bytes[offset : offset+32]) {
			potentialMaker := fmt.Sprintf("0x%x", bytes[offset+12:offset+32])

			// Check subsequent slots for tokenId, amounts, and side
			if offset+32+32 <= len(bytes) {
				// Skip signer and taker (2 more address slots)
				tokenOffset := offset + 32*3
				if tokenOffset+32*7 <= len(bytes) {
					potentialTokenID := new(big.Int).SetBytes(bytes[tokenOffset : tokenOffset+32])
					potentialMakerAmt := new(big.Int).SetBytes(bytes[tokenOffset+32 : tokenOffset+64])
					potentialTakerAmt := new(big.Int).SetBytes(bytes[tokenOffset+64 : tokenOffset+96])

					// Validate amounts are reasonable
					minAmt := big.NewInt(100_000)       // 0.1 USDC
					maxAmt := big.NewInt(100_000_000_000) // 100,000 USDC

					if potentialMakerAmt.Cmp(minAmt) >= 0 && potentialMakerAmt.Cmp(maxAmt) <= 0 &&
						potentialTakerAmt.Cmp(minAmt) >= 0 && potentialTakerAmt.Cmp(maxAmt) <= 0 &&
						potentialTokenID.Sign() > 0 {

						// Check for side (should be at offset + 32*9 or so)
						sideOffset := tokenOffset + 32*6
						if sideOffset+32 <= len(bytes) {
							sideValue := bytes[sideOffset+31]
							if sideValue == 0 || sideValue == 1 {
								sideStr := "BUY"
								if sideValue == 1 {
									sideStr = "SELL"
								}
								return true, potentialMaker, potentialTokenID.String(), sideStr, potentialMakerAmt, potentialTakerAmt
							}
						}
					}
				}
			}
		}
	}

	// Fallback: Simple pattern matching for amounts
	// Look for consecutive slots with reasonable amounts
	for offset := 64; offset+384 <= len(bytes); offset += 32 {
		potentialTokenID := new(big.Int).SetBytes(bytes[offset : offset+32])
		if potentialTokenID.Sign() == 0 {
			continue
		}

		potentialMakerAmt := new(big.Int).SetBytes(bytes[offset+32 : offset+64])
		potentialTakerAmt := new(big.Int).SetBytes(bytes[offset+64 : offset+96])

		minAmt := big.NewInt(100_000)
		maxAmt := big.NewInt(100_000_000_000)

		if potentialMakerAmt.Cmp(minAmt) >= 0 && potentialMakerAmt.Cmp(maxAmt) <= 0 &&
			potentialTakerAmt.Cmp(minAmt) >= 0 && potentialTakerAmt.Cmp(maxAmt) <= 0 {

			sideOffset := offset + 192
			if sideOffset+32 <= len(bytes) {
				sideValue := bytes[sideOffset+31]
				if sideValue == 0 || sideValue == 1 {
					sideStr := "BUY"
					if sideValue == 1 {
						sideStr = "SELL"
					}

					// Try to find user address by searching in calldata
					userAddr := findUserInCalldata(input)

					return true, userAddr, potentialTokenID.String(), sideStr, potentialMakerAmt, potentialTakerAmt
				}
			}
		}
	}

	return false, "", "", "", nil, nil
}

// isAddress checks if a 32-byte slot contains an Ethereum address
// Addresses are stored as 20 bytes, left-padded with 12 zero bytes
func isAddress(data []byte) bool {
	if len(data) != 32 {
		return false
	}
	// First 12 bytes should be zero
	for i := 0; i < 12; i++ {
		if data[i] != 0 {
			return false
		}
	}
	// Remaining 20 bytes should not all be zero
	allZero := true
	for i := 12; i < 32; i++ {
		if data[i] != 0 {
			allZero = false
			break
		}
	}
	return !allZero
}

// findUserInCalldata searches for a potential user address in the calldata
// by looking for address-like patterns
func findUserInCalldata(input string) string {
	if len(input) < 74 { // 0x + 8 selector + at least 64 chars
		return ""
	}

	data := input[10:] // Remove 0x and selector
	bytes, err := hex.DecodeString(data)
	if err != nil {
		return ""
	}

	// Look for address patterns (12 zero bytes + 20 address bytes)
	for offset := 0; offset+32 <= len(bytes); offset += 32 {
		if isAddress(bytes[offset : offset+32]) {
			addr := fmt.Sprintf("0x%x", bytes[offset+12:offset+32])
			// Skip known addresses
			if strings.ToLower(addr) != strings.ToLower(PolymarketOperator) &&
				strings.ToLower(addr) != strings.ToLower(RelayerAddress) &&
				strings.ToLower(addr) != "0x0000000000000000000000000000000000000000" {
				return addr
			}
		}
	}

	return ""
}

// hexToBigInt converts a hex string to big.Int
func hexToBigInt(hexStr string) (*big.Int, bool) {
	hexStr = strings.TrimPrefix(hexStr, "0x")
	n := new(big.Int)
	_, ok := n.SetString(hexStr, 16)
	return n, ok
}

// =============================================================================
// MAIN - TEST SCRIPT
// =============================================================================

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	fmt.Println("=" + strings.Repeat("=", 70))
	fmt.Println(" POLYMARKET MEMPOOL RELAYER MONITOR - Test Script")
	fmt.Println("=" + strings.Repeat("=", 70))
	fmt.Println()
	fmt.Println("KEY ADDRESSES:")
	fmt.Printf("  Relayer (watch this):     %s\n", RelayerAddress)
	fmt.Printf("  CTF Exchange (NOT this):  %s\n", CTFExchangeAddress)
	fmt.Printf("  Polymarket Operator:      %s\n", PolymarketOperator)
	fmt.Println()

	// Get optional user address filter from command line
	var filterAddresses []string
	if len(os.Args) > 1 {
		filterAddresses = os.Args[1:]
		fmt.Printf("FILTERING FOR ADDRESSES: %v\n", filterAddresses)
	} else {
		fmt.Println("WATCHING ALL TRADES (no address filter)")
	}
	fmt.Println()
	fmt.Println("Press Ctrl+C to stop")
	fmt.Println(strings.Repeat("-", 71))
	fmt.Println()

	// Create client with trade handler
	tradeCount := 0
	client := NewMempoolRelayerClient(func(trade MempoolTrade) {
		tradeCount++

		fmt.Printf("\n%s\n", strings.Repeat("=", 71))
		fmt.Printf("🚀 PENDING TRADE #%d DETECTED!\n", tradeCount)
		fmt.Printf("%s\n", strings.Repeat("=", 71))

		// Timing
		fmt.Printf("\n📍 TIMING:\n")
		fmt.Printf("   Detected at: %s\n", trade.DetectedAt.Format("15:04:05.000"))
		fmt.Printf("   TX Hash:     %s\n", trade.TxHash)

		// Transaction details
		fmt.Printf("\n📋 TRANSACTION:\n")
		fmt.Printf("   From:        %s", trade.From)
		if strings.ToLower(trade.From) == strings.ToLower(PolymarketOperator) {
			fmt.Printf(" (Polymarket Operator)")
		}
		fmt.Println()
		fmt.Printf("   To:          %s", trade.To)
		if strings.ToLower(trade.To) == strings.ToLower(RelayerAddress) {
			fmt.Printf(" (Relayer)")
		}
		fmt.Println()
		fmt.Printf("   Nonce:       %d\n", trade.Nonce)
		if trade.GasPrice != nil {
			gasPriceGwei := new(big.Float).Quo(new(big.Float).SetInt(trade.GasPrice), big.NewFloat(1e9))
			fmt.Printf("   Gas Price:   %s Gwei\n", gasPriceGwei.Text('f', 2))
		}
		fmt.Printf("   Function:    %s\n", trade.FunctionName)

		// Decoded trade details
		if trade.Decoded {
			fmt.Printf("\n✅ DECODED TRADE:\n")
			fmt.Printf("   User:        %s\n", trade.UserAddress)
			fmt.Printf("   TokenID:     %s\n", trade.TokenID)
			fmt.Printf("   Side:        %s\n", trade.Side)
			fmt.Printf("   Price:       %.4f\n", trade.Price)
			fmt.Printf("   Size:        %.4f tokens\n", trade.Size)
			fmt.Printf("   MakerAmount: %s\n", trade.MakerAmount.String())
			fmt.Printf("   TakerAmount: %s\n", trade.TakerAmount.String())

			// Calculate USD value
			if trade.Side == "BUY" {
				usdValue := float64(trade.MakerAmount.Int64()) / 1e6
				fmt.Printf("   USD Value:   $%.2f\n", usdValue)
			} else {
				usdValue := float64(trade.TakerAmount.Int64()) / 1e6
				fmt.Printf("   USD Value:   $%.2f\n", usdValue)
			}
		} else {
			fmt.Printf("\n⚠️  TRADE NOT DECODED (unknown format)\n")
		}

		// Raw data
		fmt.Printf("\n📦 RAW DATA:\n")
		fmt.Printf("   Input length: %d chars (%d bytes)\n", trade.InputLength, (trade.InputLength-2)/2)
		if len(trade.RawInput) > 200 {
			fmt.Printf("   Input (first 200): %s...\n", trade.RawInput[:200])
		} else {
			fmt.Printf("   Input: %s\n", trade.RawInput)
		}

		fmt.Println()
	})

	// Set address filter if provided
	if len(filterAddresses) > 0 {
		client.SetFollowedAddresses(filterAddresses)
	}

	// Start monitoring
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := client.Start(ctx); err != nil {
		log.Fatalf("Failed to start: %v", err)
	}

	// Print stats every 30 seconds
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				total, relayer, decoded, followed, uptime := client.GetStats()
				fmt.Printf("\n📊 [STATS] Uptime: %s | Total TX: %d | Relayer TX: %d | Decoded: %d | Followed: %d\n",
					uptime.Round(time.Second), total, relayer, decoded, followed)
			}
		}
	}()

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\n\nStopping...")
	client.Stop()

	// Final stats
	total, relayer, decoded, followed, uptime := client.GetStats()
	fmt.Println("\n" + strings.Repeat("=", 71))
	fmt.Println("FINAL STATISTICS")
	fmt.Println(strings.Repeat("=", 71))
	fmt.Printf("Uptime:            %s\n", uptime.Round(time.Second))
	fmt.Printf("Total TX seen:     %d\n", total)
	fmt.Printf("Relayer TX:        %d\n", relayer)
	fmt.Printf("Trades decoded:    %d\n", decoded)
	fmt.Printf("Followed trades:   %d\n", followed)
	fmt.Printf("Trades displayed:  %d\n", tradeCount)
	fmt.Println()
}
