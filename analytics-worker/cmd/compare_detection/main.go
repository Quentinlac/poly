// Compare detection latency between all 4 detection methods with FULL mempool data extraction
// Outputs detailed CSV with all mempool_ prefixed columns
// Usage: go run cmd/compare_detection/main.go <wallet_address> [output.csv]
package main

import (
	"context"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// Polygon RPC
	PolygonWSRPC   = "wss://polygon-bor-rpc.publicnode.com"
	PolygonHTTPRPC = "https://polygon-bor-rpc.publicnode.com"

	// Polymarket endpoints
	LiveDataWSURL = "wss://ws-live-data.polymarket.com/"
	DataAPIURL    = "https://data-api.polymarket.com"

	// Contract addresses
	RelayerAddress     = "0xe3f18acc55091e2c48d883fc8c8413319d4ab7b0"
	CTFExchangeAddr    = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
	NegRiskCTFExchange = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

	// Test duration
	TestDuration = 5 * time.Minute

	// Data API polling interval
	DataAPIPollInterval = 2 * time.Second
)

// TradeDetection represents a detected trade with ALL data from all sources
type TradeDetection struct {
	// === MEMPOOL RAW DATA (all prefixed mempool_) ===
	MempoolTxHash        string
	MempoolFrom          string // TX sender (relayer)
	MempoolTo            string // Contract address
	MempoolContractName  string // CTFExchange/NegRiskCTFExchange/NegRiskAdapter
	MempoolDetectedAt    time.Time
	MempoolGasPrice      string // wei as string
	MempoolGas           string // gas limit
	MempoolNonce         uint64 // TX nonce
	MempoolMaxFeePerGas  string // EIP-1559
	MempoolMaxPriorityFee string
	MempoolValue         string
	MempoolInputLen      int

	// Decoded order data from mempool
	MempoolDecoded       bool
	MempoolSide          string // BUY/SELL
	MempoolTokenID       string
	MempoolMakerAmount   string // raw wei
	MempoolTakerAmount   string // raw wei
	MempoolSize          float64
	MempoolPrice         float64
	MempoolRole          string // MAKER/TAKER
	MempoolMakerAddress  string
	MempoolTakerAddress  string
	MempoolSignerAddress string
	MempoolExpiration    uint64
	MempoolOrderNonce    string
	MempoolFeeRateBps    uint64
	MempoolFillAmount    string
	MempoolOrderCount    int
	MempoolFunctionSig   string
	MempoolSignatureType uint8
	MempoolSaltHex       string

	// === LIVEDATA ===
	LiveDataAt    time.Time
	LiveDataSide  string
	LiveDataSize  float64
	LiveDataPrice float64

	// === POLYGON LOGS ===
	PolygonLogsAt time.Time

	// === DATA API ===
	DataAPIAt    time.Time
	DataAPISide  string
	DataAPISize  float64
	DataAPIPrice float64

	// === DERIVED ===
	FirstMethod        string
	IsConfirmed        bool
	MempoolToConfirmMs int64
	Outcome            string // Up/Down based on price
}

var (
	targetAddress string
	csvOutputPath string
	detections    = make(map[string]*TradeDetection)
	detectionsMu  sync.Mutex
	startTime     time.Time

	// Per-method counters
	mempoolCount     int
	liveDataCount    int
	polygonLogsCount int
	dataAPICount     int

	// First detection counters
	mempoolFirst     int
	liveDataFirst    int
	polygonLogsFirst int
	dataAPIFirst     int
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	if len(os.Args) < 2 {
		log.Fatal("Usage: go run cmd/compare_detection/main.go <wallet_address> [output.csv]")
	}
	targetAddress = strings.ToLower(os.Args[1])

	// Default CSV output path
	csvOutputPath = fmt.Sprintf("/Users/Lacointa/Documents/scriptsetcode/poly/detection_full_%s.csv", time.Now().Format("20060102_150405"))
	if len(os.Args) >= 3 {
		csvOutputPath = os.Args[2]
	}

	fmt.Println("==========================================================================")
	fmt.Println("       4-WAY DETECTION WITH FULL MEMPOOL DATA EXTRACTION")
	fmt.Println("==========================================================================")
	fmt.Printf("\nTarget Address: %s\n", targetAddress)
	fmt.Printf("Test Duration:  %v\n", TestDuration)
	fmt.Printf("CSV Output:     %s\n", csvOutputPath)
	fmt.Println("\nDetection Methods:")
	fmt.Println("  1. MEMPOOL      Polygon pending TX → full calldata decode")
	fmt.Println("  2. LIVEDATA     ws-live-data.polymarket.com")
	fmt.Println("  3. POLYGON_LOGS eth_subscribe logs")
	fmt.Println("  4. DATA_API     data-api.polymarket.com/activity")
	fmt.Println("==========================================================================")
	fmt.Println()

	ctx, cancel := context.WithTimeout(context.Background(), TestDuration)
	defer cancel()

	startTime = time.Now()

	var wg sync.WaitGroup
	wg.Add(4)

	go func() { defer wg.Done(); monitorMempool(ctx) }()
	go func() { defer wg.Done(); monitorLiveData(ctx) }()
	go func() { defer wg.Done(); monitorPolygonLogs(ctx) }()
	go func() { defer wg.Done(); monitorDataAPI(ctx) }()

	// Stats printer
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				printStats()
			}
		}
	}()

	wg.Wait()
	printFinalSummary()
	writeCSV()
}

// ============================================================================
// METHOD 1: MEMPOOL (with FULL data extraction)
// ============================================================================

func monitorMempool(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := runMempoolMonitor(ctx); err != nil {
			log.Printf("[MEMPOOL] Error: %v, reconnecting in 5s...", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func runMempoolMonitor(ctx context.Context) error {
	log.Printf("[MEMPOOL] Connecting to %s", PolygonWSRPC)

	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.Dial(PolygonWSRPC, nil)
	if err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}
	defer conn.Close()

	subReq := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_subscribe",
		"params":  []string{"newPendingTransactions"},
		"id":      1,
	}
	if err := conn.WriteJSON(subReq); err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	var subResp struct{ Result string `json:"result"` }
	if err := conn.ReadJSON(&subResp); err != nil {
		return fmt.Errorf("subscribe response failed: %w", err)
	}
	log.Printf("[MEMPOOL] ✓ Subscribed (ID: %s)", subResp.Result)

	httpClient := &http.Client{Timeout: 3 * time.Second}
	targetAddrClean := strings.TrimPrefix(targetAddress, "0x")

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		var msg struct {
			Params struct{ Result string `json:"result"` } `json:"params"`
		}
		if err := conn.ReadJSON(&msg); err != nil {
			if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "deadline") {
				continue
			}
			return fmt.Errorf("read error: %w", err)
		}

		txHash := msg.Params.Result
		if txHash == "" {
			continue
		}

		go processMempoolTx(httpClient, txHash, targetAddrClean)
	}
}

func processMempoolTx(httpClient *http.Client, txHash, targetAddrClean string) {
	tx, err := getFullTransaction(httpClient, txHash)
	if err != nil || tx == nil {
		return
	}

	// Check if it's to Relayer
	if strings.ToLower(tx.To) != RelayerAddress {
		return
	}

	// Check if target address in calldata
	inputLower := strings.ToLower(tx.Input)
	if !strings.Contains(inputLower, targetAddrClean) {
		return
	}

	now := time.Now()
	det := &TradeDetection{
		MempoolTxHash:       txHash,
		MempoolFrom:         tx.From,
		MempoolTo:           tx.To,
		MempoolContractName: "NegRiskAdapter",
		MempoolDetectedAt:   now,
		MempoolGasPrice:     tx.GasPrice,
		MempoolGas:          tx.Gas,
		MempoolNonce:        parseHexUint64(tx.Nonce),
		MempoolMaxFeePerGas: tx.MaxFeePerGas,
		MempoolMaxPriorityFee: tx.MaxPriorityFeePerGas,
		MempoolValue:        tx.Value,
		MempoolInputLen:     (len(tx.Input) - 2) / 2,
	}

	// Decode the order data
	decodeFullOrder(tx.Input, det)

	// Record detection
	recordMempoolDetection(det)
	log.Printf("[MEMPOOL] 🎯 %s | %.2f @ %.4f | Role:%s | TX: %s",
		det.MempoolSide, det.MempoolSize, det.MempoolPrice, det.MempoolRole, txHash[:16])
}

func decodeFullOrder(input string, det *TradeDetection) {
	if len(input) < 10 {
		return
	}

	selector := input[2:10]
	det.MempoolFunctionSig = getFunctionName(selector)

	bytes, err := hex.DecodeString(input[10:])
	if err != nil || len(bytes) < 256 {
		return
	}

	// Find user address and order data
	targetClean := strings.TrimPrefix(targetAddress, "0x")

	for offset := 0; offset+384 <= len(bytes); offset += 32 {
		// Try to parse Order struct
		order := tryParseOrderAt(bytes, offset)
		if order == nil {
			continue
		}

		// Check if this order involves our target
		makerLower := strings.ToLower(order.Maker)
		takerLower := strings.ToLower(order.Taker)
		signerLower := strings.ToLower(order.Signer)

		if !strings.Contains(makerLower, targetClean) &&
			!strings.Contains(takerLower, targetClean) &&
			!strings.Contains(signerLower, targetClean) {
			continue
		}

		det.MempoolDecoded = true
		det.MempoolSide = order.Side
		det.MempoolTokenID = order.TokenID
		det.MempoolMakerAmount = order.MakerAmount.String()
		det.MempoolTakerAmount = order.TakerAmount.String()
		det.MempoolMakerAddress = order.Maker
		det.MempoolTakerAddress = order.Taker
		det.MempoolSignerAddress = order.Signer
		det.MempoolExpiration = order.Expiration
		det.MempoolOrderNonce = order.Nonce.String()
		det.MempoolFeeRateBps = order.FeeRateBps
		det.MempoolSignatureType = order.SignatureType
		det.MempoolSaltHex = order.Salt

		// Calculate price from Order maker/taker amounts ratio
		makerF := new(big.Float).SetInt(order.MakerAmount)
		takerF := new(big.Float).SetInt(order.TakerAmount)

		if order.Side == "BUY" {
			det.MempoolPrice, _ = new(big.Float).Quo(makerF, takerF).Float64()
		} else {
			det.MempoolPrice, _ = new(big.Float).Quo(takerF, makerF).Float64()
		}

		// Size comes from fillAmount (Word 3 = takerFillAmountShares), NOT Order amounts
		// This is calculated later after findFillAmount is called

		// Determine role
		if strings.Contains(makerLower, targetClean) || strings.Contains(signerLower, targetClean) {
			det.MempoolRole = "MAKER"
		} else {
			det.MempoolRole = "TAKER"
		}

		// Determine outcome (Up if price < 0.5, Down if price >= 0.5)
		if det.MempoolPrice < 0.5 {
			det.Outcome = "Up"
		} else {
			det.Outcome = "Down"
		}

		det.MempoolOrderCount++
		break // Found our order
	}

	// Try to find fill amount (Word 3 = takerFillAmountShares)
	det.MempoolFillAmount = findFillAmount(bytes, det.MempoolMakerAmount)

	// Calculate Size from FillAmount (shares with 6 decimals)
	if det.MempoolFillAmount != "" {
		fillAmt, ok := new(big.Int).SetString(det.MempoolFillAmount, 10)
		if ok && fillAmt.Sign() > 0 {
			fillF := new(big.Float).SetInt(fillAmt)
			det.MempoolSize, _ = new(big.Float).Quo(fillF, big.NewFloat(1e6)).Float64()
		}
	}
}

type OrderData struct {
	Salt          string
	Maker         string
	Signer        string
	Taker         string
	TokenID       string
	MakerAmount   *big.Int
	TakerAmount   *big.Int
	Expiration    uint64
	Nonce         *big.Int
	FeeRateBps    uint64
	Side          string
	SignatureType uint8
}

func tryParseOrderAt(data []byte, offset int) *OrderData {
	if offset+352 > len(data) {
		return nil
	}

	// Field 1: maker (address in last 20 bytes)
	makerField := data[offset+32 : offset+64]
	if !isAddressField(makerField) {
		return nil
	}
	maker := fmt.Sprintf("0x%x", makerField[12:32])

	// Field 2: signer
	signerField := data[offset+64 : offset+96]
	if !isAddressField(signerField) {
		return nil
	}
	signer := fmt.Sprintf("0x%x", signerField[12:32])

	// Field 3: taker
	takerField := data[offset+96 : offset+128]
	if !isAddressField(takerField) {
		return nil
	}
	taker := fmt.Sprintf("0x%x", takerField[12:32])

	// Field 4: tokenId
	tokenId := new(big.Int).SetBytes(data[offset+128 : offset+160])
	if tokenId.Sign() == 0 {
		return nil
	}

	// Field 5-6: amounts
	makerAmount := new(big.Int).SetBytes(data[offset+160 : offset+192])
	takerAmount := new(big.Int).SetBytes(data[offset+192 : offset+224])

	minAmt := big.NewInt(100_000)
	maxAmt := big.NewInt(100_000_000_000)
	if makerAmount.Cmp(minAmt) < 0 || makerAmount.Cmp(maxAmt) > 0 {
		return nil
	}
	if takerAmount.Cmp(minAmt) < 0 || takerAmount.Cmp(maxAmt) > 0 {
		return nil
	}

	// Field 7: expiration
	expiration := new(big.Int).SetBytes(data[offset+224 : offset+256]).Uint64()

	// Field 8: nonce
	nonce := new(big.Int).SetBytes(data[offset+256 : offset+288])

	// Field 9: feeRateBps
	feeRateBps := new(big.Int).SetBytes(data[offset+288 : offset+320]).Uint64()

	// Field 10: side
	sideField := data[offset+320 : offset+352]
	sideValue := sideField[31]
	if sideValue > 1 {
		return nil
	}

	side := "BUY"
	if sideValue == 1 {
		side = "SELL"
	}

	return &OrderData{
		Salt:        fmt.Sprintf("0x%x", data[offset:offset+32]),
		Maker:       maker,
		Signer:      signer,
		Taker:       taker,
		TokenID:     tokenId.String(),
		MakerAmount: makerAmount,
		TakerAmount: takerAmount,
		Expiration:  expiration,
		Nonce:       nonce,
		FeeRateBps:  feeRateBps,
		Side:        side,
	}
}

func isAddressField(field []byte) bool {
	if len(field) != 32 {
		return false
	}
	for i := 0; i < 12; i++ {
		if field[i] != 0 {
			return false
		}
	}
	return true
}

func findFillAmount(data []byte, makerAmtStr string) string {
	// For fillOrders (0x2287e350), the ABI layout is:
	// [0] offset to orders array (32 bytes)
	// [1] offset to fillAmounts array (32 bytes)
	// [2] takerFillAmountUsdc (32 bytes) - USDC amount
	// [3] takerFillAmountShares (32 bytes) - THIS IS THE FILL SIZE IN SHARES
	if len(data) >= 128 {
		// Word 3 (bytes 96-128) = takerFillAmountShares
		fillAmountShares := new(big.Int).SetBytes(data[96:128])
		if fillAmountShares.Sign() > 0 {
			return fillAmountShares.String()
		}
	}
	// Fallback to maker amount if can't extract
	if makerAmtStr != "" {
		return makerAmtStr
	}
	return ""
}

func getFunctionName(selector string) string {
	names := map[string]string{
		"2287e350": "fillOrders",
		"fe729aaf": "fillOrder",
		"a4a6c5a5": "matchOrders",
		"d798eff6": "fillOrder_v2",
	}
	if name, ok := names[selector]; ok {
		return name
	}
	return "0x" + selector
}

func recordMempoolDetection(det *TradeDetection) {
	detectionsMu.Lock()
	defer detectionsMu.Unlock()

	txHashLower := strings.ToLower(det.MempoolTxHash)
	existing, exists := detections[txHashLower]
	if !exists {
		det.FirstMethod = "MEMPOOL"
		detections[txHashLower] = det
		mempoolFirst++
		mempoolCount++
	} else {
		// Update existing with mempool data if not already set
		if existing.MempoolDetectedAt.IsZero() {
			existing.MempoolTxHash = det.MempoolTxHash
			existing.MempoolFrom = det.MempoolFrom
			existing.MempoolTo = det.MempoolTo
			existing.MempoolContractName = det.MempoolContractName
			existing.MempoolDetectedAt = det.MempoolDetectedAt
			existing.MempoolGasPrice = det.MempoolGasPrice
			existing.MempoolGas = det.MempoolGas
			existing.MempoolNonce = det.MempoolNonce
			existing.MempoolMaxFeePerGas = det.MempoolMaxFeePerGas
			existing.MempoolMaxPriorityFee = det.MempoolMaxPriorityFee
			existing.MempoolValue = det.MempoolValue
			existing.MempoolInputLen = det.MempoolInputLen
			existing.MempoolDecoded = det.MempoolDecoded
			existing.MempoolSide = det.MempoolSide
			existing.MempoolTokenID = det.MempoolTokenID
			existing.MempoolMakerAmount = det.MempoolMakerAmount
			existing.MempoolTakerAmount = det.MempoolTakerAmount
			existing.MempoolSize = det.MempoolSize
			existing.MempoolPrice = det.MempoolPrice
			existing.MempoolRole = det.MempoolRole
			existing.MempoolMakerAddress = det.MempoolMakerAddress
			existing.MempoolTakerAddress = det.MempoolTakerAddress
			existing.MempoolSignerAddress = det.MempoolSignerAddress
			existing.MempoolExpiration = det.MempoolExpiration
			existing.MempoolOrderNonce = det.MempoolOrderNonce
			existing.MempoolFeeRateBps = det.MempoolFeeRateBps
			existing.MempoolFillAmount = det.MempoolFillAmount
			existing.MempoolOrderCount = det.MempoolOrderCount
			existing.MempoolFunctionSig = det.MempoolFunctionSig
			existing.MempoolSignatureType = det.MempoolSignatureType
			existing.MempoolSaltHex = det.MempoolSaltHex
			existing.Outcome = det.Outcome
			mempoolCount++
		}
	}
}

// ============================================================================
// METHOD 2: LIVEDATA WebSocket
// ============================================================================

func monitorLiveData(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := runLiveDataMonitor(ctx); err != nil {
			log.Printf("[LIVEDATA] Error: %v, reconnecting in 5s...", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func runLiveDataMonitor(ctx context.Context) error {
	log.Printf("[LIVEDATA] Connecting to %s", LiveDataWSURL)

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	headers := http.Header{}
	headers.Set("Origin", "https://polymarket.com")

	conn, _, err := dialer.Dial(LiveDataWSURL, headers)
	if err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}
	defer conn.Close()

	subMsg := map[string]interface{}{
		"action": "subscribe",
		"subscriptions": []map[string]interface{}{
			{"topic": "activity", "type": "orders_matched"},
		},
	}
	if err := conn.WriteJSON(subMsg); err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}
	log.Printf("[LIVEDATA] ✓ Subscribed to orders_matched")

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		_, message, err := conn.ReadMessage()
		if err != nil {
			if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "deadline") {
				continue
			}
			return fmt.Errorf("read error: %w", err)
		}

		var msg struct {
			Type    string `json:"type"`
			Payload struct {
				ProxyWallet     string  `json:"proxyWallet"`
				Side            string  `json:"side"`
				Size            float64 `json:"size"`
				Price           float64 `json:"price"`
				TransactionHash string  `json:"transactionHash"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(message, &msg); err != nil || msg.Type != "orders_matched" {
			continue
		}

		if strings.ToLower(msg.Payload.ProxyWallet) != targetAddress {
			continue
		}

		recordLiveDataDetection(msg.Payload.TransactionHash, msg.Payload.Side, msg.Payload.Size, msg.Payload.Price)
		log.Printf("[LIVEDATA] 🎯 %s | %.2f @ %.4f | TX: %s",
			msg.Payload.Side, msg.Payload.Size, msg.Payload.Price, msg.Payload.TransactionHash[:16])
	}
}

func recordLiveDataDetection(txHash, side string, size, price float64) {
	now := time.Now()
	txHashLower := strings.ToLower(txHash)

	detectionsMu.Lock()
	defer detectionsMu.Unlock()

	det, exists := detections[txHashLower]
	if !exists {
		det = &TradeDetection{
			MempoolTxHash: txHash,
			FirstMethod:   "LIVEDATA",
		}
		detections[txHashLower] = det
		liveDataFirst++
	}

	if det.LiveDataAt.IsZero() {
		det.LiveDataAt = now
		det.LiveDataSide = side
		det.LiveDataSize = size
		det.LiveDataPrice = price
		det.IsConfirmed = true
		liveDataCount++

		// Calculate mempool to confirm latency
		if !det.MempoolDetectedAt.IsZero() {
			det.MempoolToConfirmMs = now.Sub(det.MempoolDetectedAt).Milliseconds()
		}
	}
}

// ============================================================================
// METHOD 3: POLYGON LOGS
// ============================================================================

func monitorPolygonLogs(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := runPolygonLogsMonitor(ctx); err != nil {
			log.Printf("[POLYGON_LOGS] Error: %v, reconnecting in 5s...", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func runPolygonLogsMonitor(ctx context.Context) error {
	log.Printf("[POLYGON_LOGS] Connecting to %s", PolygonWSRPC)

	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.Dial(PolygonWSRPC, nil)
	if err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}
	defer conn.Close()

	orderFilledTopic := "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"

	subReq := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_subscribe",
		"params": []interface{}{
			"logs",
			map[string]interface{}{
				"address": []string{CTFExchangeAddr, NegRiskCTFExchange},
				"topics":  []interface{}{orderFilledTopic},
			},
		},
		"id": 1,
	}
	if err := conn.WriteJSON(subReq); err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	var subResp struct{ Result string `json:"result"` }
	if err := conn.ReadJSON(&subResp); err != nil {
		return fmt.Errorf("subscribe response failed: %w", err)
	}
	log.Printf("[POLYGON_LOGS] ✓ Subscribed to OrderFilled (ID: %s)", subResp.Result)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		var msg struct {
			Params struct {
				Result struct {
					TransactionHash string   `json:"transactionHash"`
					Topics          []string `json:"topics"`
				} `json:"result"`
			} `json:"params"`
		}
		if err := conn.ReadJSON(&msg); err != nil {
			if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "deadline") {
				continue
			}
			return fmt.Errorf("read error: %w", err)
		}

		if msg.Params.Result.TransactionHash == "" || len(msg.Params.Result.Topics) < 4 {
			continue
		}

		// Extract taker address
		takerTopic := msg.Params.Result.Topics[3]
		if len(takerTopic) < 66 {
			continue
		}
		takerAddr := "0x" + takerTopic[26:]

		if strings.ToLower(takerAddr) != targetAddress {
			continue
		}

		recordPolygonLogsDetection(msg.Params.Result.TransactionHash)
		log.Printf("[POLYGON_LOGS] 🎯 OrderFilled | TX: %s", msg.Params.Result.TransactionHash[:16])
	}
}

func recordPolygonLogsDetection(txHash string) {
	now := time.Now()
	txHashLower := strings.ToLower(txHash)

	detectionsMu.Lock()
	defer detectionsMu.Unlock()

	det, exists := detections[txHashLower]
	if !exists {
		det = &TradeDetection{
			MempoolTxHash: txHash,
			FirstMethod:   "POLYGON_LOGS",
		}
		detections[txHashLower] = det
		polygonLogsFirst++
	}

	if det.PolygonLogsAt.IsZero() {
		det.PolygonLogsAt = now
		det.IsConfirmed = true
		polygonLogsCount++

		if !det.MempoolDetectedAt.IsZero() && det.MempoolToConfirmMs == 0 {
			det.MempoolToConfirmMs = now.Sub(det.MempoolDetectedAt).Milliseconds()
		}
	}
}

// ============================================================================
// METHOD 4: DATA API Polling
// ============================================================================

func monitorDataAPI(ctx context.Context) {
	log.Printf("[DATA_API] Starting polling every %v", DataAPIPollInterval)

	httpClient := &http.Client{Timeout: 10 * time.Second}
	ticker := time.NewTicker(DataAPIPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fetchDataAPI(httpClient)
		}
	}
}

func fetchDataAPI(client *http.Client) {
	url := fmt.Sprintf("%s/activity?user=%s&type=TRADE&limit=10", DataAPIURL, targetAddress)
	resp, err := client.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	var trades []struct {
		TransactionHash string  `json:"transactionHash"`
		Side            string  `json:"side"`
		Size            float64 `json:"size"`
		Price           float64 `json:"price"`
		Timestamp       int64   `json:"timestamp"`
	}
	if err := json.Unmarshal(body, &trades); err != nil {
		return
	}

	for _, trade := range trades {
		tradeTime := time.Unix(trade.Timestamp, 0)
		if tradeTime.Before(startTime) {
			continue
		}

		txHashLower := strings.ToLower(trade.TransactionHash)
		detectionsMu.Lock()
		det, exists := detections[txHashLower]
		if exists && !det.DataAPIAt.IsZero() {
			detectionsMu.Unlock()
			continue
		}
		detectionsMu.Unlock()

		recordDataAPIDetection(trade.TransactionHash, trade.Side, trade.Size, trade.Price)
		log.Printf("[DATA_API] 🎯 %s | %.2f @ %.4f | TX: %s",
			trade.Side, trade.Size, trade.Price, trade.TransactionHash[:16])
	}
}

func recordDataAPIDetection(txHash, side string, size, price float64) {
	now := time.Now()
	txHashLower := strings.ToLower(txHash)

	detectionsMu.Lock()
	defer detectionsMu.Unlock()

	det, exists := detections[txHashLower]
	if !exists {
		det = &TradeDetection{
			MempoolTxHash: txHash,
			FirstMethod:   "DATA_API",
		}
		detections[txHashLower] = det
		dataAPIFirst++
	}

	if det.DataAPIAt.IsZero() {
		det.DataAPIAt = now
		det.DataAPISide = side
		det.DataAPISize = size
		det.DataAPIPrice = price
		det.IsConfirmed = true
		dataAPICount++

		if !det.MempoolDetectedAt.IsZero() && det.MempoolToConfirmMs == 0 {
			det.MempoolToConfirmMs = now.Sub(det.MempoolDetectedAt).Milliseconds()
		}
	}
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

type fullTransaction struct {
	From                 string `json:"from"`
	To                   string `json:"to"`
	Input                string `json:"input"`
	Nonce                string `json:"nonce"`
	Gas                  string `json:"gas"`
	GasPrice             string `json:"gasPrice"`
	MaxFeePerGas         string `json:"maxFeePerGas"`
	MaxPriorityFeePerGas string `json:"maxPriorityFeePerGas"`
	Value                string `json:"value"`
}

func getFullTransaction(client *http.Client, txHash string) (*fullTransaction, error) {
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_getTransactionByHash",
		"params":  []string{txHash},
		"id":      1,
	}
	jsonBody, _ := json.Marshal(reqBody)
	resp, err := client.Post(PolygonHTTPRPC, "application/json", strings.NewReader(string(jsonBody)))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct{ Result *fullTransaction `json:"result"` }
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result.Result, nil
}

func parseHexUint64(s string) uint64 {
	s = strings.TrimPrefix(s, "0x")
	val, _ := new(big.Int).SetString(s, 16)
	if val == nil {
		return 0
	}
	return val.Uint64()
}

func printStats() {
	elapsed := time.Since(startTime)
	detectionsMu.Lock()
	total := len(detections)
	detectionsMu.Unlock()

	fmt.Printf("\n📊 [%v] Trades: %d | M:%d L:%d P:%d D:%d | First→ M:%d L:%d P:%d D:%d\n",
		elapsed.Round(time.Second), total,
		mempoolCount, liveDataCount, polygonLogsCount, dataAPICount,
		mempoolFirst, liveDataFirst, polygonLogsFirst, dataAPIFirst)
}

func printFinalSummary() {
	fmt.Println("\n==========================================================================")
	fmt.Println("                         FINAL SUMMARY")
	fmt.Println("==========================================================================")

	detectionsMu.Lock()
	defer detectionsMu.Unlock()

	total := len(detections)
	fmt.Printf("\nTotal Trades: %d\n", total)
	fmt.Printf("MEMPOOL first: %d (%.1f%%)\n", mempoolFirst, pct(mempoolFirst, total))
	fmt.Printf("CSV output: %s\n", csvOutputPath)
}

func writeCSV() {
	detectionsMu.Lock()
	defer detectionsMu.Unlock()

	file, err := os.Create(csvOutputPath)
	if err != nil {
		log.Printf("Failed to create CSV: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header with ALL columns
	header := []string{
		// Mempool raw data
		"mempool_tx_hash",
		"mempool_from",
		"mempool_to",
		"mempool_contract_name",
		"mempool_detected_at",
		"mempool_gas_price",
		"mempool_gas",
		"mempool_nonce",
		"mempool_max_fee_per_gas",
		"mempool_max_priority_fee",
		"mempool_value",
		"mempool_input_len",
		// Mempool decoded
		"mempool_decoded",
		"mempool_side",
		"mempool_token_id",
		"mempool_maker_amount",
		"mempool_taker_amount",
		"mempool_size",
		"mempool_price",
		"mempool_role",
		"mempool_maker_address",
		"mempool_taker_address",
		"mempool_signer_address",
		"mempool_expiration",
		"mempool_order_nonce",
		"mempool_fee_rate_bps",
		"mempool_fill_amount",
		"mempool_order_count",
		"mempool_function_sig",
		"mempool_signature_type",
		"mempool_salt_hex",
		// LiveData
		"livedata_detected_at",
		"livedata_side",
		"livedata_size",
		"livedata_price",
		// Polygon Logs
		"polygon_logs_detected_at",
		// Data API
		"data_api_detected_at",
		"data_api_side",
		"data_api_size",
		"data_api_price",
		// Derived
		"first_detector",
		"is_confirmed",
		"mempool_to_confirm_ms",
		"outcome",
	}
	writer.Write(header)

	// Sort by detection time
	sorted := make([]*TradeDetection, 0, len(detections))
	for _, det := range detections {
		sorted = append(sorted, det)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return getFirstTime(sorted[i]).Before(getFirstTime(sorted[j]))
	})

	// Write rows
	for _, det := range sorted {
		row := []string{
			det.MempoolTxHash,
			det.MempoolFrom,
			det.MempoolTo,
			det.MempoolContractName,
			formatTime(det.MempoolDetectedAt),
			det.MempoolGasPrice,
			det.MempoolGas,
			fmt.Sprintf("%d", det.MempoolNonce),
			det.MempoolMaxFeePerGas,
			det.MempoolMaxPriorityFee,
			det.MempoolValue,
			fmt.Sprintf("%d", det.MempoolInputLen),
			fmt.Sprintf("%t", det.MempoolDecoded),
			det.MempoolSide,
			det.MempoolTokenID,
			det.MempoolMakerAmount,
			det.MempoolTakerAmount,
			fmt.Sprintf("%.6f", det.MempoolSize),
			fmt.Sprintf("%.6f", det.MempoolPrice),
			det.MempoolRole,
			det.MempoolMakerAddress,
			det.MempoolTakerAddress,
			det.MempoolSignerAddress,
			fmt.Sprintf("%d", det.MempoolExpiration),
			det.MempoolOrderNonce,
			fmt.Sprintf("%d", det.MempoolFeeRateBps),
			det.MempoolFillAmount,
			fmt.Sprintf("%d", det.MempoolOrderCount),
			det.MempoolFunctionSig,
			fmt.Sprintf("%d", det.MempoolSignatureType),
			det.MempoolSaltHex,
			formatTime(det.LiveDataAt),
			det.LiveDataSide,
			fmt.Sprintf("%.6f", det.LiveDataSize),
			fmt.Sprintf("%.6f", det.LiveDataPrice),
			formatTime(det.PolygonLogsAt),
			formatTime(det.DataAPIAt),
			det.DataAPISide,
			fmt.Sprintf("%.6f", det.DataAPISize),
			fmt.Sprintf("%.6f", det.DataAPIPrice),
			det.FirstMethod,
			fmt.Sprintf("%t", det.IsConfirmed),
			fmt.Sprintf("%d", det.MempoolToConfirmMs),
			det.Outcome,
		}
		writer.Write(row)
	}

	log.Printf("✓ CSV written to %s (%d rows)", csvOutputPath, len(sorted))
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format("15:04:05.000")
}

func getFirstTime(d *TradeDetection) time.Time {
	times := []time.Time{d.MempoolDetectedAt, d.LiveDataAt, d.PolygonLogsAt, d.DataAPIAt}
	var first time.Time
	for _, t := range times {
		if !t.IsZero() && (first.IsZero() || t.Before(first)) {
			first = t
		}
	}
	return first
}

func pct(n, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(n) / float64(total) * 100
}
