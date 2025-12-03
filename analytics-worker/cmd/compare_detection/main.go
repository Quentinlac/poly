// Compare detection latency between all 4 detection methods with FULL mempool data extraction
// Outputs detailed CSV with all mempool_ prefixed columns
// Usage: go run cmd/compare_detection/main.go <wallet_address> [output.csv]
package main

import (
	"context"
	"encoding/csv"
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
	"polymarket-analyzer/api"
)

const (
	// Polygon RPC
	PolygonWSRPC   = "wss://polygon-bor-rpc.publicnode.com"
	PolygonHTTPRPC = "https://polygon-bor-rpc.publicnode.com"

	// QuickNode RPC for trace_call (faster than Alchemy)
	QuickNodeRPC = "https://wider-clean-scion.matic.quiknode.pro/147dd05f2eb43a0db2a87bb0a2bbeaf13780fc71/"

	// Contract addresses for trace parsing
	USDCContract = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
	CTFContract  = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"

	// Transfer signatures
	TransferSig         = "0xa9059cbb"
	TransferFromSig     = "0x23b872dd"
	SafeTransferFromSig = "0xf242432a"

	// Polymarket endpoints
	LiveDataWSURL = "wss://ws-live-data.polymarket.com/"
	DataAPIURL    = "https://data-api.polymarket.com"

	// Contract addresses
	RelayerAddress     = "0xe3f18acc55091e2c48d883fc8c8413319d4ab7b0"
	CTFExchangeAddr    = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
	NegRiskCTFExchange = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

	// OrderFilled event signature (for fallback/legacy)
	OrderFilledEventSig = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"

	// Test duration
	TestDuration = 90 * time.Second

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

	// === ALCHEMY EXTRACTION (simulation or receipt) ===
	AlchemyExtractedAt time.Time
	AlchemyMethod      string  // "simulation" or "receipt"
	AlchemySide        string  // BUY/SELL from OrderFilled events
	AlchemySize        float64 // tokens from OrderFilled
	AlchemyPrice       float64 // usdc/tokens
	AlchemyUSDC        float64 // total USDC
	AlchemyTokenID     string  // from non-zero assetId
	AlchemyError       string  // if extraction failed

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

	httpClient := &http.Client{Timeout: 10 * time.Second}
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

	// Decode the order data using our current decoder (for comparison)
	decodeFullOrder(tx.Input, det)

	// IMMEDIATELY call Alchemy simulation for pending TX
	alchemyClient := &http.Client{Timeout: 10 * time.Second}
	alchemyResult := tryAlchemySimulation(alchemyClient, tx.From, tx.To, tx.Input)
	if alchemyResult != nil && alchemyResult.Error == "" {
		alchemyResult.Method = "simulation"
	}
	if alchemyResult != nil {
		det.AlchemyExtractedAt = time.Now()
		det.AlchemyMethod = alchemyResult.Method
		det.AlchemySide = alchemyResult.Direction
		det.AlchemySize = alchemyResult.Tokens
		det.AlchemyPrice = alchemyResult.Price
		det.AlchemyUSDC = alchemyResult.USDC
		det.AlchemyTokenID = alchemyResult.TokenID
		det.AlchemyError = alchemyResult.Error
	}

	// Record detection
	recordMempoolDetection(det)

	// Log comparison
	if alchemyResult != nil && alchemyResult.Error == "" {
		sideMatch := det.MempoolSide == alchemyResult.Direction
		sizeDiff := det.MempoolSize - alchemyResult.Tokens
		log.Printf("[MEMPOOL] 🎯 %s | Mempool: %s %.2f | Alchemy(%s): %s %.2f | Match:%v Diff:%.2f | TX: %s",
			det.MempoolRole,
			det.MempoolSide, det.MempoolSize,
			alchemyResult.Method, alchemyResult.Direction, alchemyResult.Tokens,
			sideMatch, sizeDiff, txHash[:16])
	} else {
		errMsg := ""
		if alchemyResult != nil {
			errMsg = alchemyResult.Error
		}
		log.Printf("[MEMPOOL] 🎯 %s | %.2f @ %.4f | Role:%s | Alchemy: %s | TX: %s",
			det.MempoolSide, det.MempoolSize, det.MempoolPrice, det.MempoolRole, errMsg, txHash[:16])
	}
}

func decodeFullOrder(input string, det *TradeDetection) {
	if len(input) < 10 {
		return
	}

	selector := input[2:10]
	det.MempoolFunctionSig = getFunctionName(selector)

	// Use the improved decoder from api package
	// This correctly handles TAKER vs MAKER detection and fillAmounts
	order, _, orderCount := api.DecodeTradeInputForTarget(input, targetAddress)
	if !order.Decoded {
		return
	}

	det.MempoolDecoded = true
	det.MempoolSide = order.Side
	det.MempoolTokenID = order.TokenID
	det.MempoolMakerAddress = order.MakerAddress
	det.MempoolTakerAddress = order.TakerAddress
	det.MempoolSignerAddress = order.SignerAddress
	det.MempoolExpiration = order.Expiration
	det.MempoolFeeRateBps = order.FeeRateBps
	det.MempoolSignatureType = order.SignatureType
	det.MempoolOrderCount = orderCount

	if order.MakerAmount != nil {
		det.MempoolMakerAmount = order.MakerAmount.String()
	}
	if order.TakerAmount != nil {
		det.MempoolTakerAmount = order.TakerAmount.String()
	}
	if order.OrderNonce != nil {
		det.MempoolOrderNonce = order.OrderNonce.String()
	}
	if order.Salt != nil {
		det.MempoolSaltHex = fmt.Sprintf("0x%x", order.Salt)
	}

	// Calculate price from Order maker/taker amounts ratio
	if order.MakerAmount != nil && order.TakerAmount != nil {
		makerF := new(big.Float).SetInt(order.MakerAmount)
		takerF := new(big.Float).SetInt(order.TakerAmount)

		if order.Side == "BUY" {
			det.MempoolPrice, _ = new(big.Float).Quo(makerF, takerF).Float64()
		} else {
			det.MempoolPrice, _ = new(big.Float).Quo(takerF, makerF).Float64()
		}
	}

	// Set role based on IsTaker flag from improved decoder
	if order.IsTaker {
		det.MempoolRole = "TAKER"
	} else {
		det.MempoolRole = "MAKER"
	}

	// FillAmount is now correctly set by the improved decoder
	// - For TAKER: uses Word 3 (takerFillAmountShares)
	// - For MAKER: uses fillAmounts[orderIndex] from the correct offset
	if order.FillAmount != nil {
		det.MempoolFillAmount = order.FillAmount.String()
		fillF := new(big.Float).SetInt(order.FillAmount)
		det.MempoolSize, _ = new(big.Float).Quo(fillF, big.NewFloat(1e6)).Float64()
	}

	// Determine outcome (Up if price < 0.5, Down if price >= 0.5)
	if det.MempoolPrice < 0.5 {
		det.Outcome = "Up"
	} else {
		det.Outcome = "Down"
	}
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
	det, exists := detections[txHashLower]
	if !exists {
		det = &TradeDetection{
			MempoolTxHash: txHash,
			FirstMethod:   "LIVEDATA",
		}
		detections[txHashLower] = det
		liveDataFirst++
	}

	needsAlchemy := det.LiveDataAt.IsZero() && det.AlchemyExtractedAt.IsZero()

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
	detectionsMu.Unlock()

	// Trigger Alchemy extraction for confirmed TX (outside lock)
	if needsAlchemy {
		go extractAlchemyForTx(txHash)
	}
}

// extractAlchemyForTx gets Alchemy receipt data for a confirmed TX
func extractAlchemyForTx(txHash string) {
	alchemyClient := &http.Client{Timeout: 15 * time.Second}
	result := tryTransactionReceipt(alchemyClient, txHash)

	txHashLower := strings.ToLower(txHash)
	detectionsMu.Lock()
	defer detectionsMu.Unlock()

	det, exists := detections[txHashLower]
	if !exists || det.AlchemyExtractedAt.After(time.Time{}) {
		return
	}

	det.AlchemyExtractedAt = time.Now()
	if result != nil {
		det.AlchemyMethod = "receipt"
		det.AlchemySide = result.Direction
		det.AlchemySize = result.Tokens
		det.AlchemyPrice = result.Price
		det.AlchemyUSDC = result.USDC
		det.AlchemyTokenID = result.TokenID
		det.AlchemyError = result.Error

		// Log comparison if we have mempool data
		if det.MempoolDecoded && result.Error == "" {
			sideMatch := det.MempoolSide == result.Direction
			sizeDiff := det.MempoolSize - result.Tokens
			log.Printf("[ALCHEMY] TX %s | Match:%v | Mempool: %s %.2f | Receipt: %s %.2f | Diff: %.2f",
				txHash[:16], sideMatch,
				det.MempoolSide, det.MempoolSize,
				result.Direction, result.Tokens, sizeDiff)
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

// ============================================================================
// ALCHEMY EXTRACTION (simulation for pending, receipt for confirmed)
// ============================================================================

type alchemyTradeResult struct {
	Direction string  // BUY or SELL
	Tokens    float64 // token amount
	USDC      float64 // usdc amount
	Price     float64 // usdc per token
	TokenID   string  // the token being traded
	Method    string  // "simulation" or "receipt"
	Error     string
}

// extractViaAlchemy tries receipt first (for confirmed), falls back to simulation (for pending)
func extractViaAlchemy(client *http.Client, txHash, from, to, input string) *alchemyTradeResult {
	// Try receipt first (TX already confirmed - most accurate)
	result := tryTransactionReceipt(client, txHash)
	if result != nil && result.Error == "" {
		result.Method = "receipt"
		return result
	}

	// Fall back to simulation (works for pending TXs)
	result = tryAlchemySimulation(client, from, to, input)
	if result != nil && result.Error == "" {
		result.Method = "simulation"
		return result
	}

	// Return the receipt error if both failed
	if result == nil {
		return &alchemyTradeResult{Error: "both receipt and simulation failed"}
	}
	return result
}

// tryAlchemySimulation now uses QuickNode trace_call (faster than Alchemy)
func tryAlchemySimulation(client *http.Client, from, to, input string) *alchemyTradeResult {
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "trace_call",
		"params": []interface{}{
			map[string]string{"from": from, "to": to, "data": input},
			[]string{"trace"},
			"latest",
		},
	}
	jsonBody, _ := json.Marshal(reqBody)

	resp, err := client.Post(QuickNodeRPC, "application/json", strings.NewReader(string(jsonBody)))
	if err != nil {
		return &alchemyTradeResult{Error: fmt.Sprintf("trace_call request failed: %v", err)}
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
				Error string `json:"error"`
			} `json:"trace"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &traceResult); err != nil {
		return &alchemyTradeResult{Error: fmt.Sprintf("trace parse failed: %v", err)}
	}

	if traceResult.Error != nil {
		return &alchemyTradeResult{Error: traceResult.Error.Message}
	}

	// Check if first trace reverted (state changed - TX already confirmed)
	if len(traceResult.Result.Trace) > 0 && traceResult.Result.Trace[0].Error == "Reverted" {
		return &alchemyTradeResult{Error: "trace reverted - TX may be confirmed, use receipt"}
	}

	return parseTraceForTransfers(traceResult.Result.Trace, targetAddress)
}

// parseTraceForTransfers extracts USDC and CTF token transfers from QuickNode trace
func parseTraceForTransfers(traces []struct {
	Action struct {
		To    string `json:"to"`
		Input string `json:"input"`
	} `json:"action"`
	Error string `json:"error"`
}, target string) *alchemyTradeResult {
	var usdcIn, usdcOut, tokensIn, tokensOut float64
	var tokenID string
	targetClean := strings.ToLower(strings.TrimPrefix(target, "0x"))

	for _, t := range traces {
		toAddr := strings.ToLower(t.Action.To)
		inp := strings.ToLower(t.Action.Input)

		if len(inp) < 10 {
			continue
		}

		sig := inp[:10]

		// USDC transfer(address recipient, uint256 amount)
		if toAddr == USDCContract && sig == TransferSig && len(inp) >= 138 {
			recipient := inp[34:74]
			amountHex := inp[74:138]
			amount := parseHexToFloat(amountHex) / 1e6

			if strings.Contains(recipient, targetClean) {
				usdcIn += amount
			}
		}

		// USDC transferFrom(address from, address to, uint256 amount)
		if toAddr == USDCContract && sig == TransferFromSig && len(inp) >= 202 {
			fromAddr := inp[34:74]
			amountHex := inp[138:202]
			amount := parseHexToFloat(amountHex) / 1e6

			if strings.Contains(fromAddr, targetClean) {
				usdcOut += amount
			}
		}

		// CTF safeTransferFrom(address from, address to, uint256 id, uint256 amount, bytes data)
		if toAddr == CTFContract && sig == SafeTransferFromSig && len(inp) >= 266 {
			sender := inp[34:74]
			recipient := inp[98:138]
			tokenIDHex := inp[138:202]
			amountHex := inp[202:266]
			amount := parseHexToFloat(amountHex) / 1e6

			if strings.Contains(sender, targetClean) {
				tokensOut += amount
				if tokenID == "" {
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
		direction = "SELL"
		tokens = tokensOut
		usdc = usdcIn
	} else if tokensIn > 0 && usdcOut > 0 {
		direction = "BUY"
		tokens = tokensIn
		usdc = usdcOut
	} else if tokensIn > 0 {
		direction = "BUY"
		tokens = tokensIn
		usdc = usdcOut
	} else if tokensOut > 0 {
		direction = "SELL"
		tokens = tokensOut
		usdc = usdcIn
	} else {
		return &alchemyTradeResult{Error: "no token transfers found for target user"}
	}

	price := float64(0)
	if tokens > 0 {
		price = usdc / tokens
	}

	return &alchemyTradeResult{
		Direction: direction,
		Tokens:    tokens,
		USDC:      usdc,
		Price:     price,
		TokenID:   tokenID,
	}
}

func tryTransactionReceipt(client *http.Client, txHash string) *alchemyTradeResult {
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_getTransactionReceipt",
		"params":  []string{txHash},
		"id":      1,
	}
	jsonBody, _ := json.Marshal(reqBody)

	resp, err := client.Post(PolygonHTTPRPC, "application/json", strings.NewReader(string(jsonBody)))
	if err != nil {
		return &alchemyTradeResult{Error: fmt.Sprintf("receipt request failed: %v", err)}
	}
	defer resp.Body.Close()

	var receiptResult struct {
		Result struct {
			Status string `json:"status"`
			Logs   []struct {
				Topics []string `json:"topics"`
				Data   string   `json:"data"`
			} `json:"logs"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&receiptResult); err != nil {
		return &alchemyTradeResult{Error: fmt.Sprintf("receipt parse failed: %v", err)}
	}

	if receiptResult.Result.Status != "0x1" {
		return &alchemyTradeResult{Error: "transaction failed or not found"}
	}

	return parseOrderFilledLogs(receiptResult.Result.Logs)
}

type logEntry struct {
	Topics []string `json:"topics"`
	Data   string   `json:"data"`
}

func parseOrderFilledLogs(logs []struct {
	Topics []string `json:"topics"`
	Data   string   `json:"data"`
}) *alchemyTradeResult {
	var totalTokens, totalUSDC float64
	var direction, tokenID string

	for _, log := range logs {
		if len(log.Topics) < 4 {
			continue
		}
		// Check for OrderFilled event
		if strings.ToLower(log.Topics[0]) != OrderFilledEventSig {
			continue
		}

		// Extract maker and taker addresses from topics
		maker := "0x" + strings.ToLower(log.Topics[2][26:])
		taker := "0x" + strings.ToLower(log.Topics[3][26:])

		// Check if target is involved (either as maker or taker)
		isMaker := maker == targetAddress
		isTaker := taker == targetAddress

		if !isMaker && !isTaker {
			continue
		}

		// Parse data field: makerAssetId(32) | takerAssetId(32) | makerAmount(32) | takerAmount(32)
		data := strings.TrimPrefix(log.Data, "0x")
		if len(data) < 256 {
			continue
		}

		makerAssetID := data[0:64]
		takerAssetID := data[64:128]
		makerAmount := parseHexToFloat(data[128:192])
		takerAmount := parseHexToFloat(data[192:256])

		// Determine direction based on target's role and which asset is USDC
		isUSDCMaker := isZeroAsset(makerAssetID)

		if isMaker {
			// Target is the MAKER in this fill
			if isUSDCMaker {
				// Maker gives USDC → BUY tokens
				direction = "BUY"
				totalUSDC += makerAmount
				totalTokens += takerAmount
				if tokenID == "" {
					tokenID = "0x" + takerAssetID
				}
			} else {
				// Maker gives tokens → SELL tokens
				direction = "SELL"
				totalTokens += makerAmount
				totalUSDC += takerAmount
				if tokenID == "" {
					tokenID = "0x" + makerAssetID
				}
			}
		} else {
			// Target is the TAKER in this fill
			// takerAssetId = what taker gives
			isUSDCTaker := isZeroAsset(takerAssetID)
			if isUSDCTaker {
				// Taker gives USDC → taker is BUYING tokens
				direction = "BUY"
				totalUSDC += takerAmount
				totalTokens += makerAmount
				if tokenID == "" {
					tokenID = "0x" + makerAssetID
				}
			} else {
				// Taker gives tokens → taker is SELLING tokens
				direction = "SELL"
				totalTokens += takerAmount
				totalUSDC += makerAmount
				if tokenID == "" {
					tokenID = "0x" + takerAssetID
				}
			}
		}
	}

	if totalTokens == 0 {
		return &alchemyTradeResult{Error: "no OrderFilled events for target user"}
	}

	price := totalUSDC / totalTokens

	return &alchemyTradeResult{
		Direction: direction,
		Tokens:    totalTokens / 1e6,
		USDC:      totalUSDC / 1e6,
		Price:     price,
		TokenID:   tokenID,
	}
}

func parseHexToFloat(hex string) float64 {
	val, ok := new(big.Int).SetString(hex, 16)
	if !ok {
		return 0
	}
	f, _ := new(big.Float).SetInt(val).Float64()
	return f
}

func isZeroAsset(assetID string) bool {
	for _, c := range assetID {
		if c != '0' {
			return false
		}
	}
	return true
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
		// Alchemy extraction (ground truth)
		"alchemy_extracted_at",
		"alchemy_method",
		"alchemy_side",
		"alchemy_size",
		"alchemy_price",
		"alchemy_usdc",
		"alchemy_token_id",
		"alchemy_error",
		// Comparison
		"side_match",
		"size_diff",
		"price_diff",
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
			// Alchemy extraction
			formatTime(det.AlchemyExtractedAt),
			det.AlchemyMethod,
			det.AlchemySide,
			fmt.Sprintf("%.6f", det.AlchemySize),
			fmt.Sprintf("%.6f", det.AlchemyPrice),
			fmt.Sprintf("%.6f", det.AlchemyUSDC),
			det.AlchemyTokenID,
			det.AlchemyError,
			// Comparison columns
			fmt.Sprintf("%t", det.MempoolSide == det.AlchemySide),
			fmt.Sprintf("%.6f", det.MempoolSize-det.AlchemySize),
			fmt.Sprintf("%.6f", det.MempoolPrice-det.AlchemyPrice),
			// Derived
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
