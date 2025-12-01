// Compare detection latency between all 4 detection methods
// Usage: go run cmd/compare_detection/main.go <wallet_address>
package main

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
	RelayerAddress    = "0xe3f18acc55091e2c48d883fc8c8413319d4ab7b0"
	CTFExchangeAddr   = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
	NegRiskCTFExchange = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

	// Test duration
	TestDuration = 10 * time.Minute

	// Data API polling interval
	DataAPIPollInterval = 2 * time.Second
)

// TradeDetection represents a detected trade with timing from all sources
type TradeDetection struct {
	TxHash string
	Side   string
	Size   float64
	Price  float64

	// Detection timestamps from each method
	MempoolAt     time.Time // Method 1: Pending TX
	LiveDataAt    time.Time // Method 2: ws-live-data
	PolygonLogsAt time.Time // Method 3: eth_subscribe logs
	DataAPIAt     time.Time // Method 4: REST API polling

	FirstMethod string // Which method detected first
}

var (
	targetAddress string
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

	// For Data API tracking
	lastSeenTradeTime time.Time
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	if len(os.Args) < 2 {
		log.Fatal("Usage: go run cmd/compare_detection/main.go <wallet_address>")
	}
	targetAddress = strings.ToLower(os.Args[1])

	fmt.Println("==========================================================================")
	fmt.Println("       4-WAY DETECTION METHOD COMPARISON")
	fmt.Println("==========================================================================")
	fmt.Printf("\nTarget Address: %s\n", targetAddress)
	fmt.Printf("Test Duration:  %v\n", TestDuration)
	fmt.Println("\nDetection Methods:")
	fmt.Println("  1. MEMPOOL      Polygon pending TX → Relayer calldata    [FASTEST]")
	fmt.Println("  2. LIVEDATA     ws-live-data.polymarket.com              [~650ms after confirm]")
	fmt.Println("  3. POLYGON_LOGS eth_subscribe logs → CTF Exchange        [~10-20s]")
	fmt.Println("  4. DATA_API     data-api.polymarket.com/activity         [polling every 2s]")
	fmt.Println("==========================================================================")
	fmt.Println()

	ctx, cancel := context.WithTimeout(context.Background(), TestDuration)
	defer cancel()

	startTime = time.Now()
	lastSeenTradeTime = startTime

	var wg sync.WaitGroup
	wg.Add(4)

	// Start all 4 monitors
	go func() {
		defer wg.Done()
		monitorMempool(ctx)
	}()

	go func() {
		defer wg.Done()
		monitorLiveData(ctx)
	}()

	go func() {
		defer wg.Done()
		monitorPolygonLogs(ctx)
	}()

	go func() {
		defer wg.Done()
		monitorDataAPI(ctx)
	}()

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
}

// ============================================================================
// METHOD 1: MEMPOOL (Pending Transactions)
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
			continue
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
	}
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
			Params struct {
				Result string `json:"result"`
			} `json:"params"`
		}
		if err := conn.ReadJSON(&msg); err != nil {
			continue
		}

		txHash := msg.Params.Result
		if txHash == "" {
			continue
		}

		// Process in background goroutine to not block WebSocket reads!
		go processMempoolTx(httpClient, txHash, targetAddrClean)
	}
}

func processMempoolTx(httpClient *http.Client, txHash, targetAddrClean string) {
	// Get transaction details
	tx, err := getTransaction(httpClient, txHash)
	if err != nil || tx == nil {
		return
	}

	// Check if it's to Relayer
	if strings.ToLower(tx.To) != RelayerAddress {
		return
	}

	// Simple check: does the target address appear in the calldata?
	inputLower := strings.ToLower(tx.Input)
	if !strings.Contains(inputLower, targetAddrClean) {
		return
	}

	// Try to decode for side/price/size, but don't fail if decoding fails
	decoded, side, _, makerAmt, takerAmt, _ := decodeRelayerTrade(tx.Input)
	var price, size float64
	if decoded && makerAmt != nil && takerAmt != nil {
		makerF := new(big.Float).SetInt(makerAmt)
		takerF := new(big.Float).SetInt(takerAmt)
		if side == "BUY" {
			size, _ = new(big.Float).Quo(takerF, big.NewFloat(1e6)).Float64()
			price, _ = new(big.Float).Quo(makerF, takerF).Float64()
		} else {
			size, _ = new(big.Float).Quo(makerF, big.NewFloat(1e6)).Float64()
			price, _ = new(big.Float).Quo(takerF, makerF).Float64()
		}
	}

	recordDetection(txHash, "MEMPOOL", side, size, price)
	log.Printf("[MEMPOOL] 🎯 %s | %.2f @ %.4f | TX: %s", side, size, price, txHash[:16])
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
			continue
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

	// Subscribe to orders_matched
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
		if err := json.Unmarshal(message, &msg); err != nil {
			continue
		}

		if msg.Type != "orders_matched" {
			continue
		}

		if strings.ToLower(msg.Payload.ProxyWallet) != targetAddress {
			continue
		}

		recordDetection(msg.Payload.TransactionHash, "LIVEDATA",
			msg.Payload.Side, msg.Payload.Size, msg.Payload.Price)
		log.Printf("[LIVEDATA] 🎯 %s | %.2f @ %.4f | TX: %s",
			msg.Payload.Side, msg.Payload.Size, msg.Payload.Price, msg.Payload.TransactionHash[:16])
	}
}

// ============================================================================
// METHOD 3: POLYGON LOGS (eth_subscribe logs)
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
			continue
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

	// OrderFilled event signature
	// event OrderFilled(bytes32 indexed orderHash, address indexed maker, address indexed taker, uint256 makerAssetId, uint256 takerAssetId, uint256 makerAmountFilled, uint256 takerAmountFilled, uint256 fee)
	orderFilledTopic := "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"

	// Subscribe to logs from both CTF Exchange addresses
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

	var subResp struct {
		Result string `json:"result"`
	}
	if err := conn.ReadJSON(&subResp); err != nil {
		return fmt.Errorf("subscribe response failed: %w", err)
	}
	log.Printf("[POLYGON_LOGS] ✓ Subscribed to OrderFilled events (ID: %s)", subResp.Result)

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
					Data            string   `json:"data"`
				} `json:"result"`
			} `json:"params"`
		}
		if err := conn.ReadJSON(&msg); err != nil {
			continue
		}

		if msg.Params.Result.TransactionHash == "" {
			continue
		}

		// Topics: [0]=event sig, [1]=orderHash, [2]=maker, [3]=taker
		if len(msg.Params.Result.Topics) < 4 {
			continue
		}

		// Extract taker address from topic[3] (last 20 bytes of 32-byte topic)
		takerTopic := msg.Params.Result.Topics[3]
		if len(takerTopic) < 66 {
			continue
		}
		takerAddr := "0x" + takerTopic[26:] // Skip 0x and first 12 bytes (24 hex chars)

		if strings.ToLower(takerAddr) != targetAddress {
			continue
		}

		// Parse data for amounts (skipping tokenIds, getting amounts)
		txHash := msg.Params.Result.TransactionHash
		recordDetection(txHash, "POLYGON_LOGS", "", 0, 0)
		log.Printf("[POLYGON_LOGS] 🎯 OrderFilled detected | TX: %s", txHash[:16])
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
		Timestamp       int64   `json:"timestamp"` // Unix timestamp
	}
	if err := json.Unmarshal(body, &trades); err != nil {
		return
	}

	for _, trade := range trades {
		// Only process trades that happened after we started
		tradeTime := time.Unix(trade.Timestamp, 0)
		if tradeTime.Before(startTime) {
			continue
		}

		// Check if this is a new trade we haven't seen via Data API
		txHash := strings.ToLower(trade.TransactionHash)
		detectionsMu.Lock()
		det, exists := detections[txHash]
		if exists && !det.DataAPIAt.IsZero() {
			detectionsMu.Unlock()
			continue // Already detected via Data API
		}
		detectionsMu.Unlock()

		recordDetection(trade.TransactionHash, "DATA_API", trade.Side, trade.Size, trade.Price)
		log.Printf("[DATA_API] 🎯 %s | %.2f @ %.4f | TX: %s",
			trade.Side, trade.Size, trade.Price, trade.TransactionHash[:16])
	}
}

// ============================================================================
// COMMON FUNCTIONS
// ============================================================================

func recordDetection(txHash, method, side string, size, price float64) {
	now := time.Now()
	txHashLower := strings.ToLower(txHash)

	detectionsMu.Lock()
	defer detectionsMu.Unlock()

	det, exists := detections[txHashLower]
	if !exists {
		det = &TradeDetection{
			TxHash:      txHashLower,
			Side:        side,
			Size:        size,
			Price:       price,
			FirstMethod: method,
		}
		detections[txHashLower] = det

		// Count first detection
		switch method {
		case "MEMPOOL":
			mempoolFirst++
		case "LIVEDATA":
			liveDataFirst++
		case "POLYGON_LOGS":
			polygonLogsFirst++
		case "DATA_API":
			dataAPIFirst++
		}
	}

	// Update detection time for this method
	switch method {
	case "MEMPOOL":
		if det.MempoolAt.IsZero() {
			det.MempoolAt = now
			mempoolCount++
		}
	case "LIVEDATA":
		if det.LiveDataAt.IsZero() {
			det.LiveDataAt = now
			liveDataCount++
		}
	case "POLYGON_LOGS":
		if det.PolygonLogsAt.IsZero() {
			det.PolygonLogsAt = now
			polygonLogsCount++
		}
	case "DATA_API":
		if det.DataAPIAt.IsZero() {
			det.DataAPIAt = now
			dataAPICount++
		}
	}

	// Update side/size/price if we didn't have it
	if det.Side == "" && side != "" {
		det.Side = side
	}
	if det.Size == 0 && size != 0 {
		det.Size = size
	}
	if det.Price == 0 && price != 0 {
		det.Price = price
	}
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
	fmt.Println("\n")
	fmt.Println("==========================================================================")
	fmt.Println("                         FINAL SUMMARY")
	fmt.Println("==========================================================================")

	detectionsMu.Lock()
	defer detectionsMu.Unlock()

	total := len(detections)
	if total == 0 {
		fmt.Println("\nNo trades detected during the test period.")
		return
	}

	fmt.Printf("\nTotal Unique Trades: %d\n", total)

	// Coverage stats
	fmt.Println("\n┌──────────────────────────────────────────────────────────────────────┐")
	fmt.Println("│                    DETECTION COVERAGE                                │")
	fmt.Println("├──────────────────────────────────────────────────────────────────────┤")
	fmt.Printf("│  MEMPOOL:      %3d/%d (%.1f%%)                                         │\n",
		mempoolCount, total, pct(mempoolCount, total))
	fmt.Printf("│  LIVEDATA:     %3d/%d (%.1f%%)                                         │\n",
		liveDataCount, total, pct(liveDataCount, total))
	fmt.Printf("│  POLYGON_LOGS: %3d/%d (%.1f%%)                                         │\n",
		polygonLogsCount, total, pct(polygonLogsCount, total))
	fmt.Printf("│  DATA_API:     %3d/%d (%.1f%%)                                         │\n",
		dataAPICount, total, pct(dataAPICount, total))
	fmt.Println("└──────────────────────────────────────────────────────────────────────┘")

	// First detection stats
	fmt.Println("\n┌──────────────────────────────────────────────────────────────────────┐")
	fmt.Println("│                    FIRST TO DETECT                                   │")
	fmt.Println("├──────────────────────────────────────────────────────────────────────┤")
	fmt.Printf("│  MEMPOOL:      %3d times (%.1f%%)                                      │\n",
		mempoolFirst, pct(mempoolFirst, total))
	fmt.Printf("│  LIVEDATA:     %3d times (%.1f%%)                                      │\n",
		liveDataFirst, pct(liveDataFirst, total))
	fmt.Printf("│  POLYGON_LOGS: %3d times (%.1f%%)                                      │\n",
		polygonLogsFirst, pct(polygonLogsFirst, total))
	fmt.Printf("│  DATA_API:     %3d times (%.1f%%)                                      │\n",
		dataAPIFirst, pct(dataAPIFirst, total))
	fmt.Println("└──────────────────────────────────────────────────────────────────────┘")

	// Latency comparisons
	fmt.Println("\n┌──────────────────────────────────────────────────────────────────────┐")
	fmt.Println("│                    LATENCY COMPARISON                                │")
	fmt.Println("├──────────────────────────────────────────────────────────────────────┤")

	// Calculate average latencies relative to MEMPOOL (if available) or LIVEDATA
	var memToLive, memToLogs, memToAPI []time.Duration
	var liveToLogs, liveToAPI []time.Duration

	for _, det := range detections {
		if !det.MempoolAt.IsZero() && !det.LiveDataAt.IsZero() {
			memToLive = append(memToLive, det.LiveDataAt.Sub(det.MempoolAt))
		}
		if !det.MempoolAt.IsZero() && !det.PolygonLogsAt.IsZero() {
			memToLogs = append(memToLogs, det.PolygonLogsAt.Sub(det.MempoolAt))
		}
		if !det.MempoolAt.IsZero() && !det.DataAPIAt.IsZero() {
			memToAPI = append(memToAPI, det.DataAPIAt.Sub(det.MempoolAt))
		}
		if !det.LiveDataAt.IsZero() && !det.PolygonLogsAt.IsZero() {
			liveToLogs = append(liveToLogs, det.PolygonLogsAt.Sub(det.LiveDataAt))
		}
		if !det.LiveDataAt.IsZero() && !det.DataAPIAt.IsZero() {
			liveToAPI = append(liveToAPI, det.DataAPIAt.Sub(det.LiveDataAt))
		}
	}

	if len(memToLive) > 0 {
		fmt.Printf("│  MEMPOOL → LIVEDATA:     avg %v                        │\n", avgDuration(memToLive))
	}
	if len(memToLogs) > 0 {
		fmt.Printf("│  MEMPOOL → POLYGON_LOGS: avg %v                        │\n", avgDuration(memToLogs))
	}
	if len(memToAPI) > 0 {
		fmt.Printf("│  MEMPOOL → DATA_API:     avg %v                        │\n", avgDuration(memToAPI))
	}
	if len(liveToLogs) > 0 {
		fmt.Printf("│  LIVEDATA → POLYGON_LOGS: avg %v                       │\n", avgDuration(liveToLogs))
	}
	if len(liveToAPI) > 0 {
		fmt.Printf("│  LIVEDATA → DATA_API:     avg %v                       │\n", avgDuration(liveToAPI))
	}
	fmt.Println("└──────────────────────────────────────────────────────────────────────┘")

	// Detailed trade list
	fmt.Println("\n┌──────────────────────────────────────────────────────────────────────┐")
	fmt.Println("│                    TRADE DETAILS                                     │")
	fmt.Println("└──────────────────────────────────────────────────────────────────────┘")

	// Sort by first detection time
	type sortedDet struct {
		key string
		det *TradeDetection
	}
	sorted := make([]sortedDet, 0, total)
	for k, v := range detections {
		sorted = append(sorted, sortedDet{k, v})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return getFirstTime(sorted[i].det).Before(getFirstTime(sorted[j].det))
	})

	for i, s := range sorted {
		d := s.det
		fmt.Printf("\n#%d TX: %s...\n", i+1, d.TxHash[:30])
		fmt.Printf("   %s | %.2f @ %.4f | First: %s\n", d.Side, d.Size, d.Price, d.FirstMethod)

		printTime := func(name string, t time.Time) {
			if t.IsZero() {
				fmt.Printf("   %-12s ---\n", name+":")
			} else {
				fmt.Printf("   %-12s %s\n", name+":", t.Format("15:04:05.000"))
			}
		}
		printTime("MEMPOOL", d.MempoolAt)
		printTime("LIVEDATA", d.LiveDataAt)
		printTime("POLYGON_LOGS", d.PolygonLogsAt)
		printTime("DATA_API", d.DataAPIAt)
	}

	fmt.Println("\n==========================================================================")
}

func pct(n, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(n) / float64(total) * 100
}

func avgDuration(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range durations {
		sum += d
	}
	return (sum / time.Duration(len(durations))).Round(time.Millisecond)
}

func getFirstTime(d *TradeDetection) time.Time {
	times := []time.Time{d.MempoolAt, d.LiveDataAt, d.PolygonLogsAt, d.DataAPIAt}
	var first time.Time
	for _, t := range times {
		if !t.IsZero() && (first.IsZero() || t.Before(first)) {
			first = t
		}
	}
	return first
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

type rpcTransaction struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Input string `json:"input"`
}

func getTransaction(client *http.Client, txHash string) (*rpcTransaction, error) {
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

	var result struct {
		Result *rpcTransaction `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result.Result, nil
}

func decodeRelayerTrade(input string) (bool, string, string, *big.Int, *big.Int, string) {
	if len(input) < 10 {
		return false, "", "", nil, nil, ""
	}

	selector := input[2:10]
	// fillOrders = 0x2287e350, fillOrder = 0xfe729aaf
	if selector != "2287e350" && selector != "fe729aaf" {
		return false, "", "", nil, nil, ""
	}

	bytes, err := hex.DecodeString(input[10:])
	if err != nil {
		return false, "", "", nil, nil, ""
	}

	var side, tokenID, userAddr string
	var makerAmount, takerAmount *big.Int

	// Find user address in first few slots
	for i := 0; i < 5 && i*32+32 <= len(bytes); i++ {
		slot := bytes[i*32 : i*32+32]
		isAddress := true
		for j := 0; j < 12; j++ {
			if slot[j] != 0 {
				isAddress = false
				break
			}
		}
		if isAddress && slot[12] != 0 {
			userAddr = "0x" + hex.EncodeToString(slot[12:32])
			break
		}
	}

	// Find trade data
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
				if sideValue == 0 {
					side = "BUY"
				} else if sideValue == 1 {
					side = "SELL"
				} else {
					continue
				}

				tokenID = potentialTokenID.String()
				makerAmount = potentialMakerAmt
				takerAmount = potentialTakerAmt
				return true, side, tokenID, makerAmount, takerAmount, userAddr
			}
		}
	}

	return false, "", "", nil, nil, ""
}
