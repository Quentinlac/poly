// Tool to compare MAKER vs TAKER detection in mempool
// Option 1: TAKER detection (user actively fills orders)
// Option 2: MAKER detection (user places limit orders that get filled)
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
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	mempoolWSURL   = "wss://polygon-bor-rpc.publicnode.com"
	polygonHTTPRPC = "https://polygon-bor-rpc.publicnode.com"
	testDuration   = 20 * time.Minute
)

var polymarketContracts = map[string]string{
	"0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e": "CTFExchange",
	"0xc5d563a36ae78145c45a50134d48a1215220f80a": "NegRiskCTFExchange",
	"0xe3f18acc55091e2c48d883fc8c8413319d4ab7b0": "NegRiskAdapter",
}

type DetectedTrade struct {
	TxHash     string
	DetectedAt time.Time
	Source     string // "MEMPOOL_MAKER", "MEMPOOL_TAKER", "DATA_API"
	Side       string
	Size       float64
	Price      float64
	Role       string // "MAKER" or "TAKER" for mempool
}

type TradeTracker struct {
	mu     sync.Mutex
	trades map[string]*DetectedTrade // keyed by txHash

	// Stats
	mempoolMakerCount int
	mempoolTakerCount int
	dataAPICount      int
}

func NewTradeTracker() *TradeTracker {
	return &TradeTracker{
		trades: make(map[string]*DetectedTrade),
	}
}

func (t *TradeTracker) AddTrade(txHash, source, side string, size, price float64, role string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := strings.ToLower(txHash)
	isNew := false

	if _, exists := t.trades[key]; !exists {
		t.trades[key] = &DetectedTrade{
			TxHash:     txHash,
			DetectedAt: time.Now(),
			Source:     source,
			Side:       side,
			Size:       size,
			Price:      price,
			Role:       role,
		}
		isNew = true

		switch source {
		case "MEMPOOL_MAKER":
			t.mempoolMakerCount++
		case "MEMPOOL_TAKER":
			t.mempoolTakerCount++
		case "DATA_API":
			t.dataAPICount++
		}
	}

	return isNew
}

func (t *TradeTracker) GetStats() (makerCount, takerCount, dataAPICount, totalUnique int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mempoolMakerCount, t.mempoolTakerCount, t.dataAPICount, len(t.trades)
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run main.go <address>")
	}
	targetAddr := strings.ToLower(os.Args[1])
	if !strings.HasPrefix(targetAddr, "0x") {
		targetAddr = "0x" + targetAddr
	}

	log.SetFlags(log.Ltime | log.Lmicroseconds)

	fmt.Println("==========================================================================")
	fmt.Println("       MAKER vs TAKER DETECTION COMPARISON")
	fmt.Println("==========================================================================")
	fmt.Println()
	fmt.Printf("Target Address: %s\n", targetAddr)
	fmt.Printf("Test Duration:  %v\n", testDuration)
	fmt.Println()
	fmt.Println("Detection Methods:")
	fmt.Println("  1. MEMPOOL_MAKER  - User's address in MAKER field (passive limit order being filled)")
	fmt.Println("  2. MEMPOOL_TAKER  - User's address in order they're filling (active trade)")
	fmt.Println("  3. DATA_API       - Ground truth from Polymarket API")
	fmt.Println("==========================================================================")
	fmt.Println()

	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()

	tracker := NewTradeTracker()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	var wg sync.WaitGroup

	// Start mempool monitor
	wg.Add(1)
	go func() {
		defer wg.Done()
		monitorMempool(ctx, targetAddr, tracker, httpClient)
	}()

	// Start DATA_API poller
	wg.Add(1)
	go func() {
		defer wg.Done()
		pollDataAPI(ctx, targetAddr, tracker, httpClient)
	}()

	// Stats printer
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		startTime := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				elapsed := time.Since(startTime).Round(time.Second)
				makerCount, takerCount, dataAPICount, total := tracker.GetStats()
				fmt.Printf("\n📊 [%v] Unique Trades: %d | MAKER: %d | TAKER: %d | DATA_API: %d\n",
					elapsed, total, makerCount, takerCount, dataAPICount)
			}
		}
	}()

	wg.Wait()

	// Final summary
	printSummary(tracker)
}

func monitorMempool(ctx context.Context, targetAddr string, tracker *TradeTracker, httpClient *http.Client) {
	log.Printf("[MEMPOOL] Connecting to %s", mempoolWSURL)

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.Dial(mempoolWSURL, nil)
	if err != nil {
		log.Printf("[MEMPOOL] Connection failed: %v", err)
		return
	}
	defer conn.Close()

	// Subscribe
	subMsg := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_subscribe",
		"params":  []interface{}{"newPendingTransactions"},
		"id":      1,
	}
	if err := conn.WriteJSON(subMsg); err != nil {
		log.Printf("[MEMPOOL] Subscribe failed: %v", err)
		return
	}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		log.Printf("[MEMPOOL] Subscribe read failed: %v", err)
		return
	}
	conn.SetReadDeadline(time.Time{})

	var resp struct {
		Result string `json:"result"`
	}
	json.Unmarshal(msg, &resp)
	log.Printf("[MEMPOOL] ✓ Subscribed (ID: %s)", resp.Result)

	subID := resp.Result

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[MEMPOOL] Read error: %v, reconnecting...", err)
			time.Sleep(2 * time.Second)

			// Reconnect
			conn, _, err = dialer.Dial(mempoolWSURL, nil)
			if err != nil {
				continue
			}
			conn.WriteJSON(subMsg)
			conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			_, msg, _ = conn.ReadMessage()
			conn.SetReadDeadline(time.Time{})
			json.Unmarshal(msg, &resp)
			subID = resp.Result
			continue
		}

		var notif struct {
			Method string `json:"method"`
			Params struct {
				Subscription string `json:"subscription"`
				Result       string `json:"result"`
			} `json:"params"`
		}
		if err := json.Unmarshal(msg, &notif); err != nil {
			continue
		}

		if notif.Method != "eth_subscription" || notif.Params.Subscription != subID {
			continue
		}

		txHash := notif.Params.Result
		if txHash == "" {
			continue
		}

		go checkTransaction(ctx, txHash, targetAddr, tracker, httpClient)
	}
}

func checkTransaction(ctx context.Context, txHash, targetAddr string, tracker *TradeTracker, httpClient *http.Client) {
	tx, err := getTransaction(httpClient, txHash)
	if err != nil {
		return
	}

	// Check if Polymarket contract
	_, ok := polymarketContracts[strings.ToLower(tx.To)]
	if !ok {
		return
	}

	// Parse orders and check for target address
	makerMatch, takerMatch, side, size, price := analyzeOrders(tx.Input, targetAddr)

	if makerMatch {
		if tracker.AddTrade(txHash, "MEMPOOL_MAKER", side, size, price, "MAKER") {
			log.Printf("[MEMPOOL_MAKER] 🎯 %s | %.2f @ %.4f | TX: %s", side, size, price, txHash[:18])
		}
	}

	if takerMatch {
		if tracker.AddTrade(txHash, "MEMPOOL_TAKER", side, size, price, "TAKER") {
			log.Printf("[MEMPOOL_TAKER] 🎯 %s | %.2f @ %.4f | TX: %s", side, size, price, txHash[:18])
		}
	}
}

// analyzeOrders parses calldata and returns whether target is MAKER or TAKER
func analyzeOrders(input, targetAddr string) (makerMatch, takerMatch bool, side string, size, price float64) {
	if len(input) < 10 {
		return
	}

	data := strings.TrimPrefix(input, "0x")
	if len(data) < 8 {
		return
	}

	// Skip function selector
	data = data[8:]

	bytes, err := hex.DecodeString(data)
	if err != nil || len(bytes) < 352 {
		return
	}

	targetLower := strings.ToLower(targetAddr)

	// Scan for Order structs
	// Order layout: salt(32), maker(32), signer(32), taker(32), tokenId(32), makerAmount(32), takerAmount(32), ...
	for offset := 0; offset+352 <= len(bytes); offset += 32 {
		// Try to find an order at this offset
		order := tryParseOrderAt(bytes, offset)
		if order == nil {
			continue
		}

		// Check if target is in MAKER position
		if strings.ToLower(order.maker) == targetLower {
			makerMatch = true
			side = order.side
			size = order.size
			price = order.price
		}

		// Check if target is in SIGNER position (often same as maker for user orders)
		if strings.ToLower(order.signer) == targetLower {
			// If signer matches but maker doesn't, this might be a taker order
			if !makerMatch {
				takerMatch = true
				side = order.side
				size = order.size
				price = order.price
			}
		}

		// Skip past this order
		offset += 320
	}

	return
}

type parsedOrder struct {
	maker  string
	signer string
	taker  string
	side   string
	size   float64
	price  float64
}

func tryParseOrderAt(data []byte, offset int) *parsedOrder {
	if offset+352 > len(data) {
		return nil
	}

	// Check maker address field (offset+32): first 12 bytes should be zeros
	makerField := data[offset+32 : offset+64]
	if !isAddressField(makerField) {
		return nil
	}
	maker := fmt.Sprintf("0x%x", makerField[12:32])

	// Check signer address field
	signerField := data[offset+64 : offset+96]
	if !isAddressField(signerField) {
		return nil
	}
	signer := fmt.Sprintf("0x%x", signerField[12:32])

	// Check taker address field (can be 0x0)
	takerField := data[offset+96 : offset+128]
	if !isAddressField(takerField) {
		return nil
	}
	taker := fmt.Sprintf("0x%x", takerField[12:32])

	// TokenId should be non-zero
	tokenId := new(big.Int).SetBytes(data[offset+128 : offset+160])
	if tokenId.Sign() == 0 {
		return nil
	}

	// Amounts
	makerAmount := new(big.Int).SetBytes(data[offset+160 : offset+192])
	takerAmount := new(big.Int).SetBytes(data[offset+192 : offset+224])

	// Sanity check amounts
	minAmt := big.NewInt(100_000)
	maxAmt := big.NewInt(100_000_000_000)

	if makerAmount.Cmp(minAmt) < 0 || makerAmount.Cmp(maxAmt) > 0 {
		return nil
	}
	if takerAmount.Cmp(minAmt) < 0 || takerAmount.Cmp(maxAmt) > 0 {
		return nil
	}

	// Side (offset+320)
	sideField := data[offset+320 : offset+352]
	sideValue := sideField[31]
	if sideValue > 1 {
		return nil
	}

	side := "BUY"
	if sideValue == 1 {
		side = "SELL"
	}

	// Calculate size and price
	makerF := float64(makerAmount.Int64()) / 1e6
	takerF := float64(takerAmount.Int64()) / 1e6

	var size, price float64
	if side == "BUY" {
		size = takerF
		if takerF > 0 {
			price = makerF / takerF
		}
	} else {
		size = makerF
		if makerF > 0 {
			price = takerF / makerF
		}
	}

	return &parsedOrder{
		maker:  maker,
		signer: signer,
		taker:  taker,
		side:   side,
		size:   size,
		price:  price,
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

func pollDataAPI(ctx context.Context, targetAddr string, tracker *TradeTracker, httpClient *http.Client) {
	log.Printf("[DATA_API] Starting polling every 3s")

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	seenTxs := make(map[string]bool)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			trades, err := fetchRecentTrades(httpClient, targetAddr)
			if err != nil {
				continue
			}

			for _, trade := range trades {
				txHash := strings.ToLower(trade.TxHash)
				if seenTxs[txHash] {
					continue
				}
				seenTxs[txHash] = true

				if tracker.AddTrade(trade.TxHash, "DATA_API", trade.Side, trade.Size, trade.Price, "") {
					log.Printf("[DATA_API] 🎯 %s %s | %.2f @ %.4f | TX: %s",
						trade.Side, trade.Outcome, trade.Size, trade.Price, trade.TxHash[:18])
				}
			}
		}
	}
}

type APITrade struct {
	TxHash    string  `json:"transactionHash"`
	Side      string  `json:"side"`
	Outcome   string  `json:"outcome"`
	Size      float64 `json:"size"`
	Price     float64 `json:"price"`
	Timestamp int64   `json:"timestamp"`
}

func fetchRecentTrades(client *http.Client, addr string) ([]APITrade, error) {
	url := fmt.Sprintf("https://data-api.polymarket.com/activity?user=%s&type=TRADE&limit=20", addr)

	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var trades []APITrade
	if err := json.Unmarshal(body, &trades); err != nil {
		return nil, err
	}

	return trades, nil
}

type txResult struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Input string `json:"input"`
}

func getTransaction(client *http.Client, txHash string) (*txResult, error) {
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_getTransactionByHash",
		"params":  []string{txHash},
		"id":      1,
	}

	jsonBody, _ := json.Marshal(reqBody)
	resp, err := client.Post(polygonHTTPRPC, "application/json", strings.NewReader(string(jsonBody)))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result struct {
		Result *txResult `json:"result"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	if result.Result == nil {
		return nil, fmt.Errorf("not found")
	}

	return result.Result, nil
}

func printSummary(tracker *TradeTracker) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	fmt.Println()
	fmt.Println("==========================================================================")
	fmt.Println("                         FINAL SUMMARY")
	fmt.Println("==========================================================================")
	fmt.Println()
	fmt.Printf("Total Unique Trades: %d\n", len(tracker.trades))
	fmt.Println()
	fmt.Println("┌──────────────────────────────────────────────────────────────────────┐")
	fmt.Println("│                    DETECTION BY SOURCE                              │")
	fmt.Println("├──────────────────────────────────────────────────────────────────────┤")
	fmt.Printf("│  MEMPOOL_MAKER (passive limit orders):  %d trades                    \n", tracker.mempoolMakerCount)
	fmt.Printf("│  MEMPOOL_TAKER (active fills):          %d trades                    \n", tracker.mempoolTakerCount)
	fmt.Printf("│  DATA_API (ground truth):               %d trades                    \n", tracker.dataAPICount)
	fmt.Println("└──────────────────────────────────────────────────────────────────────┘")
	fmt.Println()

	// Show which trades were detected by which method
	fmt.Println("┌──────────────────────────────────────────────────────────────────────┐")
	fmt.Println("│                    TRADE DETAILS                                     │")
	fmt.Println("└──────────────────────────────────────────────────────────────────────┘")

	for txHash, trade := range tracker.trades {
		fmt.Printf("\nTX: %s...\n", txHash[:20])
		fmt.Printf("   First detected by: %s\n", trade.Source)
		fmt.Printf("   Side: %s | Size: %.2f | Price: %.4f\n", trade.Side, trade.Size, trade.Price)
		if trade.Role != "" {
			fmt.Printf("   Role: %s\n", trade.Role)
		}
	}

	fmt.Println()
	fmt.Println("==========================================================================")
}
