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
	PolygonWSRPC        = "wss://polygon-bor-rpc.publicnode.com"
	LiveDataWSURL       = "wss://ws-live-data.polymarket.com/"
	DataAPIURL          = "https://data-api.polymarket.com/activity"
	CLOBURL             = "https://clob.polymarket.com/data/trades"
	CTFExchangeAddr     = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
	NegRiskCTFExchange  = "0xc5d563a36ae78145c45a50134d48a1215220f80a"
	NegRiskAdapter      = "0xe3f18acc55091e2c48d883fc8c8413319d4ab7b0"
	RelayerAddress      = "0x4e29ffa29999fe5e0640e2d9fca4ccbc3f9f69d8"
)

var (
	targetAddress string
	detections    []Detection
	detectionsMu  sync.Mutex
	seenTx        = make(map[string]map[string]bool)
	seenMu        sync.Mutex
)

// Detection represents a single trade detection event
type Detection struct {
	TxHash            string
	TimestampDetected time.Time
	TimestampTx       time.Time
	Method            string
	Shares            float64
	Outcome           string
	Side              string
	TokenID           string
	Price             float64
}

// DataAPITrade is the official trade record from Polymarket
type DataAPITrade struct {
	TransactionHash string  `json:"transactionHash"`
	Side            string  `json:"side"`
	Size            float64 `json:"size"`
	Price           float64 `json:"price"`
	Outcome         string  `json:"outcome"`
	Asset           string  `json:"asset"`
	Timestamp       int64   `json:"timestamp"`
	Title           string  `json:"title"`
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: compare_methods <target_address> <duration_minutes> [output.csv]")
		os.Exit(1)
	}

	targetAddress = strings.ToLower(os.Args[1])
	durationMin, _ := time.ParseDuration(os.Args[2] + "m")
	csvPath := fmt.Sprintf("detection_%s.csv", time.Now().Format("20060102_150405"))
	if len(os.Args) > 3 {
		csvPath = os.Args[3]
	}

	fmt.Println("==========================================================================")
	fmt.Println("       TRADE DETECTION METHOD COMPARISON (with Data API Verification)")
	fmt.Println("==========================================================================")
	fmt.Printf("\nTarget Address: %s\n", targetAddress)
	fmt.Printf("Test Duration:  %v\n", durationMin)
	fmt.Printf("CSV Output:     %s\n\n", csvPath)
	fmt.Println("Detection Methods:")
	fmt.Println("  1. MEMPOOL      - Polygon pending TX (fastest, ~2-3s before confirmation)")
	fmt.Println("  2. LIVEDATA     - ws-live-data.polymarket.com (~650ms latency)")
	fmt.Println("  3. POLYGON_LOGS - eth_subscribe logs (OrderFilled events)")
	fmt.Println("==========================================================================\n")

	ctx, cancel := context.WithTimeout(context.Background(), durationMin)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(3)

	go func() { defer wg.Done(); monitorMempool(ctx) }()
	go func() { defer wg.Done(); monitorLiveData(ctx) }()
	go func() { defer wg.Done(); monitorPolygonLogs(ctx) }()

	<-ctx.Done()
	time.Sleep(2 * time.Second)

	fmt.Println("\n==========================================================================")
	fmt.Println("       FETCHING DATA API FOR VERIFICATION")
	fmt.Println("==========================================================================")

	// Fetch actual trades from Data API
	actualTrades := fetchDataAPITrades()
	fmt.Printf("Fetched %d trades from Data API\n", len(actualTrades))

	// Build a map of actual trades by tx hash
	// Note: One tx can have multiple trades (fills), so store as slice
	actualByTx := make(map[string][]DataAPITrade)
	for _, t := range actualTrades {
		actualByTx[t.TransactionHash] = append(actualByTx[t.TransactionHash], t)
	}

	// Write CSV with verification columns
	csvFile, err := os.Create(csvPath)
	if err != nil {
		log.Fatalf("Failed to create CSV: %v", err)
	}
	defer csvFile.Close()

	csvWriter := csv.NewWriter(csvFile)
	csvWriter.Write([]string{
		"tx_hash", "timestamp_detected", "method", "shares", "outcome", "side", "token_id", "price",
		"actual_shares", "actual_outcome", "actual_side", "actual_price", "actual_title", "match",
	})

	// Sort detections by timestamp
	detectionsMu.Lock()
	sort.Slice(detections, func(i, j int) bool {
		return detections[i].TimestampDetected.Before(detections[j].TimestampDetected)
	})

	matchCount := 0
	mismatchCount := 0

	for _, d := range detections {
		tsDetected := d.TimestampDetected.Format("2006-01-02T15:04:05.000")

		// Find matching actual trade
		var actualShares, actualPrice string
		var actualOutcome, actualSide, actualTitle string
		var match string

		if actuals, ok := actualByTx[d.TxHash]; ok && len(actuals) > 0 {
			// Find the best matching actual trade (by shares or side)
			best := actuals[0]
			for _, a := range actuals {
				// Prefer exact shares match
				if fmt.Sprintf("%.2f", a.Size) == fmt.Sprintf("%.2f", d.Shares) {
					best = a
					break
				}
			}
			actualShares = fmt.Sprintf("%.6f", best.Size)
			actualPrice = fmt.Sprintf("%.6f", best.Price)
			actualOutcome = best.Outcome
			actualSide = best.Side
			actualTitle = best.Title
			if len(actualTitle) > 50 {
				actualTitle = actualTitle[:50] + "..."
			}

			// Check if our detection matches
			sharesMatch := fmt.Sprintf("%.2f", d.Shares) == fmt.Sprintf("%.2f", best.Size)
			sideMatch := strings.EqualFold(d.Side, best.Side) || d.Side == ""
			if sharesMatch && sideMatch {
				match = "OK"
				matchCount++
			} else {
				match = "MISMATCH"
				mismatchCount++
			}
		} else {
			match = "NOT_FOUND"
			mismatchCount++
		}

		csvWriter.Write([]string{
			d.TxHash,
			tsDetected,
			d.Method,
			fmt.Sprintf("%.6f", d.Shares),
			d.Outcome,
			d.Side,
			d.TokenID,
			fmt.Sprintf("%.6f", d.Price),
			actualShares,
			actualOutcome,
			actualSide,
			actualPrice,
			actualTitle,
			match,
		})
	}
	detectionsMu.Unlock()

	csvWriter.Flush()

	fmt.Printf("\n==========================================================================\n")
	fmt.Printf("       SUMMARY\n")
	fmt.Printf("==========================================================================\n")
	fmt.Printf("Total detections:  %d\n", len(detections))
	fmt.Printf("Matches (OK):      %d\n", matchCount)
	fmt.Printf("Mismatches:        %d\n", mismatchCount)
	fmt.Printf("CSV written to:    %s\n", csvPath)
}

func addDetection(d Detection) {
	// Check if we already recorded this tx+method combo
	seenMu.Lock()
	if seenTx[d.TxHash] == nil {
		seenTx[d.TxHash] = make(map[string]bool)
	}
	if seenTx[d.TxHash][d.Method] {
		seenMu.Unlock()
		return
	}
	seenTx[d.TxHash][d.Method] = true
	seenMu.Unlock()

	detectionsMu.Lock()
	detections = append(detections, d)
	detectionsMu.Unlock()
}

func fetchDataAPITrades() []DataAPITrade {
	url := fmt.Sprintf("%s?user=%s&type=TRADE&limit=100", DataAPIURL, strings.TrimPrefix(targetAddress, "0x"))
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Failed to fetch Data API: %v", err)
		return nil
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var trades []DataAPITrade
	if err := json.Unmarshal(body, &trades); err != nil {
		log.Printf("Failed to parse Data API: %v", err)
		return nil
	}
	return trades
}

// ============================================================================
// METHOD 1: MEMPOOL
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
	log.Printf("[MEMPOOL] Subscribed (ID: %s)", subResp.Result)

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

		go processMempoolTx(txHash, targetAddrClean)
	}
}

func processMempoolTx(txHash, targetAddrClean string) {
	tx, err := getFullTransaction(txHash)
	if err != nil || tx == nil {
		return
	}

	toLower := strings.ToLower(tx.To)
	if toLower != RelayerAddress && toLower != strings.ToLower(CTFExchangeAddr) && toLower != strings.ToLower(NegRiskCTFExchange) && toLower != strings.ToLower(NegRiskAdapter) {
		return
	}

	inputLower := strings.ToLower(tx.Input)
	if !strings.Contains(inputLower, targetAddrClean) {
		return
	}

	now := time.Now()

	decoded, side, tokenID, makerAmount, takerAmount, fillAmount, _ := decodeOrderFullDebug(tx.Input, targetAddrClean)

	var shares, price float64

	if decoded && fillAmount != nil && fillAmount.Sign() > 0 {
		fillF := new(big.Float).SetInt(fillAmount)
		shares, _ = new(big.Float).Quo(fillF, big.NewFloat(1e6)).Float64()
	}

	if decoded && makerAmount != nil && takerAmount != nil && makerAmount.Sign() > 0 && takerAmount.Sign() > 0 {
		makerF := new(big.Float).SetInt(makerAmount)
		takerF := new(big.Float).SetInt(takerAmount)
		if side == "BUY" {
			price, _ = new(big.Float).Quo(makerF, takerF).Float64()
		} else {
			price, _ = new(big.Float).Quo(takerF, makerF).Float64()
		}
	}

	addDetection(Detection{
		TxHash:            txHash,
		TimestampDetected: now,
		Method:            "MEMPOOL",
		Shares:            shares,
		Side:              side,
		TokenID:           tokenID,
		Price:             price,
	})

	log.Printf("[MEMPOOL] %s | %.2f shares @ %.4f | TX: %s",
		side, shares, price, txHash[:16])
}

func decodeOrderFullDebug(input string, targetAddrClean string) (decoded bool, side string, tokenID string, makerAmount *big.Int, takerAmount *big.Int, fillAmount *big.Int, debugInfo string) {
	if len(input) < 10 {
		debugInfo = "input too short"
		return
	}

	data := strings.TrimPrefix(input, "0x")
	if len(data) < 8 {
		debugInfo = "data too short"
		return
	}

	selector := data[:8]
	dataBytes, err := hex.DecodeString(data[8:])
	if err != nil || len(dataBytes) < 256 {
		debugInfo = fmt.Sprintf("hex decode failed or data<256: len=%d", len(dataBytes))
		return
	}

	const orderStructSize = 384

	parseOrderAt := func(offset int) (makerAddr, signerAddr, takerAddr, tokenIDStr string, makerAmt, takerAmt *big.Int, sideStr string, valid bool) {
		if offset+orderStructSize > len(dataBytes) {
			return
		}
		makerField := dataBytes[offset+32 : offset+64]
		if !isValidAddressField(makerField) {
			return
		}
		makerAddr = fmt.Sprintf("%x", dataBytes[offset+44:offset+64])
		signerAddr = fmt.Sprintf("%x", dataBytes[offset+76:offset+96])
		takerAddr = fmt.Sprintf("%x", dataBytes[offset+108:offset+128])
		tokenIDStr = new(big.Int).SetBytes(dataBytes[offset+128 : offset+160]).String()
		makerAmt = new(big.Int).SetBytes(dataBytes[offset+160 : offset+192])
		takerAmt = new(big.Int).SetBytes(dataBytes[offset+192 : offset+224])
		sideValue := dataBytes[offset+320+31]
		if sideValue == 0 {
			sideStr = "BUY"
		} else {
			sideStr = "SELL"
		}
		valid = true
		return
	}

	if selector == "2287e350" { // fillOrders
		if len(dataBytes) < 160 {
			debugInfo = "fillOrders: dataBytes<160"
			return
		}

		// NegRiskAdapter.fillOrders ABI:
		// Word 0: offset to orders[] (but array doesn't have length prefix!)
		// Word 1: offset to signatures[]
		// Word 2: takerFillAmountUsdc
		// Word 3: takerFillAmountShares (TOTAL fill - use this!)
		// Word 4: offset to fillAmounts[]
		totalFill := new(big.Int).SetBytes(dataBytes[96:128])

		debugInfo = fmt.Sprintf("fillOrders: totalFill=%s", totalFill.String())

		// Find target address in the data by searching hex
		targetBytes := strings.ToLower(targetAddrClean)
		dataHex := strings.ToLower(hex.EncodeToString(dataBytes))
		targetPos := strings.Index(dataHex, targetBytes)

		if targetPos < 0 {
			debugInfo += ", target NOT FOUND"
			return
		}

		// Target found at position targetPos (in hex chars) = targetPos/2 in bytes
		targetBytePos := targetPos / 2
		debugInfo += fmt.Sprintf(", target at byte %d", targetBytePos)

		// In Order struct: maker is at offset 44 from start (32-byte word, last 20 bytes)
		// So order starts at: targetBytePos - 44
		orderStart := targetBytePos - 44
		if orderStart < 0 {
			// Target might be in signer field (offset 76) instead
			orderStart = targetBytePos - 76
		}

		if orderStart >= 0 && orderStart+224 <= len(dataBytes) {
			tokenID = new(big.Int).SetBytes(dataBytes[orderStart+128 : orderStart+160]).String()
			makerAmount = new(big.Int).SetBytes(dataBytes[orderStart+160 : orderStart+192])
			takerAmount = new(big.Int).SetBytes(dataBytes[orderStart+192 : orderStart+224])

			// Get side from order
			if orderStart+352 <= len(dataBytes) {
				sideVal := dataBytes[orderStart+320+31]
				if sideVal == 0 {
					side = "BUY"
				} else {
					side = "SELL"
				}
			}

			// Use totalFill (Word 3) as the fill amount - this matches Data API
			fillAmount = totalFill
			decoded = true
			debugInfo += fmt.Sprintf(", FOUND order, fill=%s", fillAmount.String())
			return
		}

		debugInfo += ", order parse failed"

	} else if selector == "a4a6c5a5" { // matchOrders
		if len(dataBytes) < 160 {
			debugInfo = "matchOrders: dataBytes<160"
			return
		}

		takerOrderOffset := int(new(big.Int).SetBytes(dataBytes[0:32]).Int64())
		makerOrdersOffset := int(new(big.Int).SetBytes(dataBytes[32:64]).Int64())
		takerReceiveAmount := new(big.Int).SetBytes(dataBytes[96:128])
		makerFillAmountsOffset := int(new(big.Int).SetBytes(dataBytes[128:160]).Int64())

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

		debugInfo = fmt.Sprintf("matchOrders: takerReceive=%s, makerFillsLen=%d", takerReceiveAmount.String(), len(makerFillAmounts))

		if takerOrderOffset+orderStructSize <= len(dataBytes) {
			makerAddr, signerAddr, _, tokenIDStr, makerAmt, takerAmt, sideStr, valid := parseOrderAt(takerOrderOffset)
			if valid && (strings.Contains(makerAddr, targetAddrClean) || strings.Contains(signerAddr, targetAddrClean)) {
				decoded = true
				tokenID = tokenIDStr
				makerAmount = makerAmt
				takerAmount = takerAmt
				side = sideStr
				fillAmount = takerReceiveAmount
				debugInfo += fmt.Sprintf(", FOUND as TAKER, fill=%s", fillAmount.String())
				return
			}
		}

		if makerOrdersOffset+32 <= len(dataBytes) {
			makersLen := int(new(big.Int).SetBytes(dataBytes[makerOrdersOffset : makerOrdersOffset+32]).Int64())
			debugInfo += fmt.Sprintf(", makersLen=%d", makersLen)

			for i := 0; i < makersLen && i < 50; i++ {
				orderStart := makerOrdersOffset + 32 + i*orderStructSize
				makerAddr, signerAddr, takerAddr, tokenIDStr, makerAmt, takerAmt, sideStr, valid := parseOrderAt(orderStart)
				if !valid {
					continue
				}

				if strings.Contains(makerAddr, targetAddrClean) || strings.Contains(signerAddr, targetAddrClean) || strings.Contains(takerAddr, targetAddrClean) {
					decoded = true
					tokenID = tokenIDStr
					makerAmount = makerAmt
					takerAmount = takerAmt
					side = sideStr

					if i < len(makerFillAmounts) {
						fillAmount = makerFillAmounts[i]
						debugInfo += fmt.Sprintf(", FOUND as MAKER[%d], fill=%s", i, fillAmount.String())
					} else {
						debugInfo += fmt.Sprintf(", FOUND as MAKER[%d] but no fill entry", i)
					}
					return
				}
			}
		}
		debugInfo += ", target NOT FOUND"

	} else {
		debugInfo = fmt.Sprintf("unknown selector: %s", selector)
	}

	return
}

func isValidAddressField(field []byte) bool {
	if len(field) != 32 {
		return false
	}
	for i := 0; i < 12; i++ {
		if field[i] != 0 {
			return false
		}
	}
	nonZeroCount := 0
	for i := 12; i < 22; i++ {
		if field[i] != 0 {
			nonZeroCount++
		}
	}
	return nonZeroCount >= 3
}

type Transaction struct {
	Hash  string `json:"hash"`
	To    string `json:"to"`
	Input string `json:"input"`
}

func getFullTransaction(txHash string) (*Transaction, error) {
	reqBody := fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getTransactionByHash","params":["%s"],"id":1}`, txHash)
	resp, err := http.Post("https://polygon-bor-rpc.publicnode.com", "application/json", strings.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Result *Transaction `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result.Result, nil
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
	log.Printf("[LIVEDATA] Subscribed to orders_matched")

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
				Outcome         string  `json:"outcome"`
				TransactionHash string  `json:"transactionHash"`
				AssetId         string  `json:"asset_id"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(message, &msg); err != nil || msg.Type != "orders_matched" {
			continue
		}

		if strings.ToLower(msg.Payload.ProxyWallet) != targetAddress {
			continue
		}

		addDetection(Detection{
			TxHash:            msg.Payload.TransactionHash,
			TimestampDetected: time.Now(),
			Method:            "LIVEDATA",
			Shares:            msg.Payload.Size,
			Outcome:           msg.Payload.Outcome,
			Side:              msg.Payload.Side,
			TokenID:           msg.Payload.AssetId,
			Price:             msg.Payload.Price,
		})

		log.Printf("[LIVEDATA] %s | %.2f shares @ %.4f | %s | TX: %s",
			msg.Payload.Side, msg.Payload.Size, msg.Payload.Price, msg.Payload.Outcome, msg.Payload.TransactionHash[:16])
	}
}

// ============================================================================
// METHOD 3: POLYGON LOGS (OrderFilled events)
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
	log.Printf("[POLYGON_LOGS] Subscribed to OrderFilled (ID: %s)", subResp.Result)

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
			if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "deadline") {
				continue
			}
			return fmt.Errorf("read error: %w", err)
		}

		if msg.Params.Result.TransactionHash == "" || len(msg.Params.Result.Topics) < 4 {
			continue
		}

		makerTopic := msg.Params.Result.Topics[2]
		takerTopic := msg.Params.Result.Topics[3]

		if len(makerTopic) < 66 || len(takerTopic) < 66 {
			continue
		}

		makerAddr := strings.ToLower("0x" + makerTopic[26:])
		takerAddr := strings.ToLower("0x" + takerTopic[26:])

		if makerAddr != targetAddress && takerAddr != targetAddress {
			continue
		}

		shares, side, price, tokenID := decodeOrderFilledLog(msg.Params.Result.Data, makerAddr == targetAddress)

		addDetection(Detection{
			TxHash:            msg.Params.Result.TransactionHash,
			TimestampDetected: time.Now(),
			Method:            "POLYGON_LOGS",
			Shares:            shares,
			Side:              side,
			TokenID:           tokenID,
			Price:             price,
		})

		log.Printf("[POLYGON_LOGS] %s | %.2f shares @ %.4f | TX: %s",
			side, shares, price, msg.Params.Result.TransactionHash[:16])
	}
}

func decodeOrderFilledLog(data string, isMaker bool) (shares float64, side string, price float64, tokenID string) {
	data = strings.TrimPrefix(data, "0x")
	if len(data) < 320 {
		return
	}

	dataBytes, err := hex.DecodeString(data)
	if err != nil || len(dataBytes) < 160 {
		return
	}

	makerAssetID := new(big.Int).SetBytes(dataBytes[0:32])
	takerAssetID := new(big.Int).SetBytes(dataBytes[32:64])
	makerAmountFilled := new(big.Int).SetBytes(dataBytes[64:96])
	takerAmountFilled := new(big.Int).SetBytes(dataBytes[96:128])

	makerIsUSDC := makerAssetID.Cmp(big.NewInt(1e15)) < 0
	takerIsUSDC := takerAssetID.Cmp(big.NewInt(1e15)) < 0

	var usdcAmount, sharesAmount *big.Int
	if makerIsUSDC && !takerIsUSDC {
		usdcAmount = makerAmountFilled
		sharesAmount = takerAmountFilled
		tokenID = takerAssetID.String()
		if isMaker {
			side = "BUY"
		} else {
			side = "SELL"
		}
	} else if !makerIsUSDC && takerIsUSDC {
		usdcAmount = takerAmountFilled
		sharesAmount = makerAmountFilled
		tokenID = makerAssetID.String()
		if isMaker {
			side = "SELL"
		} else {
			side = "BUY"
		}
	} else {
		return
	}

	sharesF := new(big.Float).SetInt(sharesAmount)
	shares, _ = new(big.Float).Quo(sharesF, big.NewFloat(1e6)).Float64()

	if sharesAmount.Sign() > 0 {
		usdcF := new(big.Float).SetInt(usdcAmount)
		price, _ = new(big.Float).Quo(usdcF, new(big.Float).SetInt(sharesAmount)).Float64()
	}

	return
}
