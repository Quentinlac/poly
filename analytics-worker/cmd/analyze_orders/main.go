// Tool to analyze Polymarket order calldata structure from mempool transactions
// This helps understand maker vs taker positions for copy trading
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
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	mempoolWSURL   = "wss://polygon-bor-rpc.publicnode.com"
	polygonHTTPRPC = "https://polygon-bor-rpc.publicnode.com"
)

// Polymarket contract addresses
var polymarketContracts = map[string]string{
	"0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e": "CTFExchange",
	"0xc5d563a36ae78145c45a50134d48a1215220f80a": "NegRiskCTFExchange",
	"0xe3f18acc55091e2c48d883fc8c8413319d4ab7b0": "NegRiskAdapter",
}

// Function selectors
var functionSelectors = map[string]string{
	"2287e350": "fillOrders (NegRiskAdapter)",
	"e20b2304": "fillOrder (CTFExchange)",
	"a4a6c5a5": "matchOrders",
	"d798eff6": "fillOrder (variant)",
}

// Order struct layout (each field is 32 bytes):
// 0: salt
// 1: maker (address - padded to 32 bytes)
// 2: signer (address)
// 3: taker (address)
// 4: tokenId (uint256)
// 5: makerAmount (uint256)
// 6: takerAmount (uint256)
// 7: expiration (uint256)
// 8: nonce (uint256)
// 9: feeRateBps (uint256)
// 10: side (uint8 - 0=BUY, 1=SELL)
// 11: signatureType (uint8)
// 12-13: signature data (bytes)

type ParsedOrder struct {
	Salt        string
	Maker       string // Who placed the order (MAKER = passive limit order)
	Signer      string
	Taker       string // Who fills the order (TAKER = active trade)
	TokenID     string
	MakerAmount *big.Int
	TakerAmount *big.Int
	Expiration  *big.Int
	Nonce       *big.Int
	FeeRateBps  *big.Int
	Side        string // "BUY" or "SELL"
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.Printf("=======================================================")
	log.Printf("   POLYMARKET ORDER STRUCTURE ANALYZER")
	log.Printf("=======================================================")
	log.Printf("")
	log.Printf("Monitoring mempool for Polymarket transactions...")
	log.Printf("Will parse and display order structure to understand maker/taker")
	log.Printf("")

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Connect to mempool
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.Dial(mempoolWSURL, nil)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Subscribe to pending transactions
	subMsg := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_subscribe",
		"params":  []interface{}{"newPendingTransactions"},
		"id":      1,
	}
	if err := conn.WriteJSON(subMsg); err != nil {
		log.Fatalf("Subscribe failed: %v", err)
	}

	// Read subscription response
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		log.Fatalf("Subscribe read failed: %v", err)
	}
	conn.SetReadDeadline(time.Time{})

	var resp struct {
		Result string `json:"result"`
	}
	json.Unmarshal(msg, &resp)
	log.Printf("✓ Subscribed to pending transactions (ID: %s)", resp.Result)
	log.Printf("")

	subID := resp.Result
	txCount := 0
	polyTxCount := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Read error: %v", err)
			break
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
		txCount++

		// Fetch transaction details
		tx, err := getTransaction(httpClient, txHash)
		if err != nil {
			continue
		}

		// Check if Polymarket contract
		contractName, ok := polymarketContracts[strings.ToLower(tx.To)]
		if !ok {
			continue
		}

		polyTxCount++
		log.Printf("═══════════════════════════════════════════════════════")
		log.Printf("🎯 POLYMARKET TX #%d (seen %d total pending)", polyTxCount, txCount)
		log.Printf("═══════════════════════════════════════════════════════")
		log.Printf("TX Hash:    %s", txHash)
		log.Printf("Contract:   %s (%s)", contractName, tx.To)
		log.Printf("TX From:    %s (operator/relayer)", tx.From)

		// Parse function selector
		if len(tx.Input) >= 10 {
			selector := tx.Input[2:10]
			funcName := functionSelectors[selector]
			if funcName == "" {
				funcName = "unknown"
			}
			log.Printf("Function:   0x%s (%s)", selector, funcName)
		}

		// Parse orders from calldata
		orders := parseOrders(tx.Input)
		if len(orders) > 0 {
			log.Printf("")
			log.Printf("📦 ORDERS IN TRANSACTION:")
			for i, order := range orders {
				log.Printf("")
				log.Printf("  Order #%d:", i+1)
				log.Printf("  ├─ MAKER:     %s  ← placed limit order (passive)", order.Maker)
				log.Printf("  ├─ SIGNER:    %s", order.Signer)
				log.Printf("  ├─ TAKER:     %s  ← fills the order (active)", order.Taker)
				log.Printf("  ├─ Side:      %s", order.Side)
				log.Printf("  ├─ TokenID:   %s...", truncate(order.TokenID, 20))
				if order.MakerAmount != nil && order.TakerAmount != nil {
					makerF, _ := new(big.Float).Quo(new(big.Float).SetInt(order.MakerAmount), big.NewFloat(1e6)).Float64()
					takerF, _ := new(big.Float).Quo(new(big.Float).SetInt(order.TakerAmount), big.NewFloat(1e6)).Float64()
					log.Printf("  ├─ MakerAmt:  %.2f", makerF)
					log.Printf("  ├─ TakerAmt:  %.2f", takerF)

					// Calculate price
					if order.Side == "BUY" {
						// BUY: maker pays USDC (makerAmount), receives tokens (takerAmount)
						if order.TakerAmount.Sign() > 0 {
							price := new(big.Float).Quo(
								new(big.Float).SetInt(order.MakerAmount),
								new(big.Float).SetInt(order.TakerAmount),
							)
							priceF, _ := price.Float64()
							log.Printf("  └─ Price:     $%.4f (USDC/token)", priceF)
						}
					} else {
						// SELL: maker sells tokens (makerAmount), receives USDC (takerAmount)
						if order.MakerAmount.Sign() > 0 {
							price := new(big.Float).Quo(
								new(big.Float).SetInt(order.TakerAmount),
								new(big.Float).SetInt(order.MakerAmount),
							)
							priceF, _ := price.Float64()
							log.Printf("  └─ Price:     $%.4f (USDC/token)", priceF)
						}
					}
				}

				// Explain the roles
				log.Printf("")
				log.Printf("  💡 INTERPRETATION:")
				if order.Taker == "0x0000000000000000000000000000000000000000" {
					log.Printf("     Taker=0x0: Anyone can fill this order")
					log.Printf("     MAKER (%s) placed a limit order", truncate(order.Maker, 12))
					log.Printf("     TX sender (%s) is filling it as TAKER", truncate(tx.From, 12))
				} else {
					log.Printf("     MAKER (%s) placed a limit order", truncate(order.Maker, 12))
					log.Printf("     TAKER (%s) is specified to fill it", truncate(order.Taker, 12))
				}
			}
		} else {
			log.Printf("")
			log.Printf("⚠️  Could not parse order structure from calldata")
			log.Printf("   Input length: %d bytes", len(tx.Input)/2)
		}

		log.Printf("")

		// Stop after a few examples
		if polyTxCount >= 10 {
			log.Printf("═══════════════════════════════════════════════════════")
			log.Printf("Captured %d Polymarket transactions. Stopping.", polyTxCount)
			log.Printf("═══════════════════════════════════════════════════════")
			break
		}
	}
}

func parseOrders(input string) []ParsedOrder {
	if len(input) < 10 {
		return nil
	}

	data := strings.TrimPrefix(input, "0x")
	if len(data) < 8 {
		return nil
	}

	// Skip function selector
	data = data[8:]

	bytes, err := hex.DecodeString(data)
	if err != nil {
		return nil
	}

	var orders []ParsedOrder

	// Order struct is 14 fields * 32 bytes = 448 bytes minimum
	// But with dynamic signature data it's longer
	// We scan for patterns that look like Order structs

	// For fillOrders, typical layout:
	// - First 64 bytes: offset to orders array, offset to fillAmounts array
	// - Then array header (length)
	// - Then Order structs

	// Try to find Order structs by pattern matching
	// Looking for: salt (random), maker (address), signer (address), taker (0 or address), tokenId (large), amounts...

	for offset := 0; offset+352 <= len(bytes); offset += 32 {
		// Try to interpret this as start of Order struct
		order := tryParseOrderAt(bytes, offset)
		if order != nil {
			orders = append(orders, *order)
			// Skip past this order (at least 11 fields = 352 bytes)
			offset += 320
		}
	}

	return orders
}

func tryParseOrderAt(data []byte, offset int) *ParsedOrder {
	if offset+352 > len(data) {
		return nil
	}

	// Read potential fields
	// Field 0: salt (32 bytes) - any value
	// Field 1: maker (address in last 20 bytes of 32-byte field)
	// Field 2: signer (address)
	// Field 3: taker (address, often 0x0)
	// Field 4: tokenId (32 bytes, should be large non-zero for real orders)
	// Field 5: makerAmount (32 bytes)
	// Field 6: takerAmount (32 bytes)
	// Field 7: expiration (32 bytes)
	// Field 8: nonce (32 bytes)
	// Field 9: feeRateBps (32 bytes, usually small)
	// Field 10: side (32 bytes, value 0 or 1)

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

	// Amounts should be reasonable
	makerAmount := new(big.Int).SetBytes(data[offset+160 : offset+192])
	takerAmount := new(big.Int).SetBytes(data[offset+192 : offset+224])

	// Sanity check amounts (0.1 USDC to 100,000 USDC)
	minAmt := big.NewInt(100_000)          // 0.1 USDC
	maxAmt := big.NewInt(100_000_000_000)  // 100,000 USDC

	if makerAmount.Cmp(minAmt) < 0 || makerAmount.Cmp(maxAmt) > 0 {
		return nil
	}
	if takerAmount.Cmp(minAmt) < 0 || takerAmount.Cmp(maxAmt) > 0 {
		return nil
	}

	// Side should be 0 or 1
	sideField := data[offset+320 : offset+352]
	sideValue := sideField[31]
	if sideValue > 1 {
		return nil
	}

	side := "BUY"
	if sideValue == 1 {
		side = "SELL"
	}

	return &ParsedOrder{
		Salt:        fmt.Sprintf("0x%x", data[offset:offset+32]),
		Maker:       maker,
		Signer:      signer,
		Taker:       taker,
		TokenID:     tokenId.String(),
		MakerAmount: makerAmount,
		TakerAmount: takerAmount,
		Side:        side,
	}
}

func isAddressField(field []byte) bool {
	if len(field) != 32 {
		return false
	}
	// First 12 bytes should be zeros for an address field
	for i := 0; i < 12; i++ {
		if field[i] != 0 {
			return false
		}
	}
	return true
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

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
