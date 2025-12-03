package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strings"
)

// Transaction represents a raw transaction
type Transaction struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Input string `json:"input"`
}

// Order represents a Polymarket Order struct
type Order struct {
	Salt          *big.Int
	Maker         string
	Signer        string
	Taker         string
	TokenID       *big.Int
	MakerAmount   *big.Int
	TakerAmount   *big.Int
	Expiration    uint64
	Nonce         *big.Int
	FeeRateBps    uint64
	Side          uint8 // 0=BUY, 1=SELL
	SignatureType uint8
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: decode_tx <tx_hash> [target_address]")
		os.Exit(1)
	}

	txHash := os.Args[1]
	targetAddr := ""
	if len(os.Args) > 2 {
		targetAddr = strings.ToLower(os.Args[2])
	}

	// Fetch transaction
	tx, err := fetchTransaction(txHash)
	if err != nil {
		fmt.Printf("Error fetching transaction: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("=== Transaction Analysis ===\n")
	fmt.Printf("TX Hash: %s\n", txHash)
	fmt.Printf("From: %s\n", tx.From)
	fmt.Printf("To: %s\n", tx.To)
	fmt.Printf("Input Length: %d bytes\n", (len(tx.Input)-2)/2)
	fmt.Println()

	// Decode the input
	decodeInput(tx.Input, targetAddr)
}

func fetchTransaction(txHash string) (*Transaction, error) {
	payload := fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getTransactionByHash","params":["%s"],"id":1}`, txHash)

	resp, err := http.Post("https://polygon-bor-rpc.publicnode.com", "application/json", strings.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result struct {
		Result *Transaction `json:"result"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	if result.Result == nil {
		return nil, fmt.Errorf("transaction not found")
	}

	return result.Result, nil
}

func decodeInput(input string, targetAddr string) {
	data := strings.TrimPrefix(input, "0x")
	if len(data) < 8 {
		fmt.Println("Input too short")
		return
	}

	selector := data[:8]
	fmt.Printf("Function Selector: 0x%s\n", selector)

	switch selector {
	case "2287e350":
		fmt.Println("Function: fillOrders (NegRiskAdapter)")
	case "a4a6c5a5":
		fmt.Println("Function: matchOrders")
	case "e20b2304":
		fmt.Println("Function: fillOrder (single)")
	default:
		fmt.Printf("Function: unknown (0x%s)\n", selector)
	}
	fmt.Println()

	// Decode bytes
	dataBytes, err := hex.DecodeString(data[8:])
	if err != nil {
		fmt.Printf("Error decoding hex: %v\n", err)
		return
	}

	fmt.Printf("Calldata size: %d bytes (%d words)\n\n", len(dataBytes), len(dataBytes)/32)

	// Print first 10 words (header)
	fmt.Println("=== Header Words ===")
	for i := 0; i < min(10, len(dataBytes)/32); i++ {
		word := dataBytes[i*32 : (i+1)*32]
		wordInt := new(big.Int).SetBytes(word)

		// Check if it looks like an offset (small number)
		if wordInt.Cmp(big.NewInt(10000)) < 0 {
			fmt.Printf("Word %d: %d (offset: 0x%x)\n", i, wordInt.Int64(), wordInt.Int64())
		} else if isAddress(word) {
			fmt.Printf("Word %d: address 0x%x\n", i, word[12:])
		} else {
			// Check if it could be USDC amount (6 decimals)
			usdc := new(big.Float).Quo(new(big.Float).SetInt(wordInt), big.NewFloat(1e6))
			usdcVal, _ := usdc.Float64()
			if usdcVal > 0.01 && usdcVal < 1000000 {
				fmt.Printf("Word %d: %s (%.6f USDC/shares)\n", i, wordInt.String(), usdcVal)
			} else {
				fmt.Printf("Word %d: %s\n", i, wordInt.String())
			}
		}
	}
	fmt.Println()

	if selector == "2287e350" {
		decodeFillOrders(dataBytes, targetAddr)
	} else if selector == "a4a6c5a5" {
		decodeMatchOrders(dataBytes, targetAddr)
	}
}

func decodeFillOrders(dataBytes []byte, targetAddr string) {
	if len(dataBytes) < 128 {
		fmt.Println("Data too short for fillOrders")
		return
	}

	// fillOrders(Order[] orders, uint256[] fillAmounts, uint256 takerFillAmountUsdc, uint256 takerFillAmountShares)
	ordersOffset := int(new(big.Int).SetBytes(dataBytes[0:32]).Int64())
	fillAmountsOffset := int(new(big.Int).SetBytes(dataBytes[32:64]).Int64())
	takerFillUsdc := new(big.Int).SetBytes(dataBytes[64:96])
	takerFillShares := new(big.Int).SetBytes(dataBytes[96:128])

	fmt.Println("=== fillOrders Parameters ===")
	fmt.Printf("Orders Array Offset: %d (0x%x)\n", ordersOffset, ordersOffset)
	fmt.Printf("FillAmounts Array Offset: %d (0x%x)\n", fillAmountsOffset, fillAmountsOffset)
	fmt.Printf("TakerFillAmountUsdc (Word 2): %s (%.6f USDC)\n", takerFillUsdc.String(), toFloat(takerFillUsdc))
	fmt.Printf("TakerFillAmountShares (Word 3): %s (%.6f shares)\n", takerFillShares.String(), toFloat(takerFillShares))
	fmt.Println()

	// Parse orders array
	if ordersOffset+32 > len(dataBytes) {
		fmt.Println("Invalid orders offset")
		return
	}
	ordersLen := int(new(big.Int).SetBytes(dataBytes[ordersOffset : ordersOffset+32]).Int64())
	fmt.Printf("=== Orders Array (length: %d) ===\n", ordersLen)

	// Parse fillAmounts array
	var fillAmounts []*big.Int
	if fillAmountsOffset+32 <= len(dataBytes) {
		fillLen := int(new(big.Int).SetBytes(dataBytes[fillAmountsOffset : fillAmountsOffset+32]).Int64())
		fmt.Printf("\n=== FillAmounts Array (length: %d) ===\n", fillLen)
		for i := 0; i < fillLen && i < 20; i++ {
			elemOff := fillAmountsOffset + 32 + i*32
			if elemOff+32 <= len(dataBytes) {
				amt := new(big.Int).SetBytes(dataBytes[elemOff : elemOff+32])
				fillAmounts = append(fillAmounts, amt)
				fmt.Printf("  fillAmounts[%d]: %s (%.6f)\n", i, amt.String(), toFloat(amt))
			}
		}
	}
	fmt.Println()

	// Parse each order
	const orderSize = 384 // 12 fields * 32 bytes
	for i := 0; i < ordersLen && i < 10; i++ {
		orderStart := ordersOffset + 32 + i*orderSize
		if orderStart+orderSize > len(dataBytes) {
			fmt.Printf("Order %d: truncated\n", i)
			continue
		}

		order := parseOrder(dataBytes[orderStart : orderStart+orderSize])

		// Check if this order matches target
		isTarget := targetAddr != "" && (
			strings.Contains(strings.ToLower(order.Maker), targetAddr) ||
			strings.Contains(strings.ToLower(order.Signer), targetAddr) ||
			strings.Contains(strings.ToLower(order.Taker), targetAddr))

		marker := ""
		if isTarget {
			marker = " <<<< TARGET"
		}

		fmt.Printf("--- Order %d%s ---\n", i, marker)
		printOrder(order)

		// Show the fill amount for this order
		if i < len(fillAmounts) {
			fmt.Printf("  FillAmount[%d]: %.6f shares\n", i, toFloat(fillAmounts[i]))
		}

		// Calculate price
		if order.MakerAmount.Sign() > 0 && order.TakerAmount.Sign() > 0 {
			makerF := new(big.Float).SetInt(order.MakerAmount)
			takerF := new(big.Float).SetInt(order.TakerAmount)

			if order.Side == 0 { // BUY
				priceF := new(big.Float).Quo(makerF, takerF)
				price, _ := priceF.Float64()
				fmt.Printf("  Price (USDC/share): %.6f\n", price)
			} else { // SELL
				priceF := new(big.Float).Quo(takerF, makerF)
				price, _ := priceF.Float64()
				fmt.Printf("  Price (USDC/share): %.6f\n", price)
			}
		}
		fmt.Println()
	}

	// Summary
	fmt.Println("=== SUMMARY ===")
	fmt.Printf("Total Orders: %d\n", ordersLen)
	fmt.Printf("Word 3 (takerFillShares): %.6f - THIS IS TOTAL, NOT PER-ORDER!\n", toFloat(takerFillShares))
	if len(fillAmounts) > 0 {
		var sum float64
		for _, amt := range fillAmounts {
			sum += toFloat(amt)
		}
		fmt.Printf("Sum of fillAmounts[]: %.6f\n", sum)
	}
}

func decodeMatchOrders(dataBytes []byte, targetAddr string) {
	if len(dataBytes) < 160 {
		fmt.Println("Data too short for matchOrders")
		return
	}

	// matchOrders(Order takerOrder, Order[] makerOrders, uint256 takerFillAmount, uint256 takerReceiveAmount, uint256[] makerFillAmounts)
	takerOrderOffset := int(new(big.Int).SetBytes(dataBytes[0:32]).Int64())
	makerOrdersOffset := int(new(big.Int).SetBytes(dataBytes[32:64]).Int64())
	takerFillAmount := new(big.Int).SetBytes(dataBytes[64:96])
	takerReceiveAmount := new(big.Int).SetBytes(dataBytes[96:128])
	makerFillAmountsOffset := int(new(big.Int).SetBytes(dataBytes[128:160]).Int64())

	fmt.Println("=== matchOrders Parameters ===")
	fmt.Printf("TakerOrder Offset: %d\n", takerOrderOffset)
	fmt.Printf("MakerOrders Offset: %d\n", makerOrdersOffset)
	fmt.Printf("TakerFillAmount (USDC): %.6f\n", toFloat(takerFillAmount))
	fmt.Printf("TakerReceiveAmount (shares): %.6f\n", toFloat(takerReceiveAmount))
	fmt.Printf("MakerFillAmounts Offset: %d\n", makerFillAmountsOffset)
	fmt.Println()

	// Parse taker order
	if takerOrderOffset+384 <= len(dataBytes) {
		order := parseOrder(dataBytes[takerOrderOffset : takerOrderOffset+384])
		fmt.Println("=== TAKER Order ===")
		printOrder(order)
		fmt.Println()
	}

	// Parse maker orders
	if makerOrdersOffset+32 <= len(dataBytes) {
		makersLen := int(new(big.Int).SetBytes(dataBytes[makerOrdersOffset : makerOrdersOffset+32]).Int64())
		fmt.Printf("=== MAKER Orders (count: %d) ===\n", makersLen)

		for i := 0; i < makersLen && i < 10; i++ {
			orderStart := makerOrdersOffset + 32 + i*384
			if orderStart+384 > len(dataBytes) {
				break
			}
			order := parseOrder(dataBytes[orderStart : orderStart+384])
			fmt.Printf("--- Maker Order %d ---\n", i)
			printOrder(order)
			fmt.Println()
		}
	}

	// Parse makerFillAmounts
	if makerFillAmountsOffset+32 <= len(dataBytes) {
		fillLen := int(new(big.Int).SetBytes(dataBytes[makerFillAmountsOffset : makerFillAmountsOffset+32]).Int64())
		fmt.Printf("=== MakerFillAmounts (count: %d) ===\n", fillLen)
		for i := 0; i < fillLen && i < 20; i++ {
			elemOff := makerFillAmountsOffset + 32 + i*32
			if elemOff+32 <= len(dataBytes) {
				amt := new(big.Int).SetBytes(dataBytes[elemOff : elemOff+32])
				fmt.Printf("  makerFillAmounts[%d]: %.6f shares\n", i, toFloat(amt))
			}
		}
	}
}

func parseOrder(data []byte) Order {
	if len(data) < 384 {
		return Order{}
	}

	return Order{
		Salt:          new(big.Int).SetBytes(data[0:32]),
		Maker:         fmt.Sprintf("0x%x", data[44:64]),
		Signer:        fmt.Sprintf("0x%x", data[76:96]),
		Taker:         fmt.Sprintf("0x%x", data[108:128]),
		TokenID:       new(big.Int).SetBytes(data[128:160]),
		MakerAmount:   new(big.Int).SetBytes(data[160:192]),
		TakerAmount:   new(big.Int).SetBytes(data[192:224]),
		Expiration:    new(big.Int).SetBytes(data[224:256]).Uint64(),
		Nonce:         new(big.Int).SetBytes(data[256:288]),
		FeeRateBps:    new(big.Int).SetBytes(data[288:320]).Uint64(),
		Side:          data[351], // Last byte of side field
		SignatureType: data[383], // Last byte of signatureType field
	}
}

func printOrder(o Order) {
	side := "BUY"
	if o.Side == 1 {
		side = "SELL"
	}
	fmt.Printf("  Maker: %s\n", o.Maker)
	fmt.Printf("  Signer: %s\n", o.Signer)
	fmt.Printf("  Taker: %s\n", o.Taker)
	fmt.Printf("  TokenID: %s\n", o.TokenID.String())
	fmt.Printf("  MakerAmount: %s (%.6f)\n", o.MakerAmount.String(), toFloat(o.MakerAmount))
	fmt.Printf("  TakerAmount: %s (%.6f)\n", o.TakerAmount.String(), toFloat(o.TakerAmount))
	fmt.Printf("  Side: %s (%d)\n", side, o.Side)
	fmt.Printf("  FeeRateBps: %d\n", o.FeeRateBps)
}

func isAddress(word []byte) bool {
	if len(word) != 32 {
		return false
	}
	for i := 0; i < 12; i++ {
		if word[i] != 0 {
			return false
		}
	}
	nonZero := 0
	for i := 12; i < 32; i++ {
		if word[i] != 0 {
			nonZero++
		}
	}
	return nonZero >= 3
}

func toFloat(n *big.Int) float64 {
	f := new(big.Float).SetInt(n)
	f = f.Quo(f, big.NewFloat(1e6))
	result, _ := f.Float64()
	return result
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
