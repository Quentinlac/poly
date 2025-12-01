// Test decoding a real Polymarket transaction
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"polymarket-analyzer/api"
)

func main() {
	// Real transaction from djangodja
	txHash := "0xd44859fecd04b2fea6a4d97d68eec925641d4a407aa815947afa833031fee2b3"

	// Fetch the transaction
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_getTransactionByHash",
		"params":  []string{txHash},
		"id":      1,
	}
	jsonBody, _ := json.Marshal(reqBody)

	resp, err := http.Post("https://polygon-rpc.com", "application/json", strings.NewReader(string(jsonBody)))
	if err != nil {
		log.Fatalf("Failed to fetch tx: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var result struct {
		Result struct {
			From  string `json:"from"`
			To    string `json:"to"`
			Input string `json:"input"`
		} `json:"result"`
	}
	json.Unmarshal(body, &result)

	fmt.Printf("TX: %s\n", txHash)
	fmt.Printf("From: %s\n", result.Result.From)
	fmt.Printf("To: %s\n", result.Result.To)
	fmt.Printf("Input length: %d chars\n", len(result.Result.Input))
	fmt.Printf("Function selector: %s\n", result.Result.Input[:10])
	fmt.Println()

	// Try to decode
	decoded, side, tokenID, makerAmt, takerAmt := api.DecodeTradeInput(result.Result.Input)

	fmt.Printf("Decoded: %v\n", decoded)
	if decoded {
		fmt.Printf("Side: %s\n", side)
		fmt.Printf("TokenID: %s\n", tokenID)
		fmt.Printf("MakerAmount: %s\n", makerAmt.String())
		fmt.Printf("TakerAmount: %s\n", takerAmt.String())

		// Calculate price
		makerF := float64(makerAmt.Int64())
		takerF := float64(takerAmt.Int64())
		if makerF > 0 {
			fmt.Printf("Price: %.4f\n", takerF/makerF)
			fmt.Printf("Size (tokens): %.4f\n", makerF/1e6)
			fmt.Printf("USDC value: %.4f\n", takerF/1e6)
		}
	} else {
		fmt.Println("Could not decode trade details")
	}
}
