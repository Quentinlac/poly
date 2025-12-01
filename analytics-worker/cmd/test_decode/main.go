// Benchmark ABI decoding speed
package main

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// Sample CTF Exchange input data (from a real transaction)
const sampleInput = "0xe20b23040000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000674c8e2a00000000000000000000000005c1882212a41aa8d7df5b70eebe03d9319345b700000000000000000000000005c1882212a41aa8d7df5b70eebe03d9319345b7000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006f9fd3f94f03c16d5f19000000000000000000000000000000000000000000000000000000000000000000000000000de0b6b3a76400000000000000000000000000000000000000000000000000000000000067537a2a000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"

func decodeOrder(input string) (side uint8, tokenID *big.Int, makerAmount *big.Int, takerAmount *big.Int, err error) {
	// Remove 0x prefix and function selector (first 4 bytes = 8 hex chars)
	data := strings.TrimPrefix(input, "0x")
	if len(data) < 8 {
		return 0, nil, nil, nil, fmt.Errorf("input too short")
	}
	data = data[8:] // Skip function selector

	// Decode hex to bytes
	bytes, err := hex.DecodeString(data)
	if err != nil {
		return 0, nil, nil, nil, err
	}

	// Order struct layout (each field is 32 bytes):
	// [0-31]   offset to Order struct
	// [32-63]  fillAmount
	// [64-95]  salt
	// [96-127] maker
	// [128-159] signer
	// [160-191] taker
	// [192-223] tokenId
	// [224-255] makerAmount
	// [256-287] takerAmount
	// [288-319] expiration
	// [320-351] nonce
	// [352-383] feeRateBps
	// [384-415] side
	// [416-447] signatureType
	// [448+]    signature

	if len(bytes) < 416 {
		return 0, nil, nil, nil, fmt.Errorf("data too short: %d bytes", len(bytes))
	}

	// Skip offset (32 bytes) + fillAmount (32 bytes) = start at 64
	offset := 64

	// TokenId at offset + 128 (after salt, maker, signer, taker)
	tokenID = new(big.Int).SetBytes(bytes[offset+128 : offset+160])

	// MakerAmount at offset + 160
	makerAmount = new(big.Int).SetBytes(bytes[offset+160 : offset+192])

	// TakerAmount at offset + 192
	takerAmount = new(big.Int).SetBytes(bytes[offset+192 : offset+224])

	// Side at offset + 320
	side = bytes[offset+320+31] // Last byte of 32-byte field

	return side, tokenID, makerAmount, takerAmount, nil
}

func main() {
	// Warm up
	for i := 0; i < 100; i++ {
		decodeOrder(sampleInput)
	}

	// Benchmark
	iterations := 100000
	start := time.Now()

	for i := 0; i < iterations; i++ {
		side, tokenID, makerAmount, takerAmount, err := decodeOrder(sampleInput)
		if err != nil && i == 0 {
			fmt.Printf("Error: %v\n", err)
		}
		_ = side
		_ = tokenID
		_ = makerAmount
		_ = takerAmount
	}

	elapsed := time.Since(start)
	perOp := elapsed / time.Duration(iterations)

	fmt.Printf("Decoded %d orders in %v\n", iterations, elapsed)
	fmt.Printf("Time per decode: %v\n", perOp)
	fmt.Printf("Decodes per second: %.0f\n", float64(iterations)/elapsed.Seconds())

	// Show one decoded result
	side, tokenID, makerAmount, takerAmount, _ := decodeOrder(sampleInput)
	fmt.Printf("\nSample decode:\n")
	fmt.Printf("  Side: %d (0=BUY, 1=SELL)\n", side)
	fmt.Printf("  TokenID: %s\n", tokenID.String())
	fmt.Printf("  MakerAmount: %s\n", makerAmount.String())
	fmt.Printf("  TakerAmount: %s\n", takerAmount.String())
}
