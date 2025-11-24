package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load()

	// Use exact Python values for comparison
	salt := int64(51367151) // From Python output
	maker := "0xffe81fbf9b7bAbb9B6914B83f82F1931735c6feA"
	signer := "0x78138a9Db875d1cA98039d29B3E8480f09572411"
	taker := "0x0000000000000000000000000000000000000000"
	tokenID := "87769991026114894163580777793845523168226980076553814689875238288185044414090"
	makerAmount := "1000000"
	takerAmount := "1538400"
	expiration := "0"
	nonce := "0"
	feeRateBps := "0"
	side := 0            // BUY
	signatureType := 1   // POLY_GNOSIS_SAFE

	verifyingContract := "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
	chainID := int64(137)

	fmt.Println("Test values:")
	fmt.Printf("  salt: %d\n", salt)
	fmt.Printf("  maker: %s\n", maker)
	fmt.Printf("  signer: %s\n", signer)
	fmt.Printf("  tokenID: %s\n", tokenID)
	fmt.Printf("  makerAmount: %s\n", makerAmount)
	fmt.Printf("  takerAmount: %s\n", takerAmount)
	fmt.Printf("  side: %d\n", side)
	fmt.Printf("  signatureType: %d\n", signatureType)

	// Convert to big.Int
	saltBig := big.NewInt(salt)
	tokenIdBig := new(big.Int)
	tokenIdBig.SetString(tokenID, 10)
	makerAmountBig := new(big.Int)
	makerAmountBig.SetString(makerAmount, 10)
	takerAmountBig := new(big.Int)
	takerAmountBig.SetString(takerAmount, 10)
	expirationBig := new(big.Int)
	expirationBig.SetString(expiration, 10)
	nonceBig := new(big.Int)
	nonceBig.SetString(nonce, 10)
	feeRateBpsBig := new(big.Int)
	feeRateBpsBig.SetString(feeRateBps, 10)

	chainIDBig := math.NewHexOrDecimal256(chainID)
	domain := apitypes.TypedDataDomain{
		Name:              "Polymarket CTF Exchange",
		Version:           "1",
		ChainId:           chainIDBig,
		VerifyingContract: verifyingContract,
	}

	message := map[string]interface{}{
		"salt":          saltBig,
		"maker":         common.HexToAddress(maker).Hex(),
		"signer":        common.HexToAddress(signer).Hex(),
		"taker":         common.HexToAddress(taker).Hex(),
		"tokenId":       tokenIdBig,
		"makerAmount":   makerAmountBig,
		"takerAmount":   takerAmountBig,
		"expiration":    expirationBig,
		"nonce":         nonceBig,
		"feeRateBps":    feeRateBpsBig,
		"side":          big.NewInt(int64(side)),
		"signatureType": big.NewInt(int64(signatureType)),
	}

	typedData := apitypes.TypedData{
		Types: apitypes.Types{
			"EIP712Domain": []apitypes.Type{
				{Name: "name", Type: "string"},
				{Name: "version", Type: "string"},
				{Name: "chainId", Type: "uint256"},
				{Name: "verifyingContract", Type: "address"},
			},
			"Order": []apitypes.Type{
				{Name: "salt", Type: "uint256"},
				{Name: "maker", Type: "address"},
				{Name: "signer", Type: "address"},
				{Name: "taker", Type: "address"},
				{Name: "tokenId", Type: "uint256"},
				{Name: "makerAmount", Type: "uint256"},
				{Name: "takerAmount", Type: "uint256"},
				{Name: "expiration", Type: "uint256"},
				{Name: "nonce", Type: "uint256"},
				{Name: "feeRateBps", Type: "uint256"},
				{Name: "side", Type: "uint8"},
				{Name: "signatureType", Type: "uint8"},
			},
		},
		PrimaryType: "Order",
		Domain:      domain,
		Message:     message,
	}

	hash, _, err := apitypes.TypedDataAndHash(typedData)
	if err != nil {
		log.Fatalf("Failed to hash: %v", err)
	}

	fmt.Printf("\nGo EIP-712 hash: 0x%x\n", hash)

	// Sign it
	privateKey := os.Getenv("POLYMARKET_PRIVATE_KEY")
	if privateKey == "" {
		log.Fatal("POLYMARKET_PRIVATE_KEY not set")
	}

	privKey, err := crypto.HexToECDSA(privateKey[2:]) // Remove 0x prefix
	if err != nil {
		log.Fatalf("Invalid private key: %v", err)
	}

	signature, err := crypto.Sign(hash, privKey)
	if err != nil {
		log.Fatalf("Failed to sign: %v", err)
	}
	signature[64] += 27

	fmt.Printf("Go signature: 0x%s\n", hex.EncodeToString(signature))
	fmt.Println("\nExpected Python hash: 0x98196149f5cb6e24a36195ac365c284040fd27ec0f9feaf83843726dc4974e41")
	fmt.Println("Expected Python sig:  0x6ce54f2763b5c1f358dcf062f925ead809c61391b1932cb94746c1658e316ff0089159e6823291293922b96cd4fedca0b55a0f2624b647b64f83176ddf7d958a1b")
}
