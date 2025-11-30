package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"polymarket-analyzer/api"
)

func main() {
	ctx := context.Background()

	// Get wallet address from env or use funder address
	walletAddr := os.Getenv("POLYMARKET_FUNDER_ADDRESS")
	if walletAddr == "" {
		walletAddr = "0x0Fd419C486d1F53c92CD09c3C52eCA61d6367750" // Default funder
	}

	// Method 1: On-chain balance (works for any address, no private key needed)
	fmt.Printf("Checking on-chain USDC balance for: %s\n\n", walletAddr)

	onChainBalance, err := api.GetOnChainUSDCBalance(ctx, walletAddr)
	if err != nil {
		log.Printf("Error getting on-chain balance: %v", err)
	} else {
		fmt.Printf("On-Chain USDC Balance: $%.2f\n\n", onChainBalance)
	}

	// Method 2: CLOB API balance (requires private key for the wallet)
	if os.Getenv("POLYMARKET_PRIVATE_KEY") != "" {
		auth, err := api.NewAuth()
		if err != nil {
			log.Fatalf("Failed to create auth: %v", err)
		}

		clobClient, err := api.NewClobClient("", auth)
		if err != nil {
			log.Fatalf("Failed to create clob client: %v", err)
		}

		// Derive API credentials
		creds, err := clobClient.DeriveAPICreds(ctx)
		if err != nil {
			log.Fatalf("Failed to derive API credentials: %v", err)
		}
		_ = creds

		fmt.Printf("CLOB API Balance (for %s):\n", auth.GetAddress().Hex())

		balance, err := clobClient.GetUSDCBalance(ctx)
		if err != nil {
			log.Printf("Error getting CLOB balance: %v", err)
		} else {
			fmt.Printf("  USDC Balance: $%.2f\n", balance)
		}

		ba, err := clobClient.GetBalanceAllowance(ctx, api.AssetTypeCollateral, "")
		if err != nil {
			log.Printf("Error getting balance allowance: %v", err)
		} else {
			fmt.Printf("  Raw Balance: %s\n", ba.Balance)
			fmt.Printf("  Raw Allowance: %s\n", ba.Allowance)
		}
	} else {
		fmt.Println("POLYMARKET_PRIVATE_KEY not set - skipping CLOB API balance check")
	}
}
