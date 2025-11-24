package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"polymarket-analyzer/api"

	"github.com/joho/godotenv"
)

func main() {
	// Load .env
	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: no .env file found")
	}

	// Check private key
	privateKey := os.Getenv("POLYMARKET_PRIVATE_KEY")
	if privateKey == "" {
		log.Fatal("POLYMARKET_PRIVATE_KEY not set")
	}
	// Private key loaded successfully (not logging for security)

	// Create auth
	auth, err := api.NewAuth()
	if err != nil {
		log.Fatalf("Failed to create auth: %v", err)
	}
	log.Printf("Auth created successfully")

	// Create CLOB client
	clobClient, err := api.NewClobClient("", auth)
	if err != nil {
		log.Fatalf("Failed to create CLOB client: %v", err)
	}

	// Configure for Magic/Email wallet if funder address is set
	funderAddress := os.Getenv("POLYMARKET_FUNDER_ADDRESS")
	if funderAddress != "" {
		clobClient.SetFunder(funderAddress)
		clobClient.SetSignatureType(1) // 1 = POLY_PROXY (Magic/Email wallet)
		log.Printf("Configured for Magic wallet: funder=%s", funderAddress)
	} else {
		log.Printf("Using EOA wallet (no funder address set)")
	}
	log.Printf("CLOB client created")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Derive API credentials
	log.Printf("Deriving API credentials...")
	creds, err := clobClient.DeriveAPICreds(ctx)
	if err != nil {
		log.Fatalf("Failed to derive API creds: %v", err)
	}
	log.Printf("API credentials derived successfully!")
	log.Printf("  API Key: %s...%s", creds.APIKey[:8], creds.APIKey[len(creds.APIKey)-4:])

	// Try markets from your recent trades
	testMarkets := []struct {
		tokenID string
		name    string
		negRisk bool
	}{
		{"87769991026114894163580777793845523168226980076553814689875238288185044414090", "Fed decreases interest rates by 25 bps after December 2025", false},
		{"18526421270059973400497927952164445523116068073465504942214949029756406262889", "Trump agrees to sell F-35 to Saudi Arabia by November 30", false},
	}

	var testTokenID string
	var testMarket string
	var negRisk bool

	for _, tm := range testMarkets {
		book, err := clobClient.GetOrderBook(ctx, tm.tokenID)
		if err != nil {
			log.Printf("Skip %s: %v", tm.name, err)
			continue
		}
		if len(book.Asks) == 0 || len(book.Bids) == 0 {
			log.Printf("Skip %s: no liquidity (asks=%d, bids=%d)", tm.name, len(book.Asks), len(book.Bids))
			continue
		}

		testTokenID = tm.tokenID
		testMarket = tm.name
		negRisk = tm.negRisk

		log.Printf("Found active market: %s", testMarket)
		log.Printf("  Token ID: %s", testTokenID)
		log.Printf("  NegRisk: %v", negRisk)
		log.Printf("  Order book: %d bids, %d asks", len(book.Bids), len(book.Asks))
		log.Printf("  Best ask: %s @ %s", book.Asks[0].Size, book.Asks[0].Price)
		log.Printf("  Best bid: %s @ %s", book.Bids[0].Size, book.Bids[0].Price)
		break
	}

	if testTokenID == "" {
		log.Fatal("Could not find any test market with liquidity")
	}

	// Ask user before placing real order
	fmt.Print("\nDo you want to place a $1 test BUY order? (yes/no): ")
	var response string
	fmt.Scanln(&response)

	if response != "yes" {
		log.Println("Skipping test order. Credentials verified successfully!")
		return
	}

	// Place a small test market order
	log.Printf("Placing $1.05 test BUY order for: %s", testMarket)
	resp, err := clobClient.PlaceMarketOrder(ctx, testTokenID, api.SideBuy, 1.05, negRisk)
	if err != nil {
		log.Fatalf("Order failed: %v", err)
	}

	if resp.Success {
		log.Printf("ORDER SUCCESS!")
		log.Printf("  Order ID: %s", resp.OrderID)
		log.Printf("  Status: %s", resp.Status)
	} else {
		log.Printf("ORDER REJECTED: %s", resp.ErrorMsg)
	}
}
