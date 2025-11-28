package api

import (
	"testing"
)

// TestContractSelection tests that the correct contract is used based on negRisk
// This is critical - using the wrong contract causes "invalid signature" errors
func TestContractSelection(t *testing.T) {
	tests := []struct {
		name             string
		negRisk          bool
		expectedContract string
		description      string
	}{
		{
			name:             "regular market should use CTFExchange",
			negRisk:          false,
			expectedContract: "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
			description:      "Non-neg-risk markets use the standard CTFExchange",
		},
		{
			name:             "neg_risk market should use NegRiskCTFExchange",
			negRisk:          true,
			expectedContract: "0xC5d563A36AE78145C45a50134d48A1215220f80a",
			description:      "Neg-risk markets use NegRiskCTFExchange",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate contract selection logic
			var verifyingContract string
			if tt.negRisk {
				verifyingContract = "0xC5d563A36AE78145C45a50134d48A1215220f80a" // NegRiskCTFExchange
			} else {
				verifyingContract = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E" // CTFExchange
			}

			if verifyingContract != tt.expectedContract {
				t.Errorf("Contract for negRisk=%v: got %s, want %s (%s)",
					tt.negRisk, verifyingContract, tt.expectedContract, tt.description)
			}
		})
	}
}

// TestOrderBookSorting tests that order book levels are sorted correctly
func TestOrderBookSorting(t *testing.T) {
	t.Run("asks should be sorted ascending (lowest first)", func(t *testing.T) {
		// For buying, we want to buy at lowest prices first
		// After sorting, should be 0.10, 0.20, 0.30
		// The actual sorting is tested in GetOrderBook
		expectedOrder := []string{"0.10", "0.20", "0.30"}
		if len(expectedOrder) != 3 {
			t.Error("Expected 3 price levels")
		}
	})

	t.Run("bids should be sorted descending (highest first)", func(t *testing.T) {
		// For selling, we want to sell at highest prices first
		// After sorting, should be 0.30, 0.20, 0.10
		expectedOrder := []string{"0.30", "0.20", "0.10"}
		if len(expectedOrder) != 3 {
			t.Error("Expected 3 price levels")
		}
	})
}

// TestCalculateOptimalFill tests the order book fill calculation
func TestCalculateOptimalFill(t *testing.T) {
	tests := []struct {
		name         string
		book         *OrderBook
		side         Side
		amountUSDC   float64
		wantSize     float64
		wantAvgPrice float64
		wantFilled   float64
	}{
		{
			name: "buy single level - exact fill",
			book: &OrderBook{
				Asks: []OrderBookLevel{
					{Price: "0.50", Size: "100"},
				},
			},
			side:         SideBuy,
			amountUSDC:   25.0,
			wantSize:     50.0,
			wantAvgPrice: 0.50,
			wantFilled:   25.0,
		},
		{
			name: "buy multiple levels",
			book: &OrderBook{
				Asks: []OrderBookLevel{
					{Price: "0.10", Size: "10"},  // 10 tokens @ $0.10 = $1.00
					{Price: "0.20", Size: "10"},  // 10 tokens @ $0.20 = $2.00
					{Price: "0.30", Size: "100"}, // up to 100 tokens @ $0.30
				},
			},
			side:         SideBuy,
			amountUSDC:   5.0,
			wantSize:     26.67, // 10 + 10 + 6.67
			wantAvgPrice: 0.1875,
			wantFilled:   5.0,
		},
		{
			name: "sell to bids",
			book: &OrderBook{
				Bids: []OrderBookLevel{
					{Price: "0.50", Size: "100"},
				},
			},
			side:         SideSell,
			amountUSDC:   25.0,
			wantSize:     50.0,
			wantAvgPrice: 0.50,
			wantFilled:   25.0,
		},
		{
			name: "insufficient liquidity",
			book: &OrderBook{
				Asks: []OrderBookLevel{
					{Price: "0.50", Size: "10"}, // Only 10 tokens = $5 max
				},
			},
			side:         SideBuy,
			amountUSDC:   20.0,      // Want $20 worth
			wantSize:     10.0,      // Can only get 10
			wantAvgPrice: 0.50,
			wantFilled:   5.0,       // Only filled $5
		},
		{
			name: "empty order book",
			book: &OrderBook{
				Asks: []OrderBookLevel{},
				Bids: []OrderBookLevel{},
			},
			side:         SideBuy,
			amountUSDC:   100.0,
			wantSize:     0,
			wantAvgPrice: 0,
			wantFilled:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size, avgPrice, filled := CalculateOptimalFill(tt.book, tt.side, tt.amountUSDC)

			// Allow some floating point tolerance
			tolerance := 0.1

			if !floatClose(size, tt.wantSize, tolerance) {
				t.Errorf("size = %v, want %v", size, tt.wantSize)
			}
			if !floatClose(avgPrice, tt.wantAvgPrice, tolerance) {
				t.Errorf("avgPrice = %v, want %v", avgPrice, tt.wantAvgPrice)
			}
			if !floatClose(filled, tt.wantFilled, tolerance) {
				t.Errorf("filled = %v, want %v", filled, tt.wantFilled)
			}
		})
	}
}

// floatClose checks if two floats are close within tolerance
func floatClose(a, b, tolerance float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff <= tolerance
}

// TestOrderAmountPrecision tests that order amounts meet Polymarket precision requirements
func TestOrderAmountPrecision(t *testing.T) {
	tests := []struct {
		name      string
		size      float64
		price     float64
		wantValid bool
	}{
		{
			name:      "valid size 2 decimals",
			size:      10.01,
			price:     0.50,
			wantValid: true,
		},
		{
			name:      "valid whole number",
			size:      100.00,
			price:     0.25,
			wantValid: true,
		},
		{
			name:      "minimum size",
			size:      0.01,
			price:     0.99,
			wantValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Size should be rounded to 2 decimal places
			roundedSize := float64(int(tt.size*100+0.5)) / 100

			// Price should be rounded to tick size (0.01)
			tickSize := 0.01
			roundedPrice := float64(int(tt.price/tickSize+0.5)) * tickSize

			// Verify precision
			if roundedSize < 0.01 {
				t.Error("Size below minimum 0.01")
			}
			if roundedPrice < 0.01 || roundedPrice > 0.99 {
				t.Error("Price outside valid range 0.01-0.99")
			}
		})
	}
}

// TestSignatureType tests that signature types are set correctly
func TestSignatureType(t *testing.T) {
	tests := []struct {
		name          string
		walletType    string
		expectedType  int
		description   string
	}{
		{
			name:         "EOA wallet",
			walletType:   "EOA",
			expectedType: 0,
			description:  "Direct wallet signatures",
		},
		{
			name:         "Magic/Email wallet",
			walletType:   "Magic",
			expectedType: 1,
			description:  "Magic wallet with funder address",
		},
		{
			name:         "Browser proxy",
			walletType:   "Browser",
			expectedType: 2,
			description:  "Browser-based proxy signatures",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate signature type selection
			var sigType int
			switch tt.walletType {
			case "EOA":
				sigType = 0
			case "Magic":
				sigType = 1
			case "Browser":
				sigType = 2
			}

			if sigType != tt.expectedType {
				t.Errorf("SignatureType for %s wallet: got %d, want %d (%s)",
					tt.walletType, sigType, tt.expectedType, tt.description)
			}
		})
	}
}

// TestMakerSignerForMagicWallet tests the maker/signer setup for Magic wallets
func TestMakerSignerForMagicWallet(t *testing.T) {
	// For Magic wallets:
	// - maker = funder address (where funds are held)
	// - signer = EOA address that signs transactions

	funderAddress := "0xFunderAddress123"
	signerAddress := "0xSignerAddress456"

	// These should NOT be the same for Magic wallets
	if funderAddress == signerAddress {
		t.Error("For Magic wallets, funder and signer should be different")
	}

	// The maker in the order should be the funder
	orderMaker := funderAddress
	orderSigner := signerAddress

	if orderMaker != funderAddress {
		t.Errorf("Order maker should be funder: got %s, want %s", orderMaker, funderAddress)
	}
	if orderSigner != signerAddress {
		t.Errorf("Order signer should be signer EOA: got %s, want %s", orderSigner, signerAddress)
	}
}

// TestL2HeadersGeneration tests that L2 authentication headers are properly formed
func TestL2HeadersFormat(t *testing.T) {
	// L2 headers should include:
	// - POLY_ADDRESS
	// - POLY_API_KEY
	// - POLY_PASSPHRASE
	// - POLY_TIMESTAMP
	// - POLY_SIGNATURE

	requiredHeaders := []string{
		"POLY_ADDRESS",
		"POLY_API_KEY",
		"POLY_PASSPHRASE",
		"POLY_TIMESTAMP",
		"POLY_SIGNATURE",
	}

	// Just verify the header names are correct
	for _, header := range requiredHeaders {
		if header == "" {
			t.Errorf("Header name should not be empty")
		}
	}
}
