package api

import (
	"testing"
)

// TestOutcomeNormalization ensures we DON'T normalize outcomes
// This was a critical bug where SELL No was converted to BUY Yes
func TestOutcomeNormalizationDisabled(t *testing.T) {
	tests := []struct {
		name           string
		inputSide      string
		inputOutcome   string
		inputPrice     float64
		wantSide       string
		wantOutcome    string
		wantPriceRange [2]float64 // min, max for floating point comparison
	}{
		{
			name:           "SELL No should stay SELL No - NOT become BUY Yes",
			inputSide:      "SELL",
			inputOutcome:   "No",
			inputPrice:     0.91,
			wantSide:       "SELL",
			wantOutcome:    "No",
			wantPriceRange: [2]float64{0.90, 0.92},
		},
		{
			name:           "BUY No should stay BUY No - NOT become SELL Yes",
			inputSide:      "BUY",
			inputOutcome:   "No",
			inputPrice:     0.10,
			wantSide:       "BUY",
			wantOutcome:    "No",
			wantPriceRange: [2]float64{0.09, 0.11},
		},
		{
			name:           "SELL Yes should stay SELL Yes",
			inputSide:      "SELL",
			inputOutcome:   "Yes",
			inputPrice:     0.80,
			wantSide:       "SELL",
			wantOutcome:    "Yes",
			wantPriceRange: [2]float64{0.79, 0.81},
		},
		{
			name:           "BUY Yes should stay BUY Yes",
			inputSide:      "BUY",
			inputOutcome:   "Yes",
			inputPrice:     0.50,
			wantSide:       "BUY",
			wantOutcome:    "Yes",
			wantPriceRange: [2]float64{0.49, 0.51},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock order filled event
			event := &OrderFilledEvent{
				ID:               "test-id",
				Maker:            "0x1234",
				Taker:            "0x5678",
				MakerAssetID:     "0", // USDC
				TakerAssetID:     "123456789",
				MakerAmountFilled: "1000000", // 1 USDC
				TakerAmountFilled: "2000000", // 2 tokens
				Timestamp:        "1700000000",
				TransactionHash:  "0xabc",
			}

			// Set up based on side
			if tt.inputSide == "SELL" {
				// For SELL, maker gives tokens, taker gives USDC
				event.MakerAssetID = "123456789"
				event.TakerAssetID = "0"
			}

			// Create token map with the outcome
			tokenMap := map[string]TokenInfo{
				"123456789": {
					TokenID:     "123456789",
					ConditionID: "cond-123",
					Outcome:     tt.inputOutcome,
					Title:       "Test Market",
					Slug:        "test-market",
				},
			}

			// Convert the event
			result := event.ConvertToDataTradeWithInfo(tokenMap, "0x1234")

			// Verify outcome is NOT normalized
			if result.Outcome != tt.wantOutcome {
				t.Errorf("Outcome = %q, want %q (outcome should NOT be normalized)", result.Outcome, tt.wantOutcome)
			}

			// Verify side is NOT flipped
			if result.Side != tt.wantSide {
				t.Errorf("Side = %q, want %q (side should NOT be flipped)", result.Side, tt.wantSide)
			}
		})
	}
}

// TestIsComplementOutcome tests the helper function
func TestIsComplementOutcome(t *testing.T) {
	tests := []struct {
		outcome string
		want    bool
	}{
		{"No", true},
		{"no", true},
		{"NO", true},
		{"  No  ", true},
		{"Yes", false},
		{"yes", false},
		{"Trump", false},
		{"Biden", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.outcome, func(t *testing.T) {
			got := isComplementOutcome(tt.outcome)
			if got != tt.want {
				t.Errorf("isComplementOutcome(%q) = %v, want %v", tt.outcome, got, tt.want)
			}
		})
	}
}

// TestGetComplementOutcome tests the complement function
func TestGetComplementOutcome(t *testing.T) {
	tests := []struct {
		outcome string
		want    string
	}{
		{"No", "Yes"},
		{"no", "Yes"},
		{"Yes", "No"},
		{"yes", "No"},
		{"Trump", "Trump"},   // Non-binary stays same
		{"Biden", "Biden"},
	}

	for _, tt := range tests {
		t.Run(tt.outcome, func(t *testing.T) {
			got := getComplementOutcome(tt.outcome)
			if got != tt.want {
				t.Errorf("getComplementOutcome(%q) = %q, want %q", tt.outcome, got, tt.want)
			}
		})
	}
}

// TestConvertToDataTradeForUser tests basic trade conversion
func TestConvertToDataTradeForUser(t *testing.T) {
	tests := []struct {
		name        string
		event       OrderFilledEvent
		userAddress string
		wantSide    string
		wantIsMaker bool
	}{
		{
			name: "user is maker buying",
			event: OrderFilledEvent{
				ID:               "test-1",
				Maker:            "0xuser",
				Taker:            "0xother",
				MakerAssetID:     "0",
				TakerAssetID:     "token123",
				MakerAmountFilled: "1000000",
				TakerAmountFilled: "2000000",
				Timestamp:        "1700000000",
				TransactionHash:  "0xabc",
			},
			userAddress: "0xuser",
			wantSide:    "BUY",
			wantIsMaker: true,
		},
		{
			name: "user is maker selling",
			event: OrderFilledEvent{
				ID:               "test-2",
				Maker:            "0xuser",
				Taker:            "0xother",
				MakerAssetID:     "token123",
				TakerAssetID:     "0",
				MakerAmountFilled: "2000000",
				TakerAmountFilled: "1000000",
				Timestamp:        "1700000000",
				TransactionHash:  "0xdef",
			},
			userAddress: "0xuser",
			wantSide:    "SELL",
			wantIsMaker: true,
		},
		{
			name: "user is taker buying",
			event: OrderFilledEvent{
				ID:               "test-3",
				Maker:            "0xother",
				Taker:            "0xuser",
				MakerAssetID:     "token123",
				TakerAssetID:     "0",
				MakerAmountFilled: "2000000",
				TakerAmountFilled: "1000000",
				Timestamp:        "1700000000",
				TransactionHash:  "0xghi",
			},
			userAddress: "0xuser",
			wantSide:    "BUY",
			wantIsMaker: false,
		},
		{
			name: "user is taker selling",
			event: OrderFilledEvent{
				ID:               "test-4",
				Maker:            "0xother",
				Taker:            "0xuser",
				MakerAssetID:     "0",
				TakerAssetID:     "token123",
				MakerAmountFilled: "1000000",
				TakerAmountFilled: "2000000",
				Timestamp:        "1700000000",
				TransactionHash:  "0xjkl",
			},
			userAddress: "0xuser",
			wantSide:    "SELL",
			wantIsMaker: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.event.ConvertToDataTradeForUser(tt.userAddress)

			if result.Side != tt.wantSide {
				t.Errorf("Side = %q, want %q", result.Side, tt.wantSide)
			}
			if result.IsMaker != tt.wantIsMaker {
				t.Errorf("IsMaker = %v, want %v", result.IsMaker, tt.wantIsMaker)
			}
		})
	}
}

// TestTokenMapEnrichment tests that token info is properly added to trades
func TestTokenMapEnrichment(t *testing.T) {
	event := &OrderFilledEvent{
		ID:               "test-id",
		Maker:            "0xuser",
		Taker:            "0xother",
		MakerAssetID:     "0",
		TakerAssetID:     "123456789",
		MakerAmountFilled: "1000000",
		TakerAmountFilled: "2000000",
		Timestamp:        "1700000000",
		TransactionHash:  "0xabc",
	}

	tokenMap := map[string]TokenInfo{
		"123456789": {
			TokenID:     "123456789",
			ConditionID: "condition-abc",
			Outcome:     "Yes",
			Title:       "Will X happen?",
			Slug:        "will-x-happen",
			EventSlug:   "event-2024",
		},
	}

	result := event.ConvertToDataTradeWithInfo(tokenMap, "0xuser")

	if result.ConditionID != "condition-abc" {
		t.Errorf("ConditionID = %q, want %q", result.ConditionID, "condition-abc")
	}
	if result.Title != "Will X happen?" {
		t.Errorf("Title = %q, want %q", result.Title, "Will X happen?")
	}
	if result.Slug != "will-x-happen" {
		t.Errorf("Slug = %q, want %q", result.Slug, "will-x-happen")
	}
	if result.Outcome != "Yes" {
		t.Errorf("Outcome = %q, want %q", result.Outcome, "Yes")
	}
}

// TestMissingTokenInfo tests behavior when token is not in map
func TestMissingTokenInfo(t *testing.T) {
	event := &OrderFilledEvent{
		ID:               "test-id",
		Maker:            "0xuser",
		Taker:            "0xother",
		MakerAssetID:     "0",
		TakerAssetID:     "unknown-token",
		MakerAmountFilled: "1000000",
		TakerAmountFilled: "2000000",
		Timestamp:        "1700000000",
		TransactionHash:  "0xabc",
	}

	// Empty token map - token not found
	tokenMap := map[string]TokenInfo{}

	result := event.ConvertToDataTradeWithInfo(tokenMap, "0xuser")

	// Should still work, just without enrichment
	if result.Side != "BUY" {
		t.Errorf("Side = %q, want BUY", result.Side)
	}
	if result.Outcome != "" {
		t.Errorf("Outcome should be empty when token not in map, got %q", result.Outcome)
	}
	if result.Title != "" {
		t.Errorf("Title should be empty when token not in map, got %q", result.Title)
	}
}
