package api

import (
	"math/big"
	"strings"
	"testing"
)

func TestPolygonWSClientFollowedAddresses(t *testing.T) {
	client := NewPolygonWSClient(nil)

	t.Run("SetFollowedAddresses normalizes addresses", func(t *testing.T) {
		addrs := []string{
			"0x05c1882212a41aa8d7df5b70eebe03d9319345b7",
			"0xABCDEF1234567890ABCDEF1234567890ABCDEF12",
			"1234567890abcdef1234567890abcdef12345678", // no 0x prefix
		}

		client.SetFollowedAddresses(addrs)

		// Check that addresses are stored normalized (lowercase, no 0x)
		client.followedAddrsMu.RLock()
		defer client.followedAddrsMu.RUnlock()

		if len(client.followedAddrs) != 3 {
			t.Errorf("expected 3 addresses, got %d", len(client.followedAddrs))
		}

		// All should be lowercase without 0x prefix
		expected := []string{
			"05c1882212a41aa8d7df5b70eebe03d9319345b7",
			"abcdef1234567890abcdef1234567890abcdef12",
			"1234567890abcdef1234567890abcdef12345678",
		}

		for _, addr := range expected {
			if !client.followedAddrs[addr] {
				t.Errorf("expected address %s to be in followedAddrs", addr)
			}
		}
	})

	t.Run("AddFollowedAddress adds single address", func(t *testing.T) {
		client := NewPolygonWSClient(nil)
		client.AddFollowedAddress("0xNEWADDRESS1234567890123456789012345678")

		client.followedAddrsMu.RLock()
		defer client.followedAddrsMu.RUnlock()

		if !client.followedAddrs["newaddress1234567890123456789012345678"] {
			t.Error("expected new address to be added")
		}
	})
}

func TestDecodeOrderFilledEvent(t *testing.T) {
	client := NewPolygonWSClient(nil)

	t.Run("decode valid OrderFilled event", func(t *testing.T) {
		// Real-world example topics from CTF Exchange
		topics := []string{
			"0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6", // OrderFilled signature
			"0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", // orderHash
			"0x00000000000000000000000005c1882212a41aa8d7df5b70eebe03d9319345b7", // maker (padded)
			"0x000000000000000000000000abcdef1234567890abcdef1234567890abcdef12", // taker (padded)
		}

		// Data: makerAssetId (32 bytes) + takerAssetId (32 bytes) + makerAmount + takerAmount + fee
		data := "0x" +
			"96c28e8b5e52d9a5f01f2d9d8f8e5c0d1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d" + // makerAssetId
			"0000000000000000000000000000000000000000000000000000000000000000" + // takerAssetId (USDC = 0)
			"0000000000000000000000000000000000000000000000000de0b6b3a7640000" + // makerAmount (1e18)
			"0000000000000000000000000000000000000000000000000000000005f5e100" + // takerAmount (100000000 = 100 USDC)
			"0000000000000000000000000000000000000000000000000000000000000064"   // fee (100)

		event, err := client.decodeOrderFilledEvent(topics, data, "0xtxhash123", "0x1234")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Check maker address (last 40 chars of topic[2])
		expectedMaker := "0x05c1882212a41aa8d7df5b70eebe03d9319345b7"
		if strings.ToLower(event.Maker) != expectedMaker {
			t.Errorf("maker = %s, want %s", event.Maker, expectedMaker)
		}

		// Check taker address
		expectedTaker := "0xabcdef1234567890abcdef1234567890abcdef12"
		if strings.ToLower(event.Taker) != expectedTaker {
			t.Errorf("taker = %s, want %s", event.Taker, expectedTaker)
		}

		// Check tx hash
		if event.TxHash != "0xtxhash123" {
			t.Errorf("txHash = %s, want 0xtxhash123", event.TxHash)
		}

		// Check block number (0x1234 = 4660)
		if event.BlockNumber != 4660 {
			t.Errorf("blockNumber = %d, want 4660", event.BlockNumber)
		}

		// Check amounts are parsed
		if event.MakerAmount == nil || event.TakerAmount == nil || event.Fee == nil {
			t.Error("expected amounts to be parsed")
		}
	})

	t.Run("insufficient topics returns error", func(t *testing.T) {
		topics := []string{
			"0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6",
			"0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			// Missing maker and taker topics
		}

		_, err := client.decodeOrderFilledEvent(topics, "0x", "0xtx", "0x1")
		if err == nil {
			t.Error("expected error for insufficient topics")
		}
	})
}

func TestAddressMatchingLogic(t *testing.T) {
	tests := []struct {
		name           string
		followedAddrs  []string
		makerAddr      string
		takerAddr      string
		expectMatch    bool
		expectAsMaker  bool
	}{
		{
			name:          "maker is followed",
			followedAddrs: []string{"0x05c1882212a41aa8d7df5b70eebe03d9319345b7"},
			makerAddr:     "0x05c1882212a41aa8d7df5b70eebe03d9319345b7",
			takerAddr:     "0xother",
			expectMatch:   true,
			expectAsMaker: true,
		},
		{
			name:          "taker is followed",
			followedAddrs: []string{"0x05c1882212a41aa8d7df5b70eebe03d9319345b7"},
			makerAddr:     "0xother",
			takerAddr:     "0x05c1882212a41aa8d7df5b70eebe03d9319345b7",
			expectMatch:   true,
			expectAsMaker: false,
		},
		{
			name:          "neither is followed",
			followedAddrs: []string{"0x05c1882212a41aa8d7df5b70eebe03d9319345b7"},
			makerAddr:     "0xother1",
			takerAddr:     "0xother2",
			expectMatch:   false,
		},
		{
			name:          "case insensitive match",
			followedAddrs: []string{"0xABCDEF1234567890123456789012345678901234"},
			makerAddr:     "0xabcdef1234567890123456789012345678901234",
			takerAddr:     "0xother",
			expectMatch:   true,
			expectAsMaker: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewPolygonWSClient(nil)
			client.SetFollowedAddresses(tt.followedAddrs)

			// Simulate the matching logic from handleMessage
			client.followedAddrsMu.RLock()
			makerNorm := strings.ToLower(strings.TrimPrefix(tt.makerAddr, "0x"))
			takerNorm := strings.ToLower(strings.TrimPrefix(tt.takerAddr, "0x"))
			isMakerFollowed := client.followedAddrs[makerNorm]
			isTakerFollowed := client.followedAddrs[takerNorm]
			isFollowed := isMakerFollowed || isTakerFollowed
			client.followedAddrsMu.RUnlock()

			if isFollowed != tt.expectMatch {
				t.Errorf("isFollowed = %v, want %v", isFollowed, tt.expectMatch)
			}

			if tt.expectMatch && tt.expectAsMaker != isMakerFollowed {
				t.Errorf("isMakerFollowed = %v, want %v", isMakerFollowed, tt.expectAsMaker)
			}
		})
	}
}

func TestHexToAddress(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			input:    "0x00000000000000000000000005c1882212a41aa8d7df5b70eebe03d9319345b7",
			expected: "0x05c1882212a41aa8d7df5b70eebe03d9319345b7",
		},
		{
			input:    "0x05c1882212a41aa8d7df5b70eebe03d9319345b7",
			expected: "0x05c1882212a41aa8d7df5b70eebe03d9319345b7",
		},
		{
			input:    "05c1882212a41aa8d7df5b70eebe03d9319345b7",
			expected: "0x05c1882212a41aa8d7df5b70eebe03d9319345b7",
		},
		{
			input:    "", // too short
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := hexToAddress(tt.input)
			if result != tt.expected {
				t.Errorf("hexToAddress(%s) = %s, want %s", tt.input, result, tt.expected)
			}
		})
	}
}

func TestPolygonTradeEventFields(t *testing.T) {
	event := PolygonTradeEvent{
		TxHash:       "0x123abc",
		BlockNumber:  12345678,
		Maker:        "0x05c1882212a41aa8d7df5b70eebe03d9319345b7",
		Taker:        "0xabcdef1234567890abcdef1234567890abcdef12",
		MakerAssetID: "0x1234",
		TakerAssetID: "0x5678",
		MakerAmount:  big.NewInt(1000000000000000000), // 1e18
		TakerAmount:  big.NewInt(100000000),           // 100 USDC (6 decimals)
		Fee:          big.NewInt(100),
	}

	if event.TxHash != "0x123abc" {
		t.Errorf("TxHash = %s, want 0x123abc", event.TxHash)
	}

	if event.BlockNumber != 12345678 {
		t.Errorf("BlockNumber = %d, want 12345678", event.BlockNumber)
	}

	if event.MakerAmount.Cmp(big.NewInt(1000000000000000000)) != 0 {
		t.Errorf("MakerAmount = %s, want 1000000000000000000", event.MakerAmount.String())
	}
}

func TestGetStats(t *testing.T) {
	client := NewPolygonWSClient(nil)

	// Initial stats should be zero
	events, matches := client.GetStats()
	if events != 0 || matches != 0 {
		t.Errorf("initial stats should be 0, got events=%d matches=%d", events, matches)
	}

	// Manually set stats
	client.statsMu.Lock()
	client.eventsReceived = 100
	client.tradesMatched = 5
	client.statsMu.Unlock()

	events, matches = client.GetStats()
	if events != 100 {
		t.Errorf("eventsReceived = %d, want 100", events)
	}
	if matches != 5 {
		t.Errorf("tradesMatched = %d, want 5", matches)
	}
}

func TestConstants(t *testing.T) {
	// Verify critical constants are correct
	if CTFExchangeAddress != "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E" {
		t.Errorf("CTFExchangeAddress = %s, want 0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E", CTFExchangeAddress)
	}

	// OrderFilled event signature should match keccak256("OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)")
	expectedTopic := "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
	if OrderFilledTopic != expectedTopic {
		t.Errorf("OrderFilledTopic = %s, want %s", OrderFilledTopic, expectedTopic)
	}
}
