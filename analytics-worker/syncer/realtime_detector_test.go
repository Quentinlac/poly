package syncer

import (
	"sync"
	"testing"
	"time"

	"polymarket-analyzer/api"
	"polymarket-analyzer/models"
)

func TestNewRealtimeDetector(t *testing.T) {
	t.Run("with blockchain WS enabled", func(t *testing.T) {
		d := NewRealtimeDetector(nil, nil, nil, true, "")

		if d.polygonWS == nil {
			t.Error("polygonWS should be initialized when enableBlockchainWS is true")
		}

		if d.followedUsers == nil {
			t.Error("followedUsers map should be initialized")
		}

		if d.processedTxs == nil {
			t.Error("processedTxs map should be initialized")
		}

		if d.metrics == nil {
			t.Error("metrics should be initialized")
		}
	})

	t.Run("with blockchain WS disabled", func(t *testing.T) {
		d := NewRealtimeDetector(nil, nil, nil, false, "")

		if d.polygonWS != nil {
			t.Error("polygonWS should be nil when enableBlockchainWS is false")
		}
	})
}

func TestHandleBlockchainTrade(t *testing.T) {
	var receivedTrade models.TradeDetail
	var mu sync.Mutex

	callback := func(trade models.TradeDetail) {
		mu.Lock()
		receivedTrade = trade
		mu.Unlock()
	}

	d := NewRealtimeDetector(nil, nil, callback, false, "")

	// Set up followed user (with 0x prefix, lowercase - as NormalizeAddress does)
	d.followedUsersMu.Lock()
	d.followedUsers["0x05c1882212a41aa8d7df5b70eebe03d9319345b7"] = true
	d.followedUsersMu.Unlock()

	t.Run("TAKER trade triggers callback with BUY", func(t *testing.T) {
		txHash := "0x123abc456def789012345678901234567890123456789012345678901234abcd"
		event := api.PolygonTradeEvent{
			TxHash:       txHash,
			BlockNumber:  12345,
			Maker:        "0xother1234567890123456789012345678901234",
			Taker:        "0x05c1882212a41aa8d7df5b70eebe03d9319345b7", // followed user is taker
			MakerAssetID: "0x96c28e8b5e52d9a5f01f2d9d8f8e5c0d1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d", // non-zero token
			TakerAssetID: "0x0000000000000000000000000000000000000000000000000000000000000000", // USDC
			Timestamp:    time.Now(),
		}

		d.handleBlockchainTrade(event)

		mu.Lock()
		defer mu.Unlock()

		if receivedTrade.ID != txHash {
			t.Errorf("ID = %s, want %s", receivedTrade.ID, txHash)
		}

		if receivedTrade.Side != "BUY" {
			t.Errorf("Side = %s, want BUY", receivedTrade.Side)
		}

		if receivedTrade.Role != "TAKER" {
			t.Errorf("Role = %s, want TAKER", receivedTrade.Role)
		}
	})

	t.Run("MAKER trade is skipped", func(t *testing.T) {
		// Reset received trade
		mu.Lock()
		receivedTrade = models.TradeDetail{}
		mu.Unlock()

		// Clear processed txs
		d.processedTxsMu.Lock()
		d.processedTxs = make(map[string]bool)
		d.processedTxsMu.Unlock()

		event := api.PolygonTradeEvent{
			TxHash:       "0xmaker12345678901234567890123456789012345678901234567890123456abcd",
			BlockNumber:  12346,
			Maker:        "0x05c1882212a41aa8D7Df5B70EeBe03d9319345b7", // followed user is maker (mixed case to test normalization)
			Taker:        "0xother1234567890123456789012345678901234",
			MakerAssetID: "0x0000000000000000000000000000000000000000000000000000000000000000",
			TakerAssetID: "0x96c28e8b5e52d9a5f01f2d9d8f8e5c0d1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d",
			Timestamp:    time.Now(),
		}

		d.handleBlockchainTrade(event)

		mu.Lock()
		defer mu.Unlock()

		// MAKER trades are skipped, so callback should not be called
		if receivedTrade.ID == "0xmaker123" {
			t.Error("MAKER trade should be skipped")
		}
	})

	t.Run("duplicate tx is skipped", func(t *testing.T) {
		// Clear and set up
		mu.Lock()
		receivedTrade = models.TradeDetail{}
		mu.Unlock()

		d.processedTxsMu.Lock()
		d.processedTxs = make(map[string]bool)
		d.processedTxsMu.Unlock()

		dupTxHash := "0xduplicate12345678901234567890123456789012345678901234567890abcd"
		event := api.PolygonTradeEvent{
			TxHash:       dupTxHash,
			Maker:        "0xother1234567890123456789012345678901234",
			Taker:        "0x05c1882212a41aa8d7df5b70eebe03d9319345b7",
			MakerAssetID: "0x96c28e8b5e52d9a5f01f2d9d8f8e5c0d1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d",
			TakerAssetID: "0x0000000000000000000000000000000000000000000000000000000000000000",
			Timestamp:    time.Now(),
		}

		// First call
		d.handleBlockchainTrade(event)

		mu.Lock()
		firstID := receivedTrade.ID
		receivedTrade = models.TradeDetail{}
		mu.Unlock()

		// Second call with same tx hash - should be skipped
		d.handleBlockchainTrade(event)

		mu.Lock()
		defer mu.Unlock()

		if firstID != dupTxHash {
			t.Errorf("first call should process the trade, got %s", firstID)
		}

		if receivedTrade.ID == dupTxHash {
			t.Error("duplicate tx should be skipped")
		}
	})

	t.Run("non-followed user is skipped", func(t *testing.T) {
		mu.Lock()
		receivedTrade = models.TradeDetail{}
		mu.Unlock()

		d.processedTxsMu.Lock()
		d.processedTxs = make(map[string]bool)
		d.processedTxsMu.Unlock()

		event := api.PolygonTradeEvent{
			TxHash:       "0xnotfollowed123456789012345678901234567890123456789012345678abcdef",
			Maker:        "0xstranger11234567890123456789012345678901",
			Taker:        "0xstranger21234567890123456789012345678902", // neither is followed
			MakerAssetID: "0x96c28e8b5e52d9a5f01f2d9d8f8e5c0d1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d",
			TakerAssetID: "0x0000000000000000000000000000000000000000000000000000000000000000",
			Timestamp:    time.Now(),
		}

		d.handleBlockchainTrade(event)

		mu.Lock()
		defer mu.Unlock()

		if receivedTrade.ID != "" {
			t.Error("non-followed user trade should be skipped")
		}
	})
}

func TestDetectorMetrics(t *testing.T) {
	d := NewRealtimeDetector(nil, nil, nil, false, "")

	// Initial metrics should be zero
	metrics := d.GetMetrics()
	if metrics.TradesDetected != 0 {
		t.Errorf("initial TradesDetected = %d, want 0", metrics.TradesDetected)
	}
	if metrics.BlockchainEvents != 0 {
		t.Errorf("initial BlockchainEvents = %d, want 0", metrics.BlockchainEvents)
	}

	// Simulate updating metrics
	d.metricsMu.Lock()
	d.metrics.TradesDetected = 10
	d.metrics.BlockchainEvents = 100
	d.metrics.BlockchainMatches = 5
	d.metrics.AvgDetectionLatency = 500 * time.Millisecond
	d.metricsMu.Unlock()

	metrics = d.GetMetrics()
	if metrics.TradesDetected != 10 {
		t.Errorf("TradesDetected = %d, want 10", metrics.TradesDetected)
	}
	if metrics.BlockchainEvents != 100 {
		t.Errorf("BlockchainEvents = %d, want 100", metrics.BlockchainEvents)
	}
	if metrics.BlockchainMatches != 5 {
		t.Errorf("BlockchainMatches = %d, want 5", metrics.BlockchainMatches)
	}
}

func TestAddRemoveFollowedUser(t *testing.T) {
	d := NewRealtimeDetector(nil, nil, nil, false, "")

	t.Run("AddFollowedUser", func(t *testing.T) {
		d.AddFollowedUser("0x05c1882212a41aa8d7df5b70eebe03d9319345b7")

		d.followedUsersMu.RLock()
		defer d.followedUsersMu.RUnlock()

		// Should be normalized (lowercase)
		if !d.followedUsers["0x05c1882212a41aa8d7df5b70eebe03d9319345b7"] {
			t.Error("user should be added to followedUsers")
		}
	})

	t.Run("RemoveFollowedUser", func(t *testing.T) {
		d.RemoveFollowedUser("0x05c1882212a41aa8d7df5b70eebe03d9319345b7")

		d.followedUsersMu.RLock()
		defer d.followedUsersMu.RUnlock()

		if d.followedUsers["0x05c1882212a41aa8d7df5b70eebe03d9319345b7"] {
			t.Error("user should be removed from followedUsers")
		}
	})
}

func TestProcessedTxsCleanup(t *testing.T) {
	d := NewRealtimeDetector(nil, nil, nil, false, "")

	// Set up followed user (with 0x prefix)
	d.followedUsersMu.Lock()
	d.followedUsers["0x05c1882212a41aa8d7df5b70eebe03d9319345b7"] = true
	d.followedUsersMu.Unlock()

	// Add 1001 transactions to trigger cleanup
	d.processedTxsMu.Lock()
	for i := 0; i < 1001; i++ {
		d.processedTxs["tx"+string(rune(i))] = true
	}
	d.processedTxsMu.Unlock()

	// Process a new trade - this should trigger cleanup
	event := api.PolygonTradeEvent{
		TxHash:       "0xnewcleanup1234567890123456789012345678901234567890123456789abcdef",
		Maker:        "0xother1234567890123456789012345678901234",
		Taker:        "0x05c1882212a41aa8d7df5b70eebe03d9319345b7",
		MakerAssetID: "0x96c28e8b5e52d9a5f01f2d9d8f8e5c0d1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d",
		TakerAssetID: "0x0000000000000000000000000000000000000000000000000000000000000000",
		Timestamp:    time.Now(),
	}

	d.handleBlockchainTrade(event)

	d.processedTxsMu.RLock()
	defer d.processedTxsMu.RUnlock()

	// After cleanup, should have around 500 entries
	if len(d.processedTxs) > 600 {
		t.Errorf("processedTxs should be cleaned up, got %d entries", len(d.processedTxs))
	}
}

func TestSellTradeDetection(t *testing.T) {
	var receivedTrade models.TradeDetail
	var mu sync.Mutex

	callback := func(trade models.TradeDetail) {
		mu.Lock()
		receivedTrade = trade
		mu.Unlock()
	}

	d := NewRealtimeDetector(nil, nil, callback, false, "")

	// Set up followed user (with 0x prefix)
	d.followedUsersMu.Lock()
	d.followedUsers["0x05c1882212a41aa8d7df5b70eebe03d9319345b7"] = true
	d.followedUsersMu.Unlock()

	t.Run("TAKER selling (receives USDC) triggers SELL", func(t *testing.T) {
		event := api.PolygonTradeEvent{
			TxHash:       "0xsell1234567890123456789012345678901234567890123456789012345678abcd",
			Maker:        "0xother1234567890123456789012345678901234",
			Taker:        "0x05c1882212a41aa8d7df5b70eebe03d9319345b7",
			MakerAssetID: "0x0000000000000000000000000000000000000000000000000000000000000000", // USDC (taker receives this)
			TakerAssetID: "0x96c28e8b5e52d9a5f01f2d9d8f8e5c0d1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d", // outcome token (taker gives this)
			Timestamp:    time.Now(),
		}

		d.handleBlockchainTrade(event)

		mu.Lock()
		defer mu.Unlock()

		if receivedTrade.Side != "SELL" {
			t.Errorf("Side = %s, want SELL", receivedTrade.Side)
		}

		// MarketID should be the token they're selling (takerAssetID)
		if receivedTrade.MarketID != event.TakerAssetID {
			t.Errorf("MarketID = %s, want %s", receivedTrade.MarketID, event.TakerAssetID)
		}
	})
}

func TestCopyTraderConfigEnableBlockchainWS(t *testing.T) {
	t.Run("EnableBlockchainWS true", func(t *testing.T) {
		config := CopyTraderConfig{
			Enabled:            true,
			Multiplier:         0.05,
			MinOrderUSDC:       1.0,
			MaxPriceSlippage:   0.20,
			CheckIntervalSec:   1,
			EnableBlockchainWS: true,
		}

		if !config.EnableBlockchainWS {
			t.Error("EnableBlockchainWS should be true")
		}
	})

	t.Run("EnableBlockchainWS false (default)", func(t *testing.T) {
		config := CopyTraderConfig{
			Enabled:          true,
			Multiplier:       0.05,
			MinOrderUSDC:     1.0,
			MaxPriceSlippage: 0.20,
			CheckIntervalSec: 1,
			// EnableBlockchainWS not set - should be false
		}

		if config.EnableBlockchainWS {
			t.Error("EnableBlockchainWS should be false by default")
		}
	})
}
