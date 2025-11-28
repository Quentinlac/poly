package api

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"
)

func TestMockClobClient(t *testing.T) {
	ctx := context.Background()

	t.Run("GetOrderBook default response", func(t *testing.T) {
		client := NewMockClobClient()

		book, err := client.GetOrderBook(ctx, "token123")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(book.Asks) != 1 || book.Asks[0].Price != "0.50" {
			t.Error("unexpected default order book")
		}
	})

	t.Run("GetOrderBook custom response", func(t *testing.T) {
		client := NewMockClobClient()
		client.OrderBook = &OrderBook{
			Asks: []OrderBookLevel{{Price: "0.60", Size: "200"}},
			Bids: []OrderBookLevel{{Price: "0.55", Size: "150"}},
		}

		book, err := client.GetOrderBook(ctx, "token123")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if book.Asks[0].Price != "0.60" {
			t.Error("should return custom order book")
		}
	})

	t.Run("GetOrderBook error injection", func(t *testing.T) {
		client := NewMockClobClient()
		expectedErr := errors.New("network timeout")
		client.ErrorOnNext["GetOrderBook"] = expectedErr

		_, err := client.GetOrderBook(ctx, "token123")
		if err != expectedErr {
			t.Errorf("expected injected error, got %v", err)
		}

		// Second call should succeed
		_, err = client.GetOrderBook(ctx, "token123")
		if err != nil {
			t.Errorf("second call should succeed, got %v", err)
		}
	})

	t.Run("GetMarket", func(t *testing.T) {
		client := NewMockClobClient()

		market, err := client.GetMarket(ctx, "condition123")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if market.ConditionID != "condition123" {
			t.Error("should return correct condition ID")
		}
	})

	t.Run("GetTokenInfoByID", func(t *testing.T) {
		client := NewMockClobClient()

		info, err := client.GetTokenInfoByID(ctx, "token456")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if info.TokenID != "token456" {
			t.Error("should return correct token ID")
		}
	})

	t.Run("DeriveAPICreds", func(t *testing.T) {
		client := NewMockClobClient()

		creds, err := client.DeriveAPICreds(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if creds.APIKey != "test-api-key" {
			t.Error("should return default API key")
		}
	})

	t.Run("PlaceOrder", func(t *testing.T) {
		client := NewMockClobClient()

		// Use empty OrderRequest - the mock doesn't validate fields
		order := OrderRequest{}

		resp, err := client.PlaceOrder(ctx, order)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if resp.OrderID != "mock-order-id" {
			t.Error("should return mock order ID")
		}
	})

	t.Run("Call tracking", func(t *testing.T) {
		client := NewMockClobClient()

		client.GetOrderBook(ctx, "t1")
		client.GetOrderBook(ctx, "t2")
		client.GetMarket(ctx, "c1")
		client.PlaceOrder(ctx, OrderRequest{})

		if client.Calls["GetOrderBook"] != 2 {
			t.Errorf("expected 2 GetOrderBook calls, got %d", client.Calls["GetOrderBook"])
		}
		if client.Calls["GetMarket"] != 1 {
			t.Errorf("expected 1 GetMarket call, got %d", client.Calls["GetMarket"])
		}
		if client.Calls["PlaceOrder"] != 1 {
			t.Errorf("expected 1 PlaceOrder call, got %d", client.Calls["PlaceOrder"])
		}
	})
}

func TestMockWSClient(t *testing.T) {
	ctx := context.Background()

	t.Run("Start and Stop", func(t *testing.T) {
		client := NewMockWSClient(nil)

		err := client.Start(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !client.Connected {
			t.Error("should be connected")
		}

		client.Stop()

		if client.Connected {
			t.Error("should be disconnected")
		}
	})

	t.Run("Subscribe", func(t *testing.T) {
		client := NewMockWSClient(nil)
		client.Start(ctx)

		err := client.Subscribe("asset1", "asset2")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(client.Subscriptions) != 2 {
			t.Errorf("expected 2 subscriptions, got %d", len(client.Subscriptions))
		}
	})

	t.Run("SimulateTradeEvent", func(t *testing.T) {
		var receivedEvent WSTradeEvent
		client := NewMockWSClient(func(event WSTradeEvent) {
			receivedEvent = event
		})

		event := WSTradeEvent{
			AssetID:   "asset123",
			Price:     0.55,
			Timestamp: time.Now(),
		}

		client.SimulateTradeEvent(event)

		if receivedEvent.AssetID != "asset123" {
			t.Error("should receive simulated event")
		}
		if receivedEvent.Price != 0.55 {
			t.Errorf("expected price 0.55, got %f", receivedEvent.Price)
		}
	})

	t.Run("Error injection on Start", func(t *testing.T) {
		client := NewMockWSClient(nil)
		expectedErr := errors.New("connection refused")
		client.ErrorOnNext["Start"] = expectedErr

		err := client.Start(ctx)
		if err != expectedErr {
			t.Errorf("expected injected error, got %v", err)
		}

		if client.Connected {
			t.Error("should not be connected after error")
		}
	})
}

func TestMockPolygonWSClient(t *testing.T) {
	ctx := context.Background()

	t.Run("Start and Stop", func(t *testing.T) {
		client := NewMockPolygonWSClient(nil)

		err := client.Start(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !client.Connected {
			t.Error("should be connected")
		}

		client.Stop()

		if client.Connected {
			t.Error("should be disconnected")
		}
	})

	t.Run("SetFollowedAddresses", func(t *testing.T) {
		client := NewMockPolygonWSClient(nil)

		addrs := []string{"0xaddr1", "0xaddr2", "0xaddr3"}
		client.SetFollowedAddresses(addrs)

		if len(client.FollowedAddrs) != 3 {
			t.Errorf("expected 3 followed addresses, got %d", len(client.FollowedAddrs))
		}

		if !client.FollowedAddrs["0xaddr1"] {
			t.Error("should contain 0xaddr1")
		}
	})

	t.Run("AddFollowedAddress", func(t *testing.T) {
		client := NewMockPolygonWSClient(nil)

		client.AddFollowedAddress("0xnewaddr")

		if !client.FollowedAddrs["0xnewaddr"] {
			t.Error("should add new address")
		}
	})

	t.Run("SimulateTradeEvent", func(t *testing.T) {
		var receivedEvent PolygonTradeEvent
		client := NewMockPolygonWSClient(func(event PolygonTradeEvent) {
			receivedEvent = event
		})

		event := PolygonTradeEvent{
			TxHash:      "0xtx123",
			BlockNumber: 12345,
			Maker:       "0xmaker",
			Taker:       "0xtaker",
			MakerAmount: big.NewInt(1000),
			TakerAmount: big.NewInt(500),
		}

		client.SimulateTradeEvent(event)

		if receivedEvent.TxHash != "0xtx123" {
			t.Error("should receive simulated event")
		}
		if receivedEvent.BlockNumber != 12345 {
			t.Error("block number should match")
		}

		// Check stats
		events, matches := client.GetStats()
		if events != 1 {
			t.Errorf("expected 1 event, got %d", events)
		}
		if matches != 1 {
			t.Errorf("expected 1 match, got %d", matches)
		}
	})

	t.Run("GetStats", func(t *testing.T) {
		client := NewMockPolygonWSClient(nil)
		client.EventsReceived = 100
		client.TradesMatched = 5

		events, matches := client.GetStats()
		if events != 100 {
			t.Errorf("expected 100 events, got %d", events)
		}
		if matches != 5 {
			t.Errorf("expected 5 matches, got %d", matches)
		}
	})

	t.Run("Error injection", func(t *testing.T) {
		client := NewMockPolygonWSClient(nil)
		expectedErr := errors.New("WebSocket connection failed")
		client.ErrorOnNext["Start"] = expectedErr

		err := client.Start(ctx)
		if err != expectedErr {
			t.Errorf("expected injected error, got %v", err)
		}
	})

	t.Run("Call tracking", func(t *testing.T) {
		client := NewMockPolygonWSClient(nil)

		client.Start(ctx)
		client.SetFollowedAddresses([]string{"0x1"})
		client.SetFollowedAddresses([]string{"0x2"})
		client.AddFollowedAddress("0x3")
		client.Stop()

		if client.Calls["Start"] != 1 {
			t.Errorf("expected 1 Start call, got %d", client.Calls["Start"])
		}
		if client.Calls["SetFollowedAddresses"] != 2 {
			t.Errorf("expected 2 SetFollowedAddresses calls, got %d", client.Calls["SetFollowedAddresses"])
		}
		if client.Calls["AddFollowedAddress"] != 1 {
			t.Errorf("expected 1 AddFollowedAddress call, got %d", client.Calls["AddFollowedAddress"])
		}
		if client.Calls["Stop"] != 1 {
			t.Errorf("expected 1 Stop call, got %d", client.Calls["Stop"])
		}
	})
}

func TestMockHTTPClient(t *testing.T) {
	client := NewMockHTTPClient()

	t.Run("trackCall", func(t *testing.T) {
		err := client.trackCall("TestMethod")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if client.Calls["TestMethod"] != 1 {
			t.Errorf("expected 1 call, got %d", client.Calls["TestMethod"])
		}
	})

	t.Run("error injection", func(t *testing.T) {
		expectedErr := errors.New("HTTP error")
		client.ErrorOnNext["TestError"] = expectedErr

		err := client.trackCall("TestError")
		if err != expectedErr {
			t.Errorf("expected injected error, got %v", err)
		}

		// Second call should succeed
		err = client.trackCall("TestError")
		if err != nil {
			t.Errorf("second call should succeed, got %v", err)
		}
	})
}
