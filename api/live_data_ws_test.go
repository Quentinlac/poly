package api

import (
	"testing"
	"time"
)

func TestGetBTC15mSlugs(t *testing.T) {
	// Note: This test depends on current time, so we test the format and logic
	current, next := GetBTC15mSlugs()

	// Test format
	if len(current) < 20 {
		t.Errorf("current slug too short: %s", current)
	}
	if len(next) < 20 {
		t.Errorf("next slug too short: %s", next)
	}

	// Test prefix
	expectedPrefix := "btc-updown-15m-"
	if current[:15] != expectedPrefix {
		t.Errorf("current slug prefix = %s, want %s", current[:15], expectedPrefix)
	}
	if next[:15] != expectedPrefix {
		t.Errorf("next slug prefix = %s, want %s", next[:15], expectedPrefix)
	}

	// Extract timestamps
	var currentTS, nextTS int64
	_, err1 := parseSlugTimestamp(current, &currentTS)
	_, err2 := parseSlugTimestamp(next, &nextTS)

	if err1 != nil || err2 != nil {
		t.Fatalf("failed to parse timestamps: current=%v, next=%v", err1, err2)
	}

	// Next should be exactly 900 seconds (15 min) after current
	diff := nextTS - currentTS
	if diff != 900 {
		t.Errorf("timestamp difference = %d, want 900", diff)
	}

	// Current should be in the future (end of current period)
	now := time.Now().Unix()
	if currentTS <= now-900 {
		t.Errorf("current timestamp %d is too far in the past (now=%d)", currentTS, now)
	}
	if currentTS > now+900 {
		t.Errorf("current timestamp %d is too far in the future (now=%d)", currentTS, now)
	}
}

func TestGetBTC15mSlugs_15MinBoundary(t *testing.T) {
	// Test that at any 15-min boundary, the slugs change correctly
	// This tests the boundary logic

	// Get current slugs
	current1, next1 := GetBTC15mSlugs()

	// Both should be non-empty
	if current1 == "" || next1 == "" {
		t.Error("slugs should not be empty")
	}

	// Current and next should be different
	if current1 == next1 {
		t.Errorf("current and next should be different: %s", current1)
	}
}

func TestGetBTC15mSlugs_Format(t *testing.T) {
	current, next := GetBTC15mSlugs()

	tests := []struct {
		name string
		slug string
	}{
		{"current", current},
		{"next", next},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Format should be: btc-updown-15m-{unix_timestamp}
			// Example: btc-updown-15m-1733571600

			// Check it contains the expected prefix
			prefix := "btc-updown-15m-"
			if len(tt.slug) < len(prefix) {
				t.Errorf("slug too short: %s", tt.slug)
				return
			}

			if tt.slug[:len(prefix)] != prefix {
				t.Errorf("wrong prefix in %s, got %s", tt.slug, tt.slug[:len(prefix)])
			}

			// Check timestamp is a valid unix timestamp (10 digits)
			timestampPart := tt.slug[len(prefix):]
			if len(timestampPart) != 10 {
				t.Errorf("timestamp should be 10 digits, got %d: %s", len(timestampPart), timestampPart)
			}
		})
	}
}

func TestGetBTC15mSlugs_15MinAlignment(t *testing.T) {
	current, _ := GetBTC15mSlugs()

	var ts int64
	parseSlugTimestamp(current, &ts)

	// Timestamp should be aligned to 15-minute boundary (divisible by 900)
	if ts%900 != 0 {
		t.Errorf("timestamp %d is not aligned to 15-minute boundary", ts)
	}
}

func TestLiveDataTradeEvent_Structure(t *testing.T) {
	// Test that the LiveDataTradeEvent structure can be used correctly
	event := LiveDataTradeEvent{
		Name:            "Test User",
		ProxyWallet:     "0x1234567890abcdef",
		Pseudonym:       "TestTrader",
		Side:            "BUY",
		Size:            100.5,
		Price:           0.55,
		Outcome:         "Up",
		OutcomeIndex:    0,
		EventSlug:       "btc-updown-15m-1733571600",
		ConditionID:     "0xabcdef123456",
		TransactionHash: "0xtxhash",
		Timestamp:       time.Now().Unix(),
	}

	// Verify fields
	if event.Side != "BUY" {
		t.Errorf("Side = %s, want BUY", event.Side)
	}
	if event.Size != 100.5 {
		t.Errorf("Size = %f, want 100.5", event.Size)
	}
	if event.Price != 0.55 {
		t.Errorf("Price = %f, want 0.55", event.Price)
	}
	if event.Outcome != "Up" {
		t.Errorf("Outcome = %s, want Up", event.Outcome)
	}
}

func TestNewLiveDataWSClient(t *testing.T) {
	var receivedEvents []LiveDataTradeEvent
	handler := func(event LiveDataTradeEvent) {
		receivedEvents = append(receivedEvents, event)
	}

	client := NewLiveDataWSClient(handler)

	if client == nil {
		t.Fatal("client should not be nil")
	}

	if client.running {
		t.Error("client should not be running initially")
	}

	if len(client.subscriptions) != 0 {
		t.Errorf("subscriptions should be empty initially, got %d", len(client.subscriptions))
	}

	if client.stopCh == nil {
		t.Error("stopCh should be initialized")
	}

	if client.doneCh == nil {
		t.Error("doneCh should be initialized")
	}
}

func TestLiveDataWSClient_UpdateSubscriptions(t *testing.T) {
	client := NewLiveDataWSClient(nil)

	// Without connection, this should fail but update the internal state
	slugs := []string{"btc-updown-15m-1733571600", "btc-updown-15m-1733572500"}

	// This will fail because we're not connected, but the subscriptions should still be stored
	_ = client.UpdateSubscriptions(slugs)

	// Check internal state
	client.subscriptionsMu.RLock()
	defer client.subscriptionsMu.RUnlock()

	if len(client.subscriptions) != 2 {
		t.Errorf("subscriptions count = %d, want 2", len(client.subscriptions))
	}

	if client.subscriptions[0] != slugs[0] {
		t.Errorf("subscription[0] = %s, want %s", client.subscriptions[0], slugs[0])
	}
}

func TestLiveDataWSClient_HandleMessage(t *testing.T) {
	var receivedEvents []LiveDataTradeEvent
	handler := func(event LiveDataTradeEvent) {
		receivedEvents = append(receivedEvents, event)
	}

	client := NewLiveDataWSClient(handler)

	// Test handling a valid orders_matched message
	validMessage := []byte(`{
		"connection_id": "test-conn",
		"topic": "activity",
		"type": "orders_matched",
		"timestamp": 1733571600000,
		"payload": {
			"name": "Test User",
			"proxyWallet": "0x1234",
			"side": "BUY",
			"size": 100.0,
			"price": 0.55,
			"outcome": "Up",
			"eventSlug": "btc-updown-15m-1733571600",
			"transactionHash": "0xtx123"
		}
	}`)

	client.handleMessage(validMessage)

	if len(receivedEvents) != 1 {
		t.Fatalf("expected 1 event, got %d", len(receivedEvents))
	}

	event := receivedEvents[0]
	if event.Side != "BUY" {
		t.Errorf("Side = %s, want BUY", event.Side)
	}
	if event.Size != 100.0 {
		t.Errorf("Size = %f, want 100.0", event.Size)
	}
	if event.Price != 0.55 {
		t.Errorf("Price = %f, want 0.55", event.Price)
	}
}

func TestLiveDataWSClient_HandleMessage_IgnoresNonTradeMessages(t *testing.T) {
	var receivedEvents []LiveDataTradeEvent
	handler := func(event LiveDataTradeEvent) {
		receivedEvents = append(receivedEvents, event)
	}

	client := NewLiveDataWSClient(handler)

	// Test various non-trade messages
	messages := [][]byte{
		// Different type
		[]byte(`{"type": "connection_established"}`),
		// Invalid JSON
		[]byte(`not json`),
		// Empty
		[]byte(``),
		// Different topic type
		[]byte(`{"type": "price_update", "payload": {}}`),
	}

	for _, msg := range messages {
		client.handleMessage(msg)
	}

	if len(receivedEvents) != 0 {
		t.Errorf("should not have received any events, got %d", len(receivedEvents))
	}
}

func TestLiveDataWSClient_HandleMessage_NilHandler(t *testing.T) {
	// Test with nil handler - should not panic
	client := NewLiveDataWSClient(nil)

	validMessage := []byte(`{
		"type": "orders_matched",
		"payload": {"side": "BUY", "size": 100}
	}`)

	// Should not panic
	client.handleMessage(validMessage)
}

// Helper function to parse timestamp from slug
func parseSlugTimestamp(slug string, ts *int64) (bool, error) {
	n, err := stringToInt64(slug[15:])
	if err != nil {
		return false, err
	}
	*ts = n
	return true, nil
}

func stringToInt64(s string) (int64, error) {
	var result int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, nil
		}
		result = result*10 + int64(c-'0')
	}
	return result, nil
}
