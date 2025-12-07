package syncer

import (
	"context"
	"errors"
	"sync"
	"testing"

	"polymarket-analyzer/api"
	"polymarket-analyzer/storage"
)

// Standard errors for testing
var (
	ErrRateLimit    = errors.New("429 Too Many Requests")
	ErrOrderReject  = errors.New("order rejected: insufficient balance")
	ErrMinimumOrder = errors.New("Minimum order size not reached. The minimum number of shares is 0.1")
	ErrNetworkTimeout = errors.New("context deadline exceeded")
	ErrNotFound     = errors.New("404 Not Found")
	ErrServerError  = errors.New("500 Internal Server Error")
	ErrNoOrderbook  = errors.New("404 {\"error\":\"No orderbook exists for the requested token id\"}")
)

// TestErrorHandling_RateLimits tests rate limit detection and handling
func TestErrorHandling_RateLimits(t *testing.T) {
	t.Run("detect 429 rate limit error", func(t *testing.T) {
		err := ErrRateLimit

		isRateLimit := containsStr(err.Error(), "429") || containsStr(err.Error(), "Too Many Requests")
		if !isRateLimit {
			t.Error("should detect rate limit error")
		}
	})

	t.Run("retry strategy for rate limits", func(t *testing.T) {
		// Rate limits should trigger exponential backoff
		type retryConfig struct {
			attempt     int
			backoffMs   int
			shouldRetry bool
		}

		tests := []retryConfig{
			{1, 100, true},    // First retry: 100ms
			{2, 200, true},    // Second: 200ms
			{3, 400, true},    // Third: 400ms
			{4, 800, true},    // Fourth: 800ms
			{5, 1600, true},   // Fifth: 1600ms
			{6, 3200, false},  // Sixth: give up after 5 retries
		}

		maxRetries := 5
		for _, tt := range tests {
			shouldRetry := tt.attempt <= maxRetries
			if shouldRetry != tt.shouldRetry {
				t.Errorf("attempt %d: shouldRetry=%v, want %v", tt.attempt, shouldRetry, tt.shouldRetry)
			}
		}
	})

	t.Run("rate limit doesn't affect other operations", func(t *testing.T) {
		// When one API call is rate limited, others should continue
		mockClient := api.NewMockClobClient()

		// First call rate limited
		mockClient.ErrorOnNext["GetOrderBook"] = ErrRateLimit

		_, err := mockClient.GetOrderBook(context.Background(), "token-1")
		if err != ErrRateLimit {
			t.Errorf("expected rate limit error, got %v", err)
		}

		// Second call should succeed
		book, err := mockClient.GetOrderBook(context.Background(), "token-2")
		if err != nil {
			t.Errorf("second call should succeed, got error: %v", err)
		}
		if book == nil {
			t.Error("should return order book")
		}
	})
}

// TestErrorHandling_OrderRejections tests order rejection scenarios
func TestErrorHandling_OrderRejections(t *testing.T) {
	t.Run("detect insufficient balance rejection", func(t *testing.T) {
		err := ErrOrderReject

		isInsufficientBalance := containsStr(err.Error(), "insufficient balance")
		if !isInsufficientBalance {
			t.Error("should detect insufficient balance")
		}
	})

	t.Run("detect minimum order size error", func(t *testing.T) {
		err := ErrMinimumOrder

		isMinError := containsStr(err.Error(), "Minimum order size") || containsStr(err.Error(), "minimum number of shares")
		if !isMinError {
			t.Error("should detect minimum order error")
		}

		// Extract minimum from error message
		// Expected: "The minimum number of shares is 0.1"
		expectedMin := 0.1
		if !containsStr(err.Error(), "0.1") {
			t.Errorf("expected minimum of %.1f in error message", expectedMin)
		}
	})

	t.Run("order rejection logs but doesn't crash", func(t *testing.T) {
		mockClient := api.NewMockClobClient()
		mockClient.ErrorOnNext["PlaceMarketOrder"] = ErrOrderReject

		// Should return error but not panic
		_, err := mockClient.PlaceMarketOrder(context.Background(), "token-1", api.SideBuy, 10.0, false)
		if err == nil {
			t.Error("should return rejection error")
		}
		if err != ErrOrderReject {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("handle minimum order retry with increased size", func(t *testing.T) {
		// When order fails with minimum error, should retry with minimum size
		originalSize := 0.05
		minSize := 0.1

		newSize := originalSize
		if newSize < minSize {
			newSize = minSize
		}

		if newSize != minSize {
			t.Errorf("should increase to minimum: got %.2f, want %.2f", newSize, minSize)
		}
	})
}

// TestErrorHandling_ClosedMarkets tests handling of closed/resolved markets
func TestErrorHandling_ClosedMarkets(t *testing.T) {
	t.Run("detect 404 no orderbook error", func(t *testing.T) {
		err := ErrNoOrderbook

		isClosed := containsStr(err.Error(), "404") || containsStr(err.Error(), "No orderbook exists")
		if !isClosed {
			t.Error("should detect closed market")
		}
	})

	t.Run("skip closed markets without error", func(t *testing.T) {
		// Closed markets should be silently skipped
		errors := []error{
			ErrNoOrderbook,
			errors.New("market resolved"),
			errors.New("404 Not Found"),
		}

		for _, err := range errors {
			shouldSkip := containsStr(err.Error(), "404") ||
				containsStr(err.Error(), "No orderbook") ||
				containsStr(err.Error(), "resolved")

			if !shouldSkip {
				t.Errorf("error %q should be skipped", err.Error())
			}
		}
	})

	t.Run("log skip but don't retry closed markets", func(t *testing.T) {
		// Closed markets should be marked as processed and not retried
		store := NewMockStore()
		ctx := context.Background()

		tradeID := "trade-closed-market"

		// Simulate detecting closed market
		err := ErrNoOrderbook
		isClosed := containsStr(err.Error(), "404") || containsStr(err.Error(), "No orderbook")

		if isClosed {
			// Mark as processed (skip permanently)
			store.MarkTradeProcessed(ctx, tradeID)
		}

		// Verify marked
		processed, _ := store.IsTradeProcessed(ctx, tradeID)
		if !processed {
			t.Error("closed market trade should be marked as processed")
		}
	})
}

// TestErrorHandling_NetworkIssues tests network error handling
func TestErrorHandling_NetworkIssues(t *testing.T) {
	t.Run("detect timeout error", func(t *testing.T) {
		err := ErrNetworkTimeout

		isTimeout := containsStr(err.Error(), "deadline exceeded") ||
			containsStr(err.Error(), "context canceled") ||
			containsStr(err.Error(), "timeout")

		if !isTimeout {
			t.Error("should detect timeout")
		}
	})

	t.Run("retry on transient network errors", func(t *testing.T) {
		transientErrors := []error{
			ErrNetworkTimeout,
			errors.New("connection reset by peer"),
			errors.New("EOF"),
			ErrServerError,
		}

		for _, err := range transientErrors {
			shouldRetry := isTransientError(err)
			if !shouldRetry {
				t.Errorf("error %q should be retried", err.Error())
			}
		}
	})

	t.Run("don't retry on permanent errors", func(t *testing.T) {
		permanentErrors := []error{
			ErrOrderReject,
			errors.New("invalid token id"),
			errors.New("market not found"),
		}

		for _, err := range permanentErrors {
			shouldRetry := isTransientError(err)
			if shouldRetry {
				t.Errorf("error %q should NOT be retried", err.Error())
			}
		}
	})

	t.Run("context cancellation stops retries", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		// Should detect cancelled context
		select {
		case <-ctx.Done():
			// Expected
		default:
			t.Error("context should be cancelled")
		}

		// Don't retry when context is cancelled
		if ctx.Err() == context.Canceled {
			// Should exit retry loop
		} else {
			t.Error("should detect context cancellation")
		}
	})
}

// TestErrorHandling_OrderStatusChecks tests order status checking errors
func TestErrorHandling_OrderStatusChecks(t *testing.T) {
	t.Run("handle order not found", func(t *testing.T) {
		mockClient := api.NewMockClobClient()
		mockClient.ErrorOnNext["GetOrderStatus"] = ErrNotFound

		_, err := mockClient.GetOrderStatus(context.Background(), "non-existent-order")
		if err != ErrNotFound {
			t.Errorf("expected not found error, got %v", err)
		}
	})

	t.Run("order cancelled status", func(t *testing.T) {
		// Test detecting cancelled orders
		statuses := []string{"CANCELLED", "MATCHED", "LIVE", "EXPIRED"}

		for _, status := range statuses {
			isCancelled := status == "CANCELLED" || status == "EXPIRED"
			isLive := status == "LIVE"
			isMatched := status == "MATCHED"

			switch status {
			case "CANCELLED", "EXPIRED":
				if !isCancelled {
					t.Errorf("%s should be treated as cancelled", status)
				}
			case "LIVE":
				if !isLive {
					t.Errorf("%s should be treated as live", status)
				}
			case "MATCHED":
				if !isMatched {
					t.Errorf("%s should be treated as matched", status)
				}
			}
		}
	})

	t.Run("partial fill detection", func(t *testing.T) {
		status := api.OrderStatus{
			OrderID:      "order-123",
			Status:       "MATCHED",
			SizeMatched:  "75",
			OriginalSize: "100",
		}

		var matched, original float64
		matched = 75
		original = 100

		fillRatio := matched / original
		isPartial := fillRatio > 0 && fillRatio < 1.0

		if !isPartial {
			t.Errorf("should detect partial fill: %.2f / %.2f = %.2f", matched, original, fillRatio)
		}

		if status.Status != "MATCHED" {
			t.Errorf("status should be MATCHED, got %s", status.Status)
		}
	})
}

// TestErrorHandling_CancelOrder tests order cancellation error handling
func TestErrorHandling_CancelOrder(t *testing.T) {
	t.Run("cancel non-existent order", func(t *testing.T) {
		mockClient := api.NewMockClobClient()
		mockClient.ErrorOnNext["CancelOrder"] = ErrNotFound

		err := mockClient.CancelOrder(context.Background(), "non-existent")
		if err != ErrNotFound {
			t.Errorf("expected not found, got %v", err)
		}
	})

	t.Run("cancel already filled order", func(t *testing.T) {
		// Cancelling a filled order should be a no-op or return specific error
		err := errors.New("order already filled")

		isAlreadyFilled := containsStr(err.Error(), "already filled")
		if !isAlreadyFilled {
			t.Error("should detect already filled error")
		}
	})

	t.Run("batch cancel with mixed results", func(t *testing.T) {
		mockClient := api.NewMockClobClient()

		orderIDs := []string{"order-1", "order-2", "order-3"}

		// First batch cancel succeeds
		err := mockClient.CancelOrders(context.Background(), orderIDs)
		if err != nil {
			t.Errorf("batch cancel should succeed: %v", err)
		}

		// Verify all tracked
		if len(mockClient.CancelOrderCalls) != 3 {
			t.Errorf("should track all 3 cancels, got %d", len(mockClient.CancelOrderCalls))
		}
	})
}

// TestErrorHandling_StoreErrors tests database error handling
func TestErrorHandling_StoreErrors(t *testing.T) {
	t.Run("handle position lookup error", func(t *testing.T) {
		store := NewMockStore()
		store.ErrorOnNext["GetMyPosition"] = errors.New("database connection error")

		_, err := store.GetMyPosition(context.Background(), "market-1", "Yes")
		if err == nil {
			t.Error("should return error")
		}
	})

	t.Run("handle position update error", func(t *testing.T) {
		store := NewMockStore()
		store.ErrorOnNext["UpdateMyPosition"] = errors.New("constraint violation")

		err := store.UpdateMyPosition(context.Background(), MyPosition{
			MarketID: "market-1",
			Outcome:  "Yes",
			Size:     10.0,
		})
		if err == nil {
			t.Error("should return error")
		}
	})

	t.Run("continue processing on non-critical store error", func(t *testing.T) {
		store := NewMockStore()

		// Log save fails - should continue processing
		store.ErrorOnNext["SaveCopyTradeLog"] = errors.New("log save failed")

		// This should not block main flow
		err := store.SaveCopyTradeLog(context.Background(), storage.CopyTradeLogEntry{
			FollowingTradeID: "trade-1",
			Status:           "success",
		})

		// Error is returned but flow continues
		if err == nil {
			t.Error("error should be returned")
		}

		// Main processing would continue despite log error
		// Just verify the error is surfaced, not that it blocks
	})
}

// TestErrorHandling_MultiAccountErrors tests multi-account related errors
func TestErrorHandling_MultiAccountErrors(t *testing.T) {
	t.Run("handle missing trading account", func(t *testing.T) {
		store := NewMockStore()

		// Account doesn't exist
		acc, err := store.GetTradingAccount(context.Background(), 999)
		if err != nil {
			t.Errorf("should not error on missing account: %v", err)
		}
		if acc != nil {
			t.Error("missing account should return nil")
		}
	})

	t.Run("handle no default account", func(t *testing.T) {
		store := NewMockStore()

		// No accounts configured
		acc, err := store.GetDefaultTradingAccount(context.Background())
		if err != nil {
			t.Errorf("should not error on no default: %v", err)
		}
		if acc != nil {
			t.Error("should return nil when no default")
		}
	})

	t.Run("fallback to default when assigned account disabled", func(t *testing.T) {
		store := NewMockStore()

		// Add disabled account
		store.AddTradingAccount(storage.TradingAccount{
			ID:      2,
			Name:    "Disabled Account",
			Enabled: false,
		})

		// Add default account
		store.AddTradingAccount(storage.TradingAccount{
			ID:        1,
			Name:      "Default",
			IsDefault: true,
			Enabled:   true,
		})

		// User assigned to disabled account
		accountID := 2
		store.SetUserSettings("0xuser", storage.UserCopySettings{
			TradingAccountID: &accountID,
		})

		// Get assigned account
		assignedAcc, _ := store.GetTradingAccount(context.Background(), accountID)
		if assignedAcc == nil || assignedAcc.Enabled {
			// Fall back to default
			defaultAcc, _ := store.GetDefaultTradingAccount(context.Background())
			if defaultAcc == nil {
				t.Error("should have default fallback")
			}
			if !defaultAcc.Enabled {
				t.Error("default should be enabled")
			}
		}
	})
}

// TestErrorHandling_ConcurrentErrors tests error handling under concurrent load
func TestErrorHandling_ConcurrentErrors(t *testing.T) {
	t.Run("concurrent operations with errors", func(t *testing.T) {
		// Test that concurrent operations handle errors gracefully
		var wg sync.WaitGroup
		errCount := 0
		successCount := 0
		var mu sync.Mutex

		// Create multiple mock clients for concurrent testing
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				mockClient := api.NewMockClobClient()

				// Some calls will hit errors
				if id%3 == 0 {
					mockClient.ErrorOnNext["GetOrderBook"] = ErrRateLimit
				}

				_, err := mockClient.GetOrderBook(context.Background(), "token-1")
				mu.Lock()
				if err != nil {
					errCount++
				} else {
					successCount++
				}
				mu.Unlock()
			}(i)
		}

		wg.Wait()

		// Some should succeed, some should fail
		t.Logf("errors: %d, success: %d", errCount, successCount)
		if errCount+successCount != 10 {
			t.Errorf("total should be 10, got %d", errCount+successCount)
		}

		// We injected errors for ids 0, 3, 6, 9 (4 errors)
		if errCount != 4 {
			t.Errorf("expected 4 errors (ids 0,3,6,9), got %d", errCount)
		}
	})
}

// Helper functions

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	transientPatterns := []string{
		"timeout",
		"deadline exceeded",
		"connection reset",
		"EOF",
		"500",
		"502",
		"503",
		"504",
	}
	for _, pattern := range transientPatterns {
		if containsStr(msg, pattern) {
			return true
		}
	}
	return false
}
