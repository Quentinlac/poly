package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"polymarket-analyzer/models"
)

func TestMockStoreUserOperations(t *testing.T) {
	store := NewMockStore()
	ctx := context.Background()

	t.Run("SaveUserSnapshot", func(t *testing.T) {
		user := models.User{
			ID:       "0x123",
			Username: "TestUser",
			TotalPNL: 1000.0,
		}

		err := store.SaveUserSnapshot(ctx, user)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if store.Calls["SaveUserSnapshot"] != 1 {
			t.Errorf("expected 1 call, got %d", store.Calls["SaveUserSnapshot"])
		}

		// Verify user was saved
		saved, err := store.GetUser(ctx, "0x123")
		if err != nil {
			t.Fatalf("GetUser error: %v", err)
		}
		if saved == nil || saved.Username != "TestUser" {
			t.Error("user not saved correctly")
		}
	})

	t.Run("ReplaceAllUsers", func(t *testing.T) {
		users := []models.User{
			{ID: "0x1", Username: "User1"},
			{ID: "0x2", Username: "User2"},
		}

		err := store.ReplaceAllUsers(ctx, users)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Old user should be gone
		old, _ := store.GetUser(ctx, "0x123")
		if old != nil {
			t.Error("old user should be replaced")
		}

		// New users should exist
		u1, _ := store.GetUser(ctx, "0x1")
		u2, _ := store.GetUser(ctx, "0x2")
		if u1 == nil || u2 == nil {
			t.Error("new users should exist")
		}
	})

	t.Run("DeleteUser", func(t *testing.T) {
		err := store.DeleteUser(ctx, "0x1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		deleted, _ := store.GetUser(ctx, "0x1")
		if deleted != nil {
			t.Error("user should be deleted")
		}
	})

	t.Run("ListUsers", func(t *testing.T) {
		users, err := store.ListUsers(ctx, models.SubjectPolitics, 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(users) != 1 { // Only 0x2 remains
			t.Errorf("expected 1 user, got %d", len(users))
		}
	})
}

func TestMockStoreTradeOperations(t *testing.T) {
	store := NewMockStore()
	ctx := context.Background()

	t.Run("SaveTrades", func(t *testing.T) {
		trades := []models.TradeDetail{
			{ID: "trade1", UserID: "0x123", Side: "BUY", Size: 100},
			{ID: "trade2", UserID: "0x123", Side: "SELL", Size: 50},
		}

		err := store.SaveTrades(ctx, trades, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify trades were saved
		saved, err := store.ListUserTrades(ctx, "0x123", 10)
		if err != nil {
			t.Fatalf("ListUserTrades error: %v", err)
		}
		if len(saved) != 2 {
			t.Errorf("expected 2 trades, got %d", len(saved))
		}

		// Verify marked as processed
		if !store.ProcessedTrades["trade1"] || !store.ProcessedTrades["trade2"] {
			t.Error("trades should be marked as processed")
		}
	})

	t.Run("GetUnprocessedTrades", func(t *testing.T) {
		// Add unprocessed trade
		store.Trades["0x456"] = []models.TradeDetail{
			{ID: "trade3", UserID: "0x456", Side: "BUY"},
		}

		unprocessed, err := store.GetUnprocessedTrades(ctx, 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Only trade3 is unprocessed
		if len(unprocessed) != 1 || unprocessed[0].ID != "trade3" {
			t.Errorf("expected 1 unprocessed trade, got %d", len(unprocessed))
		}
	})

	t.Run("MarkTradeProcessed", func(t *testing.T) {
		err := store.MarkTradeProcessed(ctx, "trade3")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !store.ProcessedTrades["trade3"] {
			t.Error("trade should be marked as processed")
		}
	})
}

func TestMockStoreTokenCache(t *testing.T) {
	store := NewMockStore()
	ctx := context.Background()

	t.Run("SaveTokenInfo", func(t *testing.T) {
		err := store.SaveTokenInfo(ctx, "token123", "condition456", "Yes", "Test Market", "test-market")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		info, err := store.GetTokenInfo(ctx, "token123")
		if err != nil {
			t.Fatalf("GetTokenInfo error: %v", err)
		}
		if info == nil || info.Outcome != "Yes" {
			t.Error("token info not saved correctly")
		}
	})

	t.Run("GetTokenByCondition", func(t *testing.T) {
		info, err := store.GetTokenByCondition(ctx, "condition456")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if info == nil || info.TokenID != "token123" {
			t.Error("should find token by condition")
		}
	})

	t.Run("GetTokenByConditionAndOutcome", func(t *testing.T) {
		info, err := store.GetTokenByConditionAndOutcome(ctx, "condition456", "Yes")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if info == nil {
			t.Error("should find token by condition and outcome")
		}

		// Non-existent outcome
		info2, _ := store.GetTokenByConditionAndOutcome(ctx, "condition456", "No")
		if info2 != nil {
			t.Error("should not find token with wrong outcome")
		}
	})

	t.Run("CacheTokenID", func(t *testing.T) {
		err := store.CacheTokenID(ctx, "market1", "Yes", "token789", false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		tokenID, _, err := store.GetTokenIDFromCache(ctx, "market1", "Yes")
		if err != nil {
			t.Fatalf("GetTokenIDFromCache error: %v", err)
		}
		if tokenID != "token789" {
			t.Errorf("expected token789, got %s", tokenID)
		}
	})
}

func TestMockStoreCopySettings(t *testing.T) {
	store := NewMockStore()
	ctx := context.Background()

	t.Run("SetUserCopySettings", func(t *testing.T) {
		maxUSD := 500.0
		settings := UserCopySettings{
			UserAddress:  "0xuser123",
			Multiplier:   0.1,
			Enabled:      true,
			MinUSDC:      1.0,
			StrategyType: StrategyBot,
			MaxUSD:       &maxUSD,
		}

		err := store.SetUserCopySettings(ctx, settings)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		saved, err := store.GetUserCopySettings(ctx, "0xuser123")
		if err != nil {
			t.Fatalf("GetUserCopySettings error: %v", err)
		}
		if saved == nil {
			t.Fatal("settings should be saved")
		}
		if saved.Multiplier != 0.1 {
			t.Errorf("expected multiplier 0.1, got %f", saved.Multiplier)
		}
		if saved.MaxUSD == nil || *saved.MaxUSD != 500.0 {
			t.Error("MaxUSD not saved correctly")
		}
	})

	t.Run("GetUserCopySettings non-existent", func(t *testing.T) {
		settings, err := store.GetUserCopySettings(ctx, "0xnonexistent")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if settings != nil {
			t.Error("should return nil for non-existent user")
		}
	})
}

func TestMockStoreAnalysisCache(t *testing.T) {
	store := NewMockStore()
	ctx := context.Background()

	t.Run("SaveAnalysisCache", func(t *testing.T) {
		err := store.SaveAnalysisCache(ctx, "0xuser", `{"pnl": 1000}`, 50)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		json, count, cachedAt, err := store.GetAnalysisCache(ctx, "0xuser")
		if err != nil {
			t.Fatalf("GetAnalysisCache error: %v", err)
		}
		if json != `{"pnl": 1000}` {
			t.Errorf("unexpected json: %s", json)
		}
		if count != 50 {
			t.Errorf("expected count 50, got %d", count)
		}
		if cachedAt.IsZero() {
			t.Error("cachedAt should be set")
		}
	})

	t.Run("InvalidateAnalysisCache", func(t *testing.T) {
		err := store.InvalidateAnalysisCache(ctx, "0xuser")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		json, _, _, _ := store.GetAnalysisCache(ctx, "0xuser")
		if json != "" {
			t.Error("cache should be invalidated")
		}
	})
}

func TestMockStoreErrorInjection(t *testing.T) {
	store := NewMockStore()
	ctx := context.Background()

	t.Run("ErrorOnNext", func(t *testing.T) {
		expectedErr := errors.New("database connection failed")
		store.ErrorOnNext["SaveUserSnapshot"] = expectedErr

		err := store.SaveUserSnapshot(ctx, models.User{ID: "0x1"})
		if err != expectedErr {
			t.Errorf("expected injected error, got %v", err)
		}

		// Second call should succeed
		err = store.SaveUserSnapshot(ctx, models.User{ID: "0x2"})
		if err != nil {
			t.Errorf("second call should succeed, got %v", err)
		}
	})
}

func TestMockStoreFollowedUsers(t *testing.T) {
	store := NewMockStore()
	ctx := context.Background()

	store.FollowedUsers = []string{"0xuser1", "0xuser2", "0xuser3"}

	users, err := store.GetFollowedUserAddresses(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(users) != 3 {
		t.Errorf("expected 3 users, got %d", len(users))
	}
}

func TestMockStoreCopyTradeLog(t *testing.T) {
	store := NewMockStore()
	ctx := context.Background()

	entry := CopyTradeLogEntry{
		FollowingAddress: "0xleader",
		FollowingTradeID: "trade123",
		FollowingTime:    time.Now(),
		FollowingShares:  -100, // negative = buy
		FollowingPrice:   0.50,
		TokenID:          "token123",
		Outcome:          "Yes",
		Status:           "success",
	}

	err := store.SaveCopyTradeLog(ctx, entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(store.CopyTradeLogs) != 1 {
		t.Errorf("expected 1 log entry, got %d", len(store.CopyTradeLogs))
	}
}

func TestMockStoreCallTracking(t *testing.T) {
	store := NewMockStore()
	ctx := context.Background()

	// Make several calls
	store.GetUser(ctx, "0x1")
	store.GetUser(ctx, "0x2")
	store.GetUser(ctx, "0x3")
	store.ListUsers(ctx, models.SubjectPolitics, 10)

	if store.Calls["GetUser"] != 3 {
		t.Errorf("expected 3 GetUser calls, got %d", store.Calls["GetUser"])
	}

	if store.Calls["ListUsers"] != 1 {
		t.Errorf("expected 1 ListUsers call, got %d", store.Calls["ListUsers"])
	}
}
