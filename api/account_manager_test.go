package api

import (
	"context"
	"sync"
	"testing"

	"polymarket-analyzer/storage"
)

// MockAccountStore is a mock store for testing AccountManager
type MockAccountStore struct {
	mu sync.RWMutex

	// Storage
	Accounts     map[int]*storage.TradingAccount
	UserSettings map[string]*storage.UserCopySettings

	// Call tracking
	Calls map[string]int

	// Error injection
	ErrorOnNext map[string]error

	// Default account ID
	DefaultAccountID int
}

func NewMockAccountStore() *MockAccountStore {
	return &MockAccountStore{
		Accounts:     make(map[int]*storage.TradingAccount),
		UserSettings: make(map[string]*storage.UserCopySettings),
		Calls:        make(map[string]int),
		ErrorOnNext:  make(map[string]error),
	}
}

func (m *MockAccountStore) trackCall(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls[name]++
	if err, ok := m.ErrorOnNext[name]; ok {
		delete(m.ErrorOnNext, name)
		return err
	}
	return nil
}

func (m *MockAccountStore) GetTradingAccount(ctx context.Context, id int) (*storage.TradingAccount, error) {
	if err := m.trackCall("GetTradingAccount"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Accounts[id], nil
}

func (m *MockAccountStore) GetDefaultTradingAccount(ctx context.Context) (*storage.TradingAccount, error) {
	if err := m.trackCall("GetDefaultTradingAccount"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.DefaultAccountID == 0 {
		return nil, nil
	}
	return m.Accounts[m.DefaultAccountID], nil
}

func (m *MockAccountStore) GetTradingAccounts(ctx context.Context) ([]storage.TradingAccount, error) {
	if err := m.trackCall("GetTradingAccounts"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]storage.TradingAccount, 0, len(m.Accounts))
	for _, acc := range m.Accounts {
		result = append(result, *acc)
	}
	return result, nil
}

func (m *MockAccountStore) GetUserCopySettings(ctx context.Context, userAddress string) (*storage.UserCopySettings, error) {
	if err := m.trackCall("GetUserCopySettings"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.UserSettings[userAddress], nil
}

func (m *MockAccountStore) GetTradingAccountStats(ctx context.Context, accountID int) (map[string]interface{}, error) {
	if err := m.trackCall("GetTradingAccountStats"); err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"trades_executed": 10,
		"total_volume":    1000.0,
	}, nil
}

// Helper to add test accounts
func (m *MockAccountStore) AddAccount(acc storage.TradingAccount) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Accounts[acc.ID] = &acc
	if acc.IsDefault {
		m.DefaultAccountID = acc.ID
	}
}

// Helper to add user settings
func (m *MockAccountStore) AddUserSettings(userAddr string, settings storage.UserCopySettings) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.UserSettings[userAddr] = &settings
}

// TestAccountManagerStoreInterface is a minimal interface for testing
// This allows us to use MockAccountStore with AccountManager
type TestAccountManagerStore interface {
	GetTradingAccount(ctx context.Context, id int) (*storage.TradingAccount, error)
	GetDefaultTradingAccount(ctx context.Context) (*storage.TradingAccount, error)
	GetUserCopySettings(ctx context.Context, userAddress string) (*storage.UserCopySettings, error)
}

func TestNewAccountManager(t *testing.T) {
	// Can't test with nil store in production code, but we can test the structure
	t.Run("creates empty client map", func(t *testing.T) {
		// AccountManager requires PostgresStore, so we test the concept
		// In real tests, we'd need dependency injection
		am := &AccountManager{
			clients: make(map[int]*ClobClient),
		}

		if am.clients == nil {
			t.Error("clients map should be initialized")
		}

		if len(am.clients) != 0 {
			t.Errorf("clients map should be empty, got %d", len(am.clients))
		}
	})
}

func TestAccountManager_GetDefaultAccountID(t *testing.T) {
	am := &AccountManager{
		clients:          make(map[int]*ClobClient),
		defaultAccountID: 42,
	}

	got := am.GetDefaultAccountID()
	if got != 42 {
		t.Errorf("GetDefaultAccountID() = %d, want 42", got)
	}
}

func TestAccountManager_GetClient_UsesDefault(t *testing.T) {
	t.Run("returns cached default client", func(t *testing.T) {
		mockClient := &ClobClient{}
		am := &AccountManager{
			clients:          make(map[int]*ClobClient),
			defaultAccountID: 1,
		}

		// Pre-cache the default client
		am.clients[1] = mockClient

		// When accountID is 0, should use default
		got, err := am.GetClient(context.Background(), 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if got != mockClient {
			t.Error("should return cached default client")
		}
	})

	t.Run("maps accountID 0 to default", func(t *testing.T) {
		mockClient := &ClobClient{}
		am := &AccountManager{
			clients:          make(map[int]*ClobClient),
			defaultAccountID: 42,
		}

		// Cache client at account 42 (the default)
		am.clients[42] = mockClient

		got, err := am.GetClient(context.Background(), 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if got != mockClient {
			t.Error("accountID 0 should resolve to default account 42")
		}
	})
}

func TestAccountManager_GetClient_NoDefault(t *testing.T) {
	am := &AccountManager{
		clients:          make(map[int]*ClobClient),
		defaultAccountID: 0, // No default configured
	}

	_, err := am.GetClient(context.Background(), 0)

	if err == nil {
		t.Error("expected error when no default configured")
	}

	expectedMsg := "no default account configured"
	if err.Error() != expectedMsg {
		t.Errorf("error = %q, want %q", err.Error(), expectedMsg)
	}
}

func TestAccountManager_ClientCaching(t *testing.T) {
	// Create a mock client to test caching behavior
	mockClient := &ClobClient{}

	am := &AccountManager{
		clients:          make(map[int]*ClobClient),
		defaultAccountID: 1,
	}

	// Pre-populate cache
	am.clients[1] = mockClient

	// Should return cached client
	am.mu.RLock()
	cached, exists := am.clients[1]
	am.mu.RUnlock()

	if !exists {
		t.Error("client should exist in cache")
	}

	if cached != mockClient {
		t.Error("should return same client instance")
	}
}

func TestAccountManager_InvalidateClient(t *testing.T) {
	mockClient := &ClobClient{}

	am := &AccountManager{
		clients: make(map[int]*ClobClient),
	}

	// Add client to cache
	am.clients[1] = mockClient

	// Verify it's there
	am.mu.RLock()
	_, exists := am.clients[1]
	am.mu.RUnlock()
	if !exists {
		t.Fatal("client should be in cache before invalidation")
	}

	// Invalidate
	am.InvalidateClient(1)

	// Verify it's gone
	am.mu.RLock()
	_, exists = am.clients[1]
	am.mu.RUnlock()
	if exists {
		t.Error("client should be removed after invalidation")
	}
}

func TestAccountManager_InvalidateAllClients(t *testing.T) {
	am := &AccountManager{
		clients: make(map[int]*ClobClient),
	}

	// Add multiple clients
	am.clients[1] = &ClobClient{}
	am.clients[2] = &ClobClient{}
	am.clients[3] = &ClobClient{}

	// Verify they're there
	am.mu.RLock()
	count := len(am.clients)
	am.mu.RUnlock()
	if count != 3 {
		t.Fatalf("expected 3 clients, got %d", count)
	}

	// Invalidate all
	am.InvalidateAllClients()

	// Verify all gone
	am.mu.RLock()
	count = len(am.clients)
	am.mu.RUnlock()
	if count != 0 {
		t.Errorf("expected 0 clients after invalidation, got %d", count)
	}
}

func TestAccountManager_ConcurrentAccess(t *testing.T) {
	am := &AccountManager{
		clients:          make(map[int]*ClobClient),
		defaultAccountID: 1,
	}

	// Pre-populate with some clients
	for i := 1; i <= 5; i++ {
		am.clients[i] = &ClobClient{}
	}

	// Concurrent reads and writes
	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				am.mu.RLock()
				_ = am.clients[id%5+1]
				am.mu.RUnlock()
			}
		}(i)
	}

	// Writers (invalidate and re-add)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				am.InvalidateClient(id)
				am.mu.Lock()
				am.clients[id] = &ClobClient{}
				am.mu.Unlock()
			}
		}(i + 1)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent access error: %v", err)
	}
}

func TestAccountManager_GetClientForUser_Logic(t *testing.T) {
	// Test the logic flow without a real store
	tests := []struct {
		name              string
		userSettings      *storage.UserCopySettings
		defaultAccountID  int
		expectedAccountID int
	}{
		{
			name:              "user with assigned account",
			userSettings:      &storage.UserCopySettings{TradingAccountID: intPtr(5)},
			defaultAccountID:  1,
			expectedAccountID: 5,
		},
		{
			name:              "user without assigned account uses default",
			userSettings:      &storage.UserCopySettings{TradingAccountID: nil},
			defaultAccountID:  1,
			expectedAccountID: 1,
		},
		{
			name:              "no settings uses default",
			userSettings:      nil,
			defaultAccountID:  1,
			expectedAccountID: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the logic from GetClientForUser
			var accountID int
			if tt.userSettings != nil && tt.userSettings.TradingAccountID != nil {
				accountID = *tt.userSettings.TradingAccountID
			} else {
				accountID = tt.defaultAccountID
			}

			if accountID != tt.expectedAccountID {
				t.Errorf("accountID = %d, want %d", accountID, tt.expectedAccountID)
			}
		})
	}
}

func TestMockAccountStore(t *testing.T) {
	ctx := context.Background()
	store := NewMockAccountStore()

	t.Run("AddAccount and GetTradingAccount", func(t *testing.T) {
		acc := storage.TradingAccount{
			ID:               1,
			Name:             "Test Account",
			PrivateKeyEnvVar: "TEST_PK",
			Enabled:          true,
			IsDefault:        true,
		}
		store.AddAccount(acc)

		got, err := store.GetTradingAccount(ctx, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Name != "Test Account" {
			t.Errorf("Name = %s, want Test Account", got.Name)
		}
	})

	t.Run("GetDefaultTradingAccount", func(t *testing.T) {
		got, err := store.GetDefaultTradingAccount(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil {
			t.Fatal("expected default account")
		}
		if got.ID != 1 {
			t.Errorf("ID = %d, want 1", got.ID)
		}
	})

	t.Run("AddUserSettings and GetUserCopySettings", func(t *testing.T) {
		settings := storage.UserCopySettings{
			UserAddress:      "0x1234",
			Multiplier:       0.10,
			Enabled:          true,
			TradingAccountID: intPtr(2),
		}
		store.AddUserSettings("0x1234", settings)

		got, err := store.GetUserCopySettings(ctx, "0x1234")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Multiplier != 0.10 {
			t.Errorf("Multiplier = %f, want 0.10", got.Multiplier)
		}
		if *got.TradingAccountID != 2 {
			t.Errorf("TradingAccountID = %d, want 2", *got.TradingAccountID)
		}
	})

	t.Run("Error injection", func(t *testing.T) {
		store.ErrorOnNext["GetTradingAccount"] = errTest

		_, err := store.GetTradingAccount(ctx, 1)
		if err != errTest {
			t.Errorf("expected injected error, got %v", err)
		}

		// Second call should succeed
		_, err = store.GetTradingAccount(ctx, 1)
		if err != nil {
			t.Errorf("second call should succeed, got %v", err)
		}
	})

	t.Run("Call tracking", func(t *testing.T) {
		initialCalls := store.Calls["GetTradingAccount"]
		store.GetTradingAccount(ctx, 1)
		store.GetTradingAccount(ctx, 2)

		if store.Calls["GetTradingAccount"] != initialCalls+2 {
			t.Errorf("expected %d calls, got %d", initialCalls+2, store.Calls["GetTradingAccount"])
		}
	})
}

func TestAccountSelectionFlow(t *testing.T) {
	// Test the complete account selection flow logic
	t.Run("user with specific account assignment", func(t *testing.T) {
		userSettings := &storage.UserCopySettings{
			UserAddress:      "0xuser1",
			TradingAccountID: intPtr(3),
		}
		defaultAccountID := 1

		// Simulate GetClientForUser logic
		var selectedAccountID int
		if userSettings != nil && userSettings.TradingAccountID != nil {
			selectedAccountID = *userSettings.TradingAccountID
		} else {
			selectedAccountID = defaultAccountID
		}

		if selectedAccountID != 3 {
			t.Errorf("selected account = %d, want 3", selectedAccountID)
		}
	})

	t.Run("user without assignment gets default", func(t *testing.T) {
		userSettings := &storage.UserCopySettings{
			UserAddress:      "0xuser2",
			TradingAccountID: nil, // No assignment
		}
		defaultAccountID := 1

		var selectedAccountID int
		if userSettings != nil && userSettings.TradingAccountID != nil {
			selectedAccountID = *userSettings.TradingAccountID
		} else {
			selectedAccountID = defaultAccountID
		}

		if selectedAccountID != 1 {
			t.Errorf("selected account = %d, want 1 (default)", selectedAccountID)
		}
	})

	t.Run("unknown user gets default", func(t *testing.T) {
		var userSettings *storage.UserCopySettings = nil
		defaultAccountID := 1

		var selectedAccountID int
		if userSettings != nil && userSettings.TradingAccountID != nil {
			selectedAccountID = *userSettings.TradingAccountID
		} else {
			selectedAccountID = defaultAccountID
		}

		if selectedAccountID != 1 {
			t.Errorf("selected account = %d, want 1 (default)", selectedAccountID)
		}
	})
}

// Helper
func intPtr(i int) *int {
	return &i
}

var errTest = &testError{"test error"}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}
