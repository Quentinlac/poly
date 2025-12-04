package api

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"polymarket-analyzer/storage"
)

// AccountManager manages multiple trading accounts and their ClobClients.
// It creates ClobClients on demand and caches them for reuse.
type AccountManager struct {
	store   *storage.PostgresStore
	clients map[int]*ClobClient // accountID -> ClobClient
	mu      sync.RWMutex

	// Default account for backward compatibility
	defaultAccountID int
}

// NewAccountManager creates a new account manager
func NewAccountManager(store *storage.PostgresStore) *AccountManager {
	return &AccountManager{
		store:   store,
		clients: make(map[int]*ClobClient),
	}
}

// Initialize loads the default account and pre-warms clients for enabled accounts
func (am *AccountManager) Initialize(ctx context.Context) error {
	// Get default account
	defaultAcc, err := am.store.GetDefaultTradingAccount(ctx)
	if err != nil {
		return fmt.Errorf("failed to get default account: %w", err)
	}
	if defaultAcc == nil {
		log.Println("[AccountManager] Warning: No default trading account configured")
		return nil
	}

	am.defaultAccountID = defaultAcc.ID
	log.Printf("[AccountManager] Default account: %s (ID: %d)", defaultAcc.Name, defaultAcc.ID)

	// Pre-warm the default account client
	_, err = am.GetClient(ctx, defaultAcc.ID)
	if err != nil {
		return fmt.Errorf("failed to initialize default account client: %w", err)
	}

	return nil
}

// GetClient returns a ClobClient for the specified account ID.
// If accountID is 0 or nil, returns the default account client.
// Creates and caches the client if it doesn't exist.
func (am *AccountManager) GetClient(ctx context.Context, accountID int) (*ClobClient, error) {
	// Use default account if not specified
	if accountID == 0 {
		accountID = am.defaultAccountID
	}
	if accountID == 0 {
		return nil, fmt.Errorf("no default account configured")
	}

	// Check cache first
	am.mu.RLock()
	client, exists := am.clients[accountID]
	am.mu.RUnlock()

	if exists && client != nil {
		return client, nil
	}

	// Need to create a new client
	am.mu.Lock()
	defer am.mu.Unlock()

	// Double-check after acquiring write lock
	if client, exists = am.clients[accountID]; exists && client != nil {
		return client, nil
	}

	// Get account details
	account, err := am.store.GetTradingAccount(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to get account %d: %w", accountID, err)
	}
	if account == nil {
		return nil, fmt.Errorf("account %d not found", accountID)
	}
	if !account.Enabled {
		return nil, fmt.Errorf("account %d (%s) is disabled", accountID, account.Name)
	}

	// Create client for this account
	client, err = am.createClientForAccount(ctx, account)
	if err != nil {
		return nil, fmt.Errorf("failed to create client for account %d (%s): %w", accountID, account.Name, err)
	}

	// Cache it
	am.clients[accountID] = client
	log.Printf("[AccountManager] Created client for account %d (%s)", accountID, account.Name)

	return client, nil
}

// GetClientForUser returns a ClobClient for a specific followed user based on their copy settings.
// If the user has no specific account assigned, uses the default account.
func (am *AccountManager) GetClientForUser(ctx context.Context, userAddress string) (*ClobClient, int, error) {
	settings, err := am.store.GetUserCopySettings(ctx, userAddress)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get user settings: %w", err)
	}

	var accountID int
	if settings != nil && settings.TradingAccountID != nil {
		accountID = *settings.TradingAccountID
	} else {
		accountID = am.defaultAccountID
	}

	client, err := am.GetClient(ctx, accountID)
	return client, accountID, err
}

// GetDefaultClient returns the default account's ClobClient
func (am *AccountManager) GetDefaultClient(ctx context.Context) (*ClobClient, error) {
	return am.GetClient(ctx, am.defaultAccountID)
}

// GetDefaultAccountID returns the default account ID
func (am *AccountManager) GetDefaultAccountID() int {
	return am.defaultAccountID
}

// createClientForAccount creates a ClobClient for a specific account
func (am *AccountManager) createClientForAccount(ctx context.Context, account *storage.TradingAccount) (*ClobClient, error) {
	// Get private key from env var
	privateKeyEnvVar := account.PrivateKeyEnvVar
	if privateKeyEnvVar == "" {
		return nil, fmt.Errorf("account has no private key env var configured")
	}

	// Create auth from env var
	auth, err := NewAuthFromEnvVar(privateKeyEnvVar)
	if err != nil {
		return nil, fmt.Errorf("failed to create auth from %s: %w", privateKeyEnvVar, err)
	}

	// Create CLOB client
	clobClient, err := NewClobClient("", auth)
	if err != nil {
		return nil, fmt.Errorf("failed to create CLOB client: %w", err)
	}

	// Configure for Magic/Email wallet if funder address env var is set
	if account.FunderAddressEnvVar != nil && *account.FunderAddressEnvVar != "" {
		funderAddress := strings.TrimSpace(os.Getenv(*account.FunderAddressEnvVar))
		if funderAddress != "" {
			clobClient.SetFunder(funderAddress)
			clobClient.SetSignatureType(account.SignatureType)
			log.Printf("[AccountManager] Account %d (%s): Magic wallet mode, funder=%s", account.ID, account.Name, funderAddress[:10]+"...")
		}
	}

	// Derive API credentials
	derivCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if _, err := clobClient.DeriveAPICreds(derivCtx); err != nil {
		return nil, fmt.Errorf("failed to derive API creds: %w", err)
	}

	return clobClient, nil
}

// InvalidateClient removes a client from the cache, forcing it to be recreated on next use
func (am *AccountManager) InvalidateClient(accountID int) {
	am.mu.Lock()
	defer am.mu.Unlock()
	delete(am.clients, accountID)
	log.Printf("[AccountManager] Invalidated client for account %d", accountID)
}

// InvalidateAllClients clears all cached clients
func (am *AccountManager) InvalidateAllClients() {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.clients = make(map[int]*ClobClient)
	log.Println("[AccountManager] Invalidated all clients")
}

// GetAccountInfo returns information about a trading account including resolved wallet address
func (am *AccountManager) GetAccountInfo(ctx context.Context, accountID int) (map[string]interface{}, error) {
	account, err := am.store.GetTradingAccount(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if account == nil {
		return nil, fmt.Errorf("account not found")
	}

	info := map[string]interface{}{
		"id":             account.ID,
		"name":           account.Name,
		"enabled":        account.Enabled,
		"is_default":     account.IsDefault,
		"signature_type": account.SignatureType,
		"created_at":     account.CreatedAt,
	}

	// Try to resolve wallet address from env vars
	if account.FunderAddressEnvVar != nil && *account.FunderAddressEnvVar != "" {
		if addr := os.Getenv(*account.FunderAddressEnvVar); addr != "" {
			info["wallet_address"] = strings.ToLower(strings.TrimSpace(addr))
		}
	} else {
		// For EOA wallets, derive address from private key
		if auth, err := NewAuthFromEnvVar(account.PrivateKeyEnvVar); err == nil {
			info["wallet_address"] = strings.ToLower(auth.GetAddress().Hex())
		}
	}

	// Check if env vars are configured
	info["private_key_configured"] = os.Getenv(account.PrivateKeyEnvVar) != ""
	if account.FunderAddressEnvVar != nil {
		info["funder_address_configured"] = os.Getenv(*account.FunderAddressEnvVar) != ""
	}

	return info, nil
}

// ListAccountsWithStatus returns all accounts with their status and wallet addresses
func (am *AccountManager) ListAccountsWithStatus(ctx context.Context) ([]map[string]interface{}, error) {
	accounts, err := am.store.GetTradingAccounts(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]map[string]interface{}, 0, len(accounts))
	for _, acc := range accounts {
		info, err := am.GetAccountInfo(ctx, acc.ID)
		if err != nil {
			// Include account even if we can't get full info
			info = map[string]interface{}{
				"id":      acc.ID,
				"name":    acc.Name,
				"enabled": acc.Enabled,
				"error":   err.Error(),
			}
		}

		// Add stats
		stats, _ := am.store.GetTradingAccountStats(ctx, acc.ID)
		if stats != nil {
			info["stats"] = stats
		}

		// Check if client is cached
		am.mu.RLock()
		_, cached := am.clients[acc.ID]
		am.mu.RUnlock()
		info["client_cached"] = cached

		result = append(result, info)
	}

	return result, nil
}
