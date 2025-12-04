package storage

import (
	"context"
	"time"

	"polymarket-analyzer/models"
)

// DataStore defines the interface for storage backends
type DataStore interface {
	Close() error

	// User operations
	SaveUserSnapshot(ctx context.Context, user models.User) error
	ReplaceAllUsers(ctx context.Context, users []models.User) error
	ListUsers(ctx context.Context, subject models.Subject, limit int) ([]models.User, error)
	GetUser(ctx context.Context, userID string) (*models.User, error)
	DeleteUser(ctx context.Context, userID string) error

	// Trade operations
	ReplaceTrades(ctx context.Context, trades map[string][]models.TradeDetail) error
	SaveTrades(ctx context.Context, trades []models.TradeDetail, markProcessed bool) error
	ListUserTrades(ctx context.Context, userID string, limit int) ([]models.TradeDetail, error)
	ListUserTradeIDs(ctx context.Context, userID string, limit int) ([]string, error) // Lightweight - only IDs
	GetUserTradeCount(ctx context.Context, userID string) (int, error)

	// Analysis cache operations
	SaveAnalysisCache(ctx context.Context, userID string, resultJSON string, tradeCount int) error
	GetAnalysisCache(ctx context.Context, userID string) (string, int, time.Time, error)
	InvalidateAnalysisCache(ctx context.Context, userID string) error

	// Position operations
	SaveUserPositions(ctx context.Context, userID string, positions []models.AggregatedPosition) error
	GetUserPositions(ctx context.Context, userID string) ([]models.AggregatedPosition, error)

	// Token cache operations
	GetCachedTokens(ctx context.Context, tokenIDs []string) (map[string]TokenInfo, error)
	SaveTokenCache(ctx context.Context, tokens map[string]TokenInfo) error
	GetTokenInfo(ctx context.Context, tokenID string) (*TokenInfo, error)
	GetTokenByCondition(ctx context.Context, conditionID string) (*TokenInfo, error)

	// Copy trading settings
	GetUserCopySettings(ctx context.Context, userAddress string) (*UserCopySettings, error)
	SetUserCopySettings(ctx context.Context, settings UserCopySettings) error

	// Trading accounts
	GetTradingAccounts(ctx context.Context) ([]TradingAccount, error)
	GetTradingAccount(ctx context.Context, id int) (*TradingAccount, error)
	GetDefaultTradingAccount(ctx context.Context) (*TradingAccount, error)
	CreateTradingAccount(ctx context.Context, account TradingAccount) (*TradingAccount, error)
	UpdateTradingAccount(ctx context.Context, account TradingAccount) (*TradingAccount, error)
	DeleteTradingAccount(ctx context.Context, id int) error
	GetTradingAccountStats(ctx context.Context, accountID int) (map[string]interface{}, error)

	// Analytics
	GetUserAnalyticsList(ctx context.Context, filter UserAnalyticsFilter) ([]UserAnalyticsRecord, int, error)

	// Cache invalidation
	InvalidateUserListCache(ctx context.Context) error

	// Redis operations (for metrics and caching)
	GetRedisValue(ctx context.Context, key string) (string, error)
}

// Ensure both implementations satisfy the interface
var _ DataStore = (*Store)(nil)
var _ DataStore = (*PostgresStore)(nil)
