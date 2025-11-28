package storage

import (
	"context"
	"sync"
	"time"

	"polymarket-analyzer/models"
)

// MockStore is a mock implementation of DataStore for testing
type MockStore struct {
	mu sync.RWMutex

	// Storage maps
	Users           map[string]models.User
	Trades          map[string][]models.TradeDetail
	ProcessedTrades map[string]bool
	TokenCache      map[string]TokenInfo
	TokenIDCache    map[string]string // marketID:outcome -> tokenID
	CopySettings    map[string]*UserCopySettings
	AnalysisCache   map[string]struct {
		JSON       string
		TradeCount int
		CachedAt   time.Time
	}
	Positions     map[string][]models.AggregatedPosition
	CopyPositions map[string]CopyTradePosition // marketID:outcome -> position
	CopyTradeLogs []CopyTradeLogEntry
	FollowedUsers []string

	// Call tracking for assertions
	Calls map[string]int

	// Error injection for testing error paths
	ErrorOnNext map[string]error
}

// NewMockStore creates a new mock store
func NewMockStore() *MockStore {
	return &MockStore{
		Users:           make(map[string]models.User),
		Trades:          make(map[string][]models.TradeDetail),
		ProcessedTrades: make(map[string]bool),
		TokenCache:      make(map[string]TokenInfo),
		TokenIDCache:    make(map[string]string),
		CopySettings:    make(map[string]*UserCopySettings),
		AnalysisCache: make(map[string]struct {
			JSON       string
			TradeCount int
			CachedAt   time.Time
		}),
		Positions:     make(map[string][]models.AggregatedPosition),
		CopyPositions: make(map[string]CopyTradePosition),
		CopyTradeLogs: []CopyTradeLogEntry{},
		FollowedUsers: []string{},
		Calls:         make(map[string]int),
		ErrorOnNext:   make(map[string]error),
	}
}

func (m *MockStore) trackCall(name string) error {
	m.Calls[name]++
	if err, ok := m.ErrorOnNext[name]; ok {
		delete(m.ErrorOnNext, name)
		return err
	}
	return nil
}

func (m *MockStore) Close() error {
	return m.trackCall("Close")
}

func (m *MockStore) SaveUserSnapshot(ctx context.Context, user models.User) error {
	if err := m.trackCall("SaveUserSnapshot"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Users[user.ID] = user
	return nil
}

func (m *MockStore) ReplaceAllUsers(ctx context.Context, users []models.User) error {
	if err := m.trackCall("ReplaceAllUsers"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Users = make(map[string]models.User)
	for _, u := range users {
		m.Users[u.ID] = u
	}
	return nil
}

func (m *MockStore) ListUsers(ctx context.Context, subject models.Subject, limit int) ([]models.User, error) {
	if err := m.trackCall("ListUsers"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]models.User, 0, len(m.Users))
	for _, u := range m.Users {
		result = append(result, u)
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result, nil
}

func (m *MockStore) GetUser(ctx context.Context, userID string) (*models.User, error) {
	if err := m.trackCall("GetUser"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if u, ok := m.Users[userID]; ok {
		return &u, nil
	}
	return nil, nil
}

func (m *MockStore) DeleteUser(ctx context.Context, userID string) error {
	if err := m.trackCall("DeleteUser"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.Users, userID)
	return nil
}

func (m *MockStore) ReplaceTrades(ctx context.Context, trades map[string][]models.TradeDetail) error {
	if err := m.trackCall("ReplaceTrades"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Trades = trades
	return nil
}

func (m *MockStore) SaveTrades(ctx context.Context, trades []models.TradeDetail, markProcessed bool) error {
	if err := m.trackCall("SaveTrades"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range trades {
		m.Trades[t.UserID] = append(m.Trades[t.UserID], t)
		if markProcessed {
			m.ProcessedTrades[t.ID] = true
		}
	}
	return nil
}

func (m *MockStore) ListUserTrades(ctx context.Context, userID string, limit int) ([]models.TradeDetail, error) {
	if err := m.trackCall("ListUserTrades"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	trades := m.Trades[userID]
	if limit > 0 && len(trades) > limit {
		return trades[:limit], nil
	}
	return trades, nil
}

func (m *MockStore) ListUserTradeIDs(ctx context.Context, userID string, limit int) ([]string, error) {
	if err := m.trackCall("ListUserTradeIDs"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	trades := m.Trades[userID]
	ids := make([]string, 0, len(trades))
	for _, t := range trades {
		ids = append(ids, t.ID)
		if limit > 0 && len(ids) >= limit {
			break
		}
	}
	return ids, nil
}

func (m *MockStore) GetUserTradeCount(ctx context.Context, userID string) (int, error) {
	if err := m.trackCall("GetUserTradeCount"); err != nil {
		return 0, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.Trades[userID]), nil
}

func (m *MockStore) SaveAnalysisCache(ctx context.Context, userID string, resultJSON string, tradeCount int) error {
	if err := m.trackCall("SaveAnalysisCache"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.AnalysisCache[userID] = struct {
		JSON       string
		TradeCount int
		CachedAt   time.Time
	}{resultJSON, tradeCount, time.Now()}
	return nil
}

func (m *MockStore) GetAnalysisCache(ctx context.Context, userID string) (string, int, time.Time, error) {
	if err := m.trackCall("GetAnalysisCache"); err != nil {
		return "", 0, time.Time{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if c, ok := m.AnalysisCache[userID]; ok {
		return c.JSON, c.TradeCount, c.CachedAt, nil
	}
	return "", 0, time.Time{}, nil
}

func (m *MockStore) InvalidateAnalysisCache(ctx context.Context, userID string) error {
	if err := m.trackCall("InvalidateAnalysisCache"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.AnalysisCache, userID)
	return nil
}

func (m *MockStore) SaveUserPositions(ctx context.Context, userID string, positions []models.AggregatedPosition) error {
	if err := m.trackCall("SaveUserPositions"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Positions[userID] = positions
	return nil
}

func (m *MockStore) GetUserPositions(ctx context.Context, userID string) ([]models.AggregatedPosition, error) {
	if err := m.trackCall("GetUserPositions"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Positions[userID], nil
}

func (m *MockStore) GetCachedTokens(ctx context.Context, tokenIDs []string) (map[string]TokenInfo, error) {
	if err := m.trackCall("GetCachedTokens"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string]TokenInfo)
	for _, id := range tokenIDs {
		if t, ok := m.TokenCache[id]; ok {
			result[id] = t
		}
	}
	return result, nil
}

func (m *MockStore) SaveTokenCache(ctx context.Context, tokens map[string]TokenInfo) error {
	if err := m.trackCall("SaveTokenCache"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range tokens {
		m.TokenCache[k] = v
	}
	return nil
}

func (m *MockStore) GetTokenInfo(ctx context.Context, tokenID string) (*TokenInfo, error) {
	if err := m.trackCall("GetTokenInfo"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if t, ok := m.TokenCache[tokenID]; ok {
		return &t, nil
	}
	return nil, nil
}

func (m *MockStore) GetTokenByCondition(ctx context.Context, conditionID string) (*TokenInfo, error) {
	if err := m.trackCall("GetTokenByCondition"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, t := range m.TokenCache {
		if t.ConditionID == conditionID {
			return &t, nil
		}
	}
	return nil, nil
}

func (m *MockStore) GetTokenByConditionAndOutcome(ctx context.Context, conditionID, outcome string) (*TokenInfo, error) {
	if err := m.trackCall("GetTokenByConditionAndOutcome"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, t := range m.TokenCache {
		if t.ConditionID == conditionID && t.Outcome == outcome {
			return &t, nil
		}
	}
	return nil, nil
}

func (m *MockStore) SaveTokenInfo(ctx context.Context, tokenID, conditionID, outcome, title, slug string) error {
	if err := m.trackCall("SaveTokenInfo"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.TokenCache[tokenID] = TokenInfo{
		TokenID:     tokenID,
		ConditionID: conditionID,
		Outcome:     outcome,
		Title:       title,
		Slug:        slug,
	}
	return nil
}

func (m *MockStore) GetUserCopySettings(ctx context.Context, userAddress string) (*UserCopySettings, error) {
	if err := m.trackCall("GetUserCopySettings"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.CopySettings[userAddress], nil
}

func (m *MockStore) SetUserCopySettings(ctx context.Context, settings UserCopySettings) error {
	if err := m.trackCall("SetUserCopySettings"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CopySettings[settings.UserAddress] = &settings
	return nil
}

func (m *MockStore) GetUserAnalyticsList(ctx context.Context, filter UserAnalyticsFilter) ([]UserAnalyticsRecord, int, error) {
	if err := m.trackCall("GetUserAnalyticsList"); err != nil {
		return nil, 0, err
	}
	return []UserAnalyticsRecord{}, 0, nil
}

func (m *MockStore) InvalidateUserListCache(ctx context.Context) error {
	return m.trackCall("InvalidateUserListCache")
}

func (m *MockStore) GetRedisValue(ctx context.Context, key string) (string, error) {
	if err := m.trackCall("GetRedisValue"); err != nil {
		return "", err
	}
	return "", nil
}

// Additional methods needed by CopyTrader

func (m *MockStore) GetUnprocessedTrades(ctx context.Context, limit int) ([]models.TradeDetail, error) {
	if err := m.trackCall("GetUnprocessedTrades"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []models.TradeDetail
	for _, trades := range m.Trades {
		for _, t := range trades {
			if !m.ProcessedTrades[t.ID] {
				result = append(result, t)
				if limit > 0 && len(result) >= limit {
					return result, nil
				}
			}
		}
	}
	return result, nil
}

func (m *MockStore) MarkTradeProcessed(ctx context.Context, tradeID string) error {
	if err := m.trackCall("MarkTradeProcessed"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ProcessedTrades[tradeID] = true
	return nil
}

func (m *MockStore) GetTokenIDFromCache(ctx context.Context, marketID, outcome string) (string, bool, error) {
	if err := m.trackCall("GetTokenIDFromCache"); err != nil {
		return "", false, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := marketID + ":" + outcome
	if tokenID, ok := m.TokenIDCache[key]; ok {
		return tokenID, false, nil // negRisk hardcoded to false
	}
	return "", false, nil
}

func (m *MockStore) CacheTokenID(ctx context.Context, marketID, outcome, tokenID string, negRisk bool) error {
	if err := m.trackCall("CacheTokenID"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := marketID + ":" + outcome
	m.TokenIDCache[key] = tokenID
	return nil
}

func (m *MockStore) GetFollowedUserAddresses(ctx context.Context) ([]string, error) {
	if err := m.trackCall("GetFollowedUserAddresses"); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.FollowedUsers, nil
}

func (m *MockStore) SaveCopyTradeLog(ctx context.Context, entry CopyTradeLogEntry) error {
	if err := m.trackCall("SaveCopyTradeLog"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CopyTradeLogs = append(m.CopyTradeLogs, entry)
	return nil
}

func (m *MockStore) GetMyPosition(ctx context.Context, marketID, outcome string) (CopyTradePosition, error) {
	if err := m.trackCall("GetMyPosition"); err != nil {
		return CopyTradePosition{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := marketID + ":" + outcome
	return m.CopyPositions[key], nil
}

func (m *MockStore) UpdateMyPosition(ctx context.Context, pos interface{}) error {
	if err := m.trackCall("UpdateMyPosition"); err != nil {
		return err
	}
	return nil
}

// Verify MockStore implements DataStore
var _ DataStore = (*MockStore)(nil)
