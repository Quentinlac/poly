package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"polymarket-analyzer/models"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// PostgresStore wraps PostgreSQL persistence with Redis caching
type PostgresStore struct {
	pool  *pgxpool.Pool
	redis *redis.Client
}

// NewPostgres creates a new PostgreSQL store with connection pooling and Redis cache
func NewPostgres() (*PostgresStore, error) {
	// Build PostgreSQL connection string
	host := getEnv("POSTGRES_HOST", "localhost")
	port := getEnv("POSTGRES_PORT", "5432")
	user := getEnv("POSTGRES_USER", "polymarket")
	password := getEnv("POSTGRES_PASSWORD", "polymarket123")
	dbname := getEnv("POSTGRES_DB", "polymarket")

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?pool_max_conns=50&pool_min_conns=10",
		user, password, host, port, dbname)

	// Configure connection pool
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse config: %w", err)
	}

	// Pool settings for high-frequency trading
	config.MaxConns = 50
	config.MinConns = 10
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute
	config.HealthCheckPeriod = 30 * time.Second

	// Add query timeout to prevent slow queries from hanging
	config.ConnConfig.RuntimeParams["statement_timeout"] = "30000"      // 30 seconds max per query
	config.ConnConfig.RuntimeParams["lock_timeout"] = "10000"           // 10 seconds max for locks
	config.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "60000" // 60 seconds

	// Create pool
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("postgres: create pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}

	// Initialize Redis
	redisHost := getEnv("REDIS_HOST", "localhost")
	redisPort := getEnv("REDIS_PORT", "6379")
	redisPassword := getEnv("REDIS_PASSWORD", "")

	rdb := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", redisHost, redisPort),
		Password:     redisPassword,
		DB:           0,
		PoolSize:     100,
		MinIdleConns: 10,
		MaxRetries:   3,
	})

	// Test Redis connection
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		pool.Close()
		return nil, fmt.Errorf("redis: ping: %w", err)
	}

	return &PostgresStore{
		pool:  pool,
		redis: rdb,
	}, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Close releases database connections
func (s *PostgresStore) Close() error {
	if s.redis != nil {
		s.redis.Close()
	}
	if s.pool != nil {
		s.pool.Close()
	}
	return nil
}

// SaveUserSnapshot upserts a single user
func (s *PostgresStore) SaveUserSnapshot(ctx context.Context, user models.User) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := s.saveUserTx(ctx, tx, user); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// Invalidate Redis cache for this user
	s.redis.Del(ctx, fmt.Sprintf("user:%s", user.ID))

	// Invalidate all user list caches (users:* pattern)
	if keys, err := s.redis.Keys(ctx, "users:*").Result(); err == nil && len(keys) > 0 {
		s.redis.Del(ctx, keys...)
	}

	return nil
}

// ReplaceAllUsers overwrites all leaderboard entries
func (s *PostgresStore) ReplaceAllUsers(ctx context.Context, users []models.User) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Delete existing data
	if _, err := tx.Exec(ctx, `DELETE FROM user_subject_metrics`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM user_red_flags`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM users`); err != nil {
		return err
	}

	for _, user := range users {
		if err := s.saveUserTx(ctx, tx, user); err != nil {
			return err
		}
	}

	// Clear all caches
	s.redis.FlushDB(ctx)

	return tx.Commit(ctx)
}

// SaveTrades upserts a batch of trades using batch insert
// If markProcessed is true, trades are also marked as processed to skip copy trading
func (s *PostgresStore) SaveTrades(ctx context.Context, trades []models.TradeDetail, markProcessed bool) error {
	if len(trades) == 0 {
		return nil
	}

	// Use batch for efficiency
	batch := &pgx.Batch{}

	for _, trade := range trades {
		isMaker := trade.Role == "MAKER"
		batch.Queue(`
			INSERT INTO user_trades (
				id, user_id, market_id, subject, type, side, is_maker, size, usdc_size, price, outcome,
				timestamp, title, slug, event_slug, transaction_hash, name, pseudonym, inserted_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, NOW())
			ON CONFLICT (id, timestamp) DO UPDATE SET
				user_id = EXCLUDED.user_id,
				market_id = EXCLUDED.market_id,
				subject = EXCLUDED.subject,
				type = EXCLUDED.type,
				side = EXCLUDED.side,
				is_maker = EXCLUDED.is_maker,
				size = EXCLUDED.size,
				usdc_size = EXCLUDED.usdc_size,
				price = EXCLUDED.price,
				outcome = EXCLUDED.outcome,
				title = EXCLUDED.title,
				slug = EXCLUDED.slug,
				event_slug = EXCLUDED.event_slug,
				transaction_hash = EXCLUDED.transaction_hash,
				name = EXCLUDED.name,
				pseudonym = EXCLUDED.pseudonym
		`,
			trade.ID, trade.UserID, trade.MarketID, string(trade.Subject), trade.Type, trade.Side,
			isMaker, trade.Size, trade.UsdcSize, trade.Price, trade.Outcome, trade.Timestamp,
			trade.Title, trade.Slug, trade.EventSlug, trade.TransactionHash, trade.Name, trade.Pseudonym,
		)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch exec %d: %w", i, err)
		}
	}

	// Mark trades as processed if requested (skip copy trading for historical trades)
	if markProcessed {
		for _, trade := range trades {
			if err := s.MarkTradeProcessed(ctx, trade.ID); err != nil {
				return fmt.Errorf("mark trade processed %s: %w", trade.ID, err)
			}
		}
	}

	// Invalidate user caches
	userIDs := make(map[string]bool)
	for _, trade := range trades {
		userIDs[trade.UserID] = true
	}
	for userID := range userIDs {
		s.redis.Del(ctx, fmt.Sprintf("trades:%s", userID))
		s.redis.Del(ctx, fmt.Sprintf("analysis:%s", userID))
	}

	return nil
}

// SaveGlobalTrades saves trades to global_trades table in smaller batches to avoid timeouts
func (s *PostgresStore) SaveGlobalTrades(ctx context.Context, trades []models.TradeDetail) error {
	if len(trades) == 0 {
		return nil
	}

	// Process in batches of 100 to avoid database timeouts
	const batchSize = 100
	for start := 0; start < len(trades); start += batchSize {
		end := start + batchSize
		if end > len(trades) {
			end = len(trades)
		}

		chunk := trades[start:end]
		if err := s.saveGlobalTradesBatch(ctx, chunk); err != nil {
			return fmt.Errorf("batch %d-%d: %w", start, end, err)
		}
	}

	return nil
}

// saveGlobalTradesBatch saves a small batch of trades
func (s *PostgresStore) saveGlobalTradesBatch(ctx context.Context, trades []models.TradeDetail) error {
	batch := &pgx.Batch{}

	for _, trade := range trades {
		batch.Queue(`
			INSERT INTO global_trades (
				id, user_address, asset, type, side, size, usdc_size, price, outcome,
				timestamp, title, slug, transaction_hash, role, inserted_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
			ON CONFLICT (id, timestamp) DO NOTHING
		`,
			trade.ID, trade.UserID, trade.MarketID, trade.Type, trade.Side, trade.Size,
			trade.UsdcSize, trade.Price, trade.Outcome, trade.Timestamp, trade.Title,
			trade.Slug, trade.TransactionHash, trade.Role,
		)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("exec %d: %w", i, err)
		}
	}

	return nil
}

// ListUserTrades returns trades with Redis caching
func (s *PostgresStore) ListUserTrades(ctx context.Context, userID string, limit int) ([]models.TradeDetail, error) {
	if limit <= 0 {
		limit = 200
	}

	// Check Redis cache first
	cacheKey := fmt.Sprintf("trades:%s:%d", userID, limit)
	cached, err := s.redis.Get(ctx, cacheKey).Bytes()
	if err == nil {
		var trades []models.TradeDetail
		if json.Unmarshal(cached, &trades) == nil {
			return trades, nil
		}
	}

	// Query PostgreSQL
	rows, err := s.pool.Query(ctx, `
		SELECT id, user_id, market_id, subject, type, side, is_maker, size, usdc_size, price, outcome,
			   timestamp, title, slug, event_slug, transaction_hash, name, pseudonym, inserted_at
		FROM user_trades
		WHERE user_id = $1
		ORDER BY timestamp DESC
		LIMIT $2`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var trades []models.TradeDetail
	for rows.Next() {
		var trade models.TradeDetail
		var subject, tradeType string
		var insertedAt *time.Time
		var isMaker bool
		var eventSlug *string

		if err := rows.Scan(
			&trade.ID, &trade.UserID, &trade.MarketID, &subject, &tradeType, &trade.Side,
			&isMaker, &trade.Size, &trade.UsdcSize, &trade.Price, &trade.Outcome,
			&trade.Timestamp, &trade.Title, &trade.Slug, &eventSlug,
			&trade.TransactionHash, &trade.Name, &trade.Pseudonym, &insertedAt,
		); err != nil {
			return nil, err
		}

		trade.Subject = models.Subject(subject)
		trade.Type = tradeType
		if isMaker {
			trade.Role = "MAKER"
		} else {
			trade.Role = "TAKER"
		}
		if insertedAt != nil {
			trade.InsertedAt = *insertedAt
		}
		if eventSlug != nil {
			trade.EventSlug = *eventSlug
		}
		trades = append(trades, trade)
	}

	// Cache for 2 minutes
	if data, err := json.Marshal(trades); err == nil {
		s.redis.Set(ctx, cacheKey, data, 2*time.Minute)
	}

	return trades, rows.Err()
}

// ListUserTradeIDs returns only trade IDs for a user - lightweight for deduplication
func (s *PostgresStore) ListUserTradeIDs(ctx context.Context, userID string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 10000
	}

	// Check Redis cache first
	cacheKey := fmt.Sprintf("trade_ids:%s:%d", userID, limit)
	cached, err := s.redis.SMembers(ctx, cacheKey).Result()
	if err == nil && len(cached) > 0 {
		return cached, nil
	}

	// Query PostgreSQL - only fetch id column
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM user_trades
		WHERE user_id = $1
		ORDER BY timestamp DESC
		LIMIT $2`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	// Cache for 30 seconds (shorter since it's used frequently)
	if len(ids) > 0 {
		s.redis.SAdd(ctx, cacheKey, ids)
		s.redis.Expire(ctx, cacheKey, 30*time.Second)
	}

	return ids, rows.Err()
}

func (s *PostgresStore) saveUserTx(ctx context.Context, tx pgx.Tx, user models.User) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO users (id, username, address, total_trades, total_pnl, win_rate, consistency, last_active, last_synced_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
		ON CONFLICT (id) DO UPDATE SET
			username = EXCLUDED.username,
			address = EXCLUDED.address,
			total_trades = EXCLUDED.total_trades,
			total_pnl = EXCLUDED.total_pnl,
			win_rate = EXCLUDED.win_rate,
			consistency = EXCLUDED.consistency,
			last_active = EXCLUDED.last_active,
			last_synced_at = EXCLUDED.last_synced_at,
			updated_at = NOW()
	`, user.ID, user.Username, user.Address, user.TotalTrades, user.TotalPNL, user.WinRate, user.Consistency, nullTime(user.LastActive), nullTime(user.LastSyncedAt))
	if err != nil {
		return err
	}

	// Delete and re-insert subject metrics
	if _, err := tx.Exec(ctx, `DELETE FROM user_subject_metrics WHERE user_id = $1`, user.ID); err != nil {
		return err
	}

	if len(user.SubjectScores) > 0 {
		batch := &pgx.Batch{}
		for subject, score := range user.SubjectScores {
			batch.Queue(`
				INSERT INTO user_subject_metrics (user_id, subject, trades, pnl, win_rate, consistency)
				VALUES ($1, $2, $3, $4, $5, $6)
			`, user.ID, string(subject), score.Trades, score.PNL, score.WinRate, score.Consistency)
		}
		br := tx.SendBatch(ctx, batch)
		for i := 0; i < batch.Len(); i++ {
			if _, err := br.Exec(); err != nil {
				br.Close()
				return err
			}
		}
		br.Close()
	}

	// Delete and re-insert red flags
	if _, err := tx.Exec(ctx, `DELETE FROM user_red_flags WHERE user_id = $1`, user.ID); err != nil {
		return err
	}

	if len(user.RedFlags) > 0 {
		batch := &pgx.Batch{}
		for _, flag := range user.RedFlags {
			batch.Queue(`INSERT INTO user_red_flags (user_id, flag) VALUES ($1, $2)`, user.ID, flag)
		}
		br := tx.SendBatch(ctx, batch)
		for i := 0; i < batch.Len(); i++ {
			if _, err := br.Exec(); err != nil {
				br.Close()
				return err
			}
		}
		br.Close()
	}

	return nil
}

// ListUsers returns users with Redis caching
func (s *PostgresStore) ListUsers(ctx context.Context, subject models.Subject, limit int) ([]models.User, error) {
	if limit <= 0 {
		limit = 100
	}

	// Check Redis cache
	cacheKey := fmt.Sprintf("users:%s:%d", subject, limit)
	cached, err := s.redis.Get(ctx, cacheKey).Bytes()
	if err == nil {
		var users []models.User
		if json.Unmarshal(cached, &users) == nil {
			return users, nil
		}
	}

	var rows pgx.Rows
	if subject == "" {
		rows, err = s.pool.Query(ctx, `
			SELECT id, username, address, total_trades, total_pnl, win_rate, consistency, last_active, last_synced_at
			FROM users
			ORDER BY total_pnl DESC
			LIMIT $1`, limit)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT u.id, u.username, u.address, u.total_trades, u.total_pnl, u.win_rate, u.consistency, u.last_active, u.last_synced_at
			FROM users u
			INNER JOIN user_subject_metrics m ON m.user_id = u.id AND m.subject = $1
			ORDER BY m.pnl DESC
			LIMIT $2`, string(subject), limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	users := make([]models.User, 0)
	for rows.Next() {
		var u models.User
		var lastActive, lastSynced *time.Time
		if err := rows.Scan(&u.ID, &u.Username, &u.Address, &u.TotalTrades, &u.TotalPNL, &u.WinRate, &u.Consistency, &lastActive, &lastSynced); err != nil {
			return nil, err
		}
		if lastActive != nil {
			u.LastActive = *lastActive
		}
		if lastSynced != nil {
			u.LastSyncedAt = *lastSynced
		}
		u.SubjectScores = make(map[models.Subject]models.SubjectScore)
		users = append(users, u)
	}

	if len(users) > 0 {
		ptrs := make([]*models.User, len(users))
		for i := range users {
			ptrs[i] = &users[i]
		}
		if err := s.hydrateSubjectDetails(ctx, ptrs); err != nil {
			return nil, err
		}
	}

	// Cache for 5 minutes
	if data, err := json.Marshal(users); err == nil {
		s.redis.Set(ctx, cacheKey, data, 5*time.Minute)
	}

	return users, rows.Err()
}

// GetUser returns a single user with caching
func (s *PostgresStore) GetUser(ctx context.Context, userID string) (*models.User, error) {
	// Check Redis cache
	cacheKey := fmt.Sprintf("user:%s", userID)
	cached, err := s.redis.Get(ctx, cacheKey).Bytes()
	if err == nil {
		var user models.User
		if json.Unmarshal(cached, &user) == nil {
			return &user, nil
		}
	}

	var u models.User
	var lastActive *time.Time
	err = s.pool.QueryRow(ctx, `
		SELECT id, username, address, total_trades, total_pnl, win_rate, consistency, last_active
		FROM users WHERE id = $1`, userID).Scan(
		&u.ID, &u.Username, &u.Address, &u.TotalTrades, &u.TotalPNL, &u.WinRate, &u.Consistency, &lastActive)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	if lastActive != nil {
		u.LastActive = *lastActive
	}
	u.SubjectScores = make(map[models.Subject]models.SubjectScore)

	if err := s.hydrateSubjectDetails(ctx, []*models.User{&u}); err != nil {
		return nil, err
	}

	// Cache for 5 minutes
	if data, err := json.Marshal(u); err == nil {
		s.redis.Set(ctx, cacheKey, data, 5*time.Minute)
	}

	return &u, nil
}

func (s *PostgresStore) hydrateSubjectDetails(ctx context.Context, users []*models.User) error {
	if len(users) == 0 {
		return nil
	}

	idIndex := make(map[string]*models.User, len(users))
	ids := make([]string, 0, len(users))
	for _, user := range users {
		idIndex[user.ID] = user
		ids = append(ids, user.ID)
	}

	// Fetch subject scores
	rows, err := s.pool.Query(ctx, `
		SELECT user_id, subject, trades, pnl, win_rate, consistency
		FROM user_subject_metrics
		WHERE user_id = ANY($1)`, ids)
	if err != nil {
		return err
	}
	for rows.Next() {
		var userID, subject string
		var score models.SubjectScore
		if err := rows.Scan(&userID, &subject, &score.Trades, &score.PNL, &score.WinRate, &score.Consistency); err != nil {
			rows.Close()
			return err
		}
		if u, ok := idIndex[userID]; ok {
			u.SubjectScores[models.Subject(subject)] = score
		}
	}
	rows.Close()

	// Fetch red flags
	rows, err = s.pool.Query(ctx, `SELECT user_id, flag FROM user_red_flags WHERE user_id = ANY($1)`, ids)
	if err != nil {
		return err
	}
	for rows.Next() {
		var userID, flag string
		if err := rows.Scan(&userID, &flag); err != nil {
			rows.Close()
			return err
		}
		if u, ok := idIndex[userID]; ok {
			u.RedFlags = append(u.RedFlags, flag)
		}
	}
	rows.Close()

	return nil
}

// DeleteUser removes a user
func (s *PostgresStore) DeleteUser(ctx context.Context, userID string) error {
	// Start transaction to delete from all tables
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Get trade IDs for this user to delete from processed_trades
	rows, err := tx.Query(ctx, `SELECT id FROM user_trades WHERE user_id = $1`, userID)
	if err != nil {
		return fmt.Errorf("failed to get trade IDs: %w", err)
	}
	var tradeIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan trade ID: %w", err)
		}
		tradeIDs = append(tradeIDs, id)
	}
	rows.Close()

	// Delete from processed_trades
	if len(tradeIDs) > 0 {
		_, err = tx.Exec(ctx, `DELETE FROM processed_trades WHERE trade_id = ANY($1)`, tradeIDs)
		if err != nil {
			return fmt.Errorf("failed to delete processed trades: %w", err)
		}
	}

	// Delete from user_copy_settings
	_, err = tx.Exec(ctx, `DELETE FROM user_copy_settings WHERE user_address = $1`, userID)
	if err != nil {
		return fmt.Errorf("failed to delete user copy settings: %w", err)
	}

	// Delete from user_trades
	_, err = tx.Exec(ctx, `DELETE FROM user_trades WHERE user_id = $1`, userID)
	if err != nil {
		return fmt.Errorf("failed to delete user trades: %w", err)
	}

	// Delete from users table
	result, err := tx.Exec(ctx, `DELETE FROM users WHERE id = $1`, userID)
	if err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Log what was deleted (result may be 0 if user wasn't in users table but had trades)
	log.Printf("[Storage] Deleted user %s: %d from users table, %d trades", userID, result.RowsAffected(), len(tradeIDs))

	// Clear caches
	s.redis.Del(ctx, fmt.Sprintf("user:%s", userID))
	s.redis.Del(ctx, fmt.Sprintf("trades:%s:*", userID))
	s.redis.Del(ctx, fmt.Sprintf("analysis:%s", userID))

	return nil
}

// SaveAnalysisCache stores computed analysis results
func (s *PostgresStore) SaveAnalysisCache(ctx context.Context, userID string, resultJSON string, tradeCount int) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO analysis_cache (user_id, result_json, computed_at, trade_count)
		VALUES ($1, $2::jsonb, NOW(), $3)
		ON CONFLICT (user_id) DO UPDATE SET
			result_json = EXCLUDED.result_json,
			computed_at = EXCLUDED.computed_at,
			trade_count = EXCLUDED.trade_count
	`, userID, resultJSON, tradeCount)

	// Also cache in Redis for fast access
	s.redis.Set(ctx, fmt.Sprintf("analysis:%s", userID), resultJSON, 10*time.Minute)

	return err
}

// GetAnalysisCache retrieves cached analysis
func (s *PostgresStore) GetAnalysisCache(ctx context.Context, userID string) (string, int, time.Time, error) {
	// Check Redis first
	cacheKey := fmt.Sprintf("analysis:%s", userID)
	if cached, err := s.redis.Get(ctx, cacheKey).Result(); err == nil {
		// Get metadata from PostgreSQL
		var tradeCount int
		var computedAt time.Time
		err := s.pool.QueryRow(ctx, `
			SELECT trade_count, computed_at FROM analysis_cache WHERE user_id = $1
		`, userID).Scan(&tradeCount, &computedAt)
		if err == nil {
			return cached, tradeCount, computedAt, nil
		}
	}

	// Fallback to PostgreSQL
	var resultJSON string
	var tradeCount int
	var computedAt time.Time

	err := s.pool.QueryRow(ctx, `
		SELECT result_json::text, trade_count, computed_at
		FROM analysis_cache
		WHERE user_id = $1
	`, userID).Scan(&resultJSON, &tradeCount, &computedAt)

	if err != nil {
		return "", 0, time.Time{}, err
	}

	// Populate Redis cache
	s.redis.Set(ctx, cacheKey, resultJSON, 10*time.Minute)

	return resultJSON, tradeCount, computedAt, nil
}

// InvalidateAnalysisCache removes cached analysis
func (s *PostgresStore) InvalidateAnalysisCache(ctx context.Context, userID string) error {
	s.redis.Del(ctx, fmt.Sprintf("analysis:%s", userID))
	_, err := s.pool.Exec(ctx, `DELETE FROM analysis_cache WHERE user_id = $1`, userID)
	return err
}

// GetUserTradeCount returns the number of trades for a user
func (s *PostgresStore) GetUserTradeCount(ctx context.Context, userID string) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM user_trades WHERE user_id = $1`, userID).Scan(&count)
	return count, err
}

// SaveUserPositions stores pre-computed positions
func (s *PostgresStore) SaveUserPositions(ctx context.Context, userID string, positions []models.AggregatedPosition) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM user_positions WHERE user_id = $1`, userID); err != nil {
		return err
	}

	if len(positions) > 0 {
		batch := &pgx.Batch{}
		for _, pos := range positions {
			batch.Queue(`
				INSERT INTO user_positions (
					user_id, market_outcome, title, outcome, subject,
					total_bought, total_sold, qty_bought, qty_sold, gain_loss,
					buy_count, sell_count, first_buy_at, last_buy_at, first_sell_at, last_sell_at,
					duration_mins, updated_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, NOW())
			`,
				userID, pos.MarketOutcome, pos.Title, pos.Outcome, string(pos.Subject),
				pos.TotalBought, pos.TotalSold, pos.QtyBought, pos.QtySold, pos.GainLoss,
				pos.BuyCount, pos.SellCount,
				nullTime(pos.FirstBuyAt), nullTime(pos.LastBuyAt),
				nullTime(pos.FirstSellAt), nullTime(pos.LastSellAt),
				pos.DurationMins,
			)
		}
		br := tx.SendBatch(ctx, batch)
		for i := 0; i < batch.Len(); i++ {
			if _, err := br.Exec(); err != nil {
				br.Close()
				return err
			}
		}
		br.Close()
	}

	return tx.Commit(ctx)
}

// GetUserPositions retrieves pre-computed positions
func (s *PostgresStore) GetUserPositions(ctx context.Context, userID string) ([]models.AggregatedPosition, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT market_outcome, title, outcome, subject,
			   total_bought, total_sold, qty_bought, qty_sold, gain_loss,
			   buy_count, sell_count, first_buy_at, last_buy_at, first_sell_at, last_sell_at,
			   duration_mins
		FROM user_positions
		WHERE user_id = $1
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var positions []models.AggregatedPosition
	for rows.Next() {
		var pos models.AggregatedPosition
		var subject string
		var firstBuy, lastBuy, firstSell, lastSell *time.Time

		err := rows.Scan(
			&pos.MarketOutcome, &pos.Title, &pos.Outcome, &subject,
			&pos.TotalBought, &pos.TotalSold, &pos.QtyBought, &pos.QtySold, &pos.GainLoss,
			&pos.BuyCount, &pos.SellCount,
			&firstBuy, &lastBuy, &firstSell, &lastSell,
			&pos.DurationMins,
		)
		if err != nil {
			return nil, err
		}

		pos.Subject = models.Subject(subject)
		if firstBuy != nil {
			pos.FirstBuyAt = *firstBuy
		}
		if lastBuy != nil {
			pos.LastBuyAt = *lastBuy
		}
		if firstSell != nil {
			pos.FirstSellAt = *firstSell
		}
		if lastSell != nil {
			pos.LastSellAt = *lastSell
		}

		positions = append(positions, pos)
	}

	return positions, rows.Err()
}

// GetCachedTokens retrieves cached token info
func (s *PostgresStore) GetCachedTokens(ctx context.Context, tokenIDs []string) (map[string]TokenInfo, error) {
	if len(tokenIDs) == 0 {
		return make(map[string]TokenInfo), nil
	}

	rows, err := s.pool.Query(ctx, `
		SELECT token_id, condition_id, outcome, title, slug, event_slug
		FROM token_map_cache
		WHERE token_id = ANY($1)
	`, tokenIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]TokenInfo)
	for rows.Next() {
		var info TokenInfo
		if err := rows.Scan(&info.TokenID, &info.ConditionID, &info.Outcome, &info.Title, &info.Slug, &info.EventSlug); err != nil {
			return nil, err
		}
		result[info.TokenID] = info
	}

	return result, rows.Err()
}

// SaveTokenCache stores token information
func (s *PostgresStore) SaveTokenCache(ctx context.Context, tokens map[string]TokenInfo) error {
	if len(tokens) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, info := range tokens {
		batch.Queue(`
			INSERT INTO token_map_cache (token_id, condition_id, outcome, title, slug, event_slug, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, NOW())
			ON CONFLICT (token_id) DO UPDATE SET
				condition_id = EXCLUDED.condition_id,
				outcome = EXCLUDED.outcome,
				title = EXCLUDED.title,
				slug = EXCLUDED.slug,
				event_slug = EXCLUDED.event_slug,
				updated_at = EXCLUDED.updated_at
		`, info.TokenID, info.ConditionID, info.Outcome, info.Title, info.Slug, info.EventSlug)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch exec %d: %w", i, err)
		}
	}

	return nil
}

// GetTokenInfo retrieves token information for a single token ID
func (s *PostgresStore) GetTokenInfo(ctx context.Context, tokenID string) (*TokenInfo, error) {
	var info TokenInfo
	err := s.pool.QueryRow(ctx, `
		SELECT token_id, condition_id, outcome, title, slug
		FROM token_map_cache
		WHERE token_id = $1
	`, tokenID).Scan(&info.TokenID, &info.ConditionID, &info.Outcome, &info.Title, &info.Slug)

	if err != nil {
		return nil, err
	}

	return &info, nil
}

// GetTokenByCondition retrieves token information for a condition ID (returns first match)
func (s *PostgresStore) GetTokenByCondition(ctx context.Context, conditionID string) (*TokenInfo, error) {
	var info TokenInfo
	err := s.pool.QueryRow(ctx, `
		SELECT token_id, condition_id, outcome, title, slug
		FROM token_map_cache
		WHERE condition_id = $1
		LIMIT 1
	`, conditionID).Scan(&info.TokenID, &info.ConditionID, &info.Outcome, &info.Title, &info.Slug)

	if err != nil {
		return nil, err
	}

	return &info, nil
}

// GetTokenByConditionAndOutcome retrieves token information for a specific condition ID and outcome
func (s *PostgresStore) GetTokenByConditionAndOutcome(ctx context.Context, conditionID, outcome string) (*TokenInfo, error) {
	var info TokenInfo
	err := s.pool.QueryRow(ctx, `
		SELECT token_id, condition_id, outcome, title, slug
		FROM token_map_cache
		WHERE condition_id = $1 AND outcome = $2
		LIMIT 1
	`, conditionID, outcome).Scan(&info.TokenID, &info.ConditionID, &info.Outcome, &info.Title, &info.Slug)

	if err != nil {
		return nil, err
	}

	return &info, nil
}

// ReplaceTrades overwrites all trades (used for full refresh)
func (s *PostgresStore) ReplaceTrades(ctx context.Context, trades map[string][]models.TradeDetail) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `TRUNCATE user_trades`); err != nil {
		return err
	}

	for _, tradeList := range trades {
		for _, trade := range tradeList {
			isMaker := trade.Role == "MAKER"
			_, err := tx.Exec(ctx, `
				INSERT INTO user_trades (
					id, user_id, market_id, subject, type, side, is_maker, size, usdc_size, price, outcome,
					timestamp, title, slug, event_slug, transaction_hash, name, pseudonym, inserted_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, NOW())
			`,
				trade.ID, trade.UserID, trade.MarketID, string(trade.Subject), trade.Type, trade.Side,
				isMaker, trade.Size, trade.UsdcSize, trade.Price, trade.Outcome, trade.Timestamp,
				trade.Title, trade.Slug, trade.EventSlug, trade.TransactionHash, trade.Name, trade.Pseudonym,
			)
			if err != nil {
				return err
			}
		}
	}

	// Clear all trade caches
	keys, _ := s.redis.Keys(ctx, "trades:*").Result()
	if len(keys) > 0 {
		s.redis.Del(ctx, keys...)
	}

	return tx.Commit(ctx)
}

func nullTime(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t
}

// Helper for building ANY() queries
func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	parts := make([]string, n)
	for i := range parts {
		parts[i] = fmt.Sprintf("$%d", i+1)
	}
	return strings.Join(parts, ",")
}

// ============================================================================
// COPY TRADING METHODS
// ============================================================================

// GetUnprocessedTrades returns trades that haven't been copy-traded yet
func (s *PostgresStore) GetUnprocessedTrades(ctx context.Context, limit int) ([]models.TradeDetail, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ut.id, ut.user_id, ut.market_id, ut.subject, ut.type, ut.side, ut.is_maker,
			   ut.size, ut.usdc_size, ut.price, ut.outcome, ut.timestamp, ut.title,
			   ut.slug, ut.transaction_hash, ut.name, ut.pseudonym
		FROM user_trades ut
		LEFT JOIN processed_trades pt ON ut.id = pt.trade_id
		WHERE pt.trade_id IS NULL
		ORDER BY ut.timestamp ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var trades []models.TradeDetail
	for rows.Next() {
		var trade models.TradeDetail
		var subject, tradeType string
		var isMaker bool

		err := rows.Scan(
			&trade.ID, &trade.UserID, &trade.MarketID, &subject, &tradeType, &trade.Side, &isMaker,
			&trade.Size, &trade.UsdcSize, &trade.Price, &trade.Outcome, &trade.Timestamp, &trade.Title,
			&trade.Slug, &trade.TransactionHash, &trade.Name, &trade.Pseudonym,
		)
		if err != nil {
			return nil, err
		}

		trade.Subject = models.Subject(subject)
		trade.Type = tradeType
		if isMaker {
			trade.Role = "MAKER"
		} else {
			trade.Role = "TAKER"
		}

		trades = append(trades, trade)
	}

	return trades, rows.Err()
}

// MarkTradeProcessed marks a trade as processed to avoid duplicate copy trades
func (s *PostgresStore) MarkTradeProcessed(ctx context.Context, tradeID string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO processed_trades (trade_id, processed_at)
		VALUES ($1, NOW())
		ON CONFLICT (trade_id) DO NOTHING
	`, tradeID)
	return err
}

// GetTokenIDFromCache retrieves token ID and neg_risk from cache
func (s *PostgresStore) GetTokenIDFromCache(ctx context.Context, marketID, outcome string) (string, bool, error) {
	var tokenID string
	var negRisk bool

	// First try to find by market_id and outcome in token_map_cache
	err := s.pool.QueryRow(ctx, `
		SELECT token_id FROM token_map_cache
		WHERE condition_id = $1 AND outcome = $2
		LIMIT 1
	`, marketID, outcome).Scan(&tokenID)

	if err != nil {
		// Try Redis cache
		cacheKey := fmt.Sprintf("token:%s:%s", marketID, outcome)
		val, redisErr := s.redis.Get(ctx, cacheKey).Result()
		if redisErr == nil {
			parts := strings.Split(val, "|")
			if len(parts) >= 2 {
				tokenID = parts[0]
				negRisk = parts[1] == "true"
				return tokenID, negRisk, nil
			}
		}
		return "", false, err
	}

	return tokenID, negRisk, nil
}

// CacheTokenID stores token ID mapping in cache
func (s *PostgresStore) CacheTokenID(ctx context.Context, marketID, outcome, tokenID string, negRisk bool) error {
	// Store in Redis with 24h TTL
	cacheKey := fmt.Sprintf("token:%s:%s", marketID, outcome)
	negRiskStr := "false"
	if negRisk {
		negRiskStr = "true"
	}
	s.redis.Set(ctx, cacheKey, fmt.Sprintf("%s|%s", tokenID, negRiskStr), 24*time.Hour)

	// Also update token_map_cache
	_, err := s.pool.Exec(ctx, `
		INSERT INTO token_map_cache (token_id, condition_id, outcome, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (token_id) DO UPDATE SET
			condition_id = EXCLUDED.condition_id,
			outcome = EXCLUDED.outcome,
			updated_at = NOW()
	`, tokenID, marketID, outcome)

	return err
}

// CopyTradePosition represents our position for copy trading
type CopyTradePosition struct {
	MarketID  string
	TokenID   string
	Outcome   string
	Title     string
	Size      float64
	AvgPrice  float64
	TotalCost float64
	UpdatedAt time.Time
}

// UpdateMyPosition updates our copy trading position
func (s *PostgresStore) UpdateMyPosition(ctx context.Context, pos interface{}) error {
	// Type assertion to handle the syncer.MyPosition type
	type positionData struct {
		MarketID  string
		TokenID   string
		Outcome   string
		Title     string
		Size      float64
		AvgPrice  float64
		TotalCost float64
	}

	var p positionData

	// Handle both map and struct types
	switch v := pos.(type) {
	case map[string]interface{}:
		p.MarketID = v["MarketID"].(string)
		p.TokenID = v["TokenID"].(string)
		p.Outcome = v["Outcome"].(string)
		p.Title, _ = v["Title"].(string)
		p.Size, _ = v["Size"].(float64)
		p.AvgPrice, _ = v["AvgPrice"].(float64)
		p.TotalCost, _ = v["TotalCost"].(float64)
	default:
		// Use reflection-free approach - expect struct with specific fields
		// This handles syncer.MyPosition
		val := pos
		if mp, ok := val.(interface {
			GetMarketID() string
			GetTokenID() string
			GetOutcome() string
			GetTitle() string
			GetSize() float64
			GetAvgPrice() float64
			GetTotalCost() float64
		}); ok {
			p.MarketID = mp.GetMarketID()
			p.TokenID = mp.GetTokenID()
			p.Outcome = mp.GetOutcome()
			p.Title = mp.GetTitle()
			p.Size = mp.GetSize()
			p.AvgPrice = mp.GetAvgPrice()
			p.TotalCost = mp.GetTotalCost()
		} else {
			// Direct field access using type assertion
			jsonBytes, _ := json.Marshal(val)
			json.Unmarshal(jsonBytes, &p)
		}
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO my_positions (market_id, token_id, outcome, title, size, avg_price, total_cost, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		ON CONFLICT (market_id, outcome) DO UPDATE SET
			token_id = EXCLUDED.token_id,
			title = EXCLUDED.title,
			size = my_positions.size + EXCLUDED.size,
			avg_price = (my_positions.total_cost + EXCLUDED.total_cost) / NULLIF(my_positions.size + EXCLUDED.size, 0),
			total_cost = my_positions.total_cost + EXCLUDED.total_cost,
			updated_at = NOW()
	`, p.MarketID, p.TokenID, p.Outcome, p.Title, p.Size, p.AvgPrice, p.TotalCost)

	return err
}

// GetMyPosition retrieves our position for a market/outcome
func (s *PostgresStore) GetMyPosition(ctx context.Context, marketID, outcome string) (CopyTradePosition, error) {
	var pos CopyTradePosition
	err := s.pool.QueryRow(ctx, `
		SELECT market_id, token_id, outcome, title, size, avg_price, total_cost, updated_at
		FROM my_positions
		WHERE market_id = $1 AND outcome = $2
	`, marketID, outcome).Scan(
		&pos.MarketID, &pos.TokenID, &pos.Outcome, &pos.Title,
		&pos.Size, &pos.AvgPrice, &pos.TotalCost, &pos.UpdatedAt,
	)
	return pos, err
}

// ClearMyPosition removes our position after selling
func (s *PostgresStore) ClearMyPosition(ctx context.Context, marketID, outcome string) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM my_positions
		WHERE market_id = $1 AND outcome = $2
	`, marketID, outcome)
	return err
}

// CopyTradeRecord represents a copy trade for storage
type CopyTradeRecord struct {
	OriginalTradeID string
	OriginalTrader  string
	MarketID        string
	TokenID         string
	Outcome         string
	Title           string
	Side            string
	IntendedUSDC    float64
	ActualUSDC      float64
	PricePaid       float64
	SizeBought      float64
	Status          string
	ErrorReason     string
	OrderID         string
}

// SaveCopyTrade saves a copy trade record
func (s *PostgresStore) SaveCopyTrade(ctx context.Context, trade interface{}) error {
	var t CopyTradeRecord

	// Convert from syncer.CopyTrade to our type
	jsonBytes, _ := json.Marshal(trade)
	json.Unmarshal(jsonBytes, &t)

	var executedAt interface{}
	if t.Status == "executed" {
		executedAt = time.Now()
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO copy_trades (
			original_trade_id, original_trader, market_id, token_id, outcome, title,
			side, intended_usdc, actual_usdc, price_paid, size_bought,
			status, error_reason, order_id, created_at, executed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), $15)
	`,
		t.OriginalTradeID, t.OriginalTrader, t.MarketID, t.TokenID, t.Outcome, t.Title,
		t.Side, t.IntendedUSDC, t.ActualUSDC, t.PricePaid, t.SizeBought,
		t.Status, t.ErrorReason, t.OrderID, executedAt,
	)

	return err
}

// GetCopyTradeStats returns statistics about copy trading
func (s *PostgresStore) GetCopyTradeStats(ctx context.Context) (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	// Total trades
	var totalTrades, executedTrades, failedTrades int
	var totalUSDC, totalProfit float64

	err := s.pool.QueryRow(ctx, `
		SELECT
			COUNT(*) as total,
			COUNT(*) FILTER (WHERE status = 'executed') as executed,
			COUNT(*) FILTER (WHERE status = 'failed') as failed,
			COALESCE(SUM(actual_usdc) FILTER (WHERE status = 'executed'), 0) as total_usdc
		FROM copy_trades
	`).Scan(&totalTrades, &executedTrades, &failedTrades, &totalUSDC)
	if err != nil {
		return nil, err
	}

	stats["total_trades"] = totalTrades
	stats["executed_trades"] = executedTrades
	stats["failed_trades"] = failedTrades
	stats["total_usdc_spent"] = totalUSDC
	stats["total_profit"] = totalProfit

	// Current positions
	var positionCount int
	var positionValue float64

	err = s.pool.QueryRow(ctx, `
		SELECT COUNT(*), COALESCE(SUM(total_cost), 0)
		FROM my_positions
		WHERE size > 0
	`).Scan(&positionCount, &positionValue)
	if err == nil {
		stats["open_positions"] = positionCount
		stats["position_value"] = positionValue
	}

	// Recent trades
	rows, err := s.pool.Query(ctx, `
		SELECT title, outcome, side, actual_usdc, status, created_at
		FROM copy_trades
		ORDER BY created_at DESC
		LIMIT 10
	`)
	if err == nil {
		defer rows.Close()
		var recentTrades []map[string]interface{}
		for rows.Next() {
			var title, outcome, side, status string
			var usdc float64
			var createdAt time.Time
			rows.Scan(&title, &outcome, &side, &usdc, &status, &createdAt)
			recentTrades = append(recentTrades, map[string]interface{}{
				"title":      title,
				"outcome":    outcome,
				"side":       side,
				"usdc":       usdc,
				"status":     status,
				"created_at": createdAt,
			})
		}
		stats["recent_trades"] = recentTrades
	}

	return stats, nil
}

// GetAllMyPositions returns all current copy trading positions
func (s *PostgresStore) GetAllMyPositions(ctx context.Context) ([]CopyTradePosition, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT market_id, token_id, outcome, title, size, avg_price, total_cost, updated_at
		FROM my_positions
		WHERE size > 0
		ORDER BY updated_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var positions []CopyTradePosition
	for rows.Next() {
		var pos CopyTradePosition
		err := rows.Scan(
			&pos.MarketID, &pos.TokenID, &pos.Outcome, &pos.Title,
			&pos.Size, &pos.AvgPrice, &pos.TotalCost, &pos.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		positions = append(positions, pos)
	}

	return positions, rows.Err()
}

// UserCopySettings represents per-user copy trading settings
type UserCopySettings struct {
	UserAddress string
	Multiplier  float64
	Enabled     bool
	MinUSDC     float64
}

// GetUserCopySettings gets the copy trading settings for a specific user
func (s *PostgresStore) GetUserCopySettings(ctx context.Context, userAddress string) (*UserCopySettings, error) {
	var settings UserCopySettings
	err := s.pool.QueryRow(ctx, `
		SELECT user_address, multiplier, enabled, min_usdc
		FROM user_copy_settings
		WHERE user_address = $1
	`, strings.ToLower(userAddress)).Scan(
		&settings.UserAddress, &settings.Multiplier, &settings.Enabled, &settings.MinUSDC,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // No custom settings, use defaults
		}
		return nil, err
	}
	return &settings, nil
}

// SetUserCopySettings sets or updates the copy trading settings for a user
func (s *PostgresStore) SetUserCopySettings(ctx context.Context, settings UserCopySettings) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO user_copy_settings (user_address, multiplier, enabled, min_usdc, updated_at)
		VALUES ($1, $2, $3, $4, NOW())
		ON CONFLICT (user_address) DO UPDATE SET
			multiplier = EXCLUDED.multiplier,
			enabled = EXCLUDED.enabled,
			min_usdc = EXCLUDED.min_usdc,
			updated_at = NOW()
	`, strings.ToLower(settings.UserAddress), settings.Multiplier, settings.Enabled, settings.MinUSDC)
	return err
}

// GetAllUserCopySettings returns all user copy trading settings
func (s *PostgresStore) GetAllUserCopySettings(ctx context.Context) ([]UserCopySettings, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT user_address, multiplier, enabled, min_usdc
		FROM user_copy_settings
		ORDER BY user_address
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var settings []UserCopySettings
	for rows.Next() {
		var s UserCopySettings
		err := rows.Scan(&s.UserAddress, &s.Multiplier, &s.Enabled, &s.MinUSDC)
		if err != nil {
			return nil, err
		}
		settings = append(settings, s)
	}
	return settings, rows.Err()
}

// DeleteUserCopySettings removes custom settings for a user (reverts to defaults)
func (s *PostgresStore) DeleteUserCopySettings(ctx context.Context, userAddress string) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM user_copy_settings WHERE user_address = $1
	`, strings.ToLower(userAddress))
	return err
}

// ============================================================================
// USER ANALYTICS METHODS
// ============================================================================

// UserAnalyticsRecord represents a row from user_analytics table
type UserAnalyticsRecord struct {
	UserAddress            string    `json:"user_address"`
	TotalBets              int       `json:"total_bets"`
	TotalBuyUSD            float64   `json:"total_buy_usd"`
	TotalSellRedeemUSD     float64   `json:"total_sell_redeem_usd"`
	NetPnL                 float64   `json:"net_pnl"`
	PnLPercentage          float64   `json:"pnl_percentage"`
	IsBot                  bool      `json:"is_bot"`
	AvgInvestmentPerMarket float64   `json:"avg_investment_per_market"`
	FirstTradeAt           time.Time `json:"first_trade_at"`
	LastTradeAt            time.Time `json:"last_trade_at"`
	TradesPerDayAvg        float64   `json:"trades_per_day_avg"`
	TradesPerDay30d        float64   `json:"trades_per_day_30d"`
	UniqueMarkets          int       `json:"unique_markets"`
	DataComplete           bool      `json:"data_complete"`
	FastTrades             int       `json:"fast_trades"`
	AvgTrades              int       `json:"avg_trades"`
	SlowTrades             int       `json:"slow_trades"`
	FastAvgPnL             float64   `json:"fast_avg_pnl"`
	MidAvgPnL              float64   `json:"mid_avg_pnl"`
	SlowAvgPnL             float64   `json:"slow_avg_pnl"`
	UpdatedAt              time.Time `json:"updated_at"`
}

// UserAnalyticsFilter contains filter options for analytics queries
type UserAnalyticsFilter struct {
	MinPnL           *float64
	MaxPnL           *float64
	MinBets          *int
	MaxBets          *int
	MinPnLPercent    *float64
	MaxPnLPercent    *float64
	MinVolume        *float64 // Total buy USD
	MaxVolume        *float64
	MinMarkets       *int
	MaxMarkets       *int
	MinAvgInvestment *float64
	MaxAvgInvestment *float64
	MinTradesPerDay  *float64
	MaxTradesPerDay  *float64
	LastActiveDays   *int // Only users active within N days
	HideBots         bool
	DataComplete     *bool
	SortBy           string
	SortDesc         bool
	Limit            int
	Offset           int
}

// GetUserAnalyticsList returns filtered and sorted user analytics
func (s *PostgresStore) GetUserAnalyticsList(ctx context.Context, filter UserAnalyticsFilter) ([]UserAnalyticsRecord, int, error) {
	// Build WHERE clause
	conditions := []string{}
	args := []interface{}{}
	argNum := 1

	if filter.MinPnL != nil {
		conditions = append(conditions, fmt.Sprintf("net_pnl >= $%d", argNum))
		args = append(args, *filter.MinPnL)
		argNum++
	}
	if filter.MaxPnL != nil {
		conditions = append(conditions, fmt.Sprintf("net_pnl <= $%d", argNum))
		args = append(args, *filter.MaxPnL)
		argNum++
	}
	if filter.MinBets != nil {
		conditions = append(conditions, fmt.Sprintf("total_bets >= $%d", argNum))
		args = append(args, *filter.MinBets)
		argNum++
	}
	if filter.MaxBets != nil {
		conditions = append(conditions, fmt.Sprintf("total_bets <= $%d", argNum))
		args = append(args, *filter.MaxBets)
		argNum++
	}
	if filter.MinPnLPercent != nil {
		conditions = append(conditions, fmt.Sprintf("pnl_percentage >= $%d", argNum))
		args = append(args, *filter.MinPnLPercent)
		argNum++
	}
	if filter.MaxPnLPercent != nil {
		conditions = append(conditions, fmt.Sprintf("pnl_percentage <= $%d", argNum))
		args = append(args, *filter.MaxPnLPercent)
		argNum++
	}
	if filter.MinVolume != nil {
		conditions = append(conditions, fmt.Sprintf("total_buy_usd >= $%d", argNum))
		args = append(args, *filter.MinVolume)
		argNum++
	}
	if filter.MaxVolume != nil {
		conditions = append(conditions, fmt.Sprintf("total_buy_usd <= $%d", argNum))
		args = append(args, *filter.MaxVolume)
		argNum++
	}
	if filter.MinMarkets != nil {
		conditions = append(conditions, fmt.Sprintf("unique_markets >= $%d", argNum))
		args = append(args, *filter.MinMarkets)
		argNum++
	}
	if filter.MaxMarkets != nil {
		conditions = append(conditions, fmt.Sprintf("unique_markets <= $%d", argNum))
		args = append(args, *filter.MaxMarkets)
		argNum++
	}
	if filter.MinAvgInvestment != nil {
		conditions = append(conditions, fmt.Sprintf("avg_investment_per_market >= $%d", argNum))
		args = append(args, *filter.MinAvgInvestment)
		argNum++
	}
	if filter.MaxAvgInvestment != nil {
		conditions = append(conditions, fmt.Sprintf("avg_investment_per_market <= $%d", argNum))
		args = append(args, *filter.MaxAvgInvestment)
		argNum++
	}
	if filter.MinTradesPerDay != nil {
		conditions = append(conditions, fmt.Sprintf("trades_per_day_avg >= $%d", argNum))
		args = append(args, *filter.MinTradesPerDay)
		argNum++
	}
	if filter.MaxTradesPerDay != nil {
		conditions = append(conditions, fmt.Sprintf("trades_per_day_avg <= $%d", argNum))
		args = append(args, *filter.MaxTradesPerDay)
		argNum++
	}
	if filter.LastActiveDays != nil {
		conditions = append(conditions, fmt.Sprintf("last_trade_at >= NOW() - INTERVAL '%d days'", *filter.LastActiveDays))
	}
	if filter.HideBots {
		conditions = append(conditions, "is_bot = false")
	}
	if filter.DataComplete != nil {
		conditions = append(conditions, fmt.Sprintf("data_complete = $%d", argNum))
		args = append(args, *filter.DataComplete)
		argNum++
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	// Get total count
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM user_analytics %s", whereClause)
	var totalCount int
	if err := s.pool.QueryRow(ctx, countQuery, args...).Scan(&totalCount); err != nil {
		return nil, 0, err
	}

	// Build ORDER BY clause
	sortColumn := "net_pnl"
	validSorts := map[string]string{
		"pnl":            "net_pnl",
		"pnl_percent":    "pnl_percentage",
		"bets":           "total_bets",
		"buy":            "total_buy_usd",
		"sell":           "total_sell_redeem_usd",
		"avg_investment": "avg_investment_per_market",
		"trades_per_day": "trades_per_day_avg",
		"markets":        "unique_markets",
		"fast_trades":    "fast_trades",
		"avg_trades":     "avg_trades",
		"slow_trades":    "slow_trades",
		"fast_pnl":       "fast_avg_pnl",
		"mid_pnl":        "mid_avg_pnl",
		"slow_pnl":       "slow_avg_pnl",
		"first_trade":    "first_trade_at",
		"last_trade":     "last_trade_at",
		"updated":        "updated_at",
	}
	if col, ok := validSorts[filter.SortBy]; ok {
		sortColumn = col
	}

	sortDir := "DESC"
	if !filter.SortDesc {
		sortDir = "ASC"
	}

	// Apply defaults
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 10000 {
		limit = 10000
	}

	// Build final query
	query := fmt.Sprintf(`
		SELECT user_address, total_bets, total_buy_usd, total_sell_redeem_usd, net_pnl,
			   pnl_percentage, is_bot, avg_investment_per_market, first_trade_at, last_trade_at,
			   trades_per_day_avg, trades_per_day_30d, unique_markets, data_complete,
			   fast_trades, avg_trades, slow_trades, fast_avg_pnl, mid_avg_pnl, slow_avg_pnl, updated_at
		FROM user_analytics
		%s
		ORDER BY %s %s
		LIMIT $%d OFFSET $%d
	`, whereClause, sortColumn, sortDir, argNum, argNum+1)

	args = append(args, limit, filter.Offset)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var results []UserAnalyticsRecord
	for rows.Next() {
		var r UserAnalyticsRecord
		var firstTrade, lastTrade, updated *time.Time

		err := rows.Scan(
			&r.UserAddress, &r.TotalBets, &r.TotalBuyUSD, &r.TotalSellRedeemUSD, &r.NetPnL,
			&r.PnLPercentage, &r.IsBot, &r.AvgInvestmentPerMarket, &firstTrade, &lastTrade,
			&r.TradesPerDayAvg, &r.TradesPerDay30d, &r.UniqueMarkets, &r.DataComplete,
			&r.FastTrades, &r.AvgTrades, &r.SlowTrades, &r.FastAvgPnL, &r.MidAvgPnL, &r.SlowAvgPnL, &updated,
		)
		if err != nil {
			return nil, 0, err
		}

		if firstTrade != nil {
			r.FirstTradeAt = *firstTrade
		}
		if lastTrade != nil {
			r.LastTradeAt = *lastTrade
		}
		if updated != nil {
			r.UpdatedAt = *updated
		}

		results = append(results, r)
	}

	return results, totalCount, rows.Err()
}

// InvalidateUserListCache clears all cached user lists from Redis
func (s *PostgresStore) InvalidateUserListCache(ctx context.Context) error {
	// Delete all user list cache keys (pattern: users:*)
	keys, err := s.redis.Keys(ctx, "users:*").Result()
	if err != nil {
		return err
	}
	if len(keys) > 0 {
		if err := s.redis.Del(ctx, keys...).Err(); err != nil {
			return err
		}
		log.Printf("[Storage] Invalidated %d user list cache keys", len(keys))
	}
	return nil
}

// PrivilegedHit represents a single instance where a user's buy was followed by a price increase
type PrivilegedHit struct {
	Title       string    `json:"title"`
	Outcome     string    `json:"outcome"`
	BuyPrice    float64   `json:"buy_price"`
	HitPrice    float64   `json:"hit_price"`
	PriceGain   float64   `json:"price_gain"` // percentage gain
	BuyTime     time.Time `json:"buy_time"`
	HitTime     time.Time `json:"hit_time"`
	MinutesTo   float64   `json:"minutes_to"` // minutes until price hit
}

// PrivilegedUser represents a user with potential privileged knowledge
type PrivilegedUser struct {
	UserAddress string          `json:"user_address"`
	HitCount    int             `json:"hit_count"`
	TotalBuys   int             `json:"total_buys"`
	HitRate     float64         `json:"hit_rate"` // percentage of buys that hit
	Hits        []PrivilegedHit `json:"hits"`
}

// PrivilegedAnalysisMeta contains metadata about when analysis was computed
type PrivilegedAnalysisMeta struct {
	TimeWindowMinutes int
	PriceThresholdPct int
	LastComputedAt    time.Time
	DurationSec       float64
	UserCount         int
}

// GetPrivilegedKnowledgeAnalysis reads pre-computed results from database
func (s *PostgresStore) GetPrivilegedKnowledgeAnalysis(ctx context.Context, timeWindowMinutes int, priceThresholdPct int) ([]PrivilegedUser, *PrivilegedAnalysisMeta, error) {
	// Read from pre-computed table
	rows, err := s.pool.Query(ctx, `
		SELECT user_address, hit_count, total_buys, hit_rate, hits_json
		FROM privileged_analysis
		WHERE time_window_minutes = $1 AND price_threshold_pct = $2
		ORDER BY hit_count DESC
		LIMIT 100
	`, timeWindowMinutes, priceThresholdPct)
	if err != nil {
		return nil, nil, fmt.Errorf("read privileged analysis: %w", err)
	}
	defer rows.Close()

	var results []PrivilegedUser
	for rows.Next() {
		var user PrivilegedUser
		var hitsJSON []byte

		err := rows.Scan(&user.UserAddress, &user.HitCount, &user.TotalBuys, &user.HitRate, &hitsJSON)
		if err != nil {
			return nil, nil, fmt.Errorf("scan privileged user: %w", err)
		}

		if err := json.Unmarshal(hitsJSON, &user.Hits); err != nil {
			log.Printf("[Storage] Warning: failed to unmarshal hits for %s: %v", user.UserAddress, err)
			user.Hits = []PrivilegedHit{}
		}

		results = append(results, user)
	}

	// Get metadata
	var meta PrivilegedAnalysisMeta
	meta.TimeWindowMinutes = timeWindowMinutes
	meta.PriceThresholdPct = priceThresholdPct

	err = s.pool.QueryRow(ctx, `
		SELECT last_computed_at, COALESCE(computation_duration_sec, 0), COALESCE(user_count, 0)
		FROM privileged_analysis_meta
		WHERE time_window_minutes = $1 AND price_threshold_pct = $2
	`, timeWindowMinutes, priceThresholdPct).Scan(&meta.LastComputedAt, &meta.DurationSec, &meta.UserCount)
	if err != nil {
		// No meta yet, that's okay
		meta.LastComputedAt = time.Time{}
	}

	return results, &meta, rows.Err()
}

// ComputeAndSavePrivilegedAnalysis runs the expensive query and saves results to database
func (s *PostgresStore) ComputeAndSavePrivilegedAnalysis(ctx context.Context, timeWindowMinutes int, priceThresholdPct int) error {
	startTime := time.Now()
	priceThreshold := float64(priceThresholdPct) / 100.0

	log.Printf("[Privileged] Computing %dmin/+%d%% analysis...", timeWindowMinutes, priceThresholdPct)

	// Heavy query to find privileged knowledge indicators
	query := `
		WITH user_buys AS (
			SELECT
				user_address,
				asset,
				price,
				timestamp,
				title,
				outcome
			FROM global_trades
			WHERE side = 'BUY'
			AND type = 'TRADE'
			AND price > 0
			AND timestamp > NOW() - INTERVAL '30 days'
		),
		price_hits AS (
			SELECT
				ub.user_address,
				ub.asset,
				ub.title,
				ub.outcome,
				ub.price as buy_price,
				ub.timestamp as buy_time,
				MIN(gt.price) as hit_price,
				MIN(gt.timestamp) as hit_time
			FROM user_buys ub
			JOIN global_trades gt ON gt.asset = ub.asset
			WHERE gt.timestamp > ub.timestamp
			AND gt.timestamp <= ub.timestamp + $1::interval
			AND gt.price >= ub.price * (1 + $2)
			GROUP BY ub.user_address, ub.asset, ub.title, ub.outcome, ub.price, ub.timestamp
		),
		user_buy_counts AS (
			SELECT user_address, COUNT(*) as total_buys
			FROM user_buys
			GROUP BY user_address
		)
		SELECT
			ph.user_address,
			COUNT(*) as hit_count,
			COALESCE(ubc.total_buys, 0) as total_buys,
			json_agg(json_build_object(
				'title', ph.title,
				'outcome', ph.outcome,
				'buy_price', ph.buy_price,
				'hit_price', ph.hit_price,
				'price_gain', ((ph.hit_price - ph.buy_price) / ph.buy_price * 100),
				'buy_time', ph.buy_time,
				'hit_time', ph.hit_time,
				'minutes_to', EXTRACT(EPOCH FROM (ph.hit_time - ph.buy_time)) / 60
			) ORDER BY ph.buy_time DESC) as hits
		FROM price_hits ph
		LEFT JOIN user_buy_counts ubc ON ubc.user_address = ph.user_address
		GROUP BY ph.user_address, ubc.total_buys
		HAVING COUNT(*) >= 3
		ORDER BY COUNT(*) DESC
		LIMIT 100
	`

	interval := fmt.Sprintf("%d minutes", timeWindowMinutes)
	rows, err := s.pool.Query(ctx, query, interval, priceThreshold)
	if err != nil {
		return fmt.Errorf("privileged knowledge query: %w", err)
	}
	defer rows.Close()

	var results []PrivilegedUser
	for rows.Next() {
		var user PrivilegedUser
		var hitsJSON []byte

		err := rows.Scan(&user.UserAddress, &user.HitCount, &user.TotalBuys, &hitsJSON)
		if err != nil {
			return fmt.Errorf("scan privileged user: %w", err)
		}

		if err := json.Unmarshal(hitsJSON, &user.Hits); err != nil {
			user.Hits = []PrivilegedHit{}
		}

		if user.TotalBuys > 0 {
			user.HitRate = float64(user.HitCount) / float64(user.TotalBuys) * 100
		}

		results = append(results, user)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate results: %w", err)
	}

	// Delete old results for this window/threshold
	_, err = s.pool.Exec(ctx, `
		DELETE FROM privileged_analysis
		WHERE time_window_minutes = $1 AND price_threshold_pct = $2
	`, timeWindowMinutes, priceThresholdPct)
	if err != nil {
		return fmt.Errorf("delete old results: %w", err)
	}

	// Insert new results
	for _, user := range results {
		hitsJSON, _ := json.Marshal(user.Hits)
		_, err = s.pool.Exec(ctx, `
			INSERT INTO privileged_analysis (time_window_minutes, price_threshold_pct, user_address, hit_count, total_buys, hit_rate, hits_json, computed_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		`, timeWindowMinutes, priceThresholdPct, user.UserAddress, user.HitCount, user.TotalBuys, user.HitRate, hitsJSON)
		if err != nil {
			return fmt.Errorf("insert result: %w", err)
		}
	}

	// Update metadata
	duration := time.Since(startTime).Seconds()
	_, err = s.pool.Exec(ctx, `
		INSERT INTO privileged_analysis_meta (time_window_minutes, price_threshold_pct, last_computed_at, computation_duration_sec, user_count)
		VALUES ($1, $2, NOW(), $3, $4)
		ON CONFLICT (time_window_minutes, price_threshold_pct) DO UPDATE SET
			last_computed_at = NOW(),
			computation_duration_sec = $3,
			user_count = $4
	`, timeWindowMinutes, priceThresholdPct, duration, len(results))
	if err != nil {
		return fmt.Errorf("update meta: %w", err)
	}

	log.Printf("[Privileged] Computed %dmin/+%d%%: %d users in %.1fs", timeWindowMinutes, priceThresholdPct, len(results), duration)
	return nil
}
