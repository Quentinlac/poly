package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
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

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?pool_max_conns=10&pool_min_conns=2",
		user, password, host, port, dbname)

	// Configure connection pool
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse config: %w", err)
	}

	// Pool settings - reduced for lower memory usage
	config.MaxConns = 10
	config.MinConns = 2
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute
	config.HealthCheckPeriod = 30 * time.Second

	// Add query timeout to prevent slow queries from hanging
	config.ConnConfig.RuntimeParams["statement_timeout"] = "30000"                   // 30 seconds max per query
	config.ConnConfig.RuntimeParams["lock_timeout"] = "10000"                        // 10 seconds max for locks
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
		PoolSize:     10,
		MinIdleConns: 2,
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

// BackfillTradeMarketInfo updates trades that have empty title/slug/outcome
// by looking up their market_id in the token_map_cache.
// Only processes up to 100 trades per call to avoid blocking other queries.
// Returns the number of trades updated.
func (s *PostgresStore) BackfillTradeMarketInfo(ctx context.Context) (int64, error) {
	// Use a subquery with LIMIT to avoid full table lock
	result, err := s.pool.Exec(ctx, `
		UPDATE user_trades t
		SET
			title = c.title,
			slug = c.slug,
			outcome = c.outcome
		FROM token_map_cache c
		WHERE t.id IN (
			SELECT ut.id
			FROM user_trades ut
			WHERE (ut.title IS NULL OR ut.title = '')
			  AND ut.market_id IS NOT NULL
			LIMIT 100
		)
		  AND t.market_id = c.token_id
		  AND c.title IS NOT NULL AND c.title != ''
	`)
	if err != nil {
		return 0, fmt.Errorf("backfill trades: %w", err)
	}
	return result.RowsAffected(), nil
}

// SaveTokenInfo saves a single token's information to the cache
func (s *PostgresStore) SaveTokenInfo(ctx context.Context, tokenID, conditionID, outcome, title, slug string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO token_map_cache (token_id, condition_id, outcome, title, slug, updated_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (token_id) DO UPDATE SET
			condition_id = EXCLUDED.condition_id,
			outcome = EXCLUDED.outcome,
			title = EXCLUDED.title,
			slug = EXCLUDED.slug,
			updated_at = EXCLUDED.updated_at
	`, tokenID, conditionID, outcome, title, slug)
	return err
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
	DetectionSource string // How the trade was detected: clob, polygon_ws, data_api
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

	// Default to "clob" if not specified
	detectionSource := t.DetectionSource
	if detectionSource == "" {
		detectionSource = "clob"
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO copy_trades (
			original_trade_id, original_trader, market_id, token_id, outcome, title,
			side, intended_usdc, actual_usdc, price_paid, size_bought,
			status, error_reason, order_id, created_at, executed_at, detection_source
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), $15, $16)
	`,
		t.OriginalTradeID, t.OriginalTrader, t.MarketID, t.TokenID, t.Outcome, t.Title,
		t.Side, t.IntendedUSDC, t.ActualUSDC, t.PricePaid, t.SizeBought,
		t.Status, t.ErrorReason, t.OrderID, executedAt, detectionSource,
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

// Strategy types for copy trading
const (
	StrategyHuman  = 1 // Human following - market orders with slippage
	StrategyBot    = 2 // Bot following - limit orders matching exact prices
	StrategyBTC15m = 3 // BTC 15m markets - WebSocket detection via ws-live-data.polymarket.com
)

// UserCopySettings represents per-user copy trading settings
type UserCopySettings struct {
	UserAddress  string
	Multiplier   float64
	Enabled      bool
	MinUSDC      float64
	StrategyType int      // 1=human, 2=bot
	MaxUSD       *float64 // nil means no cap, otherwise max USDC per trade
}

// GetUserCopySettings gets the copy trading settings for a specific user
func (s *PostgresStore) GetUserCopySettings(ctx context.Context, userAddress string) (*UserCopySettings, error) {
	var settings UserCopySettings
	err := s.pool.QueryRow(ctx, `
		SELECT user_address, multiplier, enabled, min_usdc, COALESCE(strategy_type, 1), max_usd
		FROM user_copy_settings
		WHERE user_address = $1
	`, strings.ToLower(userAddress)).Scan(
		&settings.UserAddress, &settings.Multiplier, &settings.Enabled, &settings.MinUSDC, &settings.StrategyType, &settings.MaxUSD,
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
	// Default strategy type to human if not set
	strategyType := settings.StrategyType
	if strategyType == 0 {
		strategyType = StrategyHuman
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO user_copy_settings (user_address, multiplier, enabled, min_usdc, strategy_type, max_usd, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, NOW())
		ON CONFLICT (user_address) DO UPDATE SET
			multiplier = EXCLUDED.multiplier,
			enabled = EXCLUDED.enabled,
			min_usdc = EXCLUDED.min_usdc,
			strategy_type = EXCLUDED.strategy_type,
			max_usd = EXCLUDED.max_usd,
			updated_at = NOW()
	`, strings.ToLower(settings.UserAddress), settings.Multiplier, settings.Enabled, settings.MinUSDC, strategyType, settings.MaxUSD)
	return err
}

// GetFollowedUsersByStrategy returns followed user addresses filtered by strategy type
func (s *PostgresStore) GetFollowedUsersByStrategy(ctx context.Context, strategyType int) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT user_address
		FROM user_copy_settings
		WHERE enabled = true AND strategy_type = $1
	`, strategyType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []string
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			return nil, err
		}
		users = append(users, addr)
	}
	return users, rows.Err()
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

// GetRedisValue retrieves a value from Redis by key
func (s *PostgresStore) GetRedisValue(ctx context.Context, key string) (string, error) {
	return s.redis.Get(ctx, key).Result()
}

// ============================================================================
// COPY TRADE LOGGING
// ============================================================================

// CopyTradeLogEntry represents a detailed log of a copy trade attempt
type CopyTradeLogEntry struct {
	// Following user info
	FollowingAddress string
	FollowingTradeID string
	FollowingTime    time.Time
	FollowingShares  float64 // negative = buy, positive = sell
	FollowingPrice   float64
	// Follower (us) info
	FollowerTime   *time.Time
	FollowerShares *float64 // negative = buy, positive = sell
	FollowerPrice  *float64
	// Trade info
	MarketTitle string
	Outcome     string
	TokenID     string
	// Status
	Status       string // 'success', 'failed', 'skipped'
	FailedReason string
	StrategyType    int
	DetectionSource string // How the trade was detected: clob, mempool, live_ws, polygon_ws, data_api
	// Debug log - JSON with order book, calculations, API calls
	DebugLog map[string]interface{}
	// Timing breakdown - duration of each step in milliseconds
	TimingBreakdown map[string]interface{}
	// Full timing timestamps for latency analysis
	DetectedAt          *time.Time // When RealtimeDetector first saw the trade
	ProcessingStartedAt *time.Time // When executeBotBuy/Sell started
	OrderPlacedAt       *time.Time // When we sent order to Polymarket
	OrderConfirmedAt    *time.Time // When Polymarket confirmed (follower_time)
}

// SaveCopyTradeLog saves a detailed copy trade log entry
func (s *PostgresStore) SaveCopyTradeLog(ctx context.Context, entry CopyTradeLogEntry) error {
	// Convert debug log to JSON
	var debugLogJSON []byte
	var err error
	if entry.DebugLog != nil {
		debugLogJSON, err = json.Marshal(entry.DebugLog)
		if err != nil {
			debugLogJSON = []byte("{}")
		}
	}

	// Convert timing breakdown to JSON
	var timingJSON []byte
	if entry.TimingBreakdown != nil {
		timingJSON, err = json.Marshal(entry.TimingBreakdown)
		if err != nil {
			timingJSON = []byte("{}")
		}
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO copy_trade_log (
			following_address, following_trade_id, following_time, following_shares, following_price,
			follower_time, follower_shares, follower_price,
			market_title, outcome, token_id,
			status, failed_reason, strategy_type, detection_source, debug_log, timing_breakdown,
			detected_at, processing_started_at, order_placed_at, order_confirmed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
	`,
		entry.FollowingAddress, entry.FollowingTradeID, entry.FollowingTime, entry.FollowingShares, entry.FollowingPrice,
		entry.FollowerTime, entry.FollowerShares, entry.FollowerPrice,
		entry.MarketTitle, entry.Outcome, entry.TokenID,
		entry.Status, entry.FailedReason, entry.StrategyType, entry.DetectionSource, debugLogJSON, timingJSON,
		entry.DetectedAt, entry.ProcessingStartedAt, entry.OrderPlacedAt, entry.OrderConfirmedAt,
	)
	return err
}

// GetCopyTradeLogs retrieves recent copy trade logs
func (s *PostgresStore) GetCopyTradeLogs(ctx context.Context, limit int) ([]CopyTradeLogEntry, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := s.pool.Query(ctx, `
		SELECT following_address, following_trade_id, following_time, following_shares, following_price,
			   follower_time, follower_shares, follower_price,
			   market_title, outcome, token_id,
			   status, failed_reason, strategy_type
		FROM copy_trade_log
		ORDER BY following_time DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []CopyTradeLogEntry
	for rows.Next() {
		var entry CopyTradeLogEntry
		var followerTime *time.Time
		var followerShares, followerPrice *float64

		err := rows.Scan(
			&entry.FollowingAddress, &entry.FollowingTradeID, &entry.FollowingTime, &entry.FollowingShares, &entry.FollowingPrice,
			&followerTime, &followerShares, &followerPrice,
			&entry.MarketTitle, &entry.Outcome, &entry.TokenID,
			&entry.Status, &entry.FailedReason, &entry.StrategyType,
		)
		if err != nil {
			return nil, err
		}

		entry.FollowerTime = followerTime
		entry.FollowerShares = followerShares
		entry.FollowerPrice = followerPrice

		logs = append(logs, entry)
	}

	return logs, rows.Err()
}

// UpdateBlockchainConfirmedAt updates the blockchain confirmation timestamp for a copy trade.
// It matches by token_id and finds the most recent executed trade within a time window.
func (s *PostgresStore) UpdateBlockchainConfirmedAt(ctx context.Context, tokenID string, side string, blockchainTime time.Time, txHash string) error {
	// Find the most recent executed copy trade for this token that doesn't have blockchain_confirmed_at yet
	// Match within a 5-minute window to handle timing differences
	result, err := s.pool.Exec(ctx, `
		UPDATE copy_trade_log
		SET blockchain_confirmed_at = $1
		WHERE id = (
			SELECT id FROM copy_trade_log
			WHERE token_id = $2
			AND status = 'executed'
			AND blockchain_confirmed_at IS NULL
			AND (
				($3 = 'BUY' AND following_shares > 0) OR
				($3 = 'SELL' AND following_shares < 0)
			)
			AND follower_time IS NOT NULL
			AND follower_time > ($1::timestamptz - INTERVAL '5 minutes')
			AND follower_time < ($1::timestamptz + INTERVAL '2 minutes')
			ORDER BY follower_time DESC
			LIMIT 1
		)
	`, blockchainTime, tokenID, side)
	if err != nil {
		return err
	}

	if result.RowsAffected() > 0 {
		log.Printf("[Storage] ✓ Updated blockchain_confirmed_at for token=%s side=%s tx=%s", tokenID[:20], side, txHash[:16])
	}
	return nil
}

// ============================================================================
// BATCH OPERATIONS FOR SPEED OPTIMIZATION
// ============================================================================

// MarkTradesProcessedBatch marks multiple trades as processed in a single DB call
// This is significantly faster than marking one at a time
func (s *PostgresStore) MarkTradesProcessedBatch(ctx context.Context, tradeIDs []string) error {
	if len(tradeIDs) == 0 {
		return nil
	}

	// Use batch for better performance
	batch := &pgx.Batch{}
	for _, tradeID := range tradeIDs {
		batch.Queue(`
			INSERT INTO processed_trades (trade_id, processed_at)
			VALUES ($1, NOW())
			ON CONFLICT (trade_id) DO NOTHING
		`, tradeID)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	// Check for errors
	for range tradeIDs {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch mark processed failed: %w", err)
		}
	}

	return nil
}

// SaveTradesBatch saves multiple trades in a single database transaction
// Much faster than saving trades one at a time
func (s *PostgresStore) SaveTradesBatch(ctx context.Context, trades []models.TradeDetail, markProcessed bool) error {
	if len(trades) == 0 {
		return nil
	}

	// Use COPY protocol for maximum speed (10-100x faster than INSERT)
	// Fall back to batch INSERT if COPY fails

	batch := &pgx.Batch{}
	processedBatch := &pgx.Batch{}

	for _, trade := range trades {
		isMaker := trade.Role == "MAKER"

		// Queue INSERT
		batch.Queue(`
			INSERT INTO user_trades (id, user_id, market_id, subject, type, side, is_maker,
				size, usdc_size, price, outcome, timestamp, title, slug, transaction_hash, name, pseudonym)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
			ON CONFLICT (id, timestamp) DO NOTHING
		`, trade.ID, trade.UserID, trade.MarketID, string(trade.Subject), trade.Type, trade.Side, isMaker,
			trade.Size, trade.UsdcSize, trade.Price, trade.Outcome, trade.Timestamp, trade.Title,
			trade.Slug, trade.TransactionHash, trade.Name, trade.Pseudonym)

		// Queue processed marker if requested
		if markProcessed {
			processedBatch.Queue(`
				INSERT INTO processed_trades (trade_id, processed_at)
				VALUES ($1, NOW())
				ON CONFLICT (trade_id) DO NOTHING
			`, trade.ID)
		}
	}

	// Execute trades batch
	br := s.pool.SendBatch(ctx, batch)
	if err := br.Close(); err != nil {
		return fmt.Errorf("save trades batch failed: %w", err)
	}

	// Execute processed batch if needed
	if markProcessed && processedBatch.Len() > 0 {
		pbr := s.pool.SendBatch(ctx, processedBatch)
		if err := pbr.Close(); err != nil {
			return fmt.Errorf("mark processed batch failed: %w", err)
		}
	}

	return nil
}

// GetUnprocessedTradesBatch returns unprocessed trades with optimized query
// Uses LEFT JOIN instead of NOT EXISTS for better performance
func (s *PostgresStore) GetUnprocessedTradesBatch(ctx context.Context, limit int, userAddresses []string) ([]models.TradeDetail, error) {
	var rows pgx.Rows
	var err error

	if len(userAddresses) > 0 {
		// Filter by specific users (followed traders) - uses LEFT JOIN for performance
		rows, err = s.pool.Query(ctx, `
			SELECT ut.id, ut.user_id, ut.market_id, ut.subject, ut.type, ut.side, ut.is_maker,
				   ut.size, ut.usdc_size, ut.price, ut.outcome, ut.timestamp, ut.title,
				   ut.slug, ut.transaction_hash, ut.name, ut.pseudonym
			FROM user_trades ut
			LEFT JOIN processed_trades pt ON pt.trade_id = ut.id
			WHERE ut.user_id = ANY($1)
			  AND pt.trade_id IS NULL
			ORDER BY ut.timestamp ASC
			LIMIT $2
		`, userAddresses, limit)
	} else {
		// Get all unprocessed trades - uses LEFT JOIN for performance
		rows, err = s.pool.Query(ctx, `
			SELECT ut.id, ut.user_id, ut.market_id, ut.subject, ut.type, ut.side, ut.is_maker,
				   ut.size, ut.usdc_size, ut.price, ut.outcome, ut.timestamp, ut.title,
				   ut.slug, ut.transaction_hash, ut.name, ut.pseudonym
			FROM user_trades ut
			LEFT JOIN processed_trades pt ON pt.trade_id = ut.id
			WHERE pt.trade_id IS NULL
			ORDER BY ut.timestamp ASC
			LIMIT $1
		`, limit)
	}

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

// PreWarmTokenCache loads all tokens from database into memory/Redis cache
// Called at startup to ensure fast token lookups during copy trading
func (s *PostgresStore) PreWarmTokenCache(ctx context.Context) (int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT token_id, condition_id, outcome, title, slug, event_slug
		FROM token_map_cache
		WHERE token_id IS NOT NULL AND condition_id IS NOT NULL
	`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var tokenID, conditionID, outcome, title, slug, eventSlug string
		if err := rows.Scan(&tokenID, &conditionID, &outcome, &title, &slug, &eventSlug); err != nil {
			continue
		}

		// Cache in Redis for fast lookup (24h TTL)
		cacheKey := fmt.Sprintf("token:%s:%s", conditionID, outcome)
		s.redis.Set(ctx, cacheKey, fmt.Sprintf("%s|false", tokenID), 24*time.Hour)

		// Also cache reverse lookup (tokenID -> info)
		infoKey := fmt.Sprintf("tokeninfo:%s", tokenID)
		infoVal := fmt.Sprintf("%s|%s|%s|%s", conditionID, outcome, title, slug)
		s.redis.Set(ctx, infoKey, infoVal, 24*time.Hour)

		count++
	}

	return count, rows.Err()
}

// GetFollowedUserAddresses returns addresses of users with copy trading enabled
func (s *PostgresStore) GetFollowedUserAddresses(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT user_address FROM user_copy_settings WHERE enabled = true
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var addresses []string
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			continue
		}
		addresses = append(addresses, addr)
	}

	return addresses, rows.Err()
}

// BalanceRecord represents a single balance history entry
type BalanceRecord struct {
	WalletAddress string
	USDCBalance   float64
	RecordedAt    time.Time
}

// SaveBalanceHistory saves a balance snapshot to the database
func (s *PostgresStore) SaveBalanceHistory(ctx context.Context, walletAddress string, usdcBalance float64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO balance_history (wallet_address, usdc_balance)
		VALUES ($1, $2)
	`, walletAddress, usdcBalance)
	return err
}

// GetBalanceHistory retrieves recent balance history for a wallet
func (s *PostgresStore) GetBalanceHistory(ctx context.Context, walletAddress string, limit int) ([]BalanceRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT wallet_address, usdc_balance, recorded_at
		FROM balance_history
		WHERE wallet_address = $1
		ORDER BY recorded_at DESC
		LIMIT $2
	`, walletAddress, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []BalanceRecord
	for rows.Next() {
		var r BalanceRecord
		if err := rows.Scan(&r.WalletAddress, &r.USDCBalance, &r.RecordedAt); err != nil {
			continue
		}
		records = append(records, r)
	}

	return records, rows.Err()
}

// GetLatestBalance retrieves the most recent balance for a wallet
func (s *PostgresStore) GetLatestBalance(ctx context.Context, walletAddress string) (*BalanceRecord, error) {
	var r BalanceRecord
	err := s.pool.QueryRow(ctx, `
		SELECT wallet_address, usdc_balance, recorded_at
		FROM balance_history
		WHERE wallet_address = $1
		ORDER BY recorded_at DESC
		LIMIT 1
	`, walletAddress).Scan(&r.WalletAddress, &r.USDCBalance, &r.RecordedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &r, nil
}

// BackfillMissingOutcomes updates user_trades with outcome from token_map_cache
// This fixes trades that were inserted before the token cache was populated
func (s *PostgresStore) BackfillMissingOutcomes(ctx context.Context) (int64, error) {
	result, err := s.pool.Exec(ctx, `
		UPDATE user_trades ut
		SET outcome = c.outcome
		FROM token_map_cache c
		WHERE ut.market_id = c.token_id
		  AND (ut.outcome IS NULL OR ut.outcome = '')
		  AND c.outcome IS NOT NULL AND c.outcome != ''
	`)
	if err != nil {
		return 0, fmt.Errorf("backfill outcomes: %w", err)
	}
	return result.RowsAffected(), nil
}

// RefreshCopyTradePnL updates the copy_trade_pnl table with latest data from copy_trade_log
// This aggregates all trades by token_id and following_address, then updates redemption data
func (s *PostgresStore) RefreshCopyTradePnL(ctx context.Context) (int64, error) {
	// Step 1: Upsert aggregated trade data from copy_trade_log
	result, err := s.pool.Exec(ctx, `
		INSERT INTO copy_trade_pnl (
			token_id, market_title, outcome, following_address,
			following_shares_bought, following_shares_sold, following_shares_remaining,
			following_usdc_spent, following_usdc_received, following_avg_buy_price, following_avg_sell_price,
			follower_shares_bought, follower_shares_sold, follower_shares_remaining,
			follower_usdc_spent, follower_usdc_received, follower_avg_buy_price, follower_avg_sell_price,
			first_trade_at, last_trade_at
		)
		SELECT
			token_id,
			MAX(market_title) as market_title,
			MAX(outcome) as outcome,
			following_address,
			-- Note: negative shares = BUY (spent USDC), positive shares = SELL (received USDC)
			-- following_shares_bought (negative shares means bought)
			COALESCE(SUM(CASE WHEN following_shares < 0 THEN ABS(following_shares) ELSE 0 END), 0),
			-- following_shares_sold (positive shares means sold)
			COALESCE(SUM(CASE WHEN following_shares > 0 THEN following_shares ELSE 0 END), 0),
			-- following_shares_remaining = bought - sold (can be negative if they sold pre-existing shares)
			COALESCE(SUM(CASE WHEN following_shares < 0 THEN ABS(following_shares) ELSE 0 END), 0) -
			COALESCE(SUM(CASE WHEN following_shares > 0 THEN following_shares ELSE 0 END), 0),
			-- following_usdc_spent (when buying: negative shares * price)
			COALESCE(SUM(CASE WHEN following_shares < 0 THEN ABS(following_shares) * following_price ELSE 0 END), 0),
			-- following_usdc_received (when selling: positive shares * price)
			COALESCE(SUM(CASE WHEN following_shares > 0 THEN following_shares * following_price ELSE 0 END), 0),
			-- following_avg_buy_price
			CASE WHEN SUM(CASE WHEN following_shares < 0 THEN ABS(following_shares) ELSE 0 END) > 0
				 THEN SUM(CASE WHEN following_shares < 0 THEN ABS(following_shares) * following_price ELSE 0 END) /
					  SUM(CASE WHEN following_shares < 0 THEN ABS(following_shares) ELSE 0 END)
				 ELSE 0 END,
			-- following_avg_sell_price
			CASE WHEN SUM(CASE WHEN following_shares > 0 THEN following_shares ELSE 0 END) > 0
				 THEN SUM(CASE WHEN following_shares > 0 THEN following_shares * following_price ELSE 0 END) /
					  SUM(CASE WHEN following_shares > 0 THEN following_shares ELSE 0 END)
				 ELSE 0 END,
			-- follower_shares_bought (when following bought, we bought: negative following_shares)
			COALESCE(SUM(CASE WHEN status = 'executed' AND following_shares < 0 THEN ABS(follower_shares) ELSE 0 END), 0),
			-- follower_shares_sold (when following sold, we sold: positive following_shares)
			COALESCE(SUM(CASE WHEN status = 'executed' AND following_shares > 0 THEN ABS(follower_shares) ELSE 0 END), 0),
			-- follower_shares_remaining = bought - sold, capped at 0 (we can't go negative)
			GREATEST(
				COALESCE(SUM(CASE WHEN status = 'executed' AND following_shares < 0 THEN ABS(follower_shares) ELSE 0 END), 0) -
				COALESCE(SUM(CASE WHEN status = 'executed' AND following_shares > 0 THEN ABS(follower_shares) ELSE 0 END), 0),
				0
			),
			-- follower_usdc_spent (when buying)
			COALESCE(SUM(CASE WHEN status = 'executed' AND following_shares < 0 THEN ABS(follower_shares) * follower_price ELSE 0 END), 0),
			-- follower_usdc_received (when selling)
			COALESCE(SUM(CASE WHEN status = 'executed' AND following_shares > 0 THEN ABS(follower_shares) * follower_price ELSE 0 END), 0),
			-- follower_avg_buy_price
			CASE WHEN SUM(CASE WHEN status = 'executed' AND following_shares < 0 THEN ABS(follower_shares) ELSE 0 END) > 0
				 THEN SUM(CASE WHEN status = 'executed' AND following_shares < 0 THEN ABS(follower_shares) * follower_price ELSE 0 END) /
					  SUM(CASE WHEN status = 'executed' AND following_shares < 0 THEN ABS(follower_shares) ELSE 0 END)
				 ELSE 0 END,
			-- follower_avg_sell_price
			CASE WHEN SUM(CASE WHEN status = 'executed' AND following_shares > 0 THEN ABS(follower_shares) ELSE 0 END) > 0
				 THEN SUM(CASE WHEN status = 'executed' AND following_shares > 0 THEN ABS(follower_shares) * follower_price ELSE 0 END) /
					  SUM(CASE WHEN status = 'executed' AND following_shares > 0 THEN ABS(follower_shares) ELSE 0 END)
				 ELSE 0 END,
			MIN(following_time),
			MAX(following_time)
		FROM copy_trade_log
		WHERE token_id IS NOT NULL
		  AND status = 'executed'
		GROUP BY token_id, following_address
		ON CONFLICT (token_id, following_address) DO UPDATE SET
			following_shares_bought = EXCLUDED.following_shares_bought,
			following_shares_sold = EXCLUDED.following_shares_sold,
			following_shares_remaining = EXCLUDED.following_shares_remaining,
			following_usdc_spent = EXCLUDED.following_usdc_spent,
			following_usdc_received = EXCLUDED.following_usdc_received,
			following_avg_buy_price = EXCLUDED.following_avg_buy_price,
			following_avg_sell_price = EXCLUDED.following_avg_sell_price,
			follower_shares_bought = EXCLUDED.follower_shares_bought,
			follower_shares_sold = EXCLUDED.follower_shares_sold,
			follower_shares_remaining = GREATEST(EXCLUDED.follower_shares_remaining, 0),
			follower_usdc_spent = EXCLUDED.follower_usdc_spent,
			follower_usdc_received = EXCLUDED.follower_usdc_received,
			follower_avg_buy_price = EXCLUDED.follower_avg_buy_price,
			follower_avg_sell_price = EXCLUDED.follower_avg_sell_price,
			first_trade_at = EXCLUDED.first_trade_at,
			last_trade_at = EXCLUDED.last_trade_at,
			updated_at = NOW()
	`)
	if err != nil {
		return 0, fmt.Errorf("upsert copy_trade_pnl: %w", err)
	}
	upsertCount := result.RowsAffected()

	// Step 1b: Update market_title and market_start_time from token_map_cache where missing
	// Uses slug as the title since it's more readable (e.g., "btc-updown-15m-1764577800")
	// Extracts Unix timestamp from slug to get market_start_time
	_, err = s.pool.Exec(ctx, `
		UPDATE copy_trade_pnl p
		SET
			market_title = c.slug,
			market_start_time = CASE
				WHEN c.slug ~ '-[0-9]+$'
				THEN to_timestamp(CAST(substring(c.slug from '-([0-9]+)$') AS bigint))
				ELSE NULL
			END,
			updated_at = NOW()
		FROM token_map_cache c
		WHERE p.token_id = c.token_id
		  AND (p.market_title IS NULL OR p.market_title = '')
		  AND c.slug IS NOT NULL AND c.slug != ''
	`)
	if err != nil {
		log.Printf("[pnl] Warning: failed to update market_title from cache: %v", err)
	}

	// Step 2: Update redemption data from user_trades for resolved markets
	_, err = s.pool.Exec(ctx, `
		WITH redeems AS (
			SELECT
				market_id as token_id,
				user_id,
				outcome,
				SUM(usdc_size) as redeem_amount,
				MAX(timestamp) as redeem_time
			FROM user_trades
			WHERE type = 'REDEEM'
			GROUP BY market_id, user_id, outcome
		)
		UPDATE copy_trade_pnl p
		SET
			market_resolved = TRUE,
			winning_outcome = r.outcome,
			following_redeem_amount = r.redeem_amount,
			-- Follower redeem amount: if our outcome matches the winner, our remaining shares are worth $1 each
			follower_redeem_amount = CASE WHEN p.outcome = r.outcome THEN GREATEST(p.follower_shares_remaining, 0) ELSE 0 END,
			resolved_at = r.redeem_time,
			following_net_pnl = p.following_usdc_received + r.redeem_amount - p.following_usdc_spent,
			-- Follower P&L: if followed user redeemed, this outcome won, so our shares are worth $1
			follower_net_pnl = p.follower_usdc_received - p.follower_usdc_spent +
				CASE WHEN p.outcome = r.outcome THEN GREATEST(p.follower_shares_remaining, 0) ELSE 0 END,
			updated_at = NOW()
		FROM redeems r
		WHERE p.token_id = r.token_id
		  AND p.following_address = r.user_id
		  AND p.outcome = r.outcome
		  AND (p.market_resolved = FALSE OR p.following_redeem_amount != r.redeem_amount)
	`)
	if err != nil {
		return upsertCount, fmt.Errorf("update redemptions: %w", err)
	}

	// Step 3: Calculate slippage per market
	// Sign convention: following_shares < 0 = BUY, following_shares > 0 = SELL
	// BUY slippage = (follower_price - following_price) * |follower_shares| (positive = we paid more)
	// SELL slippage = (following_price - follower_price) * |follower_shares| (positive = we got less)
	// Percentage is based on OUR cost/value, not the followed user's
	_, err = s.pool.Exec(ctx, `
		WITH slippage AS (
			SELECT
				token_id,
				following_address,
				-- BUY slippage USD (negative following_shares = buy, filter bad prices)
				COALESCE(SUM(CASE
					WHEN status = 'executed' AND following_shares < 0 AND follower_price >= 0.02
					THEN (follower_price - following_price) * ABS(follower_shares)
					ELSE 0 END), 0) as buy_slip_usd,
				-- SELL slippage USD (positive following_shares = sell, filter bad prices)
				COALESCE(SUM(CASE
					WHEN status = 'executed' AND following_shares > 0 AND follower_price >= 0.02
					THEN (following_price - follower_price) * ABS(follower_shares)
					ELSE 0 END), 0) as sell_slip_usd,
				-- OUR buy cost (what we actually paid)
				COALESCE(SUM(CASE WHEN status = 'executed' AND following_shares < 0 AND follower_price >= 0.02
					THEN ABS(follower_shares) * follower_price ELSE 0 END), 0) as our_buy_cost,
				-- OUR sell value (what we actually received)
				COALESCE(SUM(CASE WHEN status = 'executed' AND following_shares > 0 AND follower_price >= 0.02
					THEN ABS(follower_shares) * follower_price ELSE 0 END), 0) as our_sell_value
			FROM copy_trade_log
			WHERE token_id IS NOT NULL
			GROUP BY token_id, following_address
		)
		UPDATE copy_trade_pnl p
		SET
			buy_slippage_usd = s.buy_slip_usd,
			sell_slippage_usd = s.sell_slip_usd,
			-- Buy slippage % = (our_avg_price / their_avg_price) - 1 (positive = we paid more)
			buy_slippage_pct = CASE WHEN p.following_avg_buy_price > 0
				THEN (p.follower_avg_buy_price / p.following_avg_buy_price) - 1
				ELSE 0 END,
			-- Sell slippage % = (their_avg_price / our_avg_price) - 1 (positive = we got less)
			sell_slippage_pct = CASE WHEN p.follower_avg_sell_price > 0
				THEN (p.following_avg_sell_price / p.follower_avg_sell_price) - 1
				ELSE 0 END,
			total_slippage_usd = s.buy_slip_usd + s.sell_slip_usd
		FROM slippage s
		WHERE p.token_id = s.token_id
		  AND p.following_address = s.following_address
	`)
	if err != nil {
		return upsertCount, fmt.Errorf("update slippage: %w", err)
	}

	// Step 4: Calculate estimated multiplier (rounded to nearest 0.05)
	// Multiplier = follower_shares_bought / following_shares_bought
	_, err = s.pool.Exec(ctx, `
		UPDATE copy_trade_pnl
		SET estimated_multiplier = ROUND(
			CASE
				WHEN following_shares_bought > 0
				THEN (follower_shares_bought / following_shares_bought) / 0.05
				ELSE 0
			END
		) * 0.05
		WHERE following_shares_bought > 0
	`)
	if err != nil {
		return upsertCount, fmt.Errorf("update estimated_multiplier: %w", err)
	}

	return upsertCount, nil
}

// CheckMarketResolutions queries Gamma API for unresolved markets and updates their status
// This determines the winning outcome and calculates theoretical P&L
func (s *PostgresStore) CheckMarketResolutions(ctx context.Context) (int, error) {
	// Get unresolved markets (older than 15 min to catch BTC 15m markets faster)
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT token_id
		FROM copy_trade_pnl
		WHERE market_resolved = FALSE
		  AND last_trade_at < NOW() - INTERVAL '15 minutes'
		LIMIT 50
	`)
	if err != nil {
		return 0, fmt.Errorf("query unresolved markets: %w", err)
	}
	defer rows.Close()

	var tokenIDs []string
	for rows.Next() {
		var tokenID string
		if err := rows.Scan(&tokenID); err != nil {
			continue
		}
		tokenIDs = append(tokenIDs, tokenID)
	}

	if len(tokenIDs) == 0 {
		return 0, nil
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resolved := 0

	for _, tokenID := range tokenIDs {
		// Check context cancellation
		if ctx.Err() != nil {
			break
		}

		// Query Gamma API for this token
		url := "https://gamma-api.polymarket.com/markets?clob_token_ids=" + tokenID
		resp, err := client.Get(url)
		if err != nil {
			log.Printf("[pnl] failed to query Gamma API for token %s: %v", tokenID[:20], err)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}

		var markets []struct {
			Closed        bool   `json:"closed"`
			Outcomes      string `json:"outcomes"`
			OutcomePrices string `json:"outcomePrices"`
		}
		if err := json.Unmarshal(body, &markets); err != nil || len(markets) == 0 {
			continue
		}

		market := markets[0]
		if !market.Closed {
			continue // Market not closed yet
		}

		// Parse outcomes and prices to find winner
		var outcomes []string
		var prices []string
		json.Unmarshal([]byte(market.Outcomes), &outcomes)
		json.Unmarshal([]byte(market.OutcomePrices), &prices)

		if len(outcomes) == 0 || len(prices) == 0 {
			continue
		}

		// Find winning outcome (price = "1" or close to it)
		var winningOutcome string
		for i, price := range prices {
			if price == "1" || strings.HasPrefix(price, "0.99") {
				if i < len(outcomes) {
					winningOutcome = outcomes[i]
				}
				break
			}
		}

		if winningOutcome == "" {
			continue // Can't determine winner
		}

		// Update all rows with this token_id
		// Calculate P&L: if our outcome matches winner, remaining shares * $1
		// Also calculate redeem amounts = remaining shares if outcome won (shares worth $1 each)
		result, err := s.pool.Exec(ctx, `
			UPDATE copy_trade_pnl
			SET
				market_resolved = TRUE,
				winning_outcome = $2::text,
				-- Redeem amounts: remaining shares are worth $1 each if outcome won
				following_redeem_amount = CASE WHEN outcome = $2::text THEN GREATEST(following_shares_remaining, 0) ELSE 0::numeric END,
				follower_redeem_amount = CASE WHEN outcome = $2::text THEN GREATEST(follower_shares_remaining, 0) ELSE 0::numeric END,
				-- Following user P&L: if their outcome won, remaining shares are worth $1 each
				following_net_pnl = following_usdc_received - following_usdc_spent +
					CASE WHEN outcome = $2::text THEN GREATEST(following_shares_remaining, 0) ELSE 0::numeric END,
				-- Follower P&L: same logic
				follower_net_pnl = follower_usdc_received - follower_usdc_spent +
					CASE WHEN outcome = $2::text THEN GREATEST(follower_shares_remaining, 0) ELSE 0::numeric END,
				resolved_at = NOW(),
				updated_at = NOW()
			WHERE token_id = $1
			  AND market_resolved = FALSE
		`, tokenID, winningOutcome)

		if err != nil {
			log.Printf("[pnl] failed to update resolution for token %s: %v", tokenID[:20], err)
			continue
		}

		if result.RowsAffected() > 0 {
			resolved += int(result.RowsAffected())
			log.Printf("[pnl] resolved market %s: winner=%s", tokenID[:20], winningOutcome)
		}

		// Small delay to avoid hammering API
		time.Sleep(100 * time.Millisecond)
	}

	return resolved, nil
}

// ============================================================================
// Potential PNL Calculation (What-If Analysis)
// ============================================================================

// RefreshPotentialPnL calculates what PNL we would have made if we copied ALL trades
// for each followed user, using a fixed 0.5 multiplier and average observed slippage.
// Only considers resolved markets. Calculates per week (1=0-7 days, 2=7-14 days, 3=14-21 days).
func (s *PostgresStore) RefreshPotentialPnL(ctx context.Context) error {
	log.Printf("[potential-pnl] Starting potential PNL refresh...")

	// Step 1: Get global average slippage from actual copy trades (excluding zeros)
	var avgBuySlippage, avgSellSlippage float64
	err := s.pool.QueryRow(ctx, `
		SELECT
			COALESCE(AVG(CASE WHEN buy_slippage_pct != 0 THEN buy_slippage_pct END), 0.005) as avg_buy,
			COALESCE(AVG(CASE WHEN sell_slippage_pct != 0 THEN sell_slippage_pct END), -0.005) as avg_sell
		FROM copy_trade_pnl
	`).Scan(&avgBuySlippage, &avgSellSlippage)
	if err != nil {
		return fmt.Errorf("get avg slippage: %w", err)
	}
	log.Printf("[potential-pnl] Using avg slippage: buy=%.4f%%, sell=%.4f%%", avgBuySlippage*100, avgSellSlippage*100)

	// Step 2: Get list of followed users (from user_copy_settings, not just enabled)
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT user_address FROM user_copy_settings`)
	if err != nil {
		return fmt.Errorf("get followed users: %w", err)
	}
	defer rows.Close()

	var followedUsers []string
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			continue
		}
		followedUsers = append(followedUsers, addr)
	}
	log.Printf("[potential-pnl] Processing %d followed users x 3 weeks", len(followedUsers))

	// Step 3: Update market_resolutions cache for markets we haven't checked yet
	if err := s.updateMarketResolutionsCache(ctx); err != nil {
		log.Printf("[potential-pnl] Warning: failed to update resolutions cache: %v", err)
	}

	// Step 4: For each user, calculate potential PNL for each week
	const multiplier = 0.5

	for _, userAddr := range followedUsers {
		if ctx.Err() != nil {
			break
		}

		// Calculate for weeks 1, 2, 3
		for week := 1; week <= 3; week++ {
			pnl, userPnl, resolvedMarkets, resolvedTrades, unresolvedMarkets, err := s.calculateUserPotentialPnLWeek(
				ctx, userAddr, multiplier, avgBuySlippage, avgSellSlippage, week,
			)
			if err != nil {
				log.Printf("[potential-pnl] Error calculating week %d for %s: %v", week, userAddr[:10], err)
				continue
			}

			// Upsert result
			_, err = s.pool.Exec(ctx, `
				INSERT INTO potential_pnl (user_address, week_number, potential_pnl, user_actual_pnl, total_resolved_markets, total_resolved_trades,
					total_unresolved_markets, avg_buy_slippage_used, avg_sell_slippage_used, multiplier_used, updated_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
				ON CONFLICT (user_address, week_number) DO UPDATE SET
					potential_pnl = EXCLUDED.potential_pnl,
					user_actual_pnl = EXCLUDED.user_actual_pnl,
					total_resolved_markets = EXCLUDED.total_resolved_markets,
					total_resolved_trades = EXCLUDED.total_resolved_trades,
					total_unresolved_markets = EXCLUDED.total_unresolved_markets,
					avg_buy_slippage_used = EXCLUDED.avg_buy_slippage_used,
					avg_sell_slippage_used = EXCLUDED.avg_sell_slippage_used,
					multiplier_used = EXCLUDED.multiplier_used,
					updated_at = NOW()
			`, userAddr, week, pnl, userPnl, resolvedMarkets, resolvedTrades, unresolvedMarkets, avgBuySlippage, avgSellSlippage, multiplier)

			if err != nil {
				log.Printf("[potential-pnl] Failed to save week %d for %s: %v", week, userAddr[:10], err)
				continue
			}

			ratio := 0.0
			if userPnl != 0 {
				ratio = pnl / userPnl
			}
			log.Printf("[potential-pnl] User %s week %d: our_pnl=$%.2f, user_pnl=$%.2f (ratio=%.2f)",
				userAddr[:10], week, pnl, userPnl, ratio)
		}
	}

	log.Printf("[potential-pnl] Refresh complete for %d users", len(followedUsers))
	return nil
}

// updateMarketResolutionsCache checks resolution status for markets in user_trades
// that we haven't checked recently
func (s *PostgresStore) updateMarketResolutionsCache(ctx context.Context) error {
	// Get unique market_ids from user_trades that aren't in our cache or haven't been checked in 1 hour
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ut.market_id
		FROM user_trades ut
		LEFT JOIN market_resolutions mr ON ut.market_id = mr.token_id
		WHERE mr.token_id IS NULL
		   OR (mr.market_closed = FALSE AND mr.checked_at < NOW() - INTERVAL '1 hour')
		LIMIT 100
	`)
	if err != nil {
		return fmt.Errorf("query unchecked markets: %w", err)
	}
	defer rows.Close()

	var tokenIDs []string
	for rows.Next() {
		var tokenID string
		if err := rows.Scan(&tokenID); err != nil {
			continue
		}
		tokenIDs = append(tokenIDs, tokenID)
	}

	if len(tokenIDs) == 0 {
		return nil
	}

	log.Printf("[potential-pnl] Checking resolution for %d markets", len(tokenIDs))
	client := &http.Client{Timeout: 10 * time.Second}

	for _, tokenID := range tokenIDs {
		if ctx.Err() != nil {
			break
		}

		closed, winningOutcome := s.checkMarketResolutionGamma(client, tokenID)

		// Upsert to cache
		_, err := s.pool.Exec(ctx, `
			INSERT INTO market_resolutions (token_id, market_closed, winning_outcome, resolved_at, checked_at)
			VALUES ($1, $2, $3, CASE WHEN $2 THEN NOW() ELSE NULL END, NOW())
			ON CONFLICT (token_id) DO UPDATE SET
				market_closed = EXCLUDED.market_closed,
				winning_outcome = EXCLUDED.winning_outcome,
				resolved_at = CASE WHEN EXCLUDED.market_closed AND market_resolutions.resolved_at IS NULL THEN NOW() ELSE market_resolutions.resolved_at END,
				checked_at = NOW()
		`, tokenID, closed, winningOutcome)

		if err != nil {
			log.Printf("[potential-pnl] Failed to cache resolution for %s: %v", tokenID[:20], err)
		}

		// Rate limit
		time.Sleep(50 * time.Millisecond)
	}

	return nil
}

// checkMarketResolutionGamma queries Gamma API to check if a market is resolved
func (s *PostgresStore) checkMarketResolutionGamma(client *http.Client, tokenID string) (closed bool, winningOutcome string) {
	url := "https://gamma-api.polymarket.com/markets?clob_token_ids=" + tokenID
	resp, err := client.Get(url)
	if err != nil {
		return false, ""
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, ""
	}

	var markets []struct {
		Closed        bool   `json:"closed"`
		Outcomes      string `json:"outcomes"`
		OutcomePrices string `json:"outcomePrices"`
	}
	if err := json.Unmarshal(body, &markets); err != nil || len(markets) == 0 {
		return false, ""
	}

	market := markets[0]
	if !market.Closed {
		return false, ""
	}

	// Parse outcomes and prices to find winner
	var outcomes []string
	var prices []string
	json.Unmarshal([]byte(market.Outcomes), &outcomes)
	json.Unmarshal([]byte(market.OutcomePrices), &prices)

	for i, price := range prices {
		if price == "1" || strings.HasPrefix(price, "0.99") {
			if i < len(outcomes) {
				return true, outcomes[i]
			}
		}
	}

	return true, "" // Closed but can't determine winner
}

// calculateUserPotentialPnLWeek calculates the potential PNL for a single user for a specific week
// week 1 = 0-7 days ago, week 2 = 7-14 days ago, week 3 = 14-21 days ago
// Returns both our simulated PNL and the user's actual PNL
func (s *PostgresStore) calculateUserPotentialPnLWeek(
	ctx context.Context,
	userAddr string,
	multiplier, avgBuySlippage, avgSellSlippage float64,
	week int,
) (pnl, userPnl float64, resolvedMarkets, resolvedTrades, unresolvedMarkets int, err error) {

	// Calculate date range for this week
	// Week 1: NOW - 7 days to NOW
	// Week 2: NOW - 14 days to NOW - 7 days
	// Week 3: NOW - 21 days to NOW - 14 days
	startDays := (week - 1) * 7
	endDays := week * 7

	// Simple realized PNL formula: sells + redeems - buys
	// This only counts actual money flow, no estimation of unredeemed shares
	var totalBuys, totalSells, totalRedeems float64
	var tradeCount int

	err = s.pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN type = 'TRADE' AND side = 'BUY' THEN usdc_size ELSE 0 END), 0) as total_buys,
			COALESCE(SUM(CASE WHEN type = 'TRADE' AND side = 'SELL' THEN usdc_size ELSE 0 END), 0) as total_sells,
			COALESCE(SUM(CASE WHEN type = 'REDEEM' THEN usdc_size ELSE 0 END), 0) as total_redeems,
			COUNT(CASE WHEN type = 'TRADE' THEN 1 END) as trade_count
		FROM user_trades
		WHERE user_id = $1
		  AND timestamp >= NOW() - INTERVAL '1 day' * $3
		  AND timestamp < NOW() - INTERVAL '1 day' * $2
	`, userAddr, startDays, endDays).Scan(&totalBuys, &totalSells, &totalRedeems, &tradeCount)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("query user trades: %w", err)
	}

	// User's actual PNL = sells + redeems - buys
	userPnl = totalSells + totalRedeems - totalBuys

	// Our simulated PNL with multiplier and slippage
	// Buy: apply slippage (we pay more if positive slippage)
	simBuyCost := totalBuys * multiplier * (1 + avgBuySlippage)
	// Sell: apply slippage (we receive less if positive slippage)
	simSellProceeds := totalSells * multiplier * (1 - avgSellSlippage)
	// Redeems: same multiplier, no slippage (it's just $1 per winning share)
	simRedeems := totalRedeems * multiplier

	pnl = simSellProceeds + simRedeems - simBuyCost

	// Count markets (for info only)
	err = s.pool.QueryRow(ctx, `
		SELECT
			COUNT(DISTINCT market_id) as total_markets,
			COUNT(DISTINCT CASE WHEN mr.market_closed = true THEN ut.market_id END) as resolved_markets
		FROM user_trades ut
		LEFT JOIN market_resolutions mr ON ut.market_id = mr.token_id
		WHERE ut.user_id = $1
		  AND ut.type = 'TRADE'
		  AND ut.timestamp >= NOW() - INTERVAL '1 day' * $3
		  AND ut.timestamp < NOW() - INTERVAL '1 day' * $2
	`, userAddr, startDays, endDays).Scan(&unresolvedMarkets, &resolvedMarkets)
	if err != nil {
		// Non-fatal, just use zeros
		unresolvedMarkets = 0
		resolvedMarkets = 0
	}
	unresolvedMarkets = unresolvedMarkets - resolvedMarkets // Adjust to get actual unresolved count
	resolvedTrades = tradeCount

	return pnl, userPnl, resolvedMarkets, resolvedTrades, unresolvedMarkets, nil
}

// ============================================================================
// Binance Price Data Storage
// ============================================================================

// BinancePrice represents a single price data point
type BinancePrice struct {
	Timestamp        time.Time
	Open             float64
	High             float64
	Low              float64
	Close            float64
	Volume           float64
	QuoteAssetVolume float64
	NumTrades        int64
}

// BinanceKline matches api.BinanceKline for storage
type BinanceKline struct {
	OpenTime         int64
	Open             float64
	High             float64
	Low              float64
	Close            float64
	Volume           float64
	CloseTime        int64
	QuoteAssetVolume float64
	NumTrades        int64
}

// EnsureBinancePriceTable creates the binance_prices table if it doesn't exist
func (s *PostgresStore) EnsureBinancePriceTable(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS binance_prices (
			symbol VARCHAR(20) NOT NULL,
			timestamp TIMESTAMPTZ NOT NULL,
			open_price DECIMAL(20, 8) NOT NULL,
			high_price DECIMAL(20, 8) NOT NULL,
			low_price DECIMAL(20, 8) NOT NULL,
			close_price DECIMAL(20, 8) NOT NULL,
			volume DECIMAL(30, 8) NOT NULL,
			quote_volume DECIMAL(30, 8) NOT NULL,
			num_trades BIGINT NOT NULL,
			PRIMARY KEY (symbol, timestamp)
		);

		-- Index for time-based queries
		CREATE INDEX IF NOT EXISTS idx_binance_prices_symbol_time
			ON binance_prices (symbol, timestamp DESC);
	`)
	if err != nil {
		return fmt.Errorf("create binance_prices table: %w", err)
	}
	log.Printf("[Storage] Ensured binance_prices table exists")
	return nil
}

// SaveBinancePrices saves a batch of klines to the database using efficient bulk insert.
// Uses multi-row INSERT with ON CONFLICT for upsert behavior.
// Processes in chunks of 1000 rows to balance memory and performance.
func (s *PostgresStore) SaveBinancePrices(ctx context.Context, symbol string, klines []BinanceKline) error {
	if len(klines) == 0 {
		return nil
	}

	const chunkSize = 1000 // Rows per INSERT statement

	for i := 0; i < len(klines); i += chunkSize {
		end := i + chunkSize
		if end > len(klines) {
			end = len(klines)
		}
		chunk := klines[i:end]

		if err := s.insertBinancePriceChunk(ctx, symbol, chunk); err != nil {
			return fmt.Errorf("insert chunk %d-%d: %w", i, end, err)
		}
	}

	return nil
}

// insertBinancePriceChunk inserts a chunk using multi-row INSERT (much faster than individual inserts)
func (s *PostgresStore) insertBinancePriceChunk(ctx context.Context, symbol string, klines []BinanceKline) error {
	if len(klines) == 0 {
		return nil
	}

	// Build multi-row INSERT: INSERT INTO ... VALUES (...), (...), (...) ON CONFLICT DO UPDATE
	// This is 10-50x faster than individual INSERTs
	args := make([]interface{}, 0, len(klines)*9)
	valueStrings := make([]string, 0, len(klines))

	for i, k := range klines {
		ts := time.UnixMilli(k.OpenTime).UTC()
		base := i * 9
		valueStrings = append(valueStrings,
			fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
				base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9))
		args = append(args, symbol, ts, k.Open, k.High, k.Low, k.Close, k.Volume, k.QuoteAssetVolume, k.NumTrades)
	}

	query := `
		INSERT INTO binance_prices (symbol, timestamp, open_price, high_price, low_price, close_price, volume, quote_volume, num_trades)
		VALUES ` + strings.Join(valueStrings, ", ") + `
		ON CONFLICT (symbol, timestamp) DO UPDATE SET
			open_price = EXCLUDED.open_price,
			high_price = EXCLUDED.high_price,
			low_price = EXCLUDED.low_price,
			close_price = EXCLUDED.close_price,
			volume = EXCLUDED.volume,
			quote_volume = EXCLUDED.quote_volume,
			num_trades = EXCLUDED.num_trades
	`

	_, err := s.pool.Exec(ctx, query, args...)
	return err
}

// GetLatestBinancePrice returns the most recent price timestamp for a symbol
func (s *PostgresStore) GetLatestBinancePrice(ctx context.Context, symbol string) (time.Time, error) {
	var ts time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT timestamp FROM binance_prices
		WHERE symbol = $1
		ORDER BY timestamp DESC
		LIMIT 1
	`, symbol).Scan(&ts)
	if err != nil {
		return time.Time{}, err
	}
	return ts, nil
}

// GetOldestBinancePrice returns the oldest price timestamp for a symbol
func (s *PostgresStore) GetOldestBinancePrice(ctx context.Context, symbol string) (time.Time, error) {
	var ts time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT timestamp FROM binance_prices
		WHERE symbol = $1
		ORDER BY timestamp ASC
		LIMIT 1
	`, symbol).Scan(&ts)
	if err != nil {
		return time.Time{}, err
	}
	return ts, nil
}

// GetBinancePriceCount returns the total number of price records for a symbol
func (s *PostgresStore) GetBinancePriceCount(ctx context.Context, symbol string) (int64, error) {
	var count int64
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM binance_prices WHERE symbol = $1
	`, symbol).Scan(&count)
	return count, err
}

// GetBinancePriceAt returns the price at or just before a specific timestamp
func (s *PostgresStore) GetBinancePriceAt(ctx context.Context, symbol string, timestamp time.Time) (float64, error) {
	var price float64
	err := s.pool.QueryRow(ctx, `
		SELECT close_price FROM binance_prices
		WHERE symbol = $1 AND timestamp <= $2
		ORDER BY timestamp DESC
		LIMIT 1
	`, symbol, timestamp.UTC()).Scan(&price)
	if err != nil {
		return 0, fmt.Errorf("get price at %s: %w", timestamp, err)
	}
	return price, nil
}

// GetBinancePriceRange returns all prices in a time range
func (s *PostgresStore) GetBinancePriceRange(ctx context.Context, symbol string, startTime, endTime time.Time) ([]BinancePrice, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT timestamp, open_price, high_price, low_price, close_price, volume, quote_volume, num_trades
		FROM binance_prices
		WHERE symbol = $1 AND timestamp >= $2 AND timestamp <= $3
		ORDER BY timestamp ASC
	`, symbol, startTime.UTC(), endTime.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var prices []BinancePrice
	for rows.Next() {
		var p BinancePrice
		if err := rows.Scan(&p.Timestamp, &p.Open, &p.High, &p.Low, &p.Close, &p.Volume, &p.QuoteAssetVolume, &p.NumTrades); err != nil {
			continue
		}
		prices = append(prices, p)
	}

	return prices, rows.Err()
}
