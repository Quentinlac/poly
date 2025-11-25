package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"polymarket-analyzer/models"

	_ "modernc.org/sqlite"
)

// Store wraps SQLite persistence for markets, users, and trades.
type Store struct {
	db *sql.DB
}

// New opens (and creates if needed) the SQLite database at dbPath.
func New(dbPath string) (*Store, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("storage: db path is empty")
	}

	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("storage: mkdir %s: %w", filepath.Dir(dbPath), err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("storage: open database: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetConnMaxIdleTime(0)

	store := &Store{db: db}
	if err := store.runMigrations(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil
}

// Close releases the underlying database handle.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// SaveUserSnapshot upserts a single user along with subject metrics and red flags.
func (s *Store) SaveUserSnapshot(ctx context.Context, user models.User) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := s.saveUserTx(ctx, tx, user); err != nil {
		return err
	}

	return tx.Commit()
}

// ReplaceAllUsers overwrites all leaderboard entries with the provided users.
func (s *Store) ReplaceAllUsers(ctx context.Context, users []models.User) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM user_subject_metrics`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM user_red_flags`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM users`); err != nil {
		return err
	}

	for _, user := range users {
		if err := s.saveUserTx(ctx, tx, user); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// ReplaceTrades overwrites the stored trades snapshot with the provided trades.
func (s *Store) ReplaceTrades(ctx context.Context, trades map[string][]models.TradeDetail) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM user_trades`); err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, `
        INSERT INTO user_trades (
            id, user_address, asset, subject, type, side, role, size, usdc_size, price, outcome,
            timestamp, title, slug, transaction_hash, name, pseudonym, inserted_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for userID, tradeList := range trades {
		for _, trade := range tradeList {
			if _, err := stmt.ExecContext(
				ctx,
				trade.ID,
				userID,
				trade.MarketID,
				string(trade.Subject),
				trade.Type,
				trade.Side,
				trade.Role,
				trade.Size,
				trade.UsdcSize,
				trade.Price,
				trade.Outcome,
				timeString(trade.Timestamp),
				trade.Title,
				trade.Slug,
				trade.TransactionHash,
				trade.Name,
				trade.Pseudonym,
				timeString(time.Now()), // Set inserted_at to now
			); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

// SaveTrades upserts a batch of trades (used for importing user data).
// If markProcessed is true, trades are also marked as processed to skip copy trading.
// Note: SQLite backend does not support copy trading, so markProcessed is ignored here.
func (s *Store) SaveTrades(ctx context.Context, trades []models.TradeDetail, markProcessed bool) error {
	if len(trades) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
        INSERT INTO user_trades (
            id, user_address, asset, subject, type, side, role, size, usdc_size, price, outcome,
            timestamp, title, slug, transaction_hash, name, pseudonym, inserted_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            user_address = excluded.user_address,
            asset = excluded.asset,
            subject = excluded.subject,
            type = excluded.type,
            side = excluded.side,
            role = excluded.role,
            size = excluded.size,
            usdc_size = excluded.usdc_size,
            price = excluded.price,
            outcome = excluded.outcome,
            timestamp = excluded.timestamp,
            title = excluded.title,
            slug = excluded.slug,
            transaction_hash = excluded.transaction_hash,
            name = excluded.name,
            pseudonym = excluded.pseudonym
            -- Do NOT update inserted_at on conflict, keep original insertion time
    `)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, trade := range trades {
		if _, err := stmt.ExecContext(
			ctx,
			trade.ID,
			trade.UserID,
			trade.MarketID,
			string(trade.Subject),
			trade.Type,
			trade.Side,
			trade.Role,
			trade.Size,
			trade.UsdcSize,
			trade.Price,
			trade.Outcome,
			timeString(trade.Timestamp),
			trade.Title,
			trade.Slug,
			trade.TransactionHash,
			trade.Name,
			trade.Pseudonym,
			timeString(time.Now()), // Set inserted_at to now
		); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// SaveGlobalTrades saves trades to the global_trades table for platform-wide monitoring.
// Unlike SaveTrades, this doesn't require users to exist in the users table.
func (s *Store) SaveGlobalTrades(ctx context.Context, trades []models.TradeDetail) error {
	if len(trades) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
        INSERT INTO global_trades (
            id, user_address, asset, type, side, size, usdc_size, price, outcome,
            timestamp, title, slug, transaction_hash, inserted_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO NOTHING
    `)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, trade := range trades {
		if _, err := stmt.ExecContext(
			ctx,
			trade.ID,
			trade.UserID,
			trade.MarketID,
			trade.Type,
			trade.Side,
			trade.Size,
			trade.UsdcSize,
			trade.Price,
			trade.Outcome,
			timeString(trade.Timestamp),
			trade.Title,
			trade.Slug,
			trade.TransactionHash,
			timeString(time.Now()),
		); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// ListUserTrades returns the most recent trades for a user.
func (s *Store) ListUserTrades(ctx context.Context, userID string, limit int) ([]models.TradeDetail, error) {
	if limit <= 0 {
		limit = 200
	}

	rows, err := s.db.QueryContext(ctx, `
        SELECT id, user_address, asset, subject, type, side, role, size, usdc_size, price, outcome,
               timestamp, title, slug, transaction_hash, name, pseudonym, inserted_at
        FROM user_trades
        WHERE user_address = ?
        ORDER BY datetime(timestamp) DESC
        LIMIT ?`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var trades []models.TradeDetail
	for rows.Next() {
		var trade models.TradeDetail
		var subject string
		var tradeType sql.NullString
		var role sql.NullString
		var usdcSize sql.NullFloat64
		var ts, insertedAt sql.NullString
		if err := rows.Scan(
			&trade.ID,
			&trade.UserID,
			&trade.MarketID,
			&subject,
			&tradeType,
			&trade.Side,
			&role,
			&trade.Size,
			&usdcSize,
			&trade.Price,
			&trade.Outcome,
			&ts,
			&trade.Title,
			&trade.Slug,
			&trade.TransactionHash,
			&trade.Name,
			&trade.Pseudonym,
			&insertedAt,
		); err != nil {
			return nil, err
		}
		trade.Subject = models.Subject(subject)
		if tradeType.Valid {
			trade.Type = tradeType.String
		} else {
			trade.Type = "TRADE"
		}
		if role.Valid {
			trade.Role = role.String
		}
		if usdcSize.Valid {
			trade.UsdcSize = usdcSize.Float64
		}
		if ts.Valid {
			if parsed, err := time.Parse(time.RFC3339, ts.String); err == nil {
				trade.Timestamp = parsed
			}
		}
		if insertedAt.Valid {
			if parsed, err := time.Parse(time.RFC3339, insertedAt.String); err == nil {
				trade.InsertedAt = parsed
			}
		}
		trades = append(trades, trade)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return trades, nil
}

// ListUserTradeIDs returns only trade IDs for a user - lightweight for deduplication
func (s *Store) ListUserTradeIDs(ctx context.Context, userID string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 10000
	}

	rows, err := s.db.QueryContext(ctx, `
        SELECT id FROM user_trades
        WHERE user_address = ?
        ORDER BY datetime(timestamp) DESC
        LIMIT ?`, userID, limit)
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

	return ids, rows.Err()
}

func (s *Store) saveUserTx(ctx context.Context, tx *sql.Tx, user models.User) error {
	lastActive := timeString(user.LastActive)
	lastSynced := timeString(user.LastSyncedAt)
	_, err := tx.ExecContext(ctx, `
        INSERT INTO users (id, username, address, total_trades, total_pnl, win_rate, consistency, last_active, last_synced_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            username = excluded.username,
            address = excluded.address,
            total_trades = excluded.total_trades,
            total_pnl = excluded.total_pnl,
            win_rate = excluded.win_rate,
            consistency = excluded.consistency,
            last_active = excluded.last_active,
            last_synced_at = excluded.last_synced_at
    `, user.ID, user.Username, user.Address, user.TotalTrades, user.TotalPNL, user.WinRate, user.Consistency, lastActive, lastSynced)
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM user_subject_metrics WHERE user_id = ?`, user.ID); err != nil {
		return err
	}
	if len(user.SubjectScores) > 0 {
		stmt, err := tx.PrepareContext(ctx, `
            INSERT INTO user_subject_metrics (user_id, subject, trades, pnl, win_rate, consistency)
            VALUES (?, ?, ?, ?, ?, ?)`)
		if err != nil {
			return err
		}
		for subject, score := range user.SubjectScores {
			if _, err := stmt.ExecContext(ctx, user.ID, string(subject), score.Trades, score.PNL, score.WinRate, score.Consistency); err != nil {
				_ = stmt.Close()
				return err
			}
		}
		_ = stmt.Close()
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM user_red_flags WHERE user_id = ?`, user.ID); err != nil {
		return err
	}
	if len(user.RedFlags) > 0 {
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO user_red_flags (user_id, flag) VALUES (?, ?)`)
		if err != nil {
			return err
		}
		for _, flag := range user.RedFlags {
			if _, err := stmt.ExecContext(ctx, user.ID, flag); err != nil {
				_ = stmt.Close()
				return err
			}
		}
		_ = stmt.Close()
	}

	return nil
}

// ListUsers returns stored users filtered by subject (if provided).
func (s *Store) ListUsers(ctx context.Context, subject models.Subject, limit int) ([]models.User, error) {
	if limit <= 0 {
		limit = 100
	}

	var (
		rows *sql.Rows
		err  error
	)

	if subject == "" {
		rows, err = s.db.QueryContext(ctx, `
            SELECT id, username, address, total_trades, total_pnl, win_rate, consistency, last_active, last_synced_at
            FROM users
            ORDER BY total_pnl DESC
            LIMIT ?`, limit)
	} else {
		rows, err = s.db.QueryContext(ctx, `
            SELECT u.id, u.username, u.address, u.total_trades, u.total_pnl, u.win_rate, u.consistency, u.last_active, u.last_synced_at
            FROM users u
            INNER JOIN user_subject_metrics m ON m.user_id = u.id AND m.subject = ?
            ORDER BY m.pnl DESC
            LIMIT ?`, string(subject), limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []models.User
	for rows.Next() {
		var u models.User
		var lastActive, lastSynced sql.NullString
		if err := rows.Scan(&u.ID, &u.Username, &u.Address, &u.TotalTrades, &u.TotalPNL, &u.WinRate, &u.Consistency, &lastActive, &lastSynced); err != nil {
			return nil, err
		}
		if lastActive.Valid {
			if parsed, err := time.Parse(time.RFC3339, lastActive.String); err == nil {
				u.LastActive = parsed
			}
		}
		if lastSynced.Valid {
			if parsed, err := time.Parse(time.RFC3339, lastSynced.String); err == nil {
				u.LastSyncedAt = parsed
			}
		}
		u.SubjectScores = make(map[models.Subject]models.SubjectScore)
		users = append(users, u)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(users) == 0 {
		return users, nil
	}

	ptrs := make([]*models.User, len(users))
	for i := range users {
		ptrs[i] = &users[i]
	}

	if err := s.hydrateSubjectDetails(ctx, ptrs); err != nil {
		return nil, err
	}

	return users, nil
}

// GetUser returns a single user with metrics if present.
func (s *Store) GetUser(ctx context.Context, userID string) (*models.User, error) {
	row := s.db.QueryRowContext(ctx, `
        SELECT id, username, address, total_trades, total_pnl, win_rate, consistency, last_active
        FROM users WHERE id = ?`, userID)

	var u models.User
	var lastActive sql.NullString
	if err := row.Scan(&u.ID, &u.Username, &u.Address, &u.TotalTrades, &u.TotalPNL, &u.WinRate, &u.Consistency, &lastActive); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if lastActive.Valid {
		if parsed, err := time.Parse(time.RFC3339, lastActive.String); err == nil {
			u.LastActive = parsed
		}
	}
	u.SubjectScores = make(map[models.Subject]models.SubjectScore)

	if err := s.hydrateSubjectDetails(ctx, []*models.User{&u}); err != nil {
		return nil, err
	}

	return &u, nil
}

func (s *Store) hydrateSubjectDetails(ctx context.Context, users []*models.User) error {
	if len(users) == 0 {
		return nil
	}

	idIndex := make(map[string]*models.User, len(users))
	placeholders := make([]string, 0, len(users))
	args := make([]any, 0, len(users))
	for _, user := range users {
		idIndex[user.ID] = user
		placeholders = append(placeholders, "?")
		args = append(args, user.ID)
	}

	scoresQuery := fmt.Sprintf(`SELECT user_id, subject, trades, pnl, win_rate, consistency FROM user_subject_metrics WHERE user_id IN (%s)`, strings.Join(placeholders, ","))
	scoreRows, err := s.db.QueryContext(ctx, scoresQuery, args...)
	if err != nil {
		return err
	}
	for scoreRows.Next() {
		var userID string
		var subject string
		var score models.SubjectScore
		if err := scoreRows.Scan(&userID, &subject, &score.Trades, &score.PNL, &score.WinRate, &score.Consistency); err != nil {
			scoreRows.Close()
			return err
		}
		if u, ok := idIndex[userID]; ok {
			u.SubjectScores[models.Subject(subject)] = score
		}
	}
	if err := scoreRows.Close(); err != nil {
		return err
	}

	flagsQuery := fmt.Sprintf(`SELECT user_id, flag FROM user_red_flags WHERE user_id IN (%s)`, strings.Join(placeholders, ","))
	flagRows, err := s.db.QueryContext(ctx, flagsQuery, args...)
	if err != nil {
		return err
	}
	for flagRows.Next() {
		var userID string
		var flag string
		if err := flagRows.Scan(&userID, &flag); err != nil {
			flagRows.Close()
			return err
		}
		if u, ok := idIndex[userID]; ok {
			u.RedFlags = append(u.RedFlags, flag)
		}
	}
	if err := flagRows.Close(); err != nil {
		return err
	}

	return nil
}

func (s *Store) runMigrations(ctx context.Context) error {
	const schema = `
    PRAGMA foreign_keys = ON;

    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        username TEXT,
        address TEXT,
        total_trades INTEGER,
        total_pnl REAL,
        win_rate REAL,
        consistency REAL,
        last_active TEXT,
        last_synced_at TEXT
    );

    CREATE TABLE IF NOT EXISTS user_subject_metrics (
        user_id TEXT NOT NULL,
        subject TEXT NOT NULL,
        trades INTEGER,
        pnl REAL,
        win_rate REAL,
        consistency REAL,
        PRIMARY KEY (user_id, subject),
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS user_red_flags (
        user_id TEXT NOT NULL,
        flag TEXT NOT NULL,
        PRIMARY KEY (user_id, flag),
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_users_total_pnl ON users(total_pnl DESC);
    CREATE INDEX IF NOT EXISTS idx_metrics_subject ON user_subject_metrics(subject, pnl DESC);

    CREATE TABLE IF NOT EXISTS user_trades (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        market_id TEXT,
        subject TEXT,
        type TEXT,
        side TEXT,
        is_maker INTEGER,
        size REAL,
        usdc_size REAL,
        price REAL,
        outcome TEXT,
        timestamp TEXT,
        title TEXT,
        slug TEXT,
        event_slug TEXT,
        transaction_hash TEXT,
        name TEXT,
        pseudonym TEXT,
        inserted_at TEXT,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_trades_user_time ON user_trades(user_id, datetime(timestamp) DESC);

    -- Performance indexes for position aggregation
    CREATE INDEX IF NOT EXISTS idx_trades_user_title_outcome ON user_trades(user_id, title, outcome);
    CREATE INDEX IF NOT EXISTS idx_trades_user_side ON user_trades(user_id, side);

    -- Analysis cache table for fast lookups
    CREATE TABLE IF NOT EXISTS analysis_cache (
        user_id TEXT PRIMARY KEY,
        result_json TEXT NOT NULL,
        computed_at TEXT NOT NULL,
        trade_count INTEGER NOT NULL,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );

    -- Materialized positions table for pre-computed aggregations
    CREATE TABLE IF NOT EXISTS user_positions (
        user_id TEXT NOT NULL,
        market_outcome TEXT NOT NULL,
        title TEXT,
        outcome TEXT,
        subject TEXT,
        total_bought REAL,
        total_sold REAL,
        qty_bought REAL,
        qty_sold REAL,
        gain_loss REAL,
        buy_count INTEGER,
        sell_count INTEGER,
        first_buy_at TEXT,
        last_buy_at TEXT,
        first_sell_at TEXT,
        last_sell_at TEXT,
        duration_mins REAL,
        updated_at TEXT,
        PRIMARY KEY (user_id, market_outcome),
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_positions_user ON user_positions(user_id);
    CREATE INDEX IF NOT EXISTS idx_positions_gain_loss ON user_positions(user_id, gain_loss);

    -- Global token map cache (avoid re-fetching all markets)
    CREATE TABLE IF NOT EXISTS token_map_cache (
        token_id TEXT PRIMARY KEY,
        condition_id TEXT,
        outcome TEXT,
        title TEXT,
        slug TEXT,
        event_slug TEXT,
        updated_at TEXT
    );

    -- Global trades table for platform-wide monitoring (no FK constraints)
    CREATE TABLE IF NOT EXISTS global_trades (
        id TEXT PRIMARY KEY,
        user_address TEXT NOT NULL,
        asset TEXT,
        type TEXT,
        side TEXT,
        size REAL,
        usdc_size REAL,
        price REAL,
        outcome TEXT,
        timestamp TEXT,
        title TEXT,
        slug TEXT,
        transaction_hash TEXT,
        inserted_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_global_trades_user ON global_trades(user_address);
    CREATE INDEX IF NOT EXISTS idx_global_trades_timestamp ON global_trades(timestamp);
    CREATE INDEX IF NOT EXISTS idx_global_trades_type ON global_trades(type);
    `

	_, err := s.db.ExecContext(ctx, schema)
	return err
}

// DeleteUser removes a user and all associated data (trades, metrics, flags).
func (s *Store) DeleteUser(ctx context.Context, userID string) error {
	// Thanks to CASCADE constraints, this will also delete:
	// - user_subject_metrics
	// - user_red_flags
	// - user_trades
	result, err := s.db.ExecContext(ctx, `DELETE FROM users WHERE id = ?`, userID)
	if err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check deletion: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("user not found: %s", userID)
	}

	return nil
}

func timeString(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}

// SaveAnalysisCache stores computed analysis results for fast retrieval
func (s *Store) SaveAnalysisCache(ctx context.Context, userID string, resultJSON string, tradeCount int) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO analysis_cache (user_id, result_json, computed_at, trade_count)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(user_id) DO UPDATE SET
			result_json = excluded.result_json,
			computed_at = excluded.computed_at,
			trade_count = excluded.trade_count
	`, userID, resultJSON, time.Now().UTC().Format(time.RFC3339), tradeCount)
	return err
}

// GetAnalysisCache retrieves cached analysis results if still valid
func (s *Store) GetAnalysisCache(ctx context.Context, userID string) (string, int, time.Time, error) {
	var resultJSON string
	var tradeCount int
	var computedAt sql.NullString

	err := s.db.QueryRowContext(ctx, `
		SELECT result_json, trade_count, computed_at
		FROM analysis_cache
		WHERE user_id = ?
	`, userID).Scan(&resultJSON, &tradeCount, &computedAt)

	if err != nil {
		return "", 0, time.Time{}, err
	}

	var computed time.Time
	if computedAt.Valid {
		computed, _ = time.Parse(time.RFC3339, computedAt.String)
	}

	return resultJSON, tradeCount, computed, nil
}

// InvalidateAnalysisCache removes cached analysis for a user (call when trades change)
func (s *Store) InvalidateAnalysisCache(ctx context.Context, userID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM analysis_cache WHERE user_id = ?`, userID)
	return err
}

// GetUserTradeCount returns the number of trades for a user (for cache validation)
func (s *Store) GetUserTradeCount(ctx context.Context, userID string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM user_trades WHERE user_address = ?`, userID).Scan(&count)
	return count, err
}

// SaveUserPositions stores pre-computed aggregated positions
func (s *Store) SaveUserPositions(ctx context.Context, userID string, positions []models.AggregatedPosition) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Delete existing positions for this user
	if _, err := tx.ExecContext(ctx, `DELETE FROM user_positions WHERE user_id = ?`, userID); err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO user_positions (
			user_id, market_outcome, title, outcome, subject,
			total_bought, total_sold, qty_bought, qty_sold, gain_loss,
			buy_count, sell_count, first_buy_at, last_buy_at, first_sell_at, last_sell_at,
			duration_mins, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	now := time.Now().UTC().Format(time.RFC3339)
	for _, pos := range positions {
		_, err := stmt.ExecContext(ctx,
			userID, pos.MarketOutcome, pos.Title, pos.Outcome, string(pos.Subject),
			pos.TotalBought, pos.TotalSold, pos.QtyBought, pos.QtySold, pos.GainLoss,
			pos.BuyCount, pos.SellCount,
			timeString(pos.FirstBuyAt), timeString(pos.LastBuyAt),
			timeString(pos.FirstSellAt), timeString(pos.LastSellAt),
			pos.DurationMins, now,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetUserPositions retrieves pre-computed aggregated positions
func (s *Store) GetUserPositions(ctx context.Context, userID string) ([]models.AggregatedPosition, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT market_outcome, title, outcome, subject,
			   total_bought, total_sold, qty_bought, qty_sold, gain_loss,
			   buy_count, sell_count, first_buy_at, last_buy_at, first_sell_at, last_sell_at,
			   duration_mins
		FROM user_positions
		WHERE user_id = ?
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var positions []models.AggregatedPosition
	for rows.Next() {
		var pos models.AggregatedPosition
		var subject string
		var firstBuy, lastBuy, firstSell, lastSell sql.NullString

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
		if firstBuy.Valid {
			pos.FirstBuyAt, _ = time.Parse(time.RFC3339, firstBuy.String)
		}
		if lastBuy.Valid {
			pos.LastBuyAt, _ = time.Parse(time.RFC3339, lastBuy.String)
		}
		if firstSell.Valid {
			pos.FirstSellAt, _ = time.Parse(time.RFC3339, firstSell.String)
		}
		if lastSell.Valid {
			pos.LastSellAt, _ = time.Parse(time.RFC3339, lastSell.String)
		}

		positions = append(positions, pos)
	}

	return positions, rows.Err()
}

// TokenInfo represents cached token information
type TokenInfo struct {
	TokenID     string
	ConditionID string
	Outcome     string
	Title       string
	Slug        string
	EventSlug   string
}

// GetCachedTokens retrieves cached token info for the given token IDs
func (s *Store) GetCachedTokens(ctx context.Context, tokenIDs []string) (map[string]TokenInfo, error) {
	if len(tokenIDs) == 0 {
		return make(map[string]TokenInfo), nil
	}

	// Build query with placeholders
	placeholders := make([]string, len(tokenIDs))
	args := make([]interface{}, len(tokenIDs))
	for i, id := range tokenIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT token_id, condition_id, outcome, title, slug, event_slug
		FROM token_map_cache
		WHERE token_id IN (%s)
	`, strings.Join(placeholders, ","))

	rows, err := s.db.QueryContext(ctx, query, args...)
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

// SaveTokenCache stores token information in the cache
func (s *Store) SaveTokenCache(ctx context.Context, tokens map[string]TokenInfo) error {
	if len(tokens) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO token_map_cache (token_id, condition_id, outcome, title, slug, event_slug, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(token_id) DO UPDATE SET
			condition_id = excluded.condition_id,
			outcome = excluded.outcome,
			title = excluded.title,
			slug = excluded.slug,
			event_slug = excluded.event_slug,
			updated_at = excluded.updated_at
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	now := time.Now().UTC().Format(time.RFC3339)
	for _, info := range tokens {
		if _, err := stmt.ExecContext(ctx, info.TokenID, info.ConditionID, info.Outcome, info.Title, info.Slug, info.EventSlug, now); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetTokenInfo retrieves token information from the cache
func (s *Store) GetTokenInfo(ctx context.Context, tokenID string) (*TokenInfo, error) {
	var info TokenInfo
	err := s.db.QueryRowContext(ctx, `
		SELECT token_id, condition_id, outcome, title, slug
		FROM token_map_cache
		WHERE token_id = ?
	`, tokenID).Scan(&info.TokenID, &info.ConditionID, &info.Outcome, &info.Title, &info.Slug)
	if err != nil {
		return nil, err
	}
	return &info, nil
}

// GetTokenByCondition retrieves token information for a condition ID (returns first match)
func (s *Store) GetTokenByCondition(ctx context.Context, conditionID string) (*TokenInfo, error) {
	var info TokenInfo
	err := s.db.QueryRowContext(ctx, `
		SELECT token_id, condition_id, outcome, title, slug
		FROM token_map_cache
		WHERE condition_id = ?
		LIMIT 1
	`, conditionID).Scan(&info.TokenID, &info.ConditionID, &info.Outcome, &info.Title, &info.Slug)
	if err != nil {
		return nil, err
	}
	return &info, nil
}

// Copy trading settings stubs (not implemented for SQLite Store)
func (s *Store) GetUserCopySettings(ctx context.Context, userAddress string) (*UserCopySettings, error) {
	return nil, nil
}

func (s *Store) SetUserCopySettings(ctx context.Context, settings UserCopySettings) error {
	return nil
}

func (s *Store) GetAllUserCopySettings(ctx context.Context) ([]UserCopySettings, error) {
	return nil, nil
}

func (s *Store) DeleteUserCopySettings(ctx context.Context, userAddress string) error {
	return nil
}

// GetUserAnalyticsList stub (not implemented for SQLite Store)
func (s *Store) GetUserAnalyticsList(ctx context.Context, filter UserAnalyticsFilter) ([]UserAnalyticsRecord, int, error) {
	// SQLite implementation doesn't have user analytics
	return []UserAnalyticsRecord{}, 0, nil
}

// InvalidateUserListCache is a no-op for SQLite as it doesn't use external caching for user lists
func (s *Store) InvalidateUserListCache(ctx context.Context) error {
	// SQLite doesn't use Redis caching, so there's nothing to invalidate
	return nil
}
