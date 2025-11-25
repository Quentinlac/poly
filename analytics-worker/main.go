package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ============================================================================
// SCALABLE ANALYTICS WORKER v3
// ============================================================================
// Architecture:
// 1. Process trades incrementally as they arrive (cursor-based)
// 2. Update running aggregates per trade (O(1) per trade)
// 3. Reverse spike detection: new trades validate past BUYs (O(log n))
// 4. Archive completed spike checks to permanent storage
// 5. Derive user analytics from pre-aggregated position data
// ============================================================================

func main() {
	log.Println("[Analytics Worker] Starting v3 (scalable)...")

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://polymarket:polymarket@localhost:15432/polymarket?sslmode=disable"
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("[Analytics Worker] Connected to database")

	// Create all required tables
	log.Println("[Analytics Worker] Creating tables...")
	if err := createTables(ctx, pool); err != nil {
		log.Fatalf("Failed to create tables: %v", err)
	}
	log.Println("[Analytics Worker] Tables ready")

	// Check if we need initial migration
	needsMigration, err := checkNeedsMigration(ctx, pool)
	if err != nil {
		log.Printf("[Analytics Worker] Warning: couldn't check migration status: %v", err)
	}
	if needsMigration {
		log.Println("[Analytics Worker] Running initial migration...")
		if err := runInitialMigration(ctx, pool); err != nil {
			log.Printf("[Analytics Worker] Migration error: %v", err)
		}
	}

	// Setup signal handling
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// Main processing loop - run every 2 seconds
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Archive expired spikes every 5 minutes
	archiveTicker := time.NewTicker(5 * time.Minute)
	defer archiveTicker.Stop()

	// Recompute user analytics every 5 minutes (fast since using aggregates)
	analyticsTicker := time.NewTicker(5 * time.Minute)
	defer analyticsTicker.Stop()

	log.Println("[Analytics Worker] Starting main loop (2s trade processing, 5m analytics)")

	// Run immediately on start
	processNewTrades(ctx, pool)

	for {
		select {
		case <-stop:
			log.Println("[Analytics Worker] Shutting down...")
			return
		case <-ticker.C:
			processNewTrades(ctx, pool)
		case <-archiveTicker.C:
			archiveExpiredSpikes(ctx, pool)
		case <-analyticsTicker.C:
			computeUserAnalytics(ctx, pool)
		}
	}
}

// ============================================================================
// TABLE CREATION
// ============================================================================

func createTables(ctx context.Context, pool *pgxpool.Pool) error {
	// 1. Analytics cursor - track last processed trade
	log.Println("[Tables] Creating analytics_cursor...")
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS analytics_cursor (
			id INT PRIMARY KEY DEFAULT 1,
			last_processed_timestamp TIMESTAMPTZ,
			last_processed_id VARCHAR(150),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("create analytics_cursor: %w", err)
	}

	// Initialize cursor if empty
	_, err = pool.Exec(ctx, `
		INSERT INTO analytics_cursor (id, last_processed_timestamp)
		VALUES (1, '1970-01-01'::timestamptz)
		ON CONFLICT (id) DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("init cursor: %w", err)
	}

	// 2. User position aggregates - running totals per (user, title, outcome)
	log.Println("[Tables] Creating user_position_agg...")
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS user_position_agg (
			user_address VARCHAR(42) NOT NULL,
			title TEXT NOT NULL,
			outcome TEXT NOT NULL,
			qty_bought DECIMAL DEFAULT 0,
			qty_sold DECIMAL DEFAULT 0,
			usd_bought DECIMAL DEFAULT 0,
			usd_sold DECIMAL DEFAULT 0,
			trade_count INT DEFAULT 0,
			first_trade_at TIMESTAMPTZ,
			last_trade_at TIMESTAMPTZ,
			PRIMARY KEY (user_address, title, outcome)
		)
	`)
	if err != nil {
		return fmt.Errorf("create user_position_agg: %w", err)
	}

	// 3. Pending spike checks - BUYs awaiting validation (last 6 hours)
	log.Println("[Tables] Creating pending_spike_checks...")
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS pending_spike_checks (
			trade_id VARCHAR(150) PRIMARY KEY,
			asset VARCHAR(100) NOT NULL,
			user_address VARCHAR(42) NOT NULL,
			buy_price DECIMAL NOT NULL,
			buy_time TIMESTAMPTZ NOT NULL,
			title TEXT,
			outcome TEXT,
			expires_at TIMESTAMPTZ NOT NULL,
			spike_5m_30 BOOLEAN DEFAULT FALSE,
			spike_5m_50 BOOLEAN DEFAULT FALSE,
			spike_5m_80 BOOLEAN DEFAULT FALSE,
			spike_10m_30 BOOLEAN DEFAULT FALSE,
			spike_10m_50 BOOLEAN DEFAULT FALSE,
			spike_10m_80 BOOLEAN DEFAULT FALSE,
			spike_30m_30 BOOLEAN DEFAULT FALSE,
			spike_30m_50 BOOLEAN DEFAULT FALSE,
			spike_30m_80 BOOLEAN DEFAULT FALSE,
			spike_2h_30 BOOLEAN DEFAULT FALSE,
			spike_2h_50 BOOLEAN DEFAULT FALSE,
			spike_2h_80 BOOLEAN DEFAULT FALSE,
			spike_6h_30 BOOLEAN DEFAULT FALSE,
			spike_6h_50 BOOLEAN DEFAULT FALSE,
			spike_6h_80 BOOLEAN DEFAULT FALSE
		)
	`)
	if err != nil {
		return fmt.Errorf("create pending_spike_checks: %w", err)
	}

	// Index for reverse spike lookups
	pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_pending_asset_time ON pending_spike_checks(asset, buy_time)`)
	pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_pending_expires ON pending_spike_checks(expires_at)`)

	// 4. Permanent spike storage (already exists, but ensure it's there)
	log.Println("[Tables] Creating trade_spike_flags...")
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS trade_spike_flags (
			trade_id VARCHAR(150) PRIMARY KEY,
			user_address VARCHAR(42) NOT NULL,
			asset VARCHAR(100) NOT NULL,
			title TEXT,
			outcome TEXT,
			buy_price DECIMAL NOT NULL,
			buy_time TIMESTAMPTZ NOT NULL,
			spike_5m_30 BOOLEAN DEFAULT FALSE,
			spike_5m_50 BOOLEAN DEFAULT FALSE,
			spike_5m_80 BOOLEAN DEFAULT FALSE,
			spike_10m_30 BOOLEAN DEFAULT FALSE,
			spike_10m_50 BOOLEAN DEFAULT FALSE,
			spike_10m_80 BOOLEAN DEFAULT FALSE,
			spike_30m_30 BOOLEAN DEFAULT FALSE,
			spike_30m_50 BOOLEAN DEFAULT FALSE,
			spike_30m_80 BOOLEAN DEFAULT FALSE,
			spike_2h_30 BOOLEAN DEFAULT FALSE,
			spike_2h_50 BOOLEAN DEFAULT FALSE,
			spike_2h_80 BOOLEAN DEFAULT FALSE,
			spike_6h_30 BOOLEAN DEFAULT FALSE,
			spike_6h_50 BOOLEAN DEFAULT FALSE,
			spike_6h_80 BOOLEAN DEFAULT FALSE,
			computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("create trade_spike_flags: %w", err)
	}

	pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_spike_flags_user ON trade_spike_flags(user_address)`)
	pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_spike_flags_time ON trade_spike_flags(buy_time)`)

	// 5. User analytics (already exists)
	log.Println("[Tables] Creating user_analytics...")
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS user_analytics (
			user_address VARCHAR(42) PRIMARY KEY,
			total_bets INTEGER NOT NULL DEFAULT 0,
			total_buy_usd NUMERIC(20, 2) NOT NULL DEFAULT 0,
			total_sell_redeem_usd NUMERIC(20, 2) NOT NULL DEFAULT 0,
			net_pnl NUMERIC(20, 2) NOT NULL DEFAULT 0,
			pnl_percentage NUMERIC(10, 2) NOT NULL DEFAULT 0,
			is_bot BOOLEAN NOT NULL DEFAULT FALSE,
			avg_investment_per_market NUMERIC(20, 2) NOT NULL DEFAULT 0,
			first_trade_at TIMESTAMPTZ,
			last_trade_at TIMESTAMPTZ,
			trades_per_day_avg NUMERIC(10, 2) NOT NULL DEFAULT 0,
			trades_per_day_30d NUMERIC(10, 2) NOT NULL DEFAULT 0,
			unique_markets INTEGER NOT NULL DEFAULT 0,
			data_complete BOOLEAN NOT NULL DEFAULT FALSE,
			fast_trades INTEGER NOT NULL DEFAULT 0,
			avg_trades INTEGER NOT NULL DEFAULT 0,
			slow_trades INTEGER NOT NULL DEFAULT 0,
			fast_avg_pnl NUMERIC(20, 2) NOT NULL DEFAULT 0,
			mid_avg_pnl NUMERIC(20, 2) NOT NULL DEFAULT 0,
			slow_avg_pnl NUMERIC(20, 2) NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("create user_analytics: %w", err)
	}

	// 6. Privileged analysis aggregates (already exists)
	log.Println("[Tables] Creating privileged_analysis...")
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS privileged_analysis (
			id SERIAL PRIMARY KEY,
			time_window_minutes INTEGER NOT NULL,
			price_threshold_pct INTEGER NOT NULL,
			user_address VARCHAR(42) NOT NULL,
			hit_count INTEGER NOT NULL,
			total_buys INTEGER NOT NULL,
			hit_rate DECIMAL(10, 4) NOT NULL,
			hits_json JSONB NOT NULL,
			computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE(time_window_minutes, price_threshold_pct, user_address)
		)
	`)
	if err != nil {
		return fmt.Errorf("create privileged_analysis: %w", err)
	}

	pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_privileged_window_threshold ON privileged_analysis(time_window_minutes, price_threshold_pct)`)

	// 7. Privileged analysis metadata
	log.Println("[Tables] Creating privileged_analysis_meta...")
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS privileged_analysis_meta (
			time_window_minutes INTEGER NOT NULL,
			price_threshold_pct INTEGER NOT NULL,
			last_computed_at TIMESTAMPTZ NOT NULL,
			computation_duration_sec DECIMAL(10, 2),
			user_count INTEGER,
			PRIMARY KEY (time_window_minutes, price_threshold_pct)
		)
	`)
	if err != nil {
		return fmt.Errorf("create privileged_analysis_meta: %w", err)
	}

	log.Println("[Tables] All tables created")
	return nil
}

// ============================================================================
// INCREMENTAL TRADE PROCESSING
// ============================================================================

func processNewTrades(ctx context.Context, pool *pgxpool.Pool) {
	// Get cursor
	var lastTimestamp time.Time
	var lastID *string
	err := pool.QueryRow(ctx, `
		SELECT last_processed_timestamp, last_processed_id FROM analytics_cursor WHERE id = 1
	`).Scan(&lastTimestamp, &lastID)
	if err != nil {
		log.Printf("[Process] Error getting cursor: %v", err)
		return
	}

	// Get new trades since cursor (batch of 5000)
	// Handle nullable lastID by using COALESCE
	var rows pgx.Rows
	if lastID == nil {
		rows, err = pool.Query(ctx, `
			SELECT id, user_address, asset, title, outcome, side, type, size, usdc_size, price, timestamp
			FROM global_trades
			WHERE timestamp > $1
			ORDER BY timestamp ASC, id ASC
			LIMIT 5000
		`, lastTimestamp)
	} else {
		rows, err = pool.Query(ctx, `
			SELECT id, user_address, asset, title, outcome, side, type, size, usdc_size, price, timestamp
			FROM global_trades
			WHERE (timestamp > $1 OR (timestamp = $1 AND id > $2))
			ORDER BY timestamp ASC, id ASC
			LIMIT 5000
		`, lastTimestamp, *lastID)
	}
	if err != nil {
		log.Printf("[Process] Error querying trades: %v", err)
		return
	}
	defer rows.Close()

	type trade struct {
		ID          string
		UserAddress string
		Asset       string
		Title       *string
		Outcome     *string
		Side        string
		Type        string
		Size        float64
		USDCSize    float64
		Price       float64
		Timestamp   time.Time
	}

	var trades []trade
	for rows.Next() {
		var t trade
		if err := rows.Scan(&t.ID, &t.UserAddress, &t.Asset, &t.Title, &t.Outcome, &t.Side, &t.Type, &t.Size, &t.USDCSize, &t.Price, &t.Timestamp); err != nil {
			log.Printf("[Process] Error scanning trade: %v", err)
			continue
		}
		trades = append(trades, t)
	}

	if len(trades) == 0 {
		return // No new trades
	}

	log.Printf("[Process] Processing %d new trades", len(trades))

	// Process each trade
	for _, t := range trades {
		title := ""
		outcome := ""
		if t.Title != nil {
			title = *t.Title
		}
		if t.Outcome != nil {
			outcome = *t.Outcome
		}

		// 1. Update position aggregates
		updatePositionAggregate(ctx, pool, t.UserAddress, title, outcome, t.Side, t.Type, t.Size, t.USDCSize, t.Timestamp)

		// 2. If BUY trade, add to pending spike checks
		if t.Side == "BUY" && t.Type == "TRADE" && t.Price > 0 {
			insertPendingSpike(ctx, pool, t.ID, t.Asset, t.UserAddress, t.Price, t.Timestamp, title, outcome)
		}

		// 3. Reverse spike validation - this trade's price may validate past BUYs
		if t.Price > 0 {
			validatePendingSpikes(ctx, pool, t.Asset, t.Price, t.Timestamp)
		}
	}

	// Update cursor to last processed trade
	lastTrade := trades[len(trades)-1]
	_, err = pool.Exec(ctx, `
		UPDATE analytics_cursor
		SET last_processed_timestamp = $1, last_processed_id = $2, updated_at = NOW()
		WHERE id = 1
	`, lastTrade.Timestamp, lastTrade.ID)
	if err != nil {
		log.Printf("[Process] Error updating cursor: %v", err)
	}

	log.Printf("[Process] Processed %d trades, cursor at %s", len(trades), lastTrade.Timestamp.Format("2006-01-02 15:04:05"))
}

func updatePositionAggregate(ctx context.Context, pool *pgxpool.Pool, userAddress, title, outcome, side, tradeType string, size, usdcSize float64, timestamp time.Time) {
	if title == "" {
		return // Skip trades without title
	}

	// Determine what to update based on side/type
	qtyBought := 0.0
	qtySold := 0.0
	usdBought := 0.0
	usdSold := 0.0

	if side == "BUY" {
		qtyBought = size
		usdBought = usdcSize
	} else if side == "SELL" || tradeType == "REDEEM" {
		qtySold = size
		usdSold = usdcSize
	}

	_, err := pool.Exec(ctx, `
		INSERT INTO user_position_agg (user_address, title, outcome, qty_bought, qty_sold, usd_bought, usd_sold, trade_count, first_trade_at, last_trade_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 1, $8, $8)
		ON CONFLICT (user_address, title, outcome) DO UPDATE SET
			qty_bought = user_position_agg.qty_bought + $4,
			qty_sold = user_position_agg.qty_sold + $5,
			usd_bought = user_position_agg.usd_bought + $6,
			usd_sold = user_position_agg.usd_sold + $7,
			trade_count = user_position_agg.trade_count + 1,
			first_trade_at = LEAST(user_position_agg.first_trade_at, $8),
			last_trade_at = GREATEST(user_position_agg.last_trade_at, $8)
	`, userAddress, title, outcome, qtyBought, qtySold, usdBought, usdSold, timestamp)

	if err != nil {
		log.Printf("[Position] Error updating aggregate: %v", err)
	}
}

func insertPendingSpike(ctx context.Context, pool *pgxpool.Pool, tradeID, asset, userAddress string, price float64, timestamp time.Time, title, outcome string) {
	expiresAt := timestamp.Add(6 * time.Hour)

	_, err := pool.Exec(ctx, `
		INSERT INTO pending_spike_checks (trade_id, asset, user_address, buy_price, buy_time, title, outcome, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (trade_id) DO NOTHING
	`, tradeID, asset, userAddress, price, timestamp, title, outcome, expiresAt)

	if err != nil {
		log.Printf("[Spike] Error inserting pending: %v", err)
	}
}

func validatePendingSpikes(ctx context.Context, pool *pgxpool.Pool, asset string, currentPrice float64, currentTime time.Time) {
	// Single UPDATE that checks all 15 conditions at once
	// For each window/threshold: if buy_time is within window AND buy_price qualifies for spike, set flag TRUE
	_, err := pool.Exec(ctx, `
		UPDATE pending_spike_checks SET
			spike_5m_30  = spike_5m_30  OR (buy_time > $3 - INTERVAL '5 minutes'   AND buy_price <= $2 / 1.30),
			spike_5m_50  = spike_5m_50  OR (buy_time > $3 - INTERVAL '5 minutes'   AND buy_price <= $2 / 1.50),
			spike_5m_80  = spike_5m_80  OR (buy_time > $3 - INTERVAL '5 minutes'   AND buy_price <= $2 / 1.80),
			spike_10m_30 = spike_10m_30 OR (buy_time > $3 - INTERVAL '10 minutes'  AND buy_price <= $2 / 1.30),
			spike_10m_50 = spike_10m_50 OR (buy_time > $3 - INTERVAL '10 minutes'  AND buy_price <= $2 / 1.50),
			spike_10m_80 = spike_10m_80 OR (buy_time > $3 - INTERVAL '10 minutes'  AND buy_price <= $2 / 1.80),
			spike_30m_30 = spike_30m_30 OR (buy_time > $3 - INTERVAL '30 minutes'  AND buy_price <= $2 / 1.30),
			spike_30m_50 = spike_30m_50 OR (buy_time > $3 - INTERVAL '30 minutes'  AND buy_price <= $2 / 1.50),
			spike_30m_80 = spike_30m_80 OR (buy_time > $3 - INTERVAL '30 minutes'  AND buy_price <= $2 / 1.80),
			spike_2h_30  = spike_2h_30  OR (buy_time > $3 - INTERVAL '2 hours'     AND buy_price <= $2 / 1.30),
			spike_2h_50  = spike_2h_50  OR (buy_time > $3 - INTERVAL '2 hours'     AND buy_price <= $2 / 1.50),
			spike_2h_80  = spike_2h_80  OR (buy_time > $3 - INTERVAL '2 hours'     AND buy_price <= $2 / 1.80),
			spike_6h_30  = spike_6h_30  OR (buy_time > $3 - INTERVAL '6 hours'     AND buy_price <= $2 / 1.30),
			spike_6h_50  = spike_6h_50  OR (buy_time > $3 - INTERVAL '6 hours'     AND buy_price <= $2 / 1.50),
			spike_6h_80  = spike_6h_80  OR (buy_time > $3 - INTERVAL '6 hours'     AND buy_price <= $2 / 1.80)
		WHERE asset = $1
		  AND buy_time > $3 - INTERVAL '6 hours'
		  AND buy_time < $3
	`, asset, currentPrice, currentTime)

	if err != nil {
		log.Printf("[Spike] Error validating: %v", err)
	}
}

// ============================================================================
// ARCHIVE EXPIRED SPIKES
// ============================================================================

func archiveExpiredSpikes(ctx context.Context, pool *pgxpool.Pool) {
	start := time.Now()

	// Move expired pending checks to permanent storage
	result, err := pool.Exec(ctx, `
		INSERT INTO trade_spike_flags (
			trade_id, user_address, asset, title, outcome, buy_price, buy_time,
			spike_5m_30, spike_5m_50, spike_5m_80,
			spike_10m_30, spike_10m_50, spike_10m_80,
			spike_30m_30, spike_30m_50, spike_30m_80,
			spike_2h_30, spike_2h_50, spike_2h_80,
			spike_6h_30, spike_6h_50, spike_6h_80,
			computed_at
		)
		SELECT
			trade_id, user_address, asset, title, outcome, buy_price, buy_time,
			spike_5m_30, spike_5m_50, spike_5m_80,
			spike_10m_30, spike_10m_50, spike_10m_80,
			spike_30m_30, spike_30m_50, spike_30m_80,
			spike_2h_30, spike_2h_50, spike_2h_80,
			spike_6h_30, spike_6h_50, spike_6h_80,
			NOW()
		FROM pending_spike_checks
		WHERE expires_at < NOW()
		ON CONFLICT (trade_id) DO NOTHING
	`)
	if err != nil {
		log.Printf("[Archive] Error archiving: %v", err)
		return
	}

	archived := result.RowsAffected()

	// Delete archived records
	_, err = pool.Exec(ctx, `DELETE FROM pending_spike_checks WHERE expires_at < NOW()`)
	if err != nil {
		log.Printf("[Archive] Error deleting: %v", err)
	}

	if archived > 0 {
		log.Printf("[Archive] Archived %d spike records in %v", archived, time.Since(start).Round(time.Millisecond))
	}

	// Also aggregate into privileged_analysis
	aggregatePrivilegedAnalysis(ctx, pool)
}

func aggregatePrivilegedAnalysis(ctx context.Context, pool *pgxpool.Pool) {
	combinations := []struct {
		Window    int
		Threshold int
		Column    string
	}{
		{5, 30, "spike_5m_30"}, {5, 50, "spike_5m_50"}, {5, 80, "spike_5m_80"},
		{10, 30, "spike_10m_30"}, {10, 50, "spike_10m_50"}, {10, 80, "spike_10m_80"},
		{30, 30, "spike_30m_30"}, {30, 50, "spike_30m_50"}, {30, 80, "spike_30m_80"},
		{120, 30, "spike_2h_30"}, {120, 50, "spike_2h_50"}, {120, 80, "spike_2h_80"},
		{360, 30, "spike_6h_30"}, {360, 50, "spike_6h_50"}, {360, 80, "spike_6h_80"},
	}

	for _, combo := range combinations {
		start := time.Now()

		// Delete old and insert new in one transaction
		_, err := pool.Exec(ctx, `DELETE FROM privileged_analysis WHERE time_window_minutes = $1 AND price_threshold_pct = $2`, combo.Window, combo.Threshold)
		if err != nil {
			log.Printf("[Privileged] Error deleting %dm/%d%%: %v", combo.Window, combo.Threshold, err)
			continue
		}

		query := fmt.Sprintf(`
			WITH user_stats AS (
				SELECT
					user_address,
					COUNT(*) as total_buys,
					SUM(CASE WHEN %s THEN 1 ELSE 0 END) as hit_count
				FROM trade_spike_flags
				GROUP BY user_address
				HAVING SUM(CASE WHEN %s THEN 1 ELSE 0 END) >= 3
			),
			user_hits AS (
				SELECT user_address, title, outcome, buy_price, buy_time
				FROM trade_spike_flags
				WHERE %s = true
			)
			INSERT INTO privileged_analysis (time_window_minutes, price_threshold_pct, user_address, hit_count, total_buys, hit_rate, hits_json)
			SELECT
				$1, $2,
				us.user_address,
				us.hit_count,
				us.total_buys,
				(us.hit_count::decimal / us.total_buys * 100),
				COALESCE((
					SELECT json_agg(json_build_object(
						'title', uh.title,
						'outcome', uh.outcome,
						'buy_price', uh.buy_price,
						'buy_time', uh.buy_time
					) ORDER BY uh.buy_time DESC)
					FROM user_hits uh WHERE uh.user_address = us.user_address
				), '[]'::json)
			FROM user_stats us
			ORDER BY us.hit_count DESC
			LIMIT 100
		`, combo.Column, combo.Column, combo.Column)

		result, err := pool.Exec(ctx, query, combo.Window, combo.Threshold)
		if err != nil {
			log.Printf("[Privileged] Error aggregating %dm/%d%%: %v", combo.Window, combo.Threshold, err)
			continue
		}

		// Update metadata
		pool.Exec(ctx, `
			INSERT INTO privileged_analysis_meta (time_window_minutes, price_threshold_pct, last_computed_at, computation_duration_sec, user_count)
			VALUES ($1, $2, NOW(), $3, $4)
			ON CONFLICT (time_window_minutes, price_threshold_pct) DO UPDATE SET
				last_computed_at = NOW(),
				computation_duration_sec = $3,
				user_count = $4
		`, combo.Window, combo.Threshold, time.Since(start).Seconds(), result.RowsAffected())
	}

	log.Println("[Privileged] Aggregation complete")
}

// ============================================================================
// USER ANALYTICS (from pre-aggregated position data)
// ============================================================================

func computeUserAnalytics(ctx context.Context, pool *pgxpool.Pool) {
	start := time.Now()

	// Compute user analytics from position aggregates - much faster than scanning global_trades
	query := `
	WITH user_totals AS (
		SELECT
			user_address,
			SUM(trade_count) as total_bets,
			SUM(usd_bought) as total_buy_usd,
			-- Cap sells at bought quantity for PnL calculation
			SUM(
				CASE
					WHEN qty_bought = 0 THEN 0
					WHEN qty_sold <= qty_bought THEN usd_sold
					ELSE usd_sold * (qty_bought / NULLIF(qty_sold, 0))
				END
			) as total_sell_usd,
			COUNT(DISTINCT title) as unique_markets,
			MIN(first_trade_at) as first_trade_at,
			MAX(last_trade_at) as last_trade_at,
			AVG(usd_bought) as avg_investment_per_market
		FROM user_position_agg
		GROUP BY user_address
	),
	trades_30d AS (
		SELECT user_address, SUM(trade_count) / 30.0 as trades_per_day_30d
		FROM user_position_agg
		WHERE last_trade_at >= NOW() - INTERVAL '30 days'
		GROUP BY user_address
	)
	INSERT INTO user_analytics (
		user_address, total_bets, total_buy_usd, total_sell_redeem_usd,
		net_pnl, pnl_percentage, unique_markets, avg_investment_per_market,
		first_trade_at, last_trade_at, trades_per_day_avg, trades_per_day_30d,
		is_bot, data_complete, updated_at
	)
	SELECT
		ut.user_address,
		ut.total_bets,
		ut.total_buy_usd,
		ut.total_sell_usd,
		(ut.total_sell_usd - ut.total_buy_usd) as net_pnl,
		CASE WHEN ut.total_buy_usd > 0 THEN ((ut.total_sell_usd - ut.total_buy_usd) / ut.total_buy_usd * 100) ELSE 0 END,
		ut.unique_markets,
		COALESCE(ut.avg_investment_per_market, 0),
		ut.first_trade_at,
		ut.last_trade_at,
		CASE
			WHEN ut.first_trade_at IS NOT NULL AND ut.last_trade_at > ut.first_trade_at
			THEN ut.total_bets::numeric / GREATEST(EXTRACT(EPOCH FROM (ut.last_trade_at - ut.first_trade_at)) / 86400, 1)
			ELSE ut.total_bets::numeric
		END,
		COALESCE(t30.trades_per_day_30d, 0),
		COALESCE(t30.trades_per_day_30d, 0) > 100,
		ut.total_buy_usd > 0,
		NOW()
	FROM user_totals ut
	LEFT JOIN trades_30d t30 ON ut.user_address = t30.user_address
	ON CONFLICT (user_address) DO UPDATE SET
		total_bets = EXCLUDED.total_bets,
		total_buy_usd = EXCLUDED.total_buy_usd,
		total_sell_redeem_usd = EXCLUDED.total_sell_redeem_usd,
		net_pnl = EXCLUDED.net_pnl,
		pnl_percentage = EXCLUDED.pnl_percentage,
		unique_markets = EXCLUDED.unique_markets,
		avg_investment_per_market = EXCLUDED.avg_investment_per_market,
		first_trade_at = EXCLUDED.first_trade_at,
		last_trade_at = EXCLUDED.last_trade_at,
		trades_per_day_avg = EXCLUDED.trades_per_day_avg,
		trades_per_day_30d = EXCLUDED.trades_per_day_30d,
		is_bot = EXCLUDED.is_bot,
		data_complete = EXCLUDED.data_complete,
		updated_at = NOW()
	`

	result, err := pool.Exec(ctx, query)
	if err != nil {
		log.Printf("[Analytics] Error computing: %v", err)
		return
	}

	log.Printf("[Analytics] Updated %d users in %v", result.RowsAffected(), time.Since(start).Round(time.Millisecond))
}

// ============================================================================
// INITIAL MIGRATION (one-time for existing data)
// ============================================================================

func checkNeedsMigration(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	// Check if user_position_agg has data
	var count int
	err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM user_position_agg`).Scan(&count)
	if err != nil {
		return true, err // Table might not exist
	}
	return count == 0, nil
}

func runInitialMigration(ctx context.Context, pool *pgxpool.Pool) error {
	start := time.Now()

	// Step 1: Populate user_position_agg from global_trades
	log.Println("[Migration] Populating position aggregates...")
	_, err := pool.Exec(ctx, `
		INSERT INTO user_position_agg (user_address, title, outcome, qty_bought, qty_sold, usd_bought, usd_sold, trade_count, first_trade_at, last_trade_at)
		SELECT
			user_address,
			COALESCE(title, ''),
			COALESCE(outcome, ''),
			COALESCE(SUM(CASE WHEN side = 'BUY' THEN size ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN side = 'SELL' OR type = 'REDEEM' THEN size ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN side = 'BUY' THEN usdc_size ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN side = 'SELL' OR type = 'REDEEM' THEN usdc_size ELSE 0 END), 0),
			COUNT(*),
			MIN(timestamp),
			MAX(timestamp)
		FROM global_trades
		WHERE title IS NOT NULL AND title != ''
		GROUP BY user_address, title, outcome
		ON CONFLICT (user_address, title, outcome) DO UPDATE SET
			qty_bought = EXCLUDED.qty_bought,
			qty_sold = EXCLUDED.qty_sold,
			usd_bought = EXCLUDED.usd_bought,
			usd_sold = EXCLUDED.usd_sold,
			trade_count = EXCLUDED.trade_count,
			first_trade_at = EXCLUDED.first_trade_at,
			last_trade_at = EXCLUDED.last_trade_at
	`)
	if err != nil {
		return fmt.Errorf("populate position_agg: %w", err)
	}
	log.Printf("[Migration] Position aggregates populated in %v", time.Since(start).Round(time.Second))

	// Step 2: Set cursor to latest trade
	log.Println("[Migration] Setting cursor...")
	_, err = pool.Exec(ctx, `
		UPDATE analytics_cursor SET
			last_processed_timestamp = (SELECT MAX(timestamp) FROM global_trades),
			last_processed_id = (SELECT id FROM global_trades ORDER BY timestamp DESC, id DESC LIMIT 1),
			updated_at = NOW()
		WHERE id = 1
	`)
	if err != nil {
		return fmt.Errorf("set cursor: %w", err)
	}

	// Step 3: Compute initial user analytics
	log.Println("[Migration] Computing user analytics...")
	computeUserAnalytics(ctx, pool)

	// Step 4: For historical spike detection, process in batches
	// Only process BUYs from last 30 days for initial spike data
	log.Println("[Migration] Processing historical spikes (last 30 days)...")
	err = migrateHistoricalSpikes(ctx, pool)
	if err != nil {
		log.Printf("[Migration] Warning: historical spike migration error: %v", err)
	}

	log.Printf("[Migration] Complete in %v", time.Since(start).Round(time.Second))
	return nil
}

func migrateHistoricalSpikes(ctx context.Context, pool *pgxpool.Pool) error {
	// Process historical BUYs in batches
	// For each BUY, we need to check if price spiked - use the old forward-looking approach
	// but only for initial population

	batchSize := 10000
	offset := 0
	totalProcessed := 0

	for {
		rows, err := pool.Query(ctx, `
			SELECT g.id, g.user_address, g.asset, g.title, g.outcome, g.price, g.timestamp
			FROM global_trades g
			LEFT JOIN trade_spike_flags t ON g.id = t.trade_id
			WHERE g.side = 'BUY'
			AND g.type = 'TRADE'
			AND g.price > 0
			AND g.timestamp > NOW() - INTERVAL '30 days'
			AND g.timestamp < NOW() - INTERVAL '6 hours'
			AND t.trade_id IS NULL
			ORDER BY g.timestamp ASC
			LIMIT $1 OFFSET $2
		`, batchSize, offset)
		if err != nil {
			return err
		}

		type buyTrade struct {
			ID, UserAddress, Asset string
			Title, Outcome         *string
			Price                  float64
			Timestamp              time.Time
		}

		var buys []buyTrade
		for rows.Next() {
			var b buyTrade
			rows.Scan(&b.ID, &b.UserAddress, &b.Asset, &b.Title, &b.Outcome, &b.Price, &b.Timestamp)
			buys = append(buys, b)
		}
		rows.Close()

		if len(buys) == 0 {
			break
		}

		// For each buy, check all 15 spike conditions
		batch := &pgx.Batch{}
		for _, buy := range buys {
			title := ""
			outcome := ""
			if buy.Title != nil {
				title = *buy.Title
			}
			if buy.Outcome != nil {
				outcome = *buy.Outcome
			}

			// Check spikes with a single query per buy
			var flags [15]bool
			pool.QueryRow(ctx, `
				SELECT
					EXISTS(SELECT 1 FROM global_trades WHERE asset = $1 AND timestamp > $2 AND timestamp <= $2 + INTERVAL '5 min' AND price >= $3 * 1.30 LIMIT 1),
					EXISTS(SELECT 1 FROM global_trades WHERE asset = $1 AND timestamp > $2 AND timestamp <= $2 + INTERVAL '5 min' AND price >= $3 * 1.50 LIMIT 1),
					EXISTS(SELECT 1 FROM global_trades WHERE asset = $1 AND timestamp > $2 AND timestamp <= $2 + INTERVAL '5 min' AND price >= $3 * 1.80 LIMIT 1),
					EXISTS(SELECT 1 FROM global_trades WHERE asset = $1 AND timestamp > $2 AND timestamp <= $2 + INTERVAL '10 min' AND price >= $3 * 1.30 LIMIT 1),
					EXISTS(SELECT 1 FROM global_trades WHERE asset = $1 AND timestamp > $2 AND timestamp <= $2 + INTERVAL '10 min' AND price >= $3 * 1.50 LIMIT 1),
					EXISTS(SELECT 1 FROM global_trades WHERE asset = $1 AND timestamp > $2 AND timestamp <= $2 + INTERVAL '10 min' AND price >= $3 * 1.80 LIMIT 1),
					EXISTS(SELECT 1 FROM global_trades WHERE asset = $1 AND timestamp > $2 AND timestamp <= $2 + INTERVAL '30 min' AND price >= $3 * 1.30 LIMIT 1),
					EXISTS(SELECT 1 FROM global_trades WHERE asset = $1 AND timestamp > $2 AND timestamp <= $2 + INTERVAL '30 min' AND price >= $3 * 1.50 LIMIT 1),
					EXISTS(SELECT 1 FROM global_trades WHERE asset = $1 AND timestamp > $2 AND timestamp <= $2 + INTERVAL '30 min' AND price >= $3 * 1.80 LIMIT 1),
					EXISTS(SELECT 1 FROM global_trades WHERE asset = $1 AND timestamp > $2 AND timestamp <= $2 + INTERVAL '2 hours' AND price >= $3 * 1.30 LIMIT 1),
					EXISTS(SELECT 1 FROM global_trades WHERE asset = $1 AND timestamp > $2 AND timestamp <= $2 + INTERVAL '2 hours' AND price >= $3 * 1.50 LIMIT 1),
					EXISTS(SELECT 1 FROM global_trades WHERE asset = $1 AND timestamp > $2 AND timestamp <= $2 + INTERVAL '2 hours' AND price >= $3 * 1.80 LIMIT 1),
					EXISTS(SELECT 1 FROM global_trades WHERE asset = $1 AND timestamp > $2 AND timestamp <= $2 + INTERVAL '6 hours' AND price >= $3 * 1.30 LIMIT 1),
					EXISTS(SELECT 1 FROM global_trades WHERE asset = $1 AND timestamp > $2 AND timestamp <= $2 + INTERVAL '6 hours' AND price >= $3 * 1.50 LIMIT 1),
					EXISTS(SELECT 1 FROM global_trades WHERE asset = $1 AND timestamp > $2 AND timestamp <= $2 + INTERVAL '6 hours' AND price >= $3 * 1.80 LIMIT 1)
			`, buy.Asset, buy.Timestamp, buy.Price).Scan(
				&flags[0], &flags[1], &flags[2], &flags[3], &flags[4], &flags[5],
				&flags[6], &flags[7], &flags[8], &flags[9], &flags[10], &flags[11],
				&flags[12], &flags[13], &flags[14],
			)

			batch.Queue(`
				INSERT INTO trade_spike_flags (
					trade_id, user_address, asset, title, outcome, buy_price, buy_time,
					spike_5m_30, spike_5m_50, spike_5m_80,
					spike_10m_30, spike_10m_50, spike_10m_80,
					spike_30m_30, spike_30m_50, spike_30m_80,
					spike_2h_30, spike_2h_50, spike_2h_80,
					spike_6h_30, spike_6h_50, spike_6h_80
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
				ON CONFLICT (trade_id) DO NOTHING
			`, buy.ID, buy.UserAddress, buy.Asset, title, outcome, buy.Price, buy.Timestamp,
				flags[0], flags[1], flags[2], flags[3], flags[4], flags[5],
				flags[6], flags[7], flags[8], flags[9], flags[10], flags[11],
				flags[12], flags[13], flags[14])
		}

		br := pool.SendBatch(ctx, batch)
		br.Close()

		totalProcessed += len(buys)
		log.Printf("[Migration] Processed %d historical spikes...", totalProcessed)

		if len(buys) < batchSize {
			break
		}
		offset += batchSize
	}

	// Aggregate privileged analysis
	aggregatePrivilegedAnalysis(ctx, pool)

	return nil
}
