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

// UserAnalytics represents computed metrics for a user
type UserAnalytics struct {
	UserAddress            string
	TotalBets              int
	TotalBuyUSD            float64
	TotalSellRedeemUSD     float64
	NetPnL                 float64
	PnLPercentage          float64
	IsBot                  bool
	AvgInvestmentPerMarket float64
	FirstTradeAt           time.Time
	LastTradeAt            time.Time
	TradesPerDayAvg        float64
	TradesPerDay30d        float64
	UniqueMarkets          int
	DataComplete           bool
	FastTrades             int     // Positions closed in < 2 hours
	AvgTrades              int     // Positions closed in 2-6 hours
	SlowTrades             int     // Positions closed in > 6 hours
	FastAvgPnL             float64 // Avg PnL for fast trades
	MidAvgPnL              float64 // Avg PnL for avg trades
	SlowAvgPnL             float64 // Avg PnL for slow trades
}

func main() {
	log.Println("[Analytics Worker] Starting v2...")

	// Get database connection string from environment
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://polymarket:polymarket@localhost:15432/polymarket?sslmode=disable"
	}

	// Parse interval from environment (default 5 minutes)
	intervalStr := os.Getenv("ANALYTICS_INTERVAL")
	interval := 5 * time.Minute
	if intervalStr != "" {
		if d, err := time.ParseDuration(intervalStr); err == nil {
			interval = d
		}
	}

	// Bot detection threshold (trades per day)
	botThreshold := 100.0

	// Connect to database
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("[Analytics Worker] Connected to database")

	// Ensure analytics table exists
	if err := createAnalyticsTable(ctx, pool); err != nil {
		log.Fatalf("Failed to create analytics table: %v", err)
	}

	// Setup signal handling for graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// Ensure privileged analysis tables exist
	if err := createPrivilegedTables(ctx, pool); err != nil {
		log.Fatalf("Failed to create privileged tables: %v", err)
	}

	// Run immediately on start
	log.Printf("[Analytics Worker] Running initial analytics computation...")
	if err := computeAnalytics(ctx, pool, botThreshold); err != nil {
		log.Printf("[Analytics Worker] Error computing analytics: %v", err)
	}

	// Run privileged analysis on start
	log.Printf("[Analytics Worker] Running initial privileged knowledge computation...")
	computeAllPrivileged(ctx, pool)

	// Start ticker for periodic runs
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Privileged analysis runs every 15 minutes (3x per analytics interval)
	privilegedTicker := time.NewTicker(15 * time.Minute)
	defer privilegedTicker.Stop()

	log.Printf("[Analytics Worker] Running analytics every %v, privileged every 15m", interval)

	for {
		select {
		case <-stop:
			log.Println("[Analytics Worker] Shutting down...")
			return
		case <-ticker.C:
			log.Printf("[Analytics Worker] Running scheduled analytics computation...")
			if err := computeAnalytics(ctx, pool, botThreshold); err != nil {
				log.Printf("[Analytics Worker] Error computing analytics: %v", err)
			}
		case <-privilegedTicker.C:
			log.Printf("[Analytics Worker] Running scheduled privileged computation...")
			computeAllPrivileged(ctx, pool)
		}
	}
}

func createAnalyticsTable(ctx context.Context, pool *pgxpool.Pool) error {
	query := `
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
	);

	CREATE INDEX IF NOT EXISTS idx_user_analytics_pnl ON user_analytics(net_pnl DESC);
	CREATE INDEX IF NOT EXISTS idx_user_analytics_bot ON user_analytics(is_bot);
	CREATE INDEX IF NOT EXISTS idx_user_analytics_volume ON user_analytics(total_buy_usd DESC);
	`

	_, err := pool.Exec(ctx, query)
	return err
}

func computeAnalytics(ctx context.Context, pool *pgxpool.Pool, botThreshold float64) error {
	start := time.Now()

	// Use a single efficient SQL query to compute all metrics per user
	// Key insight: Cap sells/redeems by the quantity bought per position
	// This ensures we only count profit on positions we saw them buy
	query := `
	WITH position_metrics AS (
		-- Calculate buy/sell quantities and USD per position (user + title + outcome)
		SELECT
			user_address,
			title,
			outcome,
			-- Quantities (shares)
			COALESCE(SUM(CASE WHEN side = 'BUY' THEN size ELSE 0 END), 0) as qty_bought,
			COALESCE(SUM(CASE WHEN side = 'SELL' OR type = 'REDEEM' THEN size ELSE 0 END), 0) as qty_sold,
			-- USD values
			COALESCE(SUM(CASE WHEN side = 'BUY' THEN usdc_size ELSE 0 END), 0) as buy_usd,
			COALESCE(SUM(CASE WHEN side = 'SELL' OR type = 'REDEEM' THEN usdc_size ELSE 0 END), 0) as sell_usd
		FROM global_trades
		GROUP BY user_address, title, outcome
	),
	capped_positions AS (
		-- Cap sold quantity at bought quantity, calculate capped USD
		SELECT
			user_address,
			title,
			buy_usd,
			-- Capped sell: only count sells up to quantity bought
			CASE
				WHEN qty_bought = 0 THEN 0
				WHEN qty_sold <= qty_bought THEN sell_usd
				ELSE sell_usd * (qty_bought / NULLIF(qty_sold, 0))
			END as capped_sell_usd
		FROM position_metrics
	),
	user_metrics AS (
		-- Aggregate capped values per user
		SELECT
			user_address,
			SUM(buy_usd) as total_buy_usd,
			SUM(capped_sell_usd) as total_sell_redeem_usd
		FROM capped_positions
		GROUP BY user_address
	),
	user_counts AS (
		-- Get trade counts and timestamps
		SELECT
			user_address,
			COUNT(*) as total_bets,
			MIN(timestamp) as first_trade_at,
			MAX(timestamp) as last_trade_at,
			COUNT(DISTINCT title) as unique_markets
		FROM global_trades
		GROUP BY user_address
	),
	market_investments AS (
		-- Calculate average investment per market
		SELECT
			user_address,
			AVG(market_total_buy) as avg_investment_per_market
		FROM (
			SELECT
				user_address,
				title,
				SUM(CASE WHEN side = 'BUY' THEN usdc_size ELSE 0 END) as market_total_buy
			FROM global_trades
			GROUP BY user_address, title
		) market_totals
		GROUP BY user_address
	),
	trades_30d AS (
		-- Calculate trades per day in last 30 days
		SELECT
			user_address,
			COUNT(*) / 30.0 as trades_per_day_30d
		FROM global_trades
		WHERE timestamp >= NOW() - INTERVAL '30 days'
		GROUP BY user_address
	),
	position_durations AS (
		-- Calculate holding duration and PnL for each position (first buy to last sell)
		SELECT
			pd.user_address,
			pd.title,
			pd.first_buy,
			pd.last_sell,
			cp.buy_usd,
			cp.capped_sell_usd,
			(cp.capped_sell_usd - cp.buy_usd) as position_pnl
		FROM (
			SELECT
				user_address,
				title,
				MIN(CASE WHEN side = 'BUY' THEN timestamp END) as first_buy,
				MAX(CASE WHEN side = 'SELL' OR type = 'REDEEM' THEN timestamp END) as last_sell
			FROM global_trades
			GROUP BY user_address, title
			HAVING MIN(CASE WHEN side = 'BUY' THEN timestamp END) IS NOT NULL
			   AND MAX(CASE WHEN side = 'SELL' OR type = 'REDEEM' THEN timestamp END) IS NOT NULL
		) pd
		JOIN capped_positions cp ON pd.user_address = cp.user_address AND pd.title = cp.title
	),
	trade_speed AS (
		-- Categorize positions by holding duration and calculate avg PnL
		SELECT
			user_address,
			SUM(CASE WHEN EXTRACT(EPOCH FROM (last_sell - first_buy)) / 3600 < 2 THEN 1 ELSE 0 END) as fast_trades,
			SUM(CASE WHEN EXTRACT(EPOCH FROM (last_sell - first_buy)) / 3600 >= 2
			          AND EXTRACT(EPOCH FROM (last_sell - first_buy)) / 3600 < 6 THEN 1 ELSE 0 END) as avg_trades,
			SUM(CASE WHEN EXTRACT(EPOCH FROM (last_sell - first_buy)) / 3600 >= 6 THEN 1 ELSE 0 END) as slow_trades,
			-- Average PnL per category
			COALESCE(AVG(CASE WHEN EXTRACT(EPOCH FROM (last_sell - first_buy)) / 3600 < 2 THEN position_pnl END), 0) as fast_avg_pnl,
			COALESCE(AVG(CASE WHEN EXTRACT(EPOCH FROM (last_sell - first_buy)) / 3600 >= 2
			                   AND EXTRACT(EPOCH FROM (last_sell - first_buy)) / 3600 < 6 THEN position_pnl END), 0) as mid_avg_pnl,
			COALESCE(AVG(CASE WHEN EXTRACT(EPOCH FROM (last_sell - first_buy)) / 3600 >= 6 THEN position_pnl END), 0) as slow_avg_pnl
		FROM position_durations
		GROUP BY user_address
	)
	SELECT
		uc.user_address,
		uc.total_bets,
		um.total_buy_usd,
		um.total_sell_redeem_usd,
		(um.total_sell_redeem_usd - um.total_buy_usd) as net_pnl,
		CASE
			WHEN um.total_buy_usd > 0 THEN
				((um.total_sell_redeem_usd - um.total_buy_usd) / um.total_buy_usd * 100)
			ELSE 0
		END as pnl_percentage,
		uc.first_trade_at,
		uc.last_trade_at,
		CASE
			WHEN uc.first_trade_at IS NOT NULL AND uc.last_trade_at IS NOT NULL
				AND uc.last_trade_at > uc.first_trade_at THEN
				uc.total_bets::numeric / GREATEST(EXTRACT(EPOCH FROM (uc.last_trade_at - uc.first_trade_at)) / 86400, 1)
			ELSE uc.total_bets::numeric
		END as trades_per_day_avg,
		COALESCE(t30.trades_per_day_30d, 0) as trades_per_day_30d,
		uc.unique_markets,
		COALESCE(mi.avg_investment_per_market, 0) as avg_investment_per_market,
		COALESCE(ts.fast_trades, 0) as fast_trades,
		COALESCE(ts.avg_trades, 0) as avg_trades,
		COALESCE(ts.slow_trades, 0) as slow_trades,
		COALESCE(ts.fast_avg_pnl, 0) as fast_avg_pnl,
		COALESCE(ts.mid_avg_pnl, 0) as mid_avg_pnl,
		COALESCE(ts.slow_avg_pnl, 0) as slow_avg_pnl
	FROM user_counts uc
	JOIN user_metrics um ON uc.user_address = um.user_address
	LEFT JOIN market_investments mi ON uc.user_address = mi.user_address
	LEFT JOIN trades_30d t30 ON uc.user_address = t30.user_address
	LEFT JOIN trade_speed ts ON uc.user_address = ts.user_address
	`

	rows, err := pool.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("query user metrics: %w", err)
	}
	defer rows.Close()

	// Collect analytics
	var analytics []UserAnalytics
	for rows.Next() {
		var ua UserAnalytics
		err := rows.Scan(
			&ua.UserAddress,
			&ua.TotalBets,
			&ua.TotalBuyUSD,
			&ua.TotalSellRedeemUSD,
			&ua.NetPnL,
			&ua.PnLPercentage,
			&ua.FirstTradeAt,
			&ua.LastTradeAt,
			&ua.TradesPerDayAvg,
			&ua.TradesPerDay30d,
			&ua.UniqueMarkets,
			&ua.AvgInvestmentPerMarket,
			&ua.FastTrades,
			&ua.AvgTrades,
			&ua.SlowTrades,
			&ua.FastAvgPnL,
			&ua.MidAvgPnL,
			&ua.SlowAvgPnL,
		)
		if err != nil {
			return fmt.Errorf("scan row: %w", err)
		}

		// Determine if user is a bot
		// Bot if: avg > 100 trades/day overall OR > 100 trades/day in last 30 days
		ua.IsBot = ua.TradesPerDayAvg > botThreshold || ua.TradesPerDay30d > botThreshold

		// Determine if data is complete (has enough buy data to calculate meaningful PnL)
		// Data is complete if: has buys AND (sell+redeem <= 10x buy OR pnl_pct between -100% and 1000%)
		ua.DataComplete = ua.TotalBuyUSD > 0 && (ua.TotalSellRedeemUSD <= ua.TotalBuyUSD*10 || (ua.PnLPercentage >= -100 && ua.PnLPercentage <= 1000))

		analytics = append(analytics, ua)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error: %w", err)
	}

	log.Printf("[Analytics Worker] Computed metrics for %d users in %v", len(analytics), time.Since(start))

	// Upsert analytics into database
	if len(analytics) > 0 {
		if err := upsertAnalytics(ctx, pool, analytics); err != nil {
			return fmt.Errorf("upsert analytics: %w", err)
		}
	}

	log.Printf("[Analytics Worker] Completed analytics update in %v", time.Since(start))
	return nil
}

func upsertAnalytics(ctx context.Context, pool *pgxpool.Pool, analytics []UserAnalytics) error {
	// Use batch upsert for efficiency
	query := `
	INSERT INTO user_analytics (
		user_address, total_bets, total_buy_usd, total_sell_redeem_usd,
		net_pnl, pnl_percentage, is_bot, avg_investment_per_market,
		first_trade_at, last_trade_at, trades_per_day_avg, trades_per_day_30d,
		unique_markets, data_complete, fast_trades, avg_trades, slow_trades,
		fast_avg_pnl, mid_avg_pnl, slow_avg_pnl, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, NOW())
	ON CONFLICT (user_address) DO UPDATE SET
		total_bets = EXCLUDED.total_bets,
		total_buy_usd = EXCLUDED.total_buy_usd,
		total_sell_redeem_usd = EXCLUDED.total_sell_redeem_usd,
		net_pnl = EXCLUDED.net_pnl,
		pnl_percentage = EXCLUDED.pnl_percentage,
		is_bot = EXCLUDED.is_bot,
		avg_investment_per_market = EXCLUDED.avg_investment_per_market,
		first_trade_at = EXCLUDED.first_trade_at,
		last_trade_at = EXCLUDED.last_trade_at,
		trades_per_day_avg = EXCLUDED.trades_per_day_avg,
		trades_per_day_30d = EXCLUDED.trades_per_day_30d,
		unique_markets = EXCLUDED.unique_markets,
		data_complete = EXCLUDED.data_complete,
		fast_trades = EXCLUDED.fast_trades,
		avg_trades = EXCLUDED.avg_trades,
		slow_trades = EXCLUDED.slow_trades,
		fast_avg_pnl = EXCLUDED.fast_avg_pnl,
		mid_avg_pnl = EXCLUDED.mid_avg_pnl,
		slow_avg_pnl = EXCLUDED.slow_avg_pnl,
		updated_at = NOW()
	`

	// Use batch for better performance
	batch := &pgx.Batch{}
	for _, ua := range analytics {
		batch.Queue(query,
			ua.UserAddress,
			ua.TotalBets,
			ua.TotalBuyUSD,
			ua.TotalSellRedeemUSD,
			ua.NetPnL,
			ua.PnLPercentage,
			ua.IsBot,
			ua.AvgInvestmentPerMarket,
			ua.FirstTradeAt,
			ua.LastTradeAt,
			ua.TradesPerDayAvg,
			ua.TradesPerDay30d,
			ua.UniqueMarkets,
			ua.DataComplete,
			ua.FastTrades,
			ua.AvgTrades,
			ua.SlowTrades,
			ua.FastAvgPnL,
			ua.MidAvgPnL,
			ua.SlowAvgPnL,
		)
	}
	_ = batch // unused, we use chunked batches below

	// Execute batch in chunks to avoid memory issues
	batchSize := 1000
	for i := 0; i < len(analytics); i += batchSize {
		end := i + batchSize
		if end > len(analytics) {
			end = len(analytics)
		}

		chunkBatch := &pgx.Batch{}
		for j := i; j < end; j++ {
			ua := analytics[j]
			chunkBatch.Queue(query,
				ua.UserAddress,
				ua.TotalBets,
				ua.TotalBuyUSD,
				ua.TotalSellRedeemUSD,
				ua.NetPnL,
				ua.PnLPercentage,
				ua.IsBot,
				ua.AvgInvestmentPerMarket,
				ua.FirstTradeAt,
				ua.LastTradeAt,
				ua.TradesPerDayAvg,
				ua.TradesPerDay30d,
				ua.UniqueMarkets,
				ua.DataComplete,
				ua.FastTrades,
				ua.AvgTrades,
				ua.SlowTrades,
				ua.FastAvgPnL,
				ua.MidAvgPnL,
				ua.SlowAvgPnL,
			)
		}

		br := pool.SendBatch(ctx, chunkBatch)
		if err := br.Close(); err != nil {
			return fmt.Errorf("batch close: %w", err)
		}
	}

	return nil
}

// ============================================================================
// PRIVILEGED KNOWLEDGE ANALYSIS
// ============================================================================

// All combinations to compute
var privilegedCombinations = []struct {
	Window    int // minutes
	Threshold int // percentage
}{
	{5, 30}, {5, 50}, {5, 80},
	{10, 30}, {10, 50}, {10, 80},
	{30, 30}, {30, 50}, {30, 80},
	{120, 30}, {120, 50}, {120, 80},
	{360, 30}, {360, 50}, {360, 80},
}

func createPrivilegedTables(ctx context.Context, pool *pgxpool.Pool) error {
	query := `
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
	);

	CREATE INDEX IF NOT EXISTS idx_privileged_window_threshold ON privileged_analysis(time_window_minutes, price_threshold_pct);
	CREATE INDEX IF NOT EXISTS idx_privileged_hit_count ON privileged_analysis(time_window_minutes, price_threshold_pct, hit_count DESC);

	CREATE TABLE IF NOT EXISTS privileged_analysis_meta (
		time_window_minutes INTEGER NOT NULL,
		price_threshold_pct INTEGER NOT NULL,
		last_computed_at TIMESTAMPTZ NOT NULL,
		computation_duration_sec DECIMAL(10, 2),
		user_count INTEGER,
		PRIMARY KEY (time_window_minutes, price_threshold_pct)
	);
	`

	_, err := pool.Exec(ctx, query)
	return err
}

func computeAllPrivileged(ctx context.Context, pool *pgxpool.Pool) {
	startTime := time.Now()
	log.Printf("[Privileged] Starting computation of %d combinations...", len(privilegedCombinations))

	successful := 0
	for _, combo := range privilegedCombinations {
		// Use a longer timeout for heavy queries (5 minutes per query)
		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)

		err := computePrivilegedAnalysis(queryCtx, pool, combo.Window, combo.Threshold)
		cancel()

		if err != nil {
			log.Printf("[Privileged] Error computing %dmin/+%d%%: %v", combo.Window, combo.Threshold, err)
		} else {
			successful++
		}
	}

	duration := time.Since(startTime)
	log.Printf("[Privileged] Completed %d/%d combinations in %v", successful, len(privilegedCombinations), duration.Round(time.Second))
}

func computePrivilegedAnalysis(ctx context.Context, pool *pgxpool.Pool, timeWindowMinutes int, priceThresholdPct int) error {
	startTime := time.Now()
	priceThreshold := float64(priceThresholdPct) / 100.0

	log.Printf("[Privileged] Computing %dmin/+%d%% analysis...", timeWindowMinutes, priceThresholdPct)

	// Heavy query to find privileged knowledge indicators
	// Limited to 400K most recent trades to avoid timeout/disk issues
	query := `
	WITH recent_trades AS (
		SELECT * FROM global_trades
		ORDER BY timestamp DESC
		LIMIT 400000
	),
	user_buys AS (
		SELECT
			user_address,
			asset,
			price,
			timestamp,
			title,
			outcome
		FROM recent_trades
		WHERE side = 'BUY'
		AND type = 'TRADE'
		AND price > 0
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
		JOIN recent_trades gt ON gt.asset = ub.asset
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
	rows, err := pool.Query(ctx, query, interval, priceThreshold)
	if err != nil {
		return fmt.Errorf("privileged knowledge query: %w", err)
	}
	defer rows.Close()

	type privilegedUser struct {
		UserAddress string
		HitCount    int
		TotalBuys   int
		HitRate     float64
		HitsJSON    []byte
	}

	var results []privilegedUser
	for rows.Next() {
		var user privilegedUser
		err := rows.Scan(&user.UserAddress, &user.HitCount, &user.TotalBuys, &user.HitsJSON)
		if err != nil {
			return fmt.Errorf("scan privileged user: %w", err)
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
	_, err = pool.Exec(ctx, `
		DELETE FROM privileged_analysis
		WHERE time_window_minutes = $1 AND price_threshold_pct = $2
	`, timeWindowMinutes, priceThresholdPct)
	if err != nil {
		return fmt.Errorf("delete old results: %w", err)
	}

	// Insert new results
	for _, user := range results {
		_, err = pool.Exec(ctx, `
			INSERT INTO privileged_analysis (time_window_minutes, price_threshold_pct, user_address, hit_count, total_buys, hit_rate, hits_json, computed_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		`, timeWindowMinutes, priceThresholdPct, user.UserAddress, user.HitCount, user.TotalBuys, user.HitRate, user.HitsJSON)
		if err != nil {
			return fmt.Errorf("insert result: %w", err)
		}
	}

	// Update metadata
	duration := time.Since(startTime).Seconds()
	_, err = pool.Exec(ctx, `
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
