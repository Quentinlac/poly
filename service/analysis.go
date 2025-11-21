package service

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"sort"
	"time"

	"polymarket-analyzer/models"
)

// PatternAnalysis represents the analysis result for a single pattern
type PatternAnalysis struct {
	ID          int      `json:"id"`
	Name        string   `json:"name"`
	MetricType  string   `json:"metric_type"`
	Significant bool     `json:"significant"`
	PatternType string   `json:"pattern_type"` // "win" or "loss"
	WinnerAvg   float64  `json:"winner_avg"`
	LoserAvg    float64  `json:"loser_avg"`
	Deciles     []Decile `json:"deciles"`
}

// Decile represents a single decile's statistics
type Decile struct {
	Number    int     `json:"number"` // 1-10
	Count     int     `json:"count"`
	AvgPnL    float64 `json:"avg_pnl"`
	TotalPnL  float64 `json:"total_pnl"`
	MinMetric float64 `json:"min_metric"`
	MaxMetric float64 `json:"max_metric"`
	WinCount  int     `json:"win_count"`
	LossCount int     `json:"loss_count"`
}

// AnalysisResult contains all pattern analyses for a user
type AnalysisResult struct {
	UserID    string            `json:"user_id"`
	TotalPos  int               `json:"total_positions"`
	WinCount  int               `json:"win_count"`
	LossCount int               `json:"loss_count"`
	TotalPnL  float64           `json:"total_pnl"`
	Patterns  []PatternAnalysis `json:"patterns"`
}

// positionMetrics holds calculated metrics for a single position
type positionMetrics struct {
	key             string
	gainLoss        float64
	totalValue      float64
	numBuys         int
	numSells        int
	makerRatio      float64
	avgBuyPrice     float64
	avgTimeGap      float64
	sizeProgression float64
	durationMins    float64
	avgHour         float64
	totalTrades     int
	burstBuyCount   int
	timeToExit      float64
}

// GetUserAnalysis performs decile analysis on all patterns for a user
func (s *Service) GetUserAnalysis(ctx context.Context, userID string) (*AnalysisResult, error) {
	normalized := normalizeUserID(userID)
	if normalized == "" {
		return nil, nil
	}

	// Check cache first - validate by trade count
	cachedJSON, cachedTradeCount, _, err := s.store.GetAnalysisCache(ctx, normalized)
	if err == nil && cachedJSON != "" {
		// Verify cache is still valid by checking trade count
		currentTradeCount, countErr := s.store.GetUserTradeCount(ctx, normalized)
		if countErr == nil && currentTradeCount == cachedTradeCount && currentTradeCount > 0 {
			// Cache is valid, deserialize and return
			var result AnalysisResult
			if err := json.Unmarshal([]byte(cachedJSON), &result); err == nil {
				log.Printf("[Analysis] Cache hit for user %s (%d trades)", normalized[:8], currentTradeCount)
				return &result, nil
			}
		}
	}

	log.Printf("[Analysis] Cache miss for user %s, computing fresh analysis", normalized[:8])

	// Try to get pre-computed positions first (Materialized View)
	positions, err := s.store.GetUserPositions(ctx, normalized)
	if err != nil || len(positions) == 0 {
		log.Printf("[Analysis] No materialized positions for %s, falling back to live aggregation", normalized[:8])
		// Fallback: Fetch trades and aggregate on the fly
		trades, err := s.GetUserTradesLive(ctx, normalized, 500000)
		if err != nil {
			return nil, err
		}

		positions = s.aggregatePositionsFromTrades(trades)

		// Trigger background update of materialized view
		if len(positions) > 0 {
			go func() {
				if err := s.store.SaveUserPositions(context.Background(), normalized, positions); err != nil {
					log.Printf("[Analysis] Failed to save materialized positions: %v", err)
				}
			}()
		}
	} else {
		log.Printf("[Analysis] Using %d materialized positions for %s", len(positions), normalized[:8])
	}

	if len(positions) == 0 {
		result := &AnalysisResult{
			UserID:   normalized,
			Patterns: []PatternAnalysis{},
		}
		// Cache empty result too
		if jsonBytes, err := json.Marshal(result); err == nil {
			_ = s.store.SaveAnalysisCache(ctx, normalized, string(jsonBytes), 0)
		}
		return result, nil
	}

	// We still need trades for some detailed metrics (like time gaps), but we can optimize this later.
	// For now, to keep it "super fast", we will rely on the positions.
	// However, calculatePositionMetrics currently REQUIRES trades for advanced metrics.
	// To fully optimize, we should store these advanced metrics in the position itself or accept a slight accuracy tradeoff.
	// Given the "hyper fast" requirement, we will fetch trades ONLY if we didn't have positions, OR if we want to be perfect.
	// BUT, fetching 500k trades is the bottleneck.
	// Let's see if we can get away with using the positions as the primary source.
	// The current `calculatePositionMetrics` iterates over trades.
	// If we want to avoid fetching trades, we need to pre-calculate those metrics in `aggregatePositionsFromTrades` and store them.
	// The `AggregatedPosition` struct already has some, but not all (e.g. avgTimeGap, sizeProgression).
	// For this iteration, we will fetch trades to ensure correctness, BUT we can optimize `GetUserTradesLive` to be faster or cached?
	// Wait, if we have positions, we still need trades for `calculatePositionMetrics`.
	// If we want to be "hyper fast", we MUST avoid fetching all trades.
	// Let's check `GetUserTradesLive`. It queries the DB.
	// If we have 10k trades, it's fast. 500k is slow.
	// For now, let's stick to the plan: Use materialized positions for the list, but if we need deep metrics, we might still need trades.
	// Actually, `calculatePositionMetrics` is called for EACH position.
	// If we pass `nil` trades, it returns basic metrics.
	// Let's try to fetch trades only if absolutely necessary, or maybe we can skip the detailed metrics for the "fast" path?
	// No, the user wants analysis.
	// Let's fetch trades, but maybe we can optimize the query?
	// The bottleneck IS the aggregation. `aggregatePositionsFromTrades` is O(N).
	// If we already have positions, we skip that O(N) step.
	// So we still fetch trades for `calculatePositionMetrics`.
	// Let's optimize: We can fetch trades for a specific market if needed, but that's N queries.
	// Better: Fetch all trades (DB is fast enough for simple SELECT), but skip the heavy aggregation logic.

	trades, err := s.GetUserTradesLive(ctx, normalized, 500000)
	if err != nil {
		return nil, err
	}

	// Build position -> trades map
	tradeMap := make(map[string][]models.TradeDetail)
	for _, t := range trades {
		key := t.Title + "|" + t.Outcome
		tradeMap[key] = append(tradeMap[key], t)
	}

	// Calculate metrics for each position
	var metrics []positionMetrics
	var totalPnL float64
	winCount := 0
	lossCount := 0

	for _, pos := range positions {
		key := pos.Title + "|" + pos.Outcome
		posTrades := tradeMap[key]

		m := calculatePositionMetrics(pos, posTrades)
		metrics = append(metrics, m)

		totalPnL += pos.GainLoss
		if pos.GainLoss > 0 {
			winCount++
		} else if pos.GainLoss < 0 {
			lossCount++
		}
	}

	// Define patterns to analyze
	patterns := []struct {
		id             int
		name           string
		metricType     string
		getValue       func(m positionMetrics) float64
		higherIsBetter bool
	}{
		{1, "Conviction Sizing", "Total Position Value (USDC)", func(m positionMetrics) float64 { return m.totalValue }, true},
		{2, "Maker vs Taker", "Maker Trade Ratio", func(m positionMetrics) float64 { return m.makerRatio }, true},
		{3, "DCA Pattern", "Number of Buy Trades", func(m positionMetrics) float64 { return float64(m.numBuys) }, true},
		{4, "Holding Duration", "Duration (minutes)", func(m positionMetrics) float64 { return m.durationMins }, false},
		{5, "Buy/Sell Ratio", "Buy/Sell Ratio", func(m positionMetrics) float64 { return float64(m.numBuys) / float64(m.numSells+1) }, true},
		{6, "Time of Day", "Average Hour (UTC)", func(m positionMetrics) float64 { return m.avgHour }, true},
		{7, "Avg Buy Price", "Average Buy Price", func(m positionMetrics) float64 { return m.avgBuyPrice }, false},
		{8, "Position Scaling", "Size Progression", func(m positionMetrics) float64 { return m.sizeProgression }, true},
		{9, "Time Between Buys", "Avg Gap (mins)", func(m positionMetrics) float64 { return m.avgTimeGap }, true},
		{10, "Trade Frequency", "Total Trades", func(m positionMetrics) float64 { return float64(m.totalTrades) }, false},
		{11, "Burst Buying", "Burst Count (<10 min)", func(m positionMetrics) float64 { return float64(m.burstBuyCount) }, true},
		{12, "Fast Exit (<2h)", "Time to Exit (mins)", func(m positionMetrics) float64 { return m.timeToExit }, false},
	}

	// Analyze each pattern
	var patternResults []PatternAnalysis
	for _, p := range patterns {
		analysis := analyzePattern(metrics, p.id, p.name, p.metricType, p.getValue, p.higherIsBetter)
		patternResults = append(patternResults, analysis)
	}

	result := &AnalysisResult{
		UserID:    normalized,
		TotalPos:  len(positions),
		WinCount:  winCount,
		LossCount: lossCount,
		TotalPnL:  totalPnL,
		Patterns:  patternResults,
	}

	// Cache the result for fast subsequent lookups
	if jsonBytes, err := json.Marshal(result); err == nil {
		if err := s.store.SaveAnalysisCache(ctx, normalized, string(jsonBytes), len(trades)); err != nil {
			log.Printf("[Analysis] Warning: failed to cache result: %v", err)
		} else {
			log.Printf("[Analysis] Cached result for user %s (%d trades, %d positions)", normalized[:8], len(trades), len(positions))
		}
	}

	return result, nil
}

// UpdateUserPositions refreshes the materialized view for a user's positions
func (s *Service) UpdateUserPositions(ctx context.Context, userID string) error {
	normalized := normalizeUserID(userID)
	if normalized == "" {
		return nil
	}

	trades, err := s.GetUserTradesLive(ctx, normalized, 500000)
	if err != nil {
		return err
	}

	positions := s.aggregatePositionsFromTrades(trades)
	if len(positions) == 0 {
		return nil
	}

	return s.store.SaveUserPositions(ctx, normalized, positions)
}

func calculatePositionMetrics(pos models.AggregatedPosition, trades []models.TradeDetail) positionMetrics {
	m := positionMetrics{
		key:          pos.Title + "|" + pos.Outcome,
		gainLoss:     pos.GainLoss,
		totalValue:   pos.TotalBought,
		numBuys:      pos.BuyCount,
		numSells:     pos.SellCount,
		durationMins: pos.DurationMins,
		totalTrades:  len(trades),
	}

	if len(trades) == 0 {
		return m
	}

	// Calculate maker ratio
	makerCount := 0
	for _, t := range trades {
		if t.Role == "MAKER" {
			makerCount++
		}
	}
	m.makerRatio = float64(makerCount) / float64(len(trades))

	// Get buy trades
	var buyTrades []models.TradeDetail
	var sellTrades []models.TradeDetail
	for _, t := range trades {
		if t.Side == "BUY" {
			buyTrades = append(buyTrades, t)
		} else if t.Side == "SELL" || t.Type == "REDEEM" {
			sellTrades = append(sellTrades, t)
		}
	}

	// Average buy price
	if len(buyTrades) > 0 {
		totalCost := 0.0
		totalQty := 0.0
		for _, t := range buyTrades {
			totalCost += t.Size * t.Price
			totalQty += t.Size
		}
		if totalQty > 0 {
			m.avgBuyPrice = totalCost / totalQty
		}
	}

	// Time gaps and burst buying
	if len(buyTrades) > 1 {
		// Sort by timestamp
		sort.Slice(buyTrades, func(i, j int) bool {
			return buyTrades[i].Timestamp.Before(buyTrades[j].Timestamp)
		})

		var gaps []float64
		burstCount := 0
		for i := 1; i < len(buyTrades); i++ {
			gap := buyTrades[i].Timestamp.Sub(buyTrades[i-1].Timestamp).Minutes()
			gaps = append(gaps, gap)
			if gap <= 10 {
				burstCount++
			}
		}

		if len(gaps) > 0 {
			sum := 0.0
			for _, g := range gaps {
				sum += g
			}
			m.avgTimeGap = sum / float64(len(gaps))
		}
		m.burstBuyCount = burstCount
	}

	// Size progression
	if len(buyTrades) > 1 {
		var sizes []float64
		for _, t := range buyTrades {
			sizes = append(sizes, t.Size*t.Price)
		}
		mid := len(sizes) / 2
		firstHalfSum := 0.0
		secondHalfSum := 0.0
		for i := 0; i < mid; i++ {
			firstHalfSum += sizes[i]
		}
		for i := mid; i < len(sizes); i++ {
			secondHalfSum += sizes[i]
		}
		firstHalfAvg := firstHalfSum / float64(mid)
		secondHalfAvg := secondHalfSum / float64(len(sizes)-mid)
		if firstHalfAvg > 0 {
			m.sizeProgression = (secondHalfAvg - firstHalfAvg) / firstHalfAvg
		}
	}

	// Average hour
	if len(buyTrades) > 0 {
		hourSum := 0
		for _, t := range buyTrades {
			hourSum += t.Timestamp.Hour()
		}
		m.avgHour = float64(hourSum) / float64(len(buyTrades))
	}

	// Time to exit
	if len(buyTrades) > 0 && len(sellTrades) > 0 {
		lastBuy := buyTrades[0].Timestamp
		for _, t := range buyTrades {
			if t.Timestamp.After(lastBuy) {
				lastBuy = t.Timestamp
			}
		}
		firstSell := sellTrades[0].Timestamp
		for _, t := range sellTrades {
			if t.Timestamp.Before(firstSell) {
				firstSell = t.Timestamp
			}
		}
		if firstSell.After(lastBuy) {
			m.timeToExit = firstSell.Sub(lastBuy).Minutes()
		}
	}

	return m
}

// aggregatePositionsFromTrades groups trades by market+outcome (used internally to avoid duplicate API calls)
func (s *Service) aggregatePositionsFromTrades(trades []models.TradeDetail) []models.AggregatedPosition {
	if len(trades) == 0 {
		return []models.AggregatedPosition{}
	}

	// First pass: build a map of title -> outcome from BUY trades
	titleToOutcome := make(map[string]string)
	for _, trade := range trades {
		if trade.Side == "BUY" && trade.Outcome != "" {
			titleToOutcome[trade.Title] = trade.Outcome
		}
	}

	// Position builder struct
	type positionBuilder struct {
		title       string
		outcome     string
		subject     models.Subject
		totalBought float64
		totalSold   float64
		qtyBought   float64
		qtySold     float64
		buyCount    int
		sellCount   int
		firstBuy    time.Time
		lastBuy     time.Time
		firstSell   time.Time
		lastSell    time.Time
	}

	positions := make(map[string]*positionBuilder)

	// Single pass through all trades - O(n)
	for _, trade := range trades {
		outcome := trade.Outcome
		if outcome == "" && trade.Type == "REDEEM" {
			if o, ok := titleToOutcome[trade.Title]; ok {
				outcome = o
			}
		}

		key := trade.Title + "|" + outcome
		pb, exists := positions[key]
		if !exists {
			pb = &positionBuilder{
				title:   trade.Title,
				outcome: outcome,
				subject: trade.Subject,
			}
			positions[key] = pb
		}

		if trade.Type == "REDEEM" {
			redeemValue := trade.UsdcSize
			if redeemValue == 0 {
				redeemValue = trade.Size
			}
			pb.totalSold += redeemValue
			pb.qtySold += trade.Size
			pb.sellCount++
			if pb.firstSell.IsZero() || trade.Timestamp.Before(pb.firstSell) {
				pb.firstSell = trade.Timestamp
			}
			if trade.Timestamp.After(pb.lastSell) {
				pb.lastSell = trade.Timestamp
			}
		} else if trade.Side == "BUY" {
			cost := trade.Size * trade.Price
			pb.totalBought += cost
			pb.qtyBought += trade.Size
			pb.buyCount++
			if pb.firstBuy.IsZero() || trade.Timestamp.Before(pb.firstBuy) {
				pb.firstBuy = trade.Timestamp
			}
			if trade.Timestamp.After(pb.lastBuy) {
				pb.lastBuy = trade.Timestamp
			}
		} else if trade.Side == "SELL" {
			cost := trade.Size * trade.Price
			pb.totalSold += cost
			pb.qtySold += trade.Size
			pb.sellCount++
			if pb.firstSell.IsZero() || trade.Timestamp.Before(pb.firstSell) {
				pb.firstSell = trade.Timestamp
			}
			if trade.Timestamp.After(pb.lastSell) {
				pb.lastSell = trade.Timestamp
			}
		}
	}

	// Convert map to slice
	result := make([]models.AggregatedPosition, 0, len(positions))
	for key, pb := range positions {
		pos := models.AggregatedPosition{
			MarketOutcome: key,
			Title:         pb.title,
			Outcome:       pb.outcome,
			Subject:       pb.subject,
			TotalBought:   pb.totalBought,
			TotalSold:     pb.totalSold,
			QtyBought:     pb.qtyBought,
			QtySold:       pb.qtySold,
			GainLoss:      pb.totalSold - pb.totalBought,
			BuyCount:      pb.buyCount,
			SellCount:     pb.sellCount,
			FirstBuyAt:    pb.firstBuy,
			LastBuyAt:     pb.lastBuy,
			FirstSellAt:   pb.firstSell,
			LastSellAt:    pb.lastSell,
		}

		if !pb.firstBuy.IsZero() && !pb.lastSell.IsZero() {
			pos.DurationMins = pb.lastSell.Sub(pb.firstBuy).Minutes()
		}

		result = append(result, pos)
	}

	return result
}

func analyzePattern(metrics []positionMetrics, id int, name, metricType string, getValue func(positionMetrics) float64, higherIsBetter bool) PatternAnalysis {
	// Sort metrics by value
	type metricWithValue struct {
		m     positionMetrics
		value float64
	}

	var sorted []metricWithValue
	for _, m := range metrics {
		sorted = append(sorted, metricWithValue{m: m, value: getValue(m)})
	}

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].value < sorted[j].value
	})

	// Calculate winner/loser averages
	var winnerSum, loserSum float64
	winnerCount, loserCount := 0, 0
	for _, s := range sorted {
		if s.m.gainLoss > 0 {
			winnerSum += getValue(s.m)
			winnerCount++
		} else if s.m.gainLoss < 0 {
			loserSum += getValue(s.m)
			loserCount++
		}
	}

	winnerAvg := 0.0
	loserAvg := 0.0
	if winnerCount > 0 {
		winnerAvg = winnerSum / float64(winnerCount)
	}
	if loserCount > 0 {
		loserAvg = loserSum / float64(loserCount)
	}

	// Determine pattern type and significance
	var patternType string
	significant := false

	if winnerCount > 0 && loserCount > 0 {
		if higherIsBetter {
			if winnerAvg > loserAvg {
				patternType = "win"
				significant = math.Abs(winnerAvg-loserAvg) > 0.1*math.Max(winnerAvg, loserAvg)
			} else {
				patternType = "loss"
				significant = math.Abs(winnerAvg-loserAvg) > 0.1*math.Max(winnerAvg, loserAvg)
			}
		} else {
			if winnerAvg < loserAvg {
				patternType = "win"
				significant = math.Abs(winnerAvg-loserAvg) > 0.1*math.Max(winnerAvg, loserAvg)
			} else {
				patternType = "loss"
				significant = math.Abs(winnerAvg-loserAvg) > 0.1*math.Max(winnerAvg, loserAvg)
			}
		}
	}

	// Split into deciles
	n := len(sorted)
	decileSize := int(math.Ceil(float64(n) / 10.0))
	var deciles []Decile

	for d := 1; d <= 10; d++ {
		start := (d - 1) * decileSize
		end := d * decileSize
		if start >= n {
			break
		}
		if end > n {
			end = n
		}

		decileItems := sorted[start:end]
		if len(decileItems) == 0 {
			continue
		}

		var totalPnL float64
		winCount := 0
		lossCount := 0
		minMetric := decileItems[0].value
		maxMetric := decileItems[0].value

		for _, item := range decileItems {
			totalPnL += item.m.gainLoss
			if item.m.gainLoss > 0 {
				winCount++
			} else if item.m.gainLoss < 0 {
				lossCount++
			}
			if item.value < minMetric {
				minMetric = item.value
			}
			if item.value > maxMetric {
				maxMetric = item.value
			}
		}

		deciles = append(deciles, Decile{
			Number:    d,
			Count:     len(decileItems),
			AvgPnL:    totalPnL / float64(len(decileItems)),
			TotalPnL:  totalPnL,
			MinMetric: minMetric,
			MaxMetric: maxMetric,
			WinCount:  winCount,
			LossCount: lossCount,
		})
	}

	return PatternAnalysis{
		ID:          id,
		Name:        name,
		MetricType:  metricType,
		Significant: significant,
		PatternType: patternType,
		WinnerAvg:   winnerAvg,
		LoserAvg:    loserAvg,
		Deciles:     deciles,
	}
}
