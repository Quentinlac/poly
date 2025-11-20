package service

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"polymarket-analyzer/analyzer"
	"polymarket-analyzer/api"
	"polymarket-analyzer/config"
	"polymarket-analyzer/models"
	"polymarket-analyzer/storage"
)

// Service handles business logic and coordinates between API, storage, and analyzer.
type Service struct {
	store          *storage.Store
	processor      *analyzer.Processor
	ranker         *analyzer.Ranker
	cfg            *config.Config
	apiClient      *api.Client
	subgraphClient *api.SubgraphClient

	cacheMu       sync.RWMutex
	rankingsCache map[string]rankingsCacheEntry
	profileCache  map[string]userCacheEntry
}

type rankingsCacheEntry struct {
	data    []models.UserRanking
	expires time.Time
}

type userCacheEntry struct {
	data    *models.User
	expires time.Time
}

// FilterOptions captures optional leaderboard filters from the UI.
type FilterOptions struct {
	MinTrades      *int
	MaxTrades      *int
	MinWinRate     *float64
	MaxWinRate     *float64
	MinConsistency *float64
	MaxConsistency *float64
	MinPNL         *float64
	MaxPNL         *float64
	HideRedFlags   bool
}

// NewService creates a new service
func NewService(store *storage.Store, cfg *config.Config, apiClient *api.Client) *Service {
	ranker := analyzer.NewRanker(cfg.Scoring)
	return &Service{
		store:          store,
		processor:      analyzer.NewProcessor(ranker),
		ranker:         ranker,
		cfg:            cfg,
		apiClient:      apiClient,
		subgraphClient: api.NewSubgraphClient(),
		rankingsCache:  make(map[string]rankingsCacheEntry),
		profileCache:   make(map[string]userCacheEntry),
	}
}

// GetTopUsersBySubject fetches and ranks top users for a subject
func (s *Service) GetTopUsersBySubject(ctx context.Context, subject models.Subject, limit int, filters FilterOptions) ([]models.UserRanking, error) {
	if limit <= 0 || limit > s.cfg.Scoring.MaxRankResults {
		limit = s.cfg.Scoring.MaxRankResults
	}

	cacheKey := fmt.Sprintf("%s:%d:%s", subject, limit, filters.cacheKey())
	if rankings, ok := s.cachedRankings(cacheKey); ok {
		return rankings, nil
	}

	users, err := s.store.ListUsers(ctx, subject, limit*2)
	if err != nil {
		return nil, err
	}

	filtered := s.applyFilters(users, subject, filters)

	rankings := s.ranker.RankUsers(filtered, subject)
	if len(rankings) > limit {
		rankings = rankings[:limit]
	}

	s.storeRankings(cacheKey, rankings)
	return rankings, nil
}

// GetUserProfile fetches detailed user profile
func (s *Service) GetUserProfile(ctx context.Context, userID string) (*models.User, error) {
	normalized := normalizeUserID(userID)
	if normalized == "" {
		return nil, nil
	}
	if user, ok := s.cachedProfile(normalized); ok {
		return user, nil
	}

	user, err := s.store.GetUser(ctx, normalized)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, nil
	}

	s.storeProfile(normalized, user)
	return user, nil
}

// GetUserTrades returns the stored trades for a user.
func (s *Service) GetUserTrades(ctx context.Context, userID string, limit int) ([]models.TradeDetail, error) {
	normalized := normalizeUserID(userID)
	if normalized == "" {
		return nil, fmt.Errorf("user id required")
	}
	return s.store.ListUserTrades(ctx, normalized, limit)
}

// GetUserTradesLive fetches trades directly from the Polymarket Subgraph.
// Uses The Graph Protocol to get ALL historical trades (no pagination limit).
// Also fetches REDEEM activities from REST API and merges them.
func (s *Service) GetUserTradesLive(ctx context.Context, userID string, limit int) ([]models.TradeDetail, error) {
	if s.subgraphClient == nil {
		return nil, fmt.Errorf("subgraph client unavailable")
	}
	normalized := normalizeUserID(userID)
	if normalized == "" {
		return nil, fmt.Errorf("user id required")
	}
	if limit <= 0 || limit > 500000 {
		limit = 500000
	}

	// Fetch all trades from the subgraph
	log.Printf("[Subgraph] Fetching all trades for user %s", normalized)
	events, err := s.subgraphClient.GetAllUserTrades(ctx, normalized)
	if err != nil {
		return nil, fmt.Errorf("subgraph fetch failed: %w", err)
	}
	log.Printf("[Subgraph] Fetched %d order filled events", len(events))

	// Fetch user profile info from REST API (name, pseudonym, profileImage)
	var profileName, profilePseudonym string
	if s.apiClient != nil {
		profileTrades, err := s.apiClient.GetActivity(ctx, api.TradeQuery{
			User:  normalized,
			Limit: 1,
		})
		if err == nil && len(profileTrades) > 0 {
			profileName = profileTrades[0].Name
			profilePseudonym = profileTrades[0].Pseudonym
			log.Printf("[Subgraph] Got profile info: name=%s, pseudonym=%s", profileName, profilePseudonym)
		}
	}

	// Fetch REDEEM activities from REST API
	var redemptions []api.DataTrade
	if s.apiClient != nil {
		redemptions, err = s.apiClient.GetRedemptions(ctx, normalized)
		if err != nil {
			log.Printf("[API] Warning: failed to fetch redemptions: %v", err)
			redemptions = []api.DataTrade{}
		} else {
			log.Printf("[API] Fetched %d redemptions", len(redemptions))
		}
	}

	if len(events) == 0 && len(redemptions) == 0 {
		return []models.TradeDetail{}, nil
	}

	// Get unique token IDs for market lookup
	tokenIDs := api.GetUniqueTokenIDs(events)
	log.Printf("[Subgraph] Found %d unique tokens to look up", len(tokenIDs))

	// Build token -> market info map
	tokenMap, err := s.subgraphClient.BuildTokenMap(ctx, tokenIDs)
	if err != nil {
		log.Printf("[Subgraph] Warning: token map build failed: %v (trades will have limited info)", err)
		tokenMap = make(map[string]api.TokenInfo)
	}
	log.Printf("[Subgraph] Built token map with %d entries", len(tokenMap))

	// Convert events to TradeDetail with market info
	var trades []models.TradeDetail
	seenIDs := make(map[string]bool)

	for _, event := range events {
		dataTrade := event.ConvertToDataTradeWithInfo(tokenMap, normalized)
		// Add profile info
		dataTrade.Name = profileName
		dataTrade.Pseudonym = profilePseudonym
		trade := s.toTradeDetail(dataTrade)

		// Deduplicate by transaction hash
		if !seenIDs[trade.ID] {
			seenIDs[trade.ID] = true
			trades = append(trades, trade)
		}
	}

	// Add redemptions
	for _, redeem := range redemptions {
		trade := s.toTradeDetail(redeem)

		// Deduplicate by transaction hash
		if !seenIDs[trade.ID] {
			seenIDs[trade.ID] = true
			trades = append(trades, trade)
		}
	}

	// Sort all activities by timestamp (descending - newest first)
	sort.Slice(trades, func(i, j int) bool {
		return trades[i].Timestamp.After(trades[j].Timestamp)
	})

	// Apply limit
	if len(trades) > limit {
		trades = trades[:limit]
	}

	log.Printf("[Service] Converted %d total activities (trades + redemptions)", len(trades))

	// Save trades to DB (this sets InsertedAt for new trades, keeps original for existing)
	if err := s.store.SaveTrades(ctx, trades); err != nil {
		log.Printf("[Service] Warning: failed to save trades to DB: %v", err)
		// Continue anyway - we still have the live data
	} else {
		log.Printf("[Service] Saved %d trades to DB", len(trades))

		// Load from DB to get InsertedAt values
		dbTrades, err := s.store.ListUserTrades(ctx, normalized, limit)
		if err != nil {
			log.Printf("[Service] Warning: failed to reload trades from DB: %v", err)
		} else {
			// Build map of ID -> InsertedAt from DB
			insertedAtMap := make(map[string]time.Time)
			for _, t := range dbTrades {
				if !t.InsertedAt.IsZero() {
					insertedAtMap[t.ID] = t.InsertedAt
				}
			}

			// Merge InsertedAt into our trades
			for i := range trades {
				if insertedAt, ok := insertedAtMap[trades[i].ID]; ok {
					trades[i].InsertedAt = insertedAt
				}
			}
		}
	}

	return trades, nil
}

// AggregateUserPositions groups trades by market+outcome and calculates metrics
// Time complexity: O(n) where n = number of trades
func (s *Service) AggregateUserPositions(ctx context.Context, userID string) ([]models.AggregatedPosition, error) {
	// Fetch all trades for the user
	trades, err := s.GetUserTradesLive(ctx, userID, 500000)
	if err != nil {
		return nil, err
	}

	if len(trades) == 0 {
		return []models.AggregatedPosition{}, nil
	}

	// First pass: build a map of title -> outcome from BUY trades
	// This helps us match REDEEMs (which have empty outcome) to their positions
	titleToOutcome := make(map[string]string)
	for _, trade := range trades {
		if trade.Side == "BUY" && trade.Outcome != "" {
			titleToOutcome[trade.Title] = trade.Outcome
		}
	}

	// Use map for O(1) lookups - key is "title|outcome"
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
		// For REDEEMs with empty outcome, look up from BUY trades
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

		// Handle different activity types
		if trade.Type == "REDEEM" {
			// REDEEM is like SELL at $1.00 per token
			// UsdcSize contains the actual payout amount
			redeemValue := trade.UsdcSize
			if redeemValue == 0 {
				redeemValue = trade.Size // Fallback: size * $1.00
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

	// Convert map to slice - O(m) where m = unique positions
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

		// Calculate duration between first buy and last sell
		if !pb.firstBuy.IsZero() && !pb.lastSell.IsZero() {
			pos.DurationMins = pb.lastSell.Sub(pb.firstBuy).Minutes()
		}

		result = append(result, pos)
	}

	log.Printf("[Aggregate] Grouped %d trades into %d positions", len(trades), len(result))
	return result, nil
}

// ImportUserResult tracks the result of importing a single user's data.
type ImportUserResult struct {
	Address     string  `json:"address"`
	Success     bool    `json:"success"`
	TradeCount  int     `json:"trade_count"`
	ErrorMsg    string  `json:"error_msg,omitempty"`
	DurationSec float64 `json:"duration_sec"`
}

// ImportTopUsers fetches all historical trades for a list of users and stores them.
func (s *Service) ImportTopUsers(ctx context.Context, addresses []string) ([]ImportUserResult, error) {
	if s.apiClient == nil {
		return nil, fmt.Errorf("API client not available")
	}

	results := make([]ImportUserResult, 0, len(addresses))

	for _, addr := range addresses {
		result := ImportUserResult{Address: addr}
		start := time.Now()

		normalized := normalizeUserID(addr)
		if normalized == "" {
			result.ErrorMsg = "invalid address"
			result.DurationSec = time.Since(start).Seconds()
			results = append(results, result)
			continue
		}

		// Fetch all trades for this user (up to 500,000)
		trades, err := s.GetUserTradesLive(ctx, normalized, 500000)
		if err != nil {
			result.ErrorMsg = err.Error()
			result.DurationSec = time.Since(start).Seconds()
			results = append(results, result)
			continue
		}

		// Fetch closed positions for PNL calculation
		positions, posErr := s.fetchClosedPositions(ctx, normalized)

		// Build user profile from trades and positions
		user := s.buildUserFromTrades(normalized, trades, positions)

		// Save user profile first (required for foreign key constraint)
		if err := s.store.SaveUserSnapshot(ctx, user); err != nil {
			result.ErrorMsg = fmt.Sprintf("failed to save user profile: %v", err)
			result.DurationSec = time.Since(start).Seconds()
			results = append(results, result)
			continue
		}

		// Now save trades (foreign key constraint satisfied)
		if len(trades) > 0 {
			if err := s.store.SaveTrades(ctx, trades); err != nil {
				result.ErrorMsg = fmt.Sprintf("failed to save trades: %v", err)
				result.DurationSec = time.Since(start).Seconds()
				results = append(results, result)
				continue
			}
		}

		if posErr != nil {
			// Don't fail the import if positions fetch failed
			result.ErrorMsg = fmt.Sprintf("trades saved but positions fetch failed: %v", posErr)
		}

		result.Success = true
		result.TradeCount = len(trades)
		result.DurationSec = time.Since(start).Seconds()
		results = append(results, result)
	}

	// Invalidate caches after bulk import
	s.InvalidateCaches()

	return results, nil
}

func (s *Service) fetchClosedPositions(ctx context.Context, userAddress string) ([]api.ClosedPosition, error) {
	if s.apiClient == nil {
		return nil, fmt.Errorf("API client not available")
	}

	const maxPositions = 500
	const batchSize = 50

	var allPositions []api.ClosedPosition
	offset := 0

	for offset < maxPositions {
		batch := batchSize
		if remain := maxPositions - offset; remain < batch {
			batch = remain
		}

		positions, err := s.apiClient.GetClosedPositions(ctx, api.ClosedPositionsQuery{
			User:          userAddress,
			Limit:         batch,
			Offset:        offset,
			SortBy:        "REALIZEDPNL",
			SortDirection: "DESC",
		})
		if err != nil {
			return allPositions, err
		}
		if len(positions) == 0 {
			break
		}

		allPositions = append(allPositions, positions...)
		offset += len(positions)

		if len(positions) < batch {
			break
		}
	}

	return allPositions, nil
}

func (s *Service) buildUserFromTrades(userID string, trades []models.TradeDetail, positions []api.ClosedPosition) models.User {
	user := models.User{
		ID:            userID,
		Address:       userID,
		SubjectScores: make(map[models.Subject]models.SubjectScore),
	}

	// Get username from first trade if available
	if len(trades) > 0 {
		if trades[0].Name != "" {
			user.Username = trades[0].Name
		} else if trades[0].Pseudonym != "" {
			user.Username = trades[0].Pseudonym
		} else {
			user.Username = shortAddress(userID)
		}
	} else {
		user.Username = shortAddress(userID)
	}

	// Aggregate trades by subject
	subjectAgg := make(map[models.Subject]*struct {
		trades int
		volume float64
	})

	for _, trade := range trades {
		if _, ok := subjectAgg[trade.Subject]; !ok {
			subjectAgg[trade.Subject] = &struct {
				trades int
				volume float64
			}{}
		}
		subjectAgg[trade.Subject].trades++
		subjectAgg[trade.Subject].volume += trade.Size * trade.Price

		if trade.Timestamp.After(user.LastActive) {
			user.LastActive = trade.Timestamp
		}
	}

	// Process closed positions for PNL and win rate
	positionsBySubject := make(map[models.Subject]*struct {
		pnl    float64
		wins   int
		losses int
	})

	for _, pos := range positions {
		// Classify the position's market
		subject := models.Subject("") // Default unknown
		if s.processor != nil {
			if subj, _, _ := s.processor.ClassifyMarket(pos.Title, pos.Title); subj != "" {
				subject = subj
			}
		}

		if _, ok := positionsBySubject[subject]; !ok {
			positionsBySubject[subject] = &struct {
				pnl    float64
				wins   int
				losses int
			}{}
		}

		pnl := pos.RealizedPNL.Float64()
		positionsBySubject[subject].pnl += pnl
		if pnl > 0 {
			positionsBySubject[subject].wins++
		} else if pnl < 0 {
			positionsBySubject[subject].losses++
		}
	}

	// Build subject scores
	var totalTrades int
	var totalPNL float64
	var totalWins, totalLosses int

	for subject, agg := range subjectAgg {
		score := models.SubjectScore{
			Trades: agg.trades,
		}

		if pos, ok := positionsBySubject[subject]; ok {
			score.PNL = pos.pnl
			if pos.wins+pos.losses > 0 {
				score.WinRate = float64(pos.wins) / float64(pos.wins+pos.losses)
			}
			totalWins += pos.wins
			totalLosses += pos.losses
			totalPNL += pos.pnl
		}

		user.SubjectScores[subject] = score
		totalTrades += agg.trades
	}

	user.TotalTrades = totalTrades
	user.TotalPNL = totalPNL
	if totalWins+totalLosses > 0 {
		user.WinRate = float64(totalWins) / float64(totalWins+totalLosses)
	}

	// Calculate consistency (variance in win rates across subjects)
	user.Consistency = calculateConsistencyScore(user.SubjectScores)

	// Detect red flags
	if s.ranker != nil {
		user.RedFlags = detectUserRedFlags(user)
	}

	return user
}

func calculateConsistencyScore(scores map[models.Subject]models.SubjectScore) float64 {
	if len(scores) == 0 {
		return 0
	}
	var total float64
	var count int
	for _, score := range scores {
		if score.Trades > 0 {
			total += score.WinRate
			count++
		}
	}
	if count == 0 {
		return 0
	}
	avg := total / float64(count)
	var variance float64
	for _, score := range scores {
		if score.Trades > 0 {
			diff := score.WinRate - avg
			variance += diff * diff
		}
	}
	variance /= float64(count)
	return 1.0 / (1.0 + variance*10)
}

func detectUserRedFlags(user models.User) []string {
	var flags []string
	if user.TotalTrades < 10 && user.TotalPNL > 10000 {
		flags = append(flags, "Very few trades but huge profit (likely one lucky bet)")
	}
	if user.TotalTrades > 50 && user.TotalPNL < -5000 {
		flags = append(flags, "Sustained negative PnL over many trades")
	}
	return flags
}

func shortAddress(addr string) string {
	if len(addr) <= 10 {
		return addr
	}
	return addr[:6] + "…" + addr[len(addr)-4:]
}

func (s *Service) cachedRankings(key string) ([]models.UserRanking, bool) {
	s.cacheMu.RLock()
	entry, ok := s.rankingsCache[key]
	s.cacheMu.RUnlock()
	if !ok || time.Now().After(entry.expires) {
		return nil, false
	}
	return entry.data, true
}

func (s *Service) storeRankings(key string, rankings []models.UserRanking) {
	ttl := time.Duration(s.cfg.Cache.RankingTTLMins) * time.Minute
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	s.cacheMu.Lock()
	s.rankingsCache[key] = rankingsCacheEntry{
		data:    rankings,
		expires: time.Now().Add(ttl),
	}
	s.cacheMu.Unlock()
}

func (s *Service) cachedProfile(userID string) (*models.User, bool) {
	s.cacheMu.RLock()
	entry, ok := s.profileCache[userID]
	s.cacheMu.RUnlock()
	if !ok || time.Now().After(entry.expires) {
		return nil, false
	}
	return entry.data, true
}

func (s *Service) storeProfile(userID string, user *models.User) {
	ttl := time.Duration(s.cfg.Cache.ProfileTTLMins) * time.Minute
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	s.cacheMu.Lock()
	s.profileCache[userID] = userCacheEntry{
		data:    user,
		expires: time.Now().Add(ttl),
	}
	s.cacheMu.Unlock()
}

// InvalidateCaches clears ranking and profile caches (used after fresh syncs).
func (s *Service) InvalidateCaches() {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	s.rankingsCache = make(map[string]rankingsCacheEntry)
	s.profileCache = make(map[string]userCacheEntry)
}

// DeleteUser removes a user and all their associated data.
func (s *Service) DeleteUser(ctx context.Context, userID string) error {
	normalized := normalizeUserID(userID)
	if normalized == "" {
		return fmt.Errorf("invalid user ID")
	}

	if err := s.store.DeleteUser(ctx, normalized); err != nil {
		return err
	}

	// Invalidate caches after deletion
	s.InvalidateCaches()

	return nil
}

func (s *Service) applyFilters(users []models.User, subject models.Subject, filters FilterOptions) []models.User {
	filtered := make([]models.User, 0, len(users))
	for _, user := range users {
		if filters.HideRedFlags && len(user.RedFlags) > 0 {
			continue
		}

		stats := subjectStats(user, subject)

		if filters.MinTrades != nil && stats.Trades < *filters.MinTrades {
			continue
		}
		if filters.MaxTrades != nil && stats.Trades > *filters.MaxTrades {
			continue
		}
		if filters.MinWinRate != nil && stats.WinRate < *filters.MinWinRate {
			continue
		}
		if filters.MaxWinRate != nil && stats.WinRate > *filters.MaxWinRate {
			continue
		}
		if filters.MinConsistency != nil && stats.Consistency < *filters.MinConsistency {
			continue
		}
		if filters.MaxConsistency != nil && stats.Consistency > *filters.MaxConsistency {
			continue
		}
		if filters.MinPNL != nil && stats.PNL < *filters.MinPNL {
			continue
		}
		if filters.MaxPNL != nil && stats.PNL > *filters.MaxPNL {
			continue
		}

		filtered = append(filtered, user)
	}

	return filtered
}

func subjectStats(user models.User, subject models.Subject) models.SubjectScore {
	if subject != "" {
		if stats, ok := user.SubjectScores[subject]; ok {
			return stats
		}
	}
	return models.SubjectScore{
		Trades:      user.TotalTrades,
		PNL:         user.TotalPNL,
		WinRate:     user.WinRate,
		Consistency: user.Consistency,
	}
}

func (f FilterOptions) cacheKey() string {
	return fmt.Sprintf("mt:%v|mwt:%v|mw:%v|xw:%v|mc:%v|xc:%v|mp:%v|xp:%v|flags:%t",
		valOrEmpty(f.MinTrades),
		valOrEmpty(f.MaxTrades),
		valOrEmpty(f.MinWinRate),
		valOrEmpty(f.MaxWinRate),
		valOrEmpty(f.MinConsistency),
		valOrEmpty(f.MaxConsistency),
		valOrEmpty(f.MinPNL),
		valOrEmpty(f.MaxPNL),
		f.HideRedFlags,
	)
}

func valOrEmpty[T any](ptr *T) interface{} {
	if ptr == nil {
		return "nil"
	}
	return *ptr
}

func normalizeUserID(id string) string {
	return strings.TrimSpace(strings.ToLower(id))
}

func (s *Service) toTradeDetail(tr api.DataTrade) models.TradeDetail {
	subject := models.Subject("")
	if s.processor != nil {
		if subj, _, _ := s.processor.ClassifyMarket(tr.Title, tr.Title); subj != "" {
			subject = subj
		}
	}

	// Default type to TRADE if not specified
	tradeType := tr.Type
	if tradeType == "" {
		tradeType = "TRADE"
	}

	// Generate unique ID
	// For REDEEMs, include conditionId to handle batch redemptions in same transaction
	tradeID := tr.TransactionHash
	if tradeID == "" {
		tradeID = fmt.Sprintf("%s-%d-%s", tr.ProxyWallet, tr.Timestamp, tr.Asset)
	} else if tradeType == "REDEEM" && tr.ConditionID != "" {
		// Batch redemptions share transaction hash, so add conditionId to make unique
		tradeID = fmt.Sprintf("%s-%s", tr.TransactionHash, tr.ConditionID)
	}

	return models.TradeDetail{
		ID:              tradeID,
		UserID:          strings.ToLower(tr.ProxyWallet),
		MarketID:        tr.ConditionID,
		Subject:         subject,
		Type:            tradeType,
		Side:            strings.ToUpper(tr.Side),
		IsMaker:         tr.IsMaker,
		Size:            tr.Size.Float64(),
		UsdcSize:        tr.UsdcSize.Float64(),
		Price:           tr.Price.Float64(),
		Outcome:         tr.Outcome,
		Title:           tr.Title,
		Slug:            tr.Slug,
		EventSlug:       tr.EventSlug,
		TransactionHash: tr.TransactionHash,
		Name:            tr.Name,
		Pseudonym:       tr.Pseudonym,
		Timestamp:       time.Unix(tr.Timestamp, 0).UTC(),
	}
}
