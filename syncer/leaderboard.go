package syncer

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"polymarket-analyzer/analyzer"
	"polymarket-analyzer/api"
	"polymarket-analyzer/config"
	"polymarket-analyzer/models"
)

type marketMeta struct {
	Market api.GammaMarket
	Volume float64
}

type subjectAggregate struct {
	Trades     int
	Volume     float64
	Wins       int
	Losses     int
	PNL        float64
	LastActive time.Time
}

type userAggregate struct {
	Address      string
	Username     string
	Pseudonym    string
	Bio          string
	ProfileImage string
	LastActive   time.Time
	Subjects     map[models.Subject]*subjectAggregate
	Trades       []models.TradeDetail
}

func (ua *userAggregate) subject(subject models.Subject) *subjectAggregate {
	if ua.Subjects == nil {
		ua.Subjects = make(map[models.Subject]*subjectAggregate)
	}
	if stats, ok := ua.Subjects[subject]; ok {
		return stats
	}
	stats := &subjectAggregate{}
	ua.Subjects[subject] = stats
	return stats
}

func (ua *userAggregate) totalTrades() int {
	total := 0
	for _, subj := range ua.Subjects {
		total += subj.Trades
	}
	return total
}

type LeaderboardBuilder struct {
	cfg            config.IngestionConfig
	subjects       []models.Subject
	subjectSet     map[models.Subject]struct{}
	ranker         *analyzer.Ranker
	processor      *analyzer.Processor
	marketSubjects map[string]models.Subject
	subjectMarkets map[models.Subject][]marketMeta
	userStats      map[string]*userAggregate
}

func NewLeaderboardBuilder(ranker *analyzer.Ranker, processor *analyzer.Processor, cfg config.IngestionConfig) *LeaderboardBuilder {
	subjects := make([]models.Subject, 0, len(cfg.Subjects))
	set := make(map[models.Subject]struct{})
	for _, raw := range cfg.Subjects {
		subject := models.Subject(strings.ToLower(strings.TrimSpace(raw)))
		if subject == "" {
			continue
		}
		if _, exists := set[subject]; exists {
			continue
		}
		set[subject] = struct{}{}
		subjects = append(subjects, subject)
	}

	return &LeaderboardBuilder{
		cfg:            cfg,
		subjects:       subjects,
		subjectSet:     set,
		ranker:         ranker,
		processor:      processor,
		marketSubjects: make(map[string]models.Subject),
		subjectMarkets: make(map[models.Subject][]marketMeta),
		userStats:      make(map[string]*userAggregate),
	}
}

func (b *LeaderboardBuilder) Subjects() []models.Subject {
	return b.subjects
}

func (b *LeaderboardBuilder) ClassifyMarket(question, description string) (models.Subject, bool) {
	if b.processor == nil {
		return "", false
	}
	subject, _, _ := b.processor.ClassifyMarket(question, description)
	subject = models.Subject(strings.ToLower(string(subject)))
	if _, ok := b.subjectSet[subject]; !ok {
		return "", false
	}
	return subject, true
}

func (b *LeaderboardBuilder) AddMarket(market api.GammaMarket, subject models.Subject) {
	if _, ok := b.subjectSet[subject]; !ok {
		return
	}
	volume := market.Volume.Float64()
	b.subjectMarkets[subject] = append(b.subjectMarkets[subject], marketMeta{
		Market: market,
		Volume: volume,
	})
	if market.ConditionID != "" {
		b.marketSubjects[strings.ToLower(market.ConditionID)] = subject
	}
}

func (b *LeaderboardBuilder) ConditionSubject(conditionID string) (models.Subject, bool) {
	subject, ok := b.marketSubjects[strings.ToLower(conditionID)]
	return subject, ok
}

func (b *LeaderboardBuilder) TopMarkets(subject models.Subject) []api.GammaMarket {
	metas := b.subjectMarkets[subject]
	if len(metas) == 0 {
		return nil
	}
	sort.Slice(metas, func(i, j int) bool {
		return metas[i].Volume > metas[j].Volume
	})

	limit := b.cfg.MaxMarketsPerSubject
	if limit <= 0 || limit > len(metas) {
		limit = len(metas)
	}
	result := make([]api.GammaMarket, 0, limit)
	for i := 0; i < limit; i++ {
		result = append(result, metas[i].Market)
	}
	return result
}

func (b *LeaderboardBuilder) RecordTrade(trade api.DataTrade, subject models.Subject) {
	address := strings.ToLower(trade.ProxyWallet)
	if address == "" {
		return
	}

	agg := b.getOrCreateUser(address)
	if agg == nil {
		return
	}

	if agg.Username == "" {
		if trade.Name != "" {
			agg.Username = trade.Name
		} else if trade.Pseudonym != "" {
			agg.Username = trade.Pseudonym
		} else {
			agg.Username = shortAddress(address)
		}
	}
	if trade.Pseudonym != "" {
		agg.Pseudonym = trade.Pseudonym
	}
	if trade.Bio != "" {
		agg.Bio = trade.Bio
	}
	if trade.ProfileImage != "" {
		agg.ProfileImage = trade.ProfileImage
	}

	tradeTime := time.Unix(trade.Timestamp, 0).UTC()
	if tradeTime.After(agg.LastActive) {
		agg.LastActive = tradeTime
	}

	stats := agg.subject(subject)
	stats.Trades++
	stats.Volume += trade.Size.Float64() * trade.Price.Float64()
	if tradeTime.After(stats.LastActive) {
		stats.LastActive = tradeTime
	}

	tradeID := trade.TransactionHash
	if tradeID == "" {
		tradeID = fmt.Sprintf("%s-%d-%s", trade.ProxyWallet, trade.Timestamp, trade.Asset)
	}
	agg.Trades = append(agg.Trades, models.TradeDetail{
		ID:              tradeID,
		UserID:          address,
		MarketID:        trade.ConditionID,
		Subject:         subject,
		Side:            strings.ToUpper(trade.Side),
		Size:            trade.Size.Float64(),
		Price:           trade.Price.Float64(),
		Outcome:         trade.Outcome,
		Title:           trade.Title,
		Slug:            trade.Slug,
		TransactionHash: trade.TransactionHash,
		Name:            trade.Name,
		Pseudonym:       trade.Pseudonym,
		Timestamp:       tradeTime,
	})
}

func (b *LeaderboardBuilder) RecordClosedPosition(address string, position api.ClosedPosition) {
	addr := strings.ToLower(address)
	agg, ok := b.userStats[addr]
	if !ok {
		return
	}
	subject, ok := b.ConditionSubject(position.ConditionID)
	if !ok {
		return
	}

	stats := agg.subject(subject)
	pnl := position.RealizedPNL.Float64()
	stats.PNL += pnl
	if pnl > 0 {
		stats.Wins++
	} else if pnl < 0 {
		stats.Losses++
	}
	posTime := time.Unix(position.Timestamp, 0).UTC()
	if posTime.After(stats.LastActive) {
		stats.LastActive = posTime
	}
	if posTime.After(agg.LastActive) {
		agg.LastActive = posTime
	}
}

func (b *LeaderboardBuilder) UserAddresses() []string {
	addresses := make([]string, 0, len(b.userStats))
	for addr := range b.userStats {
		addresses = append(addresses, addr)
	}
	return addresses
}

func (b *LeaderboardBuilder) UserCount() int {
	return len(b.userStats)
}

func (b *LeaderboardBuilder) TopUserAddresses(limit int) []string {
	if limit <= 0 || limit > len(b.userStats) {
		limit = len(b.userStats)
	}
	if limit == 0 {
		return nil
	}

	list := make([]*userAggregate, 0, len(b.userStats))
	for _, agg := range b.userStats {
		list = append(list, agg)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].totalTrades() > list[j].totalTrades()
	})

	addresses := make([]string, 0, limit)
	for i := 0; i < limit && i < len(list); i++ {
		addresses = append(addresses, list[i].Address)
	}
	return addresses
}

func (b *LeaderboardBuilder) TradesByUser() map[string][]models.TradeDetail {
	result := make(map[string][]models.TradeDetail, len(b.userStats))
	for addr, agg := range b.userStats {
		if len(agg.Trades) == 0 {
			continue
		}
		result[addr] = agg.Trades
	}
	return result
}

func (b *LeaderboardBuilder) BuildUsers() []models.User {
	users := make([]models.User, 0, len(b.userStats))
	for _, agg := range b.userStats {
		user := models.User{
			ID:            agg.Address,
			Username:      agg.Username,
			Address:       agg.Address,
			SubjectScores: make(map[models.Subject]models.SubjectScore),
			LastActive:    agg.LastActive,
		}

		var totalTrades, totalWins, totalLosses int
		var totalPNL float64
		for subject, subj := range agg.Subjects {
			winRate := 0.0
			if subj.Wins+subj.Losses > 0 {
				winRate = float64(subj.Wins) / float64(subj.Wins+subj.Losses)
			}
			user.SubjectScores[subject] = models.SubjectScore{
				Trades:      subj.Trades,
				PNL:         subj.PNL,
				WinRate:     winRate,
				Consistency: 0,
			}
			totalTrades += subj.Trades
			totalWins += subj.Wins
			totalLosses += subj.Losses
			totalPNL += subj.PNL
		}

		user.TotalTrades = totalTrades
		user.TotalPNL = totalPNL
		if totalWins+totalLosses > 0 {
			user.WinRate = float64(totalWins) / float64(totalWins+totalLosses)
		}
		user.Consistency = calculateConsistency(user.SubjectScores)
		user.RedFlags = detectRedFlags(user)

		if user.Username == "" {
			user.Username = shortAddress(user.Address)
		}

		users = append(users, user)
	}
	return users
}

func (b *LeaderboardBuilder) getOrCreateUser(address string) *userAggregate {
	if agg, ok := b.userStats[address]; ok {
		return agg
	}
	if b.cfg.MaxUsers > 0 && len(b.userStats) >= b.cfg.MaxUsers {
		return nil
	}
	agg := &userAggregate{
		Address:  address,
		Username: shortAddress(address),
		Subjects: make(map[models.Subject]*subjectAggregate),
	}
	b.userStats[address] = agg
	return agg
}

func calculateConsistency(scores map[models.Subject]models.SubjectScore) float64 {
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

func detectRedFlags(user models.User) []string {
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
