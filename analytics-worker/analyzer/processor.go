package analyzer

import (
	"polymarket-analyzer/models"
	"strings"
	"time"
)

// Processor handles data processing and metric calculation
type Processor struct {
	ranker *Ranker
}

// NewProcessor creates a new data processor using the provided ranker
func NewProcessor(ranker *Ranker) *Processor {
	return &Processor{
		ranker: ranker,
	}
}

// BuildUserFromTrades creates a User model from a list of TradeDetails (for incremental updates)
func (p *Processor) BuildUserFromTrades(address string, tradeDetails []models.TradeDetail) models.User {
	user := models.User{
		ID:            address,
		Address:       address,
		SubjectScores: make(map[models.Subject]models.SubjectScore),
	}

	if len(tradeDetails) == 0 {
		return user
	}

	// Group trades by subject
	subjectTradeDetails := make(map[models.Subject][]models.TradeDetail)
	for i := range tradeDetails {
		// Classify market if subject is not set
		if tradeDetails[i].Subject == "" {
			subject, _, _ := p.ClassifyMarket(tradeDetails[i].Title, "")
			tradeDetails[i].Subject = subject
		}
		subjectTradeDetails[tradeDetails[i].Subject] = append(subjectTradeDetails[tradeDetails[i].Subject], tradeDetails[i])
	}

	// Calculate overall metrics from trade details
	var lastActive time.Time
	for _, trade := range tradeDetails {
		if trade.Timestamp.After(lastActive) {
			lastActive = trade.Timestamp
		}
	}

	user.TotalTrades = len(tradeDetails)
	user.LastActive = lastActive

	// Calculate P&L by aggregating positions (buy/sell per market+outcome)
	positions := p.aggregatePositions(tradeDetails)
	var totalPNL float64
	var wins, losses int
	for _, pos := range positions {
		totalPNL += pos.GainLoss
		if pos.GainLoss > 0 {
			wins++
		} else if pos.GainLoss < 0 {
			losses++
		}
	}
	user.TotalPNL = totalPNL
	if wins+losses > 0 {
		user.WinRate = float64(wins) / float64(wins+losses)
	}

	// Calculate subject-specific scores from TradeDetails
	for subject, subjectTrades := range subjectTradeDetails {
		subjectPositions := p.aggregatePositions(subjectTrades)
		var subjectPNL float64
		var subjectWins, subjectLosses int
		for _, pos := range subjectPositions {
			subjectPNL += pos.GainLoss
			if pos.GainLoss > 0 {
				subjectWins++
			} else if pos.GainLoss < 0 {
				subjectLosses++
			}
		}
		var subjectWinRate float64
		if subjectWins+subjectLosses > 0 {
			subjectWinRate = float64(subjectWins) / float64(subjectWins+subjectLosses)
		}
		score := models.SubjectScore{
			Trades:  len(subjectTrades),
			PNL:     subjectPNL,
			WinRate: subjectWinRate,
		}
		user.SubjectScores[subject] = score
	}

	// Calculate consistency
	user.Consistency = p.calculateConsistency(user.SubjectScores)

	return user
}

// aggregatePositions groups trades by market+outcome and calculates P&L per position
func (p *Processor) aggregatePositions(trades []models.TradeDetail) []models.AggregatedPosition {
	if len(trades) == 0 {
		return nil
	}

	// Build title->outcome map from BUY trades (for REDEEMs that lack outcome)
	titleToOutcome := make(map[string]string)
	for _, t := range trades {
		if t.Side == "BUY" && t.Outcome != "" {
			titleToOutcome[t.Title] = t.Outcome
		}
	}

	type posBuilder struct {
		title       string
		outcome     string
		totalBought float64
		totalSold   float64
	}

	positions := make(map[string]*posBuilder)

	for _, t := range trades {
		outcome := t.Outcome
		if outcome == "" && t.Type == "REDEEM" {
			if o, ok := titleToOutcome[t.Title]; ok {
				outcome = o
			}
		}

		key := t.Title + "|" + outcome
		pb, exists := positions[key]
		if !exists {
			pb = &posBuilder{title: t.Title, outcome: outcome}
			positions[key] = pb
		}

		if t.Type == "REDEEM" {
			redeemValue := t.UsdcSize
			if redeemValue == 0 {
				redeemValue = t.Size
			}
			pb.totalSold += redeemValue
		} else if t.Side == "BUY" {
			pb.totalBought += t.Size * t.Price
		} else if t.Side == "SELL" {
			pb.totalSold += t.Size * t.Price
		}
	}

	result := make([]models.AggregatedPosition, 0, len(positions))
	for _, pb := range positions {
		result = append(result, models.AggregatedPosition{
			Title:       pb.title,
			Outcome:     pb.outcome,
			TotalBought: pb.totalBought,
			TotalSold:   pb.totalSold,
			GainLoss:    pb.totalSold - pb.totalBought,
		})
	}
	return result
}

// ProcessUserTrades calculates user metrics from trades
func (p *Processor) ProcessUserTrades(userID string, trades []models.Trade, markets map[string]models.Market) models.User {
	user := models.User{
		ID:            userID,
		SubjectScores: make(map[models.Subject]models.SubjectScore),
	}

	// Group trades by subject
	subjectTrades := make(map[models.Subject][]models.Trade)
	for _, trade := range trades {
		subjectTrades[trade.Subject] = append(subjectTrades[trade.Subject], trade)
	}

	// Calculate overall metrics
	var totalPNL float64
	var wins, losses int
	var lastActive time.Time

	for _, trade := range trades {
		totalPNL += trade.PNL
		if trade.Outcome == "win" {
			wins++
		} else if trade.Outcome == "loss" {
			losses++
		}
		if trade.Timestamp.After(lastActive) {
			lastActive = trade.Timestamp
		}
	}

	user.TotalTrades = len(trades)
	user.TotalPNL = totalPNL
	if wins+losses > 0 {
		user.WinRate = float64(wins) / float64(wins+losses)
	}
	user.LastActive = lastActive

	// Calculate subject-specific scores
	for subject, subjectTradeList := range subjectTrades {
		score := p.calculateSubjectScore(subjectTradeList)
		user.SubjectScores[subject] = score
	}

	// Calculate consistency (variance in performance across subjects)
	user.Consistency = p.calculateConsistency(user.SubjectScores)

	// Detect red flags
	user = p.ranker.DetectRedFlags(user, trades, markets)

	return user
}

// calculateSubjectScore calculates metrics for a specific subject
func (p *Processor) calculateSubjectScore(trades []models.Trade) models.SubjectScore {
	var pnl float64
	var wins, losses int

	for _, trade := range trades {
		pnl += trade.PNL
		if trade.Outcome == "win" {
			wins++
		} else if trade.Outcome == "loss" {
			losses++
		}
	}

	var winRate float64
	if wins+losses > 0 {
		winRate = float64(wins) / float64(wins+losses)
	}

	return models.SubjectScore{
		Trades:  len(trades),
		PNL:     pnl,
		WinRate: winRate,
	}
}

// calculateConsistency calculates how consistent user is across subjects
func (p *Processor) calculateConsistency(scores map[models.Subject]models.SubjectScore) float64 {
	if len(scores) == 0 {
		return 0
	}

	// Calculate average win rate
	var totalWinRate float64
	var count int
	for _, score := range scores {
		if score.Trades > 0 {
			totalWinRate += score.WinRate
			count++
		}
	}
	if count == 0 {
		return 0
	}
	avgWinRate := totalWinRate / float64(count)

	// Calculate variance (lower variance = higher consistency)
	var variance float64
	for _, score := range scores {
		if score.Trades > 0 {
			diff := score.WinRate - avgWinRate
			variance += diff * diff
		}
	}
	variance /= float64(count)

	// Convert variance to consistency score (0-1)
	// Lower variance = higher consistency
	consistency := 1.0 / (1.0 + variance*10)
	return consistency
}

// ClassifyMarket determines market subject and flags
func (p *Processor) ClassifyMarket(question string, description string) (models.Subject, bool, bool) {
	// Simple keyword-based classification
	// In production, you'd want more sophisticated NLP
	questionLower := strings.ToLower(question)
	descLower := strings.ToLower(description)
	combined := questionLower + " " + descLower

	var subject models.Subject
	isMeme := false
	isObvious := false

	// Check for meme markets
	memeKeywords := []string{"jesus", "gta", "meme", "comedy", "joke", "funny", "absurd"}
	for _, keyword := range memeKeywords {
		if strings.Contains(combined, keyword) {
			isMeme = true
			break
		}
	}

	// Classify subject
	if strings.Contains(combined, "election") || strings.Contains(combined, "president") || strings.Contains(combined, "senate") || strings.Contains(combined, "congress") || strings.Contains(combined, "political") {
		subject = models.SubjectPolitics
	} else if strings.Contains(combined, "sport") || strings.Contains(combined, "nfl") || strings.Contains(combined, "nba") || strings.Contains(combined, "soccer") || strings.Contains(combined, "football") {
		subject = models.SubjectSports
	} else if strings.Contains(combined, "stock") || strings.Contains(combined, "finance") || strings.Contains(combined, "bank") || strings.Contains(combined, "financial") {
		subject = models.SubjectFinance
	} else if strings.Contains(combined, "war") || strings.Contains(combined, "geopolitic") || strings.Contains(combined, "country") || strings.Contains(combined, "nation") {
		subject = models.SubjectGeopolitics
	} else if strings.Contains(combined, "tech") || strings.Contains(combined, "technology") || strings.Contains(combined, "apple") || strings.Contains(combined, "google") || strings.Contains(combined, "release") {
		subject = models.SubjectTech
	} else if strings.Contains(combined, "culture") || strings.Contains(combined, "entertainment") || strings.Contains(combined, "movie") || strings.Contains(combined, "music") {
		subject = models.SubjectCulture
	} else if strings.Contains(combined, "economy") || strings.Contains(combined, "gdp") || strings.Contains(combined, "inflation") || strings.Contains(combined, "unemployment") {
		subject = models.SubjectEconomy
	} else {
		subject = models.SubjectPolitics // Default
	}

	return subject, isMeme, isObvious
}
