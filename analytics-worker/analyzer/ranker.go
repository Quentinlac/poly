package analyzer

import (
	"polymarket-analyzer/config"
	"polymarket-analyzer/models"
	"sort"
)

// Ranker handles user ranking and filtering
type Ranker struct {
	minTrades      int
	minWinRate     float64
	maxResults     int
	pnlScale       float64
	redFlagFilters bool
}

// NewRanker creates a new ranker with configured settings
func NewRanker(scoringCfg config.ScoringConfig) *Ranker {
	return &Ranker{
		minTrades:      scoringCfg.MinTrades,
		minWinRate:     scoringCfg.MinWinRate,
		maxResults:     scoringCfg.MaxRankResults,
		pnlScale:       scoringCfg.PNLScale,
		redFlagFilters: true,
	}
}

// RankUsers ranks users based on performance metrics
func (r *Ranker) RankUsers(users []models.User, subject models.Subject) []models.UserRanking {
	var rankings []models.UserRanking

	for _, user := range users {
		// Filter by red flags if enabled
		if r.redFlagFilters && r.hasRedFlags(user) {
			continue
		}

		// Get subject-specific score
		score := r.calculateScore(user, subject)

		rankings = append(rankings, models.UserRanking{
			User:         user,
			OverallScore: score,
			Subject:      subject,
		})
	}

	// Sort by overall score (descending)
	sort.Slice(rankings, func(i, j int) bool {
		return rankings[i].OverallScore > rankings[j].OverallScore
	})

	// Assign ranks
	for i := range rankings {
		rankings[i].Rank = i + 1
	}

	// Limit to configured maximum
	if r.maxResults > 0 && len(rankings) > r.maxResults {
		rankings = rankings[:r.maxResults]
	}

	return rankings
}

// calculateScore calculates overall performance score
func (r *Ranker) calculateScore(user models.User, subject models.Subject) float64 {
	var score float64

	// Get subject-specific metrics if available
	var subjectScore models.SubjectScore
	if s, ok := user.SubjectScores[subject]; ok {
		subjectScore = s
	} else {
		// Fall back to overall metrics
		subjectScore = models.SubjectScore{
			Trades:      user.TotalTrades,
			PNL:         user.TotalPNL,
			WinRate:     user.WinRate,
			Consistency: user.Consistency,
		}
	}

	// Trade count score (0-25 points)
	// More trades = more experience, but diminishing returns
	tradeScore := float64(subjectScore.Trades) / 10.0
	if tradeScore > 25 {
		tradeScore = 25
	}
	score += tradeScore

	// PNL score (0-30 points)
	// Normalized PNL (positive is good)
	scale := r.pnlScale
	if scale == 0 {
		scale = 1000.0
	}
	pnlScore := subjectScore.PNL / scale // Adjust divisor based on typical PNL range
	if pnlScore > 30 {
		pnlScore = 30
	}
	if pnlScore < 0 {
		pnlScore = 0 // Negative PNL gets 0 points
	}
	score += pnlScore

	// Win rate score (0-25 points)
	// Threshold derived from configured minimum
	winRateScore := (subjectScore.WinRate - r.minWinRate) * 500
	if winRateScore < 0 {
		winRateScore = 0
	}
	if winRateScore > 25 {
		winRateScore = 25
	}
	score += winRateScore

	// Consistency score (0-20 points)
	// How consistent across different market types
	consistencyScore := subjectScore.Consistency * 20
	score += consistencyScore

	return score
}

// hasRedFlags checks if user has red flags
func (r *Ranker) hasRedFlags(user models.User) bool {
	// Check if user has any red flags
	if len(user.RedFlags) > 0 {
		return true
	}

	// Check for obvious red flags based on metrics
	// Very few trades but huge profit (probably one lucky bet)
	if user.TotalTrades < 10 && user.TotalPNL > 10000 {
		return true
	}

	// Only trades meme markets (would be in RedFlags)
	// Only bets on obvious outcomes (would be in RedFlags)

	return false
}

// FilterBySubject filters users by their activity in a specific subject
func (r *Ranker) FilterBySubject(users []models.User, subject models.Subject) []models.User {
	var filtered []models.User

	for _, user := range users {
		// If subject not specified, use total trades
		if subject == "" {
			if user.TotalTrades >= r.minTrades {
				filtered = append(filtered, user)
			}
			continue
		}

		// Check if user has activity in this subject
		if score, ok := user.SubjectScores[subject]; ok && score.Trades >= r.minTrades {
			filtered = append(filtered, user)
		}
	}

	return filtered
}

// DetectRedFlags analyzes user and adds red flags
func (r *Ranker) DetectRedFlags(user models.User, trades []models.Trade, markets map[string]models.Market) models.User {
	var redFlags []string

	// Check for obvious bets (95%+ probability)
	obviousCount := 0
	for _, trade := range trades {
		if market, ok := markets[trade.MarketID]; ok && market.IsObvious {
			obviousCount++
		}
	}
	if float64(obviousCount)/float64(len(trades)) > 0.5 {
		redFlags = append(redFlags, "Only bets on obvious outcomes (>95% probability)")
	}

	// Check for meme markets
	memeCount := 0
	for _, trade := range trades {
		if market, ok := markets[trade.MarketID]; ok && market.IsMeme {
			memeCount++
		}
	}
	if float64(memeCount)/float64(len(trades)) > 0.5 {
		redFlags = append(redFlags, "Only trades meme/comedy markets")
	}

	// Check for very few trades but huge profit
	if user.TotalTrades < 10 && user.TotalPNL > 10000 {
		redFlags = append(redFlags, "Very few trades but huge profit (likely one lucky bet)")
	}

	user.RedFlags = redFlags
	return user
}
