package models

import "time"

// Subject represents market categories
type Subject string

const (
	SubjectPolitics    Subject = "politics"
	SubjectSports      Subject = "sports"
	SubjectFinance     Subject = "finance"
	SubjectGeopolitics Subject = "geopolitics"
	SubjectTech        Subject = "tech"
	SubjectCulture     Subject = "culture"
	SubjectEconomy     Subject = "economy"
)

// User represents a Polymarket trader
type User struct {
	ID            string                   `json:"id"`
	Username      string                   `json:"username"`
	Address       string                   `json:"address"`
	TotalTrades   int                      `json:"total_trades"`
	TotalPNL      float64                  `json:"total_pnl"`
	WinRate       float64                  `json:"win_rate"`
	Consistency   float64                  `json:"consistency"` // 0-1 score based on performance across market types
	LastActive    time.Time                `json:"last_active"`
	LastSyncedAt  time.Time                `json:"last_synced_at"` // Last time we fetched this user's data
	RedFlags      []string                 `json:"red_flags"`
	SubjectScores map[Subject]SubjectScore `json:"subject_scores"`
}

// SubjectScore represents performance in a specific subject
type SubjectScore struct {
	Trades      int     `json:"trades"`
	PNL         float64 `json:"pnl"`
	WinRate     float64 `json:"win_rate"`
	Consistency float64 `json:"consistency"`
}

// Trade represents a single trade
type Trade struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	MarketID  string    `json:"market_id"`
	Subject   Subject   `json:"subject"`
	Amount    float64   `json:"amount"`
	PNL       float64   `json:"pnl"`
	Outcome   string    `json:"outcome"` // "win", "loss", "pending"
	Timestamp time.Time `json:"timestamp"`
}

// TradeDetail captures the raw trade information shown on user profiles.
type TradeDetail struct {
	ID              string    `json:"id"`
	UserID          string    `json:"user_id"`
	MarketID        string    `json:"market_id"`
	Subject         Subject   `json:"subject"`
	Type            string    `json:"type"` // TRADE, REDEEM, SPLIT, MERGE, etc.
	Side            string    `json:"side"`
	IsMaker         bool      `json:"is_maker"` // true if user was maker (limit order), false if taker (market order)
	Size            float64   `json:"size"`
	UsdcSize        float64   `json:"usdc_size"` // For REDEEM, this is the payout amount
	Price           float64   `json:"price"`
	Outcome         string    `json:"outcome"`
	Title           string    `json:"title"`
	Slug            string    `json:"slug"`
	EventSlug       string    `json:"event_slug"`
	TransactionHash string    `json:"transaction_hash"`
	Name            string    `json:"name"`
	Pseudonym       string    `json:"pseudonym"`
	Timestamp       time.Time `json:"timestamp"`
	InsertedAt      time.Time `json:"inserted_at"`
}

// Market represents a Polymarket market
type Market struct {
	ID          string    `json:"id"`
	Question    string    `json:"question"`
	Subject     Subject   `json:"subject"`
	Resolution  string    `json:"resolution"` // "resolved", "open", "closed"
	ResolvedAt  time.Time `json:"resolved_at"`
	IsMeme      bool      `json:"is_meme"`    // Red flag: comedy/joke markets
	IsObvious   bool      `json:"is_obvious"` // Red flag: 95%+ probability
	TotalVolume float64   `json:"total_volume"`
}

// UserRanking represents a ranked user with all metrics
type UserRanking struct {
	User         User    `json:"user"`
	Rank         int     `json:"rank"`
	OverallScore float64 `json:"overall_score"`
	Subject      Subject `json:"subject"` // Current filter subject
}

// AggregatedPosition represents a user's position in a specific market+outcome
type AggregatedPosition struct {
	MarketOutcome string    `json:"market_outcome"` // "Market Title | Outcome"
	Title         string    `json:"title"`
	Outcome       string    `json:"outcome"`
	Subject       Subject   `json:"subject"`
	TotalBought   float64   `json:"total_bought"`   // Sum of (size * price) for BUY
	TotalSold     float64   `json:"total_sold"`     // Sum of (size * price) for SELL
	QtyBought     float64   `json:"qty_bought"`     // Sum of size for BUY
	QtySold       float64   `json:"qty_sold"`       // Sum of size for SELL
	GainLoss      float64   `json:"gain_loss"`      // total_sold - total_bought
	BuyCount      int       `json:"buy_count"`      // Number of buy trades
	SellCount     int       `json:"sell_count"`     // Number of sell trades
	FirstBuyAt    time.Time `json:"first_buy_at"`
	LastBuyAt     time.Time `json:"last_buy_at"`
	FirstSellAt   time.Time `json:"first_sell_at"`
	LastSellAt    time.Time `json:"last_sell_at"`
	DurationMins  float64   `json:"duration_mins"`  // Minutes between first buy and last sell
}
