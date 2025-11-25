package api

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

// Numeric handles Polymarket numbers that may arrive as strings or numbers.
type Numeric float64

func (n *Numeric) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || strings.EqualFold(string(data), "null") {
		*n = 0
		return nil
	}

	// Handle quoted numbers.
	if data[0] == '"' && data[len(data)-1] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		if s == "" {
			*n = 0
			return nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		*n = Numeric(f)
		return nil
	}

	var f float64
	if err := json.Unmarshal(data, &f); err != nil {
		return err
	}
	*n = Numeric(f)
	return nil
}

func (n Numeric) Float64() float64 {
	return float64(n)
}

// GammaMarket represents a market returned by the gamma API.
type GammaMarket struct {
	ID           string     `json:"id"`
	Question     string     `json:"question"`
	Description  string     `json:"description"`
	ConditionID  string     `json:"conditionId"`
	Slug         string     `json:"slug"`
	Category     string     `json:"category"`
	Volume       Numeric    `json:"volumeNum"`
	Volume24Hr   Numeric    `json:"volume24hr"`
	Liquidity    Numeric    `json:"liquidityNum"`
	Closed       *bool      `json:"closed"`
	Tags         []GammaTag `json:"tags"`
	StartDateISO string     `json:"startDateIso"`
	EndDateISO   string     `json:"endDateIso"`
	ClobTokenIds string     `json:"clobTokenIds"` // Comma-separated token IDs
	Outcomes     string     `json:"outcomes"`     // JSON array as string e.g. "[\"Yes\",\"No\"]"
}

type GammaTag struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Slug  string `json:"slug"`
}

// DataTrade represents a trade from the data API.
type DataTrade struct {
	ProxyWallet           string  `json:"proxyWallet"`
	Type                  string  `json:"type"` // TRADE, REDEEM, SPLIT, MERGE, etc.
	Side                  string  `json:"side"`
	IsMaker               bool    `json:"isMaker"` // true if user was maker (limit order), false if taker (market order)
	Asset                 string  `json:"asset"`
	ConditionID           string  `json:"conditionId"`
	Size                  Numeric `json:"size"`
	UsdcSize              Numeric `json:"usdcSize"` // For REDEEM, this is the payout amount
	Price                 Numeric `json:"price"`
	Timestamp             int64   `json:"timestamp"`
	Title                 string  `json:"title"`
	Slug                  string  `json:"slug"`
	Icon                  string  `json:"icon"`
	EventSlug             string  `json:"eventSlug"`
	Outcome               string  `json:"outcome"`
	OutcomeIndex          int     `json:"outcomeIndex"`
	Name                  string  `json:"name"`
	Pseudonym             string  `json:"pseudonym"`
	Bio                   string  `json:"bio"`
	ProfileImage          string  `json:"profileImage"`
	ProfileImageOptimized string  `json:"profileImageOptimized"`
	TransactionHash       string  `json:"transactionHash"`
}

// Trade is an alias for DataTrade for convenience.
type Trade = DataTrade

// ClosedPosition represents a realized position for a user.
type ClosedPosition struct {
	ProxyWallet  string  `json:"proxyWallet"`
	Asset        string  `json:"asset"`
	ConditionID  string  `json:"conditionId"`
	AvgPrice     Numeric `json:"avgPrice"`
	TotalBought  Numeric `json:"totalBought"`
	RealizedPNL  Numeric `json:"realizedPnl"`
	CurPrice     Numeric `json:"curPrice"`
	Timestamp    int64   `json:"timestamp"`
	Title        string  `json:"title"`
	Slug         string  `json:"slug"`
	Outcome      string  `json:"outcome"`
	OutcomeIndex int     `json:"outcomeIndex"`
	EventSlug    string  `json:"eventSlug"`
}

// OpenPosition represents an open position (current holdings) for a user.
type OpenPosition struct {
	Asset        string  `json:"asset"`        // Token ID
	ConditionID  string  `json:"conditionId"`
	Size         Numeric `json:"size"`         // Number of tokens held
	AvgPrice     Numeric `json:"avgPrice"`     // Average purchase price
	CurPrice     Numeric `json:"curPrice"`     // Current market price
	RealizedPNL  Numeric `json:"realizedPnl"`
	Title        string  `json:"title"`
	Slug         string  `json:"slug"`
	Outcome      string  `json:"outcome"`
	OutcomeIndex int     `json:"outcomeIndex"`
	EventSlug    string  `json:"eventSlug"`
	ProxyWallet  string  `json:"proxyWallet"`
}
