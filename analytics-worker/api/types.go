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

// CLOBTrade represents a trade from the CLOB /data/trades endpoint.
// This endpoint has ~50ms latency vs 30-80s for the Data API.
// Requires L2 authentication.
type CLOBTrade struct {
	ID              string            `json:"id"`
	TakerOrderID    string            `json:"taker_order_id"`
	Market          string            `json:"market"`       // condition_id
	AssetID         string            `json:"asset_id"`     // token_id
	Side            string            `json:"side"`         // BUY or SELL
	Size            string            `json:"size"`         // string, needs parsing
	Price           string            `json:"price"`        // string, needs parsing
	FeeRateBps      string            `json:"fee_rate_bps"`
	Status          string            `json:"status"`       // MATCHED, MINED, CONFIRMED
	MatchTime       string            `json:"match_time"`   // Unix timestamp as string
	LastUpdate      string            `json:"last_update"`
	MakerAddress    string            `json:"maker_address"`
	Owner           string            `json:"owner"`
	TransactionHash string            `json:"transaction_hash"`
	BucketIndex     int               `json:"bucket_index"`
	Outcome         string            `json:"outcome"`
	MakerOrders     []CLOBMakerOrder  `json:"maker_orders"`
}

// CLOBMakerOrder represents a maker order within a CLOB trade.
type CLOBMakerOrder struct {
	OrderID       string `json:"order_id"`
	MakerAddress  string `json:"maker_address"`
	Owner         string `json:"owner"`
	MatchedAmount string `json:"matched_amount"`
	Price         string `json:"price"`
	AssetID       string `json:"asset_id"`
	FeeRateBps    string `json:"fee_rate_bps"`
	Side          string `json:"side"`
	Outcome       string `json:"outcome"`
}

// CLOBTradeParams for filtering CLOB trades via /data/trades endpoint.
type CLOBTradeParams struct {
	Maker   string // Filter by maker address (the user we're following)
	Taker   string // Filter by taker address
	Market  string // Filter by market (condition_id)
	AssetID string // Filter by token ID
	After   int64  // Unix timestamp - trades after this
	Before  int64  // Unix timestamp - trades before this
	ID      string // Specific trade ID
}
