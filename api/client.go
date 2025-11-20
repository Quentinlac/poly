package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Client handles Polymarket API interactions.
type Client struct {
	ClobBaseURL  string
	GammaBaseURL string
	DataBaseURL  string
	HTTPClient   *http.Client
	auth         *Auth
	UseAuth      bool
	maxRetries   int
	retryDelay   time.Duration
}

// MarketQueryParams controls /markets requests.
type MarketQueryParams struct {
	Limit     int
	Offset    int
	Order     string
	Ascending *bool
	Closed    *bool
}

// TradeQuery controls /trades requests.
type TradeQuery struct {
	Markets      []string
	EventIDs     []int
	User         string
	MakerAddress string // Filter by maker address (for incremental updates)
	Side         string
	TakerOnly    *bool
	Limit        int
	Offset       int
	After        int64    // Unix timestamp - only return trades after this time (for incremental updates)
	Before       int64    // Unix timestamp - only return trades before this time
	Types        []string // Activity types: TRADE, REDEEM, SPLIT, MERGE, REWARD, CONVERSION
}

// ClosedPositionsQuery controls /closed-positions requests.
type ClosedPositionsQuery struct {
	User          string
	Markets       []string
	EventIDs      []int
	Limit         int
	Offset        int
	SortBy        string
	SortDirection string
}

// NewClient creates a new Polymarket API client.
func NewClient(clobBaseURL string) *Client {
	if clobBaseURL == "" {
		clobBaseURL = "https://clob.polymarket.com"
	}

	gammaURL := os.Getenv("POLYMARKET_GAMMA_API_URL")
	if gammaURL == "" {
		gammaURL = "https://gamma-api.polymarket.com"
	}

	dataURL := os.Getenv("POLYMARKET_DATA_API_URL")
	if dataURL == "" {
		dataURL = "https://data-api.polymarket.com"
	}

	client := &Client{
		ClobBaseURL:  strings.TrimRight(clobBaseURL, "/"),
		GammaBaseURL: strings.TrimRight(gammaURL, "/"),
		DataBaseURL:  strings.TrimRight(dataURL, "/"),
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		maxRetries: 3,
		retryDelay: 2 * time.Second,
	}

	if auth, err := NewAuth(); err == nil {
		client.auth = auth
		client.UseAuth = true
	}

	return client
}

// ListMarkets fetches markets from the gamma API.
func (c *Client) ListMarkets(ctx context.Context, params MarketQueryParams) ([]GammaMarket, error) {
	values := url.Values{}
	limit := params.Limit
	if limit <= 0 {
		limit = 100
	}
	values.Set("limit", strconv.Itoa(limit))
	if params.Offset > 0 {
		values.Set("offset", strconv.Itoa(params.Offset))
	}
	if params.Order != "" {
		values.Set("order", params.Order)
	} else {
		values.Set("order", "volume")
	}
	if params.Ascending != nil {
		values.Set("ascending", strconv.FormatBool(*params.Ascending))
	}
	if params.Closed != nil {
		values.Set("closed", strconv.FormatBool(*params.Closed))
	}

	resp, err := c.doRequest(ctx, http.MethodGet, c.GammaBaseURL, "/markets", values, false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var markets []GammaMarket
	if err := json.NewDecoder(resp.Body).Decode(&markets); err != nil {
		return nil, fmt.Errorf("failed to decode markets: %w", err)
	}
	return markets, nil
}

// GetTrades fetches trades filtered by markets or events.
func (c *Client) GetTrades(ctx context.Context, params TradeQuery) ([]DataTrade, error) {
	values := url.Values{}
	limit := params.Limit
	if limit <= 0 {
		limit = 100
	}
	values.Set("limit", strconv.Itoa(limit))
	if params.Offset > 0 {
		values.Set("offset", strconv.Itoa(params.Offset))
	}
	if params.TakerOnly != nil {
		values.Set("takerOnly", strconv.FormatBool(*params.TakerOnly))
	}
	if len(params.Markets) > 0 {
		for _, market := range params.Markets {
			values.Add("market", market)
		}
	}
	if len(params.EventIDs) > 0 {
		for _, id := range params.EventIDs {
			values.Add("eventId", strconv.Itoa(id))
		}
	}
	if params.User != "" {
		values.Set("user", params.User)
	}
	if params.MakerAddress != "" {
		values.Set("maker", params.MakerAddress)
	}
	if params.Side != "" {
		values.Set("side", strings.ToUpper(params.Side))
	}
	if params.After > 0 {
		values.Set("after", strconv.FormatInt(params.After, 10))
	}
	if params.Before > 0 {
		values.Set("before", strconv.FormatInt(params.Before, 10))
	}

	resp, err := c.doRequest(ctx, http.MethodGet, c.DataBaseURL, "/trades", values, false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var trades []DataTrade
	if err := json.NewDecoder(resp.Body).Decode(&trades); err != nil {
		return nil, fmt.Errorf("failed to decode trades: %w", err)
	}
	return trades, nil
}

// GetActivity fetches user activity (trades) using the /activity endpoint.
// This endpoint supports pagination beyond the ~1500 trade limit of /trades.
func (c *Client) GetActivity(ctx context.Context, params TradeQuery) ([]DataTrade, error) {
	values := url.Values{}
	limit := params.Limit
	if limit <= 0 {
		limit = 100
	}
	values.Set("limit", strconv.Itoa(limit))
	if params.Offset > 0 {
		values.Set("offset", strconv.Itoa(params.Offset))
	}
	if params.User != "" {
		values.Set("user", params.User)
	}
	if params.Side != "" {
		values.Set("side", strings.ToUpper(params.Side))
	}
	// Set activity types - if none specified, default to TRADE only
	if len(params.Types) > 0 {
		values.Set("type", strings.Join(params.Types, ","))
	} else {
		values.Set("type", "TRADE")
	}

	resp, err := c.doRequest(ctx, http.MethodGet, c.DataBaseURL, "/activity", values, false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var activity []DataTrade
	if err := json.NewDecoder(resp.Body).Decode(&activity); err != nil {
		return nil, fmt.Errorf("failed to decode activity: %w", err)
	}
	return activity, nil
}

// GetRedemptions fetches all REDEEM activities for a user.
// This fetches token redemptions when markets resolve.
func (c *Client) GetRedemptions(ctx context.Context, userAddress string) ([]DataTrade, error) {
	var allRedemptions []DataTrade
	offset := 0
	batchSize := 500

	for {
		params := TradeQuery{
			User:   userAddress,
			Limit:  batchSize,
			Offset: offset,
			Types:  []string{"REDEEM"},
		}

		redemptions, err := c.GetActivity(ctx, params)
		if err != nil {
			return nil, fmt.Errorf("fetch redemptions at offset %d: %w", offset, err)
		}

		if len(redemptions) == 0 {
			break
		}

		allRedemptions = append(allRedemptions, redemptions...)
		log.Printf("[API] Fetched %d redemptions (offset=%d, total=%d)", len(redemptions), offset, len(allRedemptions))

		if len(redemptions) < batchSize {
			break
		}

		offset += batchSize
	}

	return allRedemptions, nil
}

// GetClosedPositions fetches realized positions for a user.
func (c *Client) GetClosedPositions(ctx context.Context, params ClosedPositionsQuery) ([]ClosedPosition, error) {
	if params.User == "" {
		return nil, fmt.Errorf("user address is required for closed positions")
	}

	values := url.Values{}
	values.Set("user", params.User)
	limit := params.Limit
	if limit <= 0 {
		limit = 10
	}
	values.Set("limit", strconv.Itoa(limit))
	if params.Offset > 0 {
		values.Set("offset", strconv.Itoa(params.Offset))
	}
	if len(params.Markets) > 0 {
		for _, market := range params.Markets {
			values.Add("market", market)
		}
	}
	if len(params.EventIDs) > 0 {
		for _, id := range params.EventIDs {
			values.Add("eventId", strconv.Itoa(id))
		}
	}
	if params.SortBy != "" {
		values.Set("sortBy", params.SortBy)
	}
	if params.SortDirection != "" {
		values.Set("sortDirection", params.SortDirection)
	}

	resp, err := c.doRequest(ctx, http.MethodGet, c.DataBaseURL, "/closed-positions", values, false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var positions []ClosedPosition
	if err := json.NewDecoder(resp.Body).Decode(&positions); err != nil {
		return nil, fmt.Errorf("failed to decode closed positions: %w", err)
	}
	return positions, nil
}

func (c *Client) doRequest(ctx context.Context, method, baseURL, path string, query url.Values, useAuth bool) (*http.Response, error) {
	endpoint := strings.TrimRight(baseURL, "/") + path
	if query != nil && len(query) > 0 {
		endpoint += "?" + query.Encode()
	}

	// Debug logging
	log.Printf("[API] %s %s", method, endpoint)

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, endpoint, nil)
		if err != nil {
			return nil, err
		}

		if useAuth && c.UseAuth && c.auth != nil {
			headers, err := c.auth.SignRequest()
			if err != nil {
				return nil, fmt.Errorf("failed to sign request: %w", err)
			}
			for k, v := range headers {
				req.Header.Set(k, v)
			}
		}

		resp, err := c.HTTPClient.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(c.retryDelay * time.Duration(attempt+1))
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < c.maxRetries {
			wait := c.retryDelay * time.Duration(attempt+1)
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if secs, parseErr := strconv.Atoi(ra); parseErr == nil {
					wait = time.Duration(secs) * time.Second
				}
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			log.Printf("[client] rate limited on %s %s (attempt %d/%d): %s", method, path, attempt+1, c.maxRetries+1, strings.TrimSpace(string(body)))
			time.Sleep(wait)
			continue
		}

		if resp.StatusCode >= 400 {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("polymarket api %s %s: %d %s", method, path, resp.StatusCode, strings.TrimSpace(string(body)))
		}

		return resp, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("request failed after retries: %w", lastErr)
	}
	return nil, fmt.Errorf("request failed after %d attempts", c.maxRetries+1)
}
