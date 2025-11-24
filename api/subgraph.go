package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// Goldsky-hosted Polymarket orderbook subgraph (free, no API key needed)
	// Has trades only, no redemptions
	SubgraphURL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"

	// The Graph hosted Polymarket subgraph (requires API key, has redemptions)
	// Get API key at: https://thegraph.com/studio/apikeys/
	TheGraphSubgraphID = "Bx1W4S7kDVxs9gC3s2G6DS8kdNBJNVhMviCtin2DiBp"

	// CLOB API for market/token mapping
	CLOBMarketsURL = "https://clob.polymarket.com/markets"

	// Gamma API for querying markets by token ID (much faster)
	GammaMarketsURL = "https://gamma-api.polymarket.com/markets"

	// Maximum results per GraphQL query
	SubgraphBatchSize = 1000
)

// TokenInfo holds market information for a specific outcome token
type TokenInfo struct {
	TokenID     string
	ConditionID string
	Outcome     string
	Title       string
	Slug        string
	EventSlug   string
}

// CLOBMarketsResponse represents the response from CLOB markets API
type CLOBMarketsResponse struct {
	Data       []CLOBMarket `json:"data"`
	NextCursor string       `json:"next_cursor"`
	Count      int          `json:"count"`
}

// CLOBMarket represents a market from the CLOB API
type CLOBMarket struct {
	ConditionID string      `json:"condition_id"`
	Question    string      `json:"question"`
	Slug        string      `json:"market_slug"`
	Tokens      []CLOBToken `json:"tokens"`
}

// CLOBToken represents an outcome token
type CLOBToken struct {
	TokenID string `json:"token_id"`
	Outcome string `json:"outcome"`
}

// SubgraphClient queries the Polymarket subgraph for historical trade data
type SubgraphClient struct {
	httpClient *http.Client
	url        string
}

// TheGraphClient queries The Graph for redemptions (requires API key)
type TheGraphClient struct {
	httpClient *http.Client
	apiKey     string
	url        string
}

// NewTheGraphClient creates a client for The Graph with API key
func NewTheGraphClient(apiKey string) *TheGraphClient {
	url := fmt.Sprintf("https://gateway.thegraph.com/api/%s/subgraphs/id/%s", apiKey, TheGraphSubgraphID)
	return &TheGraphClient{
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		apiKey: apiKey,
		url:    url,
	}
}

// GetGlobalRedemptionsSince fetches ALL redemptions from The Graph since a timestamp
func (c *TheGraphClient) GetGlobalRedemptionsSince(ctx context.Context, afterTimestamp int64) ([]RedemptionEvent, error) {
	if c.apiKey == "" {
		return nil, fmt.Errorf("The Graph API key not configured")
	}

	var allRedemptions []RedemptionEvent
	skip := 0

	for {
		query := fmt.Sprintf(`{
			redemptions(
				first: %d,
				skip: %d,
				where: {timestamp_gt: "%d"},
				orderBy: timestamp,
				orderDirection: asc
			) {
				id
				payout
				redeemer
				timestamp
				condition
				indexSets
			}
		}`, SubgraphBatchSize, skip, afterTimestamp)

		redemptions, err := c.executeRedemptionQuery(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("The Graph redemptions query failed at skip %d: %w", skip, err)
		}

		if len(redemptions) == 0 {
			break
		}

		allRedemptions = append(allRedemptions, redemptions...)

		if len(redemptions) < SubgraphBatchSize {
			break
		}

		skip += SubgraphBatchSize

		// Safety limit
		if skip > 100000 {
			log.Printf("[TheGraph] Warning: redemptions fetch hit safety limit at %d events", len(allRedemptions))
			break
		}
	}

	return allRedemptions, nil
}

// executeRedemptionQuery sends a GraphQL query to The Graph
func (c *TheGraphClient) executeRedemptionQuery(ctx context.Context, query string) ([]RedemptionEvent, error) {
	reqBody := map[string]string{"query": query}
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal query: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("The Graph returned status %d: %s", resp.StatusCode, string(body))
	}

	var result RedemptionResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	if len(result.Errors) > 0 {
		return nil, fmt.Errorf("The Graph error: %s", result.Errors[0].Message)
	}

	return result.Data.Redemptions, nil
}

// OrderFilledEvent represents a trade event from the subgraph
type OrderFilledEvent struct {
	ID                string `json:"id"`
	TransactionHash   string `json:"transactionHash"`
	Timestamp         string `json:"timestamp"`
	OrderHash         string `json:"orderHash"`
	Maker             string `json:"maker"`
	Taker             string `json:"taker"`
	MakerAssetID      string `json:"makerAssetId"`
	TakerAssetID      string `json:"takerAssetId"`
	MakerAmountFilled string `json:"makerAmountFilled"`
	TakerAmountFilled string `json:"takerAmountFilled"`
	Fee               string `json:"fee"`
}

// SubgraphResponse represents the GraphQL response structure
type SubgraphResponse struct {
	Data struct {
		OrderFilledEvents []OrderFilledEvent `json:"orderFilledEvents"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// NewSubgraphClient creates a new client for querying the Polymarket subgraph
func NewSubgraphClient() *SubgraphClient {
	return &SubgraphClient{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		url: SubgraphURL,
	}
}

// GetAllUserTrades fetches all historical trades for a user from the subgraph
// Fetches both maker and taker trades CONCURRENTLY to get complete trade history
func (c *SubgraphClient) GetAllUserTrades(ctx context.Context, userAddress string) ([]OrderFilledEvent, error) {
	userAddress = strings.ToLower(userAddress)

	// Fetch maker and taker trades concurrently
	type result struct {
		events []OrderFilledEvent
		err    error
		role   string
	}

	results := make(chan result, 2)

	// Fetch maker trades
	go func() {
		events, err := c.fetchTradesByRole(ctx, userAddress, "maker")
		results <- result{events: events, err: err, role: "maker"}
	}()

	// Fetch taker trades
	go func() {
		events, err := c.fetchTradesByRole(ctx, userAddress, "taker")
		results <- result{events: events, err: err, role: "taker"}
	}()

	// Collect results
	var makerEvents, takerEvents []OrderFilledEvent
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err != nil {
			return nil, fmt.Errorf("fetch %s trades: %w", r.role, r.err)
		}
		if r.role == "maker" {
			makerEvents = r.events
			log.Printf("[Subgraph] Fetched %d maker trades", len(makerEvents))
		} else {
			takerEvents = r.events
			log.Printf("[Subgraph] Fetched %d taker trades", len(takerEvents))
		}
	}

	// Merge and deduplicate by event ID (not transaction hash, since multiple fills can happen in one tx)
	allEvents := make([]OrderFilledEvent, 0, len(makerEvents)+len(takerEvents))
	seen := make(map[string]bool)

	for _, e := range makerEvents {
		if !seen[e.ID] {
			seen[e.ID] = true
			allEvents = append(allEvents, e)
		}
	}

	for _, e := range takerEvents {
		if !seen[e.ID] {
			seen[e.ID] = true
			allEvents = append(allEvents, e)
		}
	}

	log.Printf("[Subgraph] Total unique trades: %d (maker: %d, taker: %d)", len(allEvents), len(makerEvents), len(takerEvents))
	return allEvents, nil
}

// GetUserTradesSince fetches trades for a user after a specific timestamp.
// This is used for incremental syncing - fetches both maker and taker trades.
func (c *SubgraphClient) GetUserTradesSince(ctx context.Context, userAddress string, afterTimestamp int64) ([]OrderFilledEvent, error) {
	userAddress = strings.ToLower(userAddress)

	// Fetch maker trades since timestamp
	makerEvents, err := c.fetchTradesByRoleSince(ctx, userAddress, "maker", afterTimestamp)
	if err != nil {
		return nil, fmt.Errorf("fetch maker trades: %w", err)
	}

	// Fetch taker trades since timestamp
	takerEvents, err := c.fetchTradesByRoleSince(ctx, userAddress, "taker", afterTimestamp)
	if err != nil {
		return nil, fmt.Errorf("fetch taker trades: %w", err)
	}

	// Merge and deduplicate by event ID
	allEvents := make([]OrderFilledEvent, 0, len(makerEvents)+len(takerEvents))
	seen := make(map[string]bool)

	for _, e := range makerEvents {
		if !seen[e.ID] {
			seen[e.ID] = true
			allEvents = append(allEvents, e)
		}
	}

	for _, e := range takerEvents {
		if !seen[e.ID] {
			seen[e.ID] = true
			allEvents = append(allEvents, e)
		}
	}

	log.Printf("[Subgraph] Incremental fetch: %d trades since %d (maker: %d, taker: %d)",
		len(allEvents), afterTimestamp, len(makerEvents), len(takerEvents))
	return allEvents, nil
}

// fetchTradesByRoleSince fetches trades where user is either maker or taker after a timestamp
func (c *SubgraphClient) fetchTradesByRoleSince(ctx context.Context, userAddress string, role string, afterTimestamp int64) ([]OrderFilledEvent, error) {
	var allEvents []OrderFilledEvent
	skip := 0

	for {
		query := fmt.Sprintf(`{
			orderFilledEvents(
				first: %d,
				skip: %d,
				where: {%s: "%s", timestamp_gt: "%d"},
				orderBy: timestamp,
				orderDirection: asc
			) {
				id
				transactionHash
				timestamp
				orderHash
				maker
				taker
				makerAssetId
				takerAssetId
				makerAmountFilled
				takerAmountFilled
				fee
			}
		}`, SubgraphBatchSize, skip, role, userAddress, afterTimestamp)

		events, err := c.executeQuery(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("subgraph query failed at skip %d: %w", skip, err)
		}

		if len(events) == 0 {
			break
		}

		allEvents = append(allEvents, events...)

		if len(events) < SubgraphBatchSize {
			break
		}

		skip += SubgraphBatchSize
	}

	return allEvents, nil
}

// fetchTradesByRole fetches trades where user is either maker or taker
func (c *SubgraphClient) fetchTradesByRole(ctx context.Context, userAddress string, role string) ([]OrderFilledEvent, error) {
	var allEvents []OrderFilledEvent
	skip := 0

	for {
		query := fmt.Sprintf(`{
			orderFilledEvents(
				first: %d,
				skip: %d,
				where: {%s: "%s"},
				orderBy: timestamp,
				orderDirection: desc
			) {
				id
				transactionHash
				timestamp
				orderHash
				maker
				taker
				makerAssetId
				takerAssetId
				makerAmountFilled
				takerAmountFilled
				fee
			}
		}`, SubgraphBatchSize, skip, role, userAddress)

		events, err := c.executeQuery(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("subgraph query failed at skip %d: %w", skip, err)
		}

		if len(events) == 0 {
			break
		}

		allEvents = append(allEvents, events...)

		if len(events) < SubgraphBatchSize {
			break
		}

		skip += SubgraphBatchSize
	}

	return allEvents, nil
}

// executeQuery sends a GraphQL query to the subgraph
func (c *SubgraphClient) executeQuery(ctx context.Context, query string) ([]OrderFilledEvent, error) {
	reqBody := map[string]string{"query": query}
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal query: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("subgraph returned status %d: %s", resp.StatusCode, string(body))
	}

	var result SubgraphResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	if len(result.Errors) > 0 {
		return nil, fmt.Errorf("subgraph error: %s", result.Errors[0].Message)
	}

	return result.Data.OrderFilledEvents, nil
}

// ConvertToDataTrade converts a subgraph OrderFilledEvent to the DataTrade format
// used by the rest of the application
func (e *OrderFilledEvent) ConvertToDataTrade() DataTrade {
	return e.ConvertToDataTradeForUser("")
}

// ConvertToDataTradeForUser converts with awareness of which user we're tracking
func (e *OrderFilledEvent) ConvertToDataTradeForUser(userAddress string) DataTrade {
	timestamp, _ := strconv.ParseInt(e.Timestamp, 10, 64)
	userAddress = strings.ToLower(userAddress)

	// Determine if user is maker or taker
	isMaker := strings.ToLower(e.Maker) == userAddress || userAddress == ""

	// Determine side based on asset IDs from maker's perspective
	// If makerAssetId is "0", maker is selling USDC (buying outcome tokens) = BUY
	// Otherwise, maker is selling outcome tokens (getting USDC) = SELL
	makerSide := "SELL"
	assetID := e.MakerAssetID
	if e.MakerAssetID == "0" {
		makerSide = "BUY"
		assetID = e.TakerAssetID
	}

	// User's side depends on whether they're maker or taker
	// Taker does the opposite of maker
	side := makerSide
	if !isMaker {
		if makerSide == "BUY" {
			side = "SELL"
		} else {
			side = "BUY"
		}
	}

	// Calculate price and size from amounts
	// MakerAmountFilled and TakerAmountFilled are in base units (6 decimals for USDC)
	makerAmt, _ := strconv.ParseFloat(e.MakerAmountFilled, 64)
	takerAmt, _ := strconv.ParseFloat(e.TakerAmountFilled, 64)

	var price, size float64
	if makerSide == "BUY" {
		// Maker gives USDC (makerAmt), gets outcome tokens (takerAmt)
		// Price = USDC / tokens
		if takerAmt > 0 {
			price = makerAmt / takerAmt
			size = takerAmt / 1e6 // Convert from base units
		}
	} else {
		// Maker gives outcome tokens (makerAmt), gets USDC (takerAmt)
		// Price = USDC / tokens
		if makerAmt > 0 {
			price = takerAmt / makerAmt
			size = makerAmt / 1e6 // Convert from base units
		}
	}

	// Calculate USDC value
	usdcSize := size * price

	// ProxyWallet should be the user we're tracking
	proxyWallet := e.Maker
	if !isMaker {
		proxyWallet = e.Taker
	}

	return DataTrade{
		ProxyWallet:     proxyWallet,
		Side:            side,
		IsMaker:         isMaker,
		Asset:           assetID, // This is the outcome token ID
		ConditionID:     "",      // Would need market lookup to get this
		Size:            Numeric(size),
		UsdcSize:        Numeric(usdcSize),
		Price:           Numeric(price),
		Timestamp:       timestamp,
		Title:           "", // Would need market lookup
		Slug:            "",
		Icon:            "",
		EventSlug:       "",
		Outcome:         "", // Would need market lookup
		OutcomeIndex:    0,
		Name:            "",
		Pseudonym:       "",
		Bio:             "",
		ProfileImage:    "",
		TransactionHash: e.TransactionHash,
	}
}

// ConvertToDataTradeWithInfo converts with market info from token map
func (e *OrderFilledEvent) ConvertToDataTradeWithInfo(tokenMap map[string]TokenInfo, userAddress string) DataTrade {
	trade := e.ConvertToDataTradeForUser(userAddress)

	// Enrich with market info if available
	if info, ok := tokenMap[trade.Asset]; ok {
		trade.ConditionID = info.ConditionID
		trade.Title = info.Title
		trade.Slug = info.Slug
		trade.Outcome = info.Outcome

		// Normalize to user's outcome perspective (match Polymarket UI)
		// If user sold "No", show as BUY "Yes" (the position they're getting)
		// This makes copy trading intuitive - users see the actual position taken
		if trade.Side == "SELL" && isComplementOutcome(info.Outcome) {
			trade.Side = "BUY"
			trade.Outcome = getComplementOutcome(info.Outcome)
			trade.Price = Numeric(1 - trade.Price.Float64()) // Complement price
		} else if trade.Side == "BUY" && isComplementOutcome(info.Outcome) {
			// User bought "No" - normalize to SELL "Yes"
			trade.Side = "SELL"
			trade.Outcome = getComplementOutcome(info.Outcome)
			trade.Price = Numeric(1 - trade.Price.Float64())
		}
	}

	return trade
}

// BuildTokenMap fetches token info for missing tokens from Gamma API
// This assumes DB is pre-populated with all tokens, so only new tokens need fetching
func (c *SubgraphClient) BuildTokenMap(ctx context.Context, tokenIDs []string) (map[string]TokenInfo, error) {
	tokenMap := make(map[string]TokenInfo)

	if len(tokenIDs) == 0 {
		return tokenMap, nil
	}

	// Query Gamma API for each token individually with concurrent requests
	type result struct {
		tokenID string
		info    TokenInfo
		err     error
	}

	results := make(chan result, len(tokenIDs))
	sem := make(chan struct{}, 20) // Limit to 20 concurrent requests

	for _, tokenID := range tokenIDs {
		go func(tid string) {
			sem <- struct{}{}        // Acquire
			defer func() { <-sem }() // Release

			url := GammaMarketsURL + "?clob_token_ids=" + tid

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				results <- result{tokenID: tid, err: err}
				return
			}

			resp, err := c.httpClient.Do(req)
			if err != nil {
				results <- result{tokenID: tid, err: err}
				return
			}

			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				results <- result{tokenID: tid, err: err}
				return
			}

			var markets []GammaMarket
			if err := json.Unmarshal(body, &markets); err != nil {
				results <- result{tokenID: tid, err: err}
				return
			}

			if len(markets) == 0 {
				results <- result{tokenID: tid, err: fmt.Errorf("no market found")}
				return
			}

			market := markets[0]

			// Parse outcomes from JSON string
			var outcomes []string
			if err := json.Unmarshal([]byte(market.Outcomes), &outcomes); err != nil {
				outcomes = []string{"Yes", "No"} // Default
			}

			// Parse token IDs from JSON array string
			var marketTokens []string
			if err := json.Unmarshal([]byte(market.ClobTokenIds), &marketTokens); err != nil {
				// Fallback to comma-separated
				marketTokens = strings.Split(market.ClobTokenIds, ",")
			}

			// Find the outcome for this token
			outcome := ""
			for idx, mtid := range marketTokens {
				mtid = strings.TrimSpace(mtid)
				if mtid == tid && idx < len(outcomes) {
					outcome = outcomes[idx]
					break
				}
			}

			results <- result{
				tokenID: tid,
				info: TokenInfo{
					TokenID:     tid,
					ConditionID: market.ConditionID,
					Outcome:     outcome,
					Title:       market.Question,
					Slug:        market.Slug,
				},
			}
		}(tokenID)
	}

	// Collect results
	found := 0
	for i := 0; i < len(tokenIDs); i++ {
		r := <-results
		if r.err == nil {
			tokenMap[r.tokenID] = r.info
			found++
		}

		// Log progress every 100 tokens
		if (i+1)%100 == 0 || i+1 == len(tokenIDs) {
			log.Printf("[Subgraph] Processed %d/%d tokens, found %d", i+1, len(tokenIDs), found)
		}
	}

	log.Printf("[Subgraph] Built token map with %d/%d tokens", len(tokenMap), len(tokenIDs))
	return tokenMap, nil
}

// GetUniqueTokenIDs extracts unique token IDs from order filled events
func GetUniqueTokenIDs(events []OrderFilledEvent) []string {
	tokenSet := make(map[string]bool)

	for _, e := range events {
		// Add the outcome token ID (not USDC which is "0")
		if e.MakerAssetID != "0" {
			tokenSet[e.MakerAssetID] = true
		}
		if e.TakerAssetID != "0" {
			tokenSet[e.TakerAssetID] = true
		}
	}

	tokens := make([]string, 0, len(tokenSet))
	for id := range tokenSet {
		tokens = append(tokens, id)
	}

	return tokens
}

// RedemptionEvent represents a redemption from the subgraph
type RedemptionEvent struct {
	ID        string   `json:"id"`
	Payout    string   `json:"payout"`
	Redeemer  string   `json:"redeemer"`
	Timestamp string   `json:"timestamp"`
	Condition string   `json:"condition"` // conditionId - used to lookup market
	IndexSets []string `json:"indexSets"` // which outcomes were redeemed
}

// RedemptionResponse represents the GraphQL response for redemptions
type RedemptionResponse struct {
	Data struct {
		Redemptions []RedemptionEvent `json:"redemptions"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// GetGlobalTradesSince fetches ALL trades from the platform since a timestamp.
// This is used for global trade monitoring - no user filter.
func (c *SubgraphClient) GetGlobalTradesSince(ctx context.Context, afterTimestamp int64) ([]OrderFilledEvent, error) {
	var allEvents []OrderFilledEvent
	skip := 0

	for {
		query := fmt.Sprintf(`{
			orderFilledEvents(
				first: %d,
				skip: %d,
				where: {timestamp_gt: "%d"},
				orderBy: timestamp,
				orderDirection: asc
			) {
				id
				transactionHash
				timestamp
				orderHash
				maker
				taker
				makerAssetId
				takerAssetId
				makerAmountFilled
				takerAmountFilled
				fee
			}
		}`, SubgraphBatchSize, skip, afterTimestamp)

		events, err := c.executeQuery(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("global trades query failed at skip %d: %w", skip, err)
		}

		if len(events) == 0 {
			break
		}

		allEvents = append(allEvents, events...)

		if len(events) < SubgraphBatchSize {
			break
		}

		skip += SubgraphBatchSize

		// Safety limit to prevent infinite loops
		if skip > 100000 {
			log.Printf("[Subgraph] Warning: global trades fetch hit safety limit at %d events", len(allEvents))
			break
		}
	}

	return allEvents, nil
}

// GetGlobalRedemptionsSince fetches ALL redemptions from the platform since a timestamp.
func (c *SubgraphClient) GetGlobalRedemptionsSince(ctx context.Context, afterTimestamp int64) ([]RedemptionEvent, error) {
	var allRedemptions []RedemptionEvent
	skip := 0

	for {
		query := fmt.Sprintf(`{
			redemptions(
				first: %d,
				skip: %d,
				where: {timestamp_gt: "%d"},
				orderBy: timestamp,
				orderDirection: asc
			) {
				id
				payout
				redeemer
				timestamp
				condition
				indexSets
			}
		}`, SubgraphBatchSize, skip, afterTimestamp)

		redemptions, err := c.executeRedemptionQuery(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("global redemptions query failed at skip %d: %w", skip, err)
		}

		if len(redemptions) == 0 {
			break
		}

		allRedemptions = append(allRedemptions, redemptions...)

		if len(redemptions) < SubgraphBatchSize {
			break
		}

		skip += SubgraphBatchSize

		// Safety limit
		if skip > 100000 {
			log.Printf("[Subgraph] Warning: global redemptions fetch hit safety limit at %d events", len(allRedemptions))
			break
		}
	}

	return allRedemptions, nil
}

// executeRedemptionQuery sends a GraphQL query for redemptions
func (c *SubgraphClient) executeRedemptionQuery(ctx context.Context, query string) ([]RedemptionEvent, error) {
	reqBody := map[string]string{"query": query}
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal query: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("subgraph returned status %d: %s", resp.StatusCode, string(body))
	}

	var result RedemptionResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	if len(result.Errors) > 0 {
		return nil, fmt.Errorf("subgraph error: %s", result.Errors[0].Message)
	}

	return result.Data.Redemptions, nil
}

// FetchAllTokensFromCLOB fetches all tokens from CLOB API for database pre-population
// Returns a map of token ID -> TokenInfo
func (c *SubgraphClient) FetchAllTokensFromCLOB(ctx context.Context) (map[string]TokenInfo, error) {
	tokenMap := make(map[string]TokenInfo)
	cursor := ""
	totalMarkets := 0

	for {
		url := CLOBMarketsURL + "?limit=100"
		if cursor != "" {
			url += "&next_cursor=" + cursor
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetch markets: %w", err)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read response: %w", err)
		}

		var result CLOBMarketsResponse
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, fmt.Errorf("unmarshal markets: %w", err)
		}

		for _, market := range result.Data {
			for _, token := range market.Tokens {
				tokenMap[token.TokenID] = TokenInfo{
					TokenID:     token.TokenID,
					ConditionID: market.ConditionID,
					Outcome:     token.Outcome,
					Title:       market.Question,
					Slug:        market.Slug,
				}
			}
		}

		totalMarkets += len(result.Data)

		if result.NextCursor == "" {
			break
		}

		cursor = result.NextCursor

		// Log progress periodically
		if totalMarkets%10000 == 0 {
			log.Printf("[CLOB] Fetched %d markets, %d tokens", totalMarkets, len(tokenMap))
		}
	}

	log.Printf("[CLOB] Finished: %d markets, %d tokens total", totalMarkets, len(tokenMap))
	return tokenMap, nil
}
