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
	SubgraphURL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"

	// CLOB API for market/token mapping
	CLOBMarketsURL = "https://clob.polymarket.com/markets"

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
// Fetches both maker and taker trades to get complete trade history
func (c *SubgraphClient) GetAllUserTrades(ctx context.Context, userAddress string) ([]OrderFilledEvent, error) {
	userAddress = strings.ToLower(userAddress)

	// Fetch maker trades
	makerEvents, err := c.fetchTradesByRole(ctx, userAddress, "maker")
	if err != nil {
		return nil, fmt.Errorf("fetch maker trades: %w", err)
	}
	log.Printf("[Subgraph] Fetched %d maker trades", len(makerEvents))

	// Fetch taker trades
	takerEvents, err := c.fetchTradesByRole(ctx, userAddress, "taker")
	if err != nil {
		return nil, fmt.Errorf("fetch taker trades: %w", err)
	}
	log.Printf("[Subgraph] Fetched %d taker trades", len(takerEvents))

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
		Price:           Numeric(price),
		Timestamp:       timestamp,
		Title:           "",      // Would need market lookup
		Slug:            "",
		Icon:            "",
		EventSlug:       "",
		Outcome:         "",      // Would need market lookup
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
	}

	return trade
}

// BuildTokenMap fetches markets from CLOB API and builds a token ID -> market info map
func (c *SubgraphClient) BuildTokenMap(ctx context.Context, tokenIDs []string) (map[string]TokenInfo, error) {
	tokenMap := make(map[string]TokenInfo)
	tokenSet := make(map[string]bool)
	for _, id := range tokenIDs {
		tokenSet[id] = true
	}

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
				if tokenSet[token.TokenID] {
					tokenMap[token.TokenID] = TokenInfo{
						TokenID:     token.TokenID,
						ConditionID: market.ConditionID,
						Outcome:     token.Outcome,
						Title:       market.Question,
						Slug:        market.Slug,
					}
				}
			}
		}

		totalMarkets += len(result.Data)

		// Check if we've found all tokens or reached end of pagination
		if len(tokenMap) == len(tokenIDs) {
			log.Printf("[Subgraph] Found all %d tokens after %d markets", len(tokenMap), totalMarkets)
			break
		}

		if result.NextCursor == "" {
			log.Printf("[Subgraph] Reached end of markets (%d total), found %d/%d tokens", totalMarkets, len(tokenMap), len(tokenIDs))
			break
		}

		cursor = result.NextCursor

		// Log progress periodically
		if totalMarkets%1000 == 0 {
			log.Printf("[Subgraph] Processed %d markets, found %d/%d tokens", totalMarkets, len(tokenMap), len(tokenIDs))
		}
	}

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
