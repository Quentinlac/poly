// =============================================================================
// CLOB CLIENT - ORDER BOOK FETCHING & ORDER PLACEMENT
// =============================================================================
//
// This file handles all interactions with Polymarket's CLOB (Central Limit
// Order Book) API. It's responsible for:
//   - Fetching order books (liquidity available for buying/selling)
//   - Placing orders (FOK, GTC, market orders)
//   - Managing API credentials (L1/L2 authentication)
//   - Caching order books for faster execution
//
// ORDER BOOK FLOW:
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │  GetOrderBook(tokenID)                                                      │
// │     ↓                                                                       │
// │  HTTP GET https://clob.polymarket.com/book?token_id={tokenID}              │
// │     ↓                                                                       │
// │  Response: { bids: [[price, size]...], asks: [[price, size]...] }          │
// │     ↓                                                                       │
// │  Sort asks ascending (best/lowest price first)                             │
// │  Sort bids descending (best/highest price first)                           │
// └─────────────────────────────────────────────────────────────────────────────┘
//
// ORDER PLACEMENT FLOW:
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │  PlaceOrderFast(tokenID, side, size, price, negRisk)                       │
// │     ↓                                                                       │
// │  1. createSignedOrder() - Build Order struct with amounts                  │
// │     - BUY: makerAmount=USDC, takerAmount=tokens                            │
// │     - SELL: makerAmount=tokens, takerAmount=USDC                           │
// │     ↓                                                                       │
// │  2. signOrder() - EIP-712 typed data signature                             │
// │     - Domain: "Polymarket CTF Exchange" on chain 137 (Polygon)             │
// │     - Contract: CTFExchange or NegRiskCTFExchange based on market          │
// │     ↓                                                                       │
// │  3. postOrder() - HTTP POST to /order endpoint                             │
// │     - Add L2 auth headers (POLY_API_KEY, POLY_SIGNATURE, etc.)            │
// │     - Try FOK first (immediate execution or fail)                          │
// │     - Fall back to GTC (stays in book until filled/cancelled)             │
// │     ↓                                                                       │
// │  4. Response: { success, orderId, status }                                 │
// │     - status: "matched" (filled), "live" (in book), "delayed", "unmatched"│
// └─────────────────────────────────────────────────────────────────────────────┘
//
// KEY CONCEPTS:
// - Token amounts: 6 decimal places (1.00 token = 1000000)
// - USDC amounts: 6 decimal places (1.00 USDC = 1000000)
// - Tick size: 0.01 (prices like 0.50, 0.51, 0.52...)
// - FOK (Fill-Or-Kill): Execute immediately at specified price or fail
// - GTC (Good-Till-Cancel): Stay in order book until filled or cancelled
// - negRisk: Markets where "No" outcome uses NegRiskCTFExchange contract
//
// =============================================================================
package api

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
)

// ClobClient handles CLOB API interactions for trading
type ClobClient struct {
	baseURL       string
	httpClient    *http.Client
	auth          *Auth
	apiCreds      *APICreds
	chainID       int64
	funder        common.Address
	signatureType int // 0=EOA, 1=Magic/Email, 2=Browser proxy
}

// APICreds holds API credentials for CLOB
type APICreds struct {
	APIKey        string `json:"apiKey"`
	APISecret     string `json:"secret"`
	APIPassphrase string `json:"passphrase"`
}

// OrderBook represents the order book for a token
type OrderBook struct {
	Market    string           `json:"market"`
	AssetID   string           `json:"asset_id"`
	Hash      string           `json:"hash"`
	Timestamp string           `json:"timestamp"`
	Bids      []OrderBookLevel `json:"bids"`
	Asks      []OrderBookLevel `json:"asks"`
}

// OrderBookLevel represents a single price level
type OrderBookLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

// MarketInfo represents market information from CLOB
type MarketInfo struct {
	ConditionID           string          `json:"condition_id"`
	QuestionID            string          `json:"question_id"`
	Tokens                []ClobTokenInfo `json:"tokens"`
	MinimumOrderSize      float64         `json:"minimum_order_size"`
	MinimumTickSize       float64         `json:"minimum_tick_size"`
	Description           string          `json:"description"`
	Category              string          `json:"category"`
	EndDateISO            string          `json:"end_date_iso"`
	GameStartTime         string          `json:"game_start_time"`
	QuestionHasScreenshot bool            `json:"question_has_screenshot"`
	Active                bool            `json:"active"`
	Closed                bool            `json:"closed"`
	MarketSlug            string          `json:"market_slug"`
	Icon                  string          `json:"icon"`
	Fpmm                  string          `json:"fpmm"`
	NegRisk               bool            `json:"neg_risk"`
}

// ClobTokenInfo represents token information from CLOB
type ClobTokenInfo struct {
	TokenID string  `json:"token_id"`
	Outcome string  `json:"outcome"`
	Price   float64 `json:"price"`
	Winner  bool    `json:"winner"`
}

// OrderType represents the type of order
type OrderType string

const (
	OrderTypeFOK OrderType = "FOK" // Fill-Or-Kill: must fill entirely or cancel
	OrderTypeFAK OrderType = "FAK" // Fill-And-Kill: fill available, cancel rest (best for copy trading)
	OrderTypeGTC OrderType = "GTC" // Good-Til-Cancelled (limit order, can become maker)
	OrderTypeGTD OrderType = "GTD" // Good-Til-Date
)

// Side represents buy or sell
type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

// Order represents a signed order
type Order struct {
	Salt          int64  `json:"salt"`
	Maker         string `json:"maker"`
	Signer        string `json:"signer"`
	Taker         string `json:"taker"`
	TokenID       string `json:"tokenId"`
	MakerAmount   string `json:"makerAmount"`
	TakerAmount   string `json:"takerAmount"`
	Expiration    string `json:"expiration"`
	Nonce         string `json:"nonce"`
	FeeRateBps    string `json:"feeRateBps"`
	Side          string `json:"side"`
	SignatureType int    `json:"signatureType"`
	Signature     string `json:"signature"`
	SideInt       int    `json:"-"` // Internal use for EIP-712 signing
}

// OrderRequest is the payload for placing an order
type OrderRequest struct {
	Order     Order     `json:"order"`
	Owner     string    `json:"owner"`
	OrderType OrderType `json:"orderType"`
}

// OrderResponse is the response from placing an order
type OrderResponse struct {
	Success     bool     `json:"success"`
	ErrorMsg    string   `json:"errorMsg"`
	OrderID     string   `json:"orderId"`
	OrderHashes []string `json:"orderHashes"`
	Status      string   `json:"status"` // matched, live, delayed, unmatched
}

// NewClobClient creates a new CLOB API client
func NewClobClient(baseURL string, auth *Auth) (*ClobClient, error) {
	if baseURL == "" {
		baseURL = "https://clob.polymarket.com"
	}

	// Optimized transport with connection pooling for low latency
	// Default MaxIdleConnsPerHost is only 2, which causes connection churn
	// By keeping connections alive, we skip DNS + TCP + TLS handshakes (~200-300ms)
	transport := &http.Transport{
		MaxIdleConns:        100,              // Total idle connections across all hosts
		MaxIdleConnsPerHost: 20,               // Keep 20 connections to clob.polymarket.com
		MaxConnsPerHost:     50,               // Max concurrent connections per host
		IdleConnTimeout:     90 * time.Second, // Keep idle connections for 90s
		DisableKeepAlives:   false,            // Explicitly enable keep-alives
		ForceAttemptHTTP2:   true,             // Try HTTP/2 for multiplexing
	}

	client := &ClobClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
		auth:          auth,
		chainID:       137, // Polygon mainnet
		funder:        auth.GetAddress(),
		signatureType: 0, // Default to EOA
	}

	return client, nil
}

// SetFunder sets the funder address for Magic/Email wallets
// The funder is the Polymarket profile address where USDC is held
func (c *ClobClient) SetFunder(funderAddress string) {
	c.funder = common.HexToAddress(funderAddress)
}

// SetSignatureType sets the signature type (0=EOA, 1=Magic/Email, 2=Browser proxy)
func (c *ClobClient) SetSignatureType(sigType int) {
	c.signatureType = sigType
}

// WarmConnection pre-establishes TCP+TLS connections to the CLOB API
// Call this at startup to avoid cold-start latency on first order (~200-300ms savings)
func (c *ClobClient) WarmConnection(ctx context.Context) error {
	start := time.Now()

	// Make a lightweight request to establish connection pool
	// The /time endpoint is simple and fast
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/time", nil)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Printf("[CLOB] WarmConnection failed: %v", err)
		return err
	}
	defer resp.Body.Close()

	// Must read body completely to allow connection reuse
	io.Copy(io.Discard, resp.Body)

	log.Printf("[CLOB] ✅ Connection pool warmed in %dms", time.Since(start).Milliseconds())
	return nil
}

// DeriveAPICreds derives or creates API credentials
func (c *ClobClient) DeriveAPICreds(ctx context.Context) (*APICreds, error) {
	// First try to delete any existing credentials
	c.deleteAPICreds(ctx)

	// Try to create new credentials
	creds, err := c.createAPICreds(ctx)
	if err == nil && creds != nil {
		c.apiCreds = creds
		log.Printf("[CLOB] Created new API credentials")
		return creds, nil
	}

	// If that fails, try to derive existing credentials
	log.Printf("[CLOB] Creating creds failed (%v), trying to derive existing", err)
	creds, err = c.deriveAPICreds(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to derive API creds: %w", err)
	}

	c.apiCreds = creds
	return creds, nil
}

func (c *ClobClient) deleteAPICreds(ctx context.Context) {
	// Get L1 authentication headers
	headers, err := c.auth.SignRequest()
	if err != nil {
		return
	}

	req, err := http.NewRequestWithContext(ctx, "DELETE", c.baseURL+"/auth/api-key", nil)
	if err != nil {
		return
	}

	// Set L1 headers
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		log.Printf("[CLOB] Deleted existing API credentials")
	}
}

func (c *ClobClient) deriveAPICreds(ctx context.Context) (*APICreds, error) {
	// Get L1 authentication headers
	headers, err := c.auth.SignRequest()
	if err != nil {
		return nil, fmt.Errorf("failed to sign request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/auth/derive-api-key", nil)
	if err != nil {
		return nil, err
	}

	// Set L1 headers
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("derive API creds failed: %d %s", resp.StatusCode, string(respBody))
	}

	var creds APICreds
	if err := json.NewDecoder(resp.Body).Decode(&creds); err != nil {
		return nil, fmt.Errorf("failed to decode API creds: %w", err)
	}

	return &creds, nil
}

func (c *ClobClient) createAPICreds(ctx context.Context) (*APICreds, error) {
	// Get L1 authentication headers
	headers, err := c.auth.SignRequest()
	if err != nil {
		return nil, fmt.Errorf("failed to sign request: %w", err)
	}

	// Create with a nonce to generate unique API key
	nonce := time.Now().UnixNano()
	body := fmt.Sprintf(`{"nonce":%d}`, nonce)

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/auth/api-key", bytes.NewBufferString(body))
	if err != nil {
		return nil, err
	}

	// Set L1 headers
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("create API creds failed: %d %s", resp.StatusCode, string(respBody))
	}

	var creds APICreds
	if err := json.NewDecoder(resp.Body).Decode(&creds); err != nil {
		return nil, fmt.Errorf("failed to decode API creds: %w", err)
	}

	return &creds, nil
}

// =============================================================================
// GetOrderBook - FETCH CURRENT LIQUIDITY FOR A MARKET
// =============================================================================
//
// Retrieves the order book from Polymarket's CLOB API.
//
// INPUT: tokenID - The market token ID (decimal string, e.g., "123456789...")
//
// OUTPUT: OrderBook containing:
//   - Bids: Price levels where people want to BUY (sorted highest first)
//   - Asks: Price levels where people want to SELL (sorted lowest first)
//
// USAGE FOR COPY TRADING:
//   - When copying a BUY: Look at Asks (we need to buy from sellers)
//   - When copying a SELL: Look at Bids (we need to sell to buyers)
//
// EXAMPLE ORDER BOOK:
//   Bids (buyers):           Asks (sellers):
//   $0.48 - 500 shares       $0.52 - 200 shares  ← Best ask (buy here first)
//   $0.47 - 300 shares       $0.53 - 150 shares
//   $0.45 - 1000 shares      $0.55 - 500 shares
//
// =============================================================================
func (c *ClobClient) GetOrderBook(ctx context.Context, tokenID string) (*OrderBook, error) {
	values := url.Values{}
	values.Set("token_id", tokenID)

	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/book?"+values.Encode(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get order book failed: %d %s", resp.StatusCode, string(body))
	}

	var book OrderBook
	if err := json.NewDecoder(resp.Body).Decode(&book); err != nil {
		return nil, fmt.Errorf("failed to decode order book: %w", err)
	}

	// Sort asks ascending (lowest/best price first) - we want to buy at lowest prices
	sort.Slice(book.Asks, func(i, j int) bool {
		priceI, _ := strconv.ParseFloat(book.Asks[i].Price, 64)
		priceJ, _ := strconv.ParseFloat(book.Asks[j].Price, 64)
		return priceI < priceJ
	})

	// Sort bids descending (highest/best price first) - we want to sell at highest prices
	sort.Slice(book.Bids, func(i, j int) bool {
		priceI, _ := strconv.ParseFloat(book.Bids[i].Price, 64)
		priceJ, _ := strconv.ParseFloat(book.Bids[j].Price, 64)
		return priceI > priceJ
	})

	// Log best 2 asks and bids
	var askStr, bidStr string
	for i := 0; i < 2 && i < len(book.Asks); i++ {
		if i > 0 {
			askStr += ", "
		}
		askStr += fmt.Sprintf("%s@$%s", book.Asks[i].Size, book.Asks[i].Price)
	}
	for i := 0; i < 2 && i < len(book.Bids); i++ {
		if i > 0 {
			bidStr += ", "
		}
		bidStr += fmt.Sprintf("%s@$%s", book.Bids[i].Size, book.Bids[i].Price)
	}
	log.Printf("[OrderBook] asks(%d): %s | bids(%d): %s", len(book.Asks), askStr, len(book.Bids), bidStr)

	return &book, nil
}

// GetMarket fetches market information
func (c *ClobClient) GetMarket(ctx context.Context, conditionID string) (*MarketInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/markets/"+conditionID, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get market failed: %d %s", resp.StatusCode, string(body))
	}

	var market MarketInfo
	if err := json.NewDecoder(resp.Body).Decode(&market); err != nil {
		return nil, fmt.Errorf("failed to decode market: %w", err)
	}

	return &market, nil
}

// GetMarketBySlug fetches market info from Gamma API using the market slug
// This is used for BTC 15m candle markets where we need to look up by slug pattern
// It first gets the conditionId from Gamma, then fetches full market info (with tokens) from CLOB
func (c *ClobClient) GetMarketBySlug(ctx context.Context, slug string) (*MarketInfo, error) {
	// Step 1: Get conditionId from Gamma API
	url := "https://gamma-api.polymarket.com/markets?slug=" + slug

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("gamma API failed: %d %s", resp.StatusCode, string(body))
	}

	// Gamma API returns an array of markets
	var markets []struct {
		ConditionID string `json:"conditionId"`
		Description string `json:"question"`
		Slug        string `json:"slug"`
		NegRisk     bool   `json:"negRisk"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&markets); err != nil {
		return nil, fmt.Errorf("failed to decode gamma response: %w", err)
	}

	if len(markets) == 0 {
		return nil, fmt.Errorf("no market found for slug %s", slug)
	}

	conditionID := markets[0].ConditionID
	if conditionID == "" {
		return nil, fmt.Errorf("empty conditionId for slug %s", slug)
	}

	// Step 2: Get full market info (with tokens) from CLOB API
	return c.GetMarket(ctx, conditionID)
}

// BalanceAllowance represents the balance and allowance for an account
type BalanceAllowance struct {
	Balance   string `json:"balance"`
	Allowance string `json:"allowance"`
}

// AssetType represents the type of asset
type AssetType string

const (
	AssetTypeCollateral  AssetType = "COLLATERAL"  // USDC
	AssetTypeConditional AssetType = "CONDITIONAL" // Outcome tokens
)

// GetBalanceAllowance fetches the balance and allowance for the authenticated user
// assetType: COLLATERAL (USDC) or CONDITIONAL (outcome tokens)
// tokenID: optional, required for CONDITIONAL asset type
func (c *ClobClient) GetBalanceAllowance(ctx context.Context, assetType AssetType, tokenID string) (*BalanceAllowance, error) {
	endpoint := c.baseURL + "/balance-allowance"

	// Build query params
	params := url.Values{}
	params.Set("asset_type", string(assetType))
	if tokenID != "" {
		params.Set("token_id", tokenID)
	}
	params.Set("signature_type", strconv.Itoa(c.signatureType))

	fullURL := endpoint + "?" + params.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return nil, err
	}

	// Add browser-like headers
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Origin", "https://polymarket.com")
	req.Header.Set("Referer", "https://polymarket.com/")

	// Add L2 authentication headers
	c.addL2Headers(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get balance allowance failed: %d %s", resp.StatusCode, string(body))
	}

	var result BalanceAllowance
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to decode balance allowance: %w", err)
	}

	return &result, nil
}

// GetUSDCBalance returns the USDC balance in human-readable format (float64)
func (c *ClobClient) GetUSDCBalance(ctx context.Context) (float64, error) {
	ba, err := c.GetBalanceAllowance(ctx, AssetTypeCollateral, "")
	if err != nil {
		return 0, err
	}

	// Balance is in 6-decimal USDC format (e.g., "1000000" = $1.00)
	balanceInt, err := strconv.ParseInt(ba.Balance, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse balance: %w", err)
	}

	return float64(balanceInt) / 1e6, nil
}

// USDC contract address on Polygon
const USDCContractPolygon = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

// GetOnChainUSDCBalance queries the USDC balance directly from Polygon blockchain
// This doesn't require authentication - can query any address
func GetOnChainUSDCBalance(ctx context.Context, walletAddress string) (float64, error) {
	// Normalize address
	walletAddress = strings.ToLower(strings.TrimSpace(walletAddress))
	if !strings.HasPrefix(walletAddress, "0x") {
		walletAddress = "0x" + walletAddress
	}

	// Pad address to 32 bytes for balanceOf(address) call
	// Remove 0x prefix and pad to 64 chars (32 bytes)
	paddedAddr := strings.TrimPrefix(walletAddress, "0x")
	paddedAddr = fmt.Sprintf("%064s", paddedAddr)

	// balanceOf(address) function selector: 0x70a08231
	data := "0x70a08231" + paddedAddr

	// JSON-RPC request
	reqBody := fmt.Sprintf(`{
		"jsonrpc": "2.0",
		"method": "eth_call",
		"params": [{
			"to": "%s",
			"data": "%s"
		}, "latest"],
		"id": 1
	}`, USDCContractPolygon, data)

	req, err := http.NewRequestWithContext(ctx, "POST", "https://polygon-rpc.com", strings.NewReader(reqBody))
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("rpc request: %w", err)
	}
	defer resp.Body.Close()

	var rpcResp struct {
		Result string `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return 0, fmt.Errorf("decode response: %w", err)
	}

	if rpcResp.Error != nil {
		return 0, fmt.Errorf("rpc error: %s", rpcResp.Error.Message)
	}

	// Parse hex result to big.Int
	result := strings.TrimPrefix(rpcResp.Result, "0x")
	if result == "" || result == "0" {
		return 0, nil
	}

	balance := new(big.Int)
	balance.SetString(result, 16)

	// USDC has 6 decimals
	balanceFloat := new(big.Float).SetInt(balance)
	divisor := new(big.Float).SetFloat64(1e6)
	balanceFloat.Quo(balanceFloat, divisor)

	result64, _ := balanceFloat.Float64()
	return result64, nil
}

// GammaTokenInfo holds the parsed token info from Gamma API
type GammaTokenInfo struct {
	TokenID     string
	ConditionID string
	Outcome     string
	Title       string
	Slug        string
	NegRisk     bool
}

// GetTokenInfoByID fetches token information from Gamma API by token ID
// This is used as a fallback when the token is not in our local cache
func (c *ClobClient) GetTokenInfoByID(ctx context.Context, tokenID string) (*GammaTokenInfo, error) {
	url := "https://gamma-api.polymarket.com/markets?clob_token_ids=" + tokenID

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("gamma API error %d: %s", resp.StatusCode, string(body))
	}

	var markets []GammaMarket
	if err := json.NewDecoder(resp.Body).Decode(&markets); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if len(markets) == 0 {
		return nil, fmt.Errorf("no market found for token %s", tokenID)
	}

	market := markets[0]

	// Parse outcomes from JSON string (e.g., "[\"Yes\",\"No\"]")
	var outcomes []string
	if err := json.Unmarshal([]byte(market.Outcomes), &outcomes); err != nil {
		outcomes = []string{"Yes", "No"} // Default to binary market
	}

	// Parse token IDs from JSON array string or comma-separated
	var marketTokens []string
	if err := json.Unmarshal([]byte(market.ClobTokenIds), &marketTokens); err != nil {
		// Fallback to comma-separated
		marketTokens = strings.Split(market.ClobTokenIds, ",")
	}

	// Find the outcome for this token
	outcome := ""
	for idx, mtid := range marketTokens {
		mtid = strings.TrimSpace(mtid)
		if mtid == tokenID && idx < len(outcomes) {
			outcome = outcomes[idx]
			break
		}
	}

	// Check if neg_risk by looking up the CLOB market API
	negRisk := false
	if market.ConditionID != "" {
		marketInfo, err := c.GetMarket(ctx, market.ConditionID)
		if err == nil && marketInfo != nil {
			negRisk = marketInfo.NegRisk
		}
	}

	log.Printf("[CLOB] GetTokenInfoByID: tokenID=%s, conditionID=%s, outcome=%s, title=%s, negRisk=%v",
		tokenID, market.ConditionID, outcome, market.Question, negRisk)

	return &GammaTokenInfo{
		TokenID:     tokenID,
		ConditionID: market.ConditionID,
		Outcome:     outcome,
		Title:       market.Question,
		Slug:        market.Slug,
		NegRisk:     negRisk,
	}, nil
}

// GetCLOBTrades fetches trades from the CLOB /data/trades endpoint.
// This endpoint has ~50ms latency (vs 30-80s for Data API) because it reflects
// trades as soon as they're matched off-chain, before blockchain settlement.
// Requires L2 authentication.
func (c *ClobClient) GetCLOBTrades(ctx context.Context, params CLOBTradeParams) ([]CLOBTrade, error) {
	if c.apiCreds == nil {
		if _, err := c.DeriveAPICreds(ctx); err != nil {
			return nil, fmt.Errorf("failed to get API creds: %w", err)
		}
	}

	values := url.Values{}
	if params.Maker != "" {
		values.Set("maker", params.Maker)
	}
	if params.Taker != "" {
		values.Set("taker", params.Taker)
	}
	if params.Market != "" {
		values.Set("market", params.Market)
	}
	if params.AssetID != "" {
		values.Set("asset_id", params.AssetID)
	}
	if params.After > 0 {
		values.Set("after", strconv.FormatInt(params.After, 10))
	}
	if params.Before > 0 {
		values.Set("before", strconv.FormatInt(params.Before, 10))
	}
	if params.ID != "" {
		values.Set("id", params.ID)
	}

	endpoint := c.baseURL + "/data/trades"
	if len(values) > 0 {
		endpoint += "?" + values.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Add L2 authentication headers
	c.addL2Headers(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("CLOB trades API error %d: %s", resp.StatusCode, string(body))
	}

	var trades []CLOBTrade
	if err := json.NewDecoder(resp.Body).Decode(&trades); err != nil {
		return nil, fmt.Errorf("decode trades: %w", err)
	}

	return trades, nil
}

// PlaceMarketOrder places a market order (FOK - Fill-Or-Kill)
func (c *ClobClient) PlaceMarketOrder(ctx context.Context, tokenID string, side Side, amountUSDC float64, negRisk bool) (*OrderResponse, error) {
	if c.apiCreds == nil {
		if _, err := c.DeriveAPICreds(ctx); err != nil {
			return nil, fmt.Errorf("failed to get API creds: %w", err)
		}
	}

	// Get order book to calculate optimal price
	book, err := c.GetOrderBook(ctx, tokenID)
	if err != nil {
		return nil, fmt.Errorf("failed to get order book: %w", err)
	}

	// Calculate the average price we'll get based on order book depth
	var levels []OrderBookLevel
	if side == SideBuy {
		levels = book.Asks // Buy from asks
	} else {
		levels = book.Bids // Sell to bids
	}

	if len(levels) == 0 {
		return nil, fmt.Errorf("no liquidity in order book for %s side", side)
	}

	// Calculate how much we can buy/sell and at what average price
	remainingUSDC := amountUSDC
	totalSize := 0.0
	totalCost := 0.0

	for _, level := range levels {
		price, _ := strconv.ParseFloat(level.Price, 64)
		size, _ := strconv.ParseFloat(level.Size, 64)

		levelValue := size * price
		if levelValue <= remainingUSDC {
			totalSize += size
			totalCost += levelValue
			remainingUSDC -= levelValue
		} else {
			// Partial fill at this level
			fillSize := remainingUSDC / price
			totalSize += fillSize
			totalCost += remainingUSDC
			remainingUSDC = 0
			break
		}

		if remainingUSDC <= 0 {
			break
		}
	}

	if totalSize == 0 {
		return nil, fmt.Errorf("cannot fill order: insufficient liquidity")
	}

	avgPrice := totalCost / totalSize

	log.Printf("[CLOB] Market order: %s %.4f USDC worth of tokens at avg price %.4f (size: %.4f)",
		side, amountUSDC-remainingUSDC, avgPrice, totalSize)

	// Create and sign the order
	order, err := c.createSignedOrder(tokenID, side, totalSize, avgPrice, negRisk)
	if err != nil {
		return nil, fmt.Errorf("failed to create signed order: %w", err)
	}

	// Place the order (using GTC for better compatibility)
	return c.postOrder(ctx, order, OrderTypeGTC)
}

// PlaceLimitOrder places a limit order (GTC - Good-Til-Cancelled)
func (c *ClobClient) PlaceLimitOrder(ctx context.Context, tokenID string, side Side, size float64, price float64, negRisk bool) (*OrderResponse, error) {
	if c.apiCreds == nil {
		if _, err := c.DeriveAPICreds(ctx); err != nil {
			return nil, fmt.Errorf("failed to get API creds: %w", err)
		}
	}

	order, err := c.createSignedOrder(tokenID, side, size, price, negRisk)
	if err != nil {
		return nil, fmt.Errorf("failed to create signed order: %w", err)
	}

	return c.postOrder(ctx, order, OrderTypeGTC)
}

// PlaceOrderFOK places a Fill-Or-Kill order for immediate execution
// This is faster than PlaceMarketOrder because it skips order book fetch
func (c *ClobClient) PlaceOrderFOK(ctx context.Context, tokenID string, side Side, size float64, price float64, negRisk bool) (*OrderResponse, error) {
	if c.apiCreds == nil {
		if _, err := c.DeriveAPICreds(ctx); err != nil {
			return nil, fmt.Errorf("failed to get API creds: %w", err)
		}
	}

	order, err := c.createSignedOrder(tokenID, side, size, price, negRisk)
	if err != nil {
		return nil, fmt.Errorf("failed to create signed order: %w", err)
	}

	return c.postOrder(ctx, order, OrderTypeFOK)
}

// =============================================================================
// PlaceOrderFast - PRIMARY ORDER PLACEMENT (FOK → GTC FALLBACK)
// =============================================================================
//
// This is the main function used by CopyTrader to place orders. It tries two
// strategies to maximize fill rate while maintaining speed:
//
// STRATEGY 1: FOK (Fill-Or-Kill) - Immediate execution only
//   - Executes immediately if liquidity available at price
//   - Fails completely if can't fill (no partial fills)
//   - Requires stricter precision (2 decimal USDC, 4 decimal tokens)
//   - Fastest when it works
//
// STRATEGY 2: GTC (Good-Till-Cancel) - Falls back if FOK fails
//   - Stays in order book until filled
//   - More flexible precision requirements
//   - Better fill rate for larger orders
//
// INPUT PARAMETERS:
//   - tokenID: Market token ID (decimal string)
//   - side: SideBuy or SideSell
//   - size: Number of shares to buy/sell
//   - price: Price per share (e.g., 0.55 for 55 cents)
//   - negRisk: true if this is a negative-risk market
//
// PRECISION REQUIREMENTS (enforced internally):
//   - Price: Rounded to tick size (0.01)
//   - Size: Rounded to 2 or 4 decimal places
//   - Minimum: $1 USDC for marketable orders
//   - Minimum: 0.1 tokens
//
// OUTPUT: OrderResponse containing:
//   - Success: true if order accepted
//   - OrderID: Unique identifier for tracking
//   - Status: "matched", "live", "delayed", or "unmatched"
//
// =============================================================================
func (c *ClobClient) PlaceOrderFast(ctx context.Context, tokenID string, side Side, size float64, price float64, negRisk bool) (*OrderResponse, error) {
	totalStart := time.Now()

	if c.apiCreds == nil {
		credsStart := time.Now()
		if _, err := c.DeriveAPICreds(ctx); err != nil {
			return nil, fmt.Errorf("failed to get API creds: %w", err)
		}
		log.Printf("[PlaceOrderFast] DeriveAPICreds: %dms", time.Since(credsStart).Milliseconds())
	}

	// Use GTC directly - more reliable than FOK
	// FOK has stricter validation (5 share minimum on some markets) and same speed
	// GTC will still match immediately if there's liquidity at our price
	signStart := time.Now()
	order, err := c.createSignedOrder(tokenID, side, size, price, negRisk)
	signMs := time.Since(signStart).Milliseconds()
	if err != nil {
		return nil, fmt.Errorf("failed to create order: %w", err)
	}

	postStart := time.Now()
	resp, err := c.postOrder(ctx, order, OrderTypeGTC)
	postMs := time.Since(postStart).Milliseconds()

	// Log order details and timing
	status := "ERROR"
	if resp != nil {
		status = resp.Status
	}
	log.Printf("[PlaceOrderFast] %s %.4f @ $%.4f | sign=%dms post=%dms total=%dms | status=%s",
		side, size, price, signMs, postMs, time.Since(totalStart).Milliseconds(), status)

	return resp, err
}

// PlaceOrderFAK places a Fill-And-Kill order - BEST FOR COPY TRADING
// =============================================================================
// FAK = Fill whatever liquidity is available immediately, cancel the rest
// - Always a TAKER (never becomes a maker sitting in the book)
// - Partial fills are OK (unlike FOK which requires full fill)
// - Uses strict 2-decimal precision for maker amount (required by API)
//
// PRECISION REQUIREMENTS (same as FOK):
// - Price: 2 decimals (tick size 0.01)
// - Size: 4 decimals
// - Maker amount (USDC for buy, tokens for sell): 2 decimals
// - Taker amount: 4 decimals
// =============================================================================
func (c *ClobClient) PlaceOrderFAK(ctx context.Context, tokenID string, side Side, size float64, price float64, negRisk bool) (*OrderResponse, error) {
	// Skip API creds check if already set (saves ~0ms but cleaner code path)
	if c.apiCreds == nil {
		if _, err := c.DeriveAPICreds(ctx); err != nil {
			return nil, fmt.Errorf("failed to get API creds: %w", err)
		}
	}

	// Use FOK-compatible precision (2 decimal maker amount)
	// FAK has same precision requirements as FOK
	order, err := c.createSignedOrderFOK(tokenID, side, size, price, negRisk)
	if err != nil {
		return nil, fmt.Errorf("failed to create order: %w", err)
	}

	return c.postOrder(ctx, order, OrderTypeFAK)
}

// createSignedOrderFOK creates an order with FOK-compatible precision
// FOK requires: maker amount (USDC for buy) = 2 decimals, taker amount (tokens) = 4 decimals
func (c *ClobClient) createSignedOrderFOK(tokenID string, side Side, size float64, price float64, negRisk bool) (*Order, error) {
	// Round price to tick size (0.01 for most markets)
	tickSize := 0.01
	price = float64(int(price/tickSize+0.5)) * tickSize

	// For FOK: size can have up to 4 decimals
	size = float64(int(size*10000+0.5)) / 10000

	// Polymarket requires minimum token sizes (0.1 tokens based on testing)
	const minTokenSize = 0.1
	if size < minTokenSize {
		size = minTokenSize
	}

	// Calculate USDC value and round to 2 decimals (FOK requirement for buy orders)
	usdcValue := size * price
	usdcValue = float64(int(usdcValue*100+0.5)) / 100

	// Enforce $1 minimum for buy orders
	const minOrderUSDC = 1.0
	if side == SideBuy && usdcValue < minOrderUSDC && price > 0 {
		usdcValue = minOrderUSDC
		// Recalculate size from the rounded USDC value
		size = usdcValue / price
		size = float64(int(size*10000+0.5)) / 10000
	}

	// Convert to 6-decimal format
	// Tokens: size * 10^6, but since size is in 4 decimals, we do size * 10000 * 100
	sizeIn6Dec := int64(size*10000+0.5) * 100
	sizeInt := big.NewInt(sizeIn6Dec)

	// USDC: already 2 decimals, convert to 6 decimal (multiply by 10000)
	usdcIn6Dec := int64(usdcValue*100+0.5) * 10000
	usdcInt := big.NewInt(usdcIn6Dec)

	var makerAmount, takerAmount *big.Int
	sideInt := 0
	sideStr := "BUY"

	if side == SideBuy {
		makerAmount = usdcInt
		takerAmount = sizeInt
		sideInt = 0
		sideStr = "BUY"
	} else {
		makerAmount = sizeInt
		takerAmount = usdcInt
		sideInt = 1
		sideStr = "SELL"
	}

	// Create order with no expiration (like GTC)
	salt := generateSalt()
	expiration := int64(0) // No expiration for GTC-like behavior

	// For Magic wallets: maker = funder (where funds are), signer = private key wallet
	// For EOA wallets: maker = signer = wallet address
	makerAddress := c.funder.Hex()
	signerAddress := c.auth.GetAddress().Hex()

	order := &Order{
		Salt:          salt,
		Maker:         makerAddress,
		Signer:        signerAddress,
		Taker:         "0x0000000000000000000000000000000000000000",
		TokenID:       tokenID,
		MakerAmount:   makerAmount.String(),
		TakerAmount:   takerAmount.String(),
		Expiration:    strconv.FormatInt(expiration, 10),
		Nonce:         "0",
		FeeRateBps:    "0",
		Side:          sideStr,
		SignatureType: c.signatureType,
		SideInt:       sideInt,
	}

	// Sign the order using EIP-712
	sig, err := c.signOrder(order, negRisk)
	if err != nil {
		return nil, fmt.Errorf("failed to sign order: %w", err)
	}
	order.Signature = sig

	return order, nil
}

// =============================================================================
// createSignedOrder - BUILD AND SIGN AN ORDER FOR SUBMISSION
// =============================================================================
//
// Creates a signed order ready for submission to the CLOB API.
//
// ORDER STRUCTURE (Polymarket EIP-712 typed data):
//   - salt: Random nonce for uniqueness
//   - maker: Address that provides liquidity (funder for Magic wallets)
//   - signer: Address that signs the order (EOA private key)
//   - taker: Who can fill (0x0 = anyone)
//   - tokenId: Market token ID
//   - makerAmount: What maker gives (USDC for buy, tokens for sell)
//   - takerAmount: What maker receives (tokens for buy, USDC for sell)
//   - expiration: 0 for GTC (no expiration)
//   - nonce, feeRateBps: Usually 0
//   - side: 0=BUY, 1=SELL
//   - signatureType: 0=EOA, 1=Magic/Email wallet
//
// AMOUNT CALCULATION:
//   BUY ORDER: "I want to pay X USDC to get Y tokens"
//     makerAmount = USDC (what I give) = size × price × 10^6
//     takerAmount = tokens (what I get) = size × 10^6
//
//   SELL ORDER: "I want to sell Y tokens to get X USDC"
//     makerAmount = tokens (what I give) = size × 10^6
//     takerAmount = USDC (what I get) = size × price × 10^6
//
// PRECISION HANDLING:
//   - Price rounded to 0.01 (tick size)
//   - Size rounded to 0.01 (2 decimals)
//   - USDC: 4 decimal precision in 6-decimal format
//   - Tokens: 2 decimal precision in 6-decimal format
//
// =============================================================================
func (c *ClobClient) createSignedOrder(tokenID string, side Side, size float64, price float64, negRisk bool) (*Order, error) {
	// Round price to tick size (0.01 for most markets)
	tickSize := 0.01
	price = float64(int(price/tickSize+0.5)) * tickSize

	// Round size to 2 decimal places
	size = float64(int(size*100+0.5)) / 100

	// Convert to base units
	// USDC: 6 decimals
	// Outcome tokens: 6 decimals (same as USDC in Polymarket)
	// MakerAmount: what we're giving (USDC for buy, tokens for sell)
	// TakerAmount: what we're getting (tokens for buy, USDC for sell)

	var makerAmount, takerAmount *big.Int
	sideInt := 0     // 0 = BUY, 1 = SELL (for EIP-712)
	sideStr := "BUY" // String for JSON payload

	// Polymarket precision requirements:
	// - Token amounts: max 2 decimal places (0.01 precision) = divisible by 10000 in 6-decimal format
	// - USDC amounts: max 4 decimal places (0.0001 precision) = divisible by 100 in 6-decimal format

	// Calculate in 6-decimal format (native Polymarket precision)
	// Use math.Round to avoid floating point truncation errors

	// Size: round to 2 decimals, then convert to 6-decimal format
	// Example: 1.56 -> 1560000 (divisible by 10000 ✓)
	// Use +0.5 and truncate for rounding (equivalent to math.Round)
	sizeIn6Dec := int64(size*100+0.5) * 10000
	sizeInt := big.NewInt(sizeIn6Dec)

	// USDC: calculate size * price, round to 4 decimals, convert to 6-decimal format
	// Example: 1.69 * 0.59 = 0.9971 -> 9971 (in 4-dec) -> 997100 in 6-decimal
	// Use +0.5 for rounding to avoid floating point truncation errors
	usdcValue := size * price

	// Polymarket requires minimum token sizes (0.1 tokens based on testing)
	// This must be checked BEFORE the $1 USDC minimum to avoid conflicts
	const minTokenSize = 0.1
	if size < minTokenSize {
		log.Printf("[CLOB] Bumping size from %.4f to %.4f tokens to meet minimum token size", size, minTokenSize)
		size = minTokenSize
		// Recalculate sizeInt with new size
		sizeIn6Dec = int64(size*100+0.5) * 10000
		sizeInt = big.NewInt(sizeIn6Dec)
	}

	// Recalculate USDC value with potentially bumped size
	usdcValue = size * price

	// Polymarket also requires minimum $1 for marketable BUY orders
	const minOrderUSDC = 1.0
	if side == SideBuy && usdcValue < minOrderUSDC && price > 0 {
		// Calculate minimum size needed to reach $1
		minSize := minOrderUSDC / price
		// Round UP to 2 decimal places
		minSize = float64(int(minSize*100)+1) / 100
		if minSize > size {
			log.Printf("[CLOB] Bumping size from %.4f to %.4f to meet $1 minimum (price=%.4f)", size, minSize, price)
			size = minSize
			usdcValue = size * price
			// Recalculate sizeInt with new size
			sizeIn6Dec = int64(size*100+0.5) * 10000
			sizeInt = big.NewInt(sizeIn6Dec)
		}
	}

	usdcIn6Dec := (int64(usdcValue*10000+0.5) * 100) // Round to 4 decimals, convert to 6-decimal
	usdcInt := big.NewInt(usdcIn6Dec)

	if side == SideBuy {
		// BUY: makerAmount=USDC, takerAmount=tokens
		makerAmount = usdcInt
		takerAmount = sizeInt
		sideInt = 0
		sideStr = "BUY"
	} else {
		// SELL: makerAmount=tokens, takerAmount=USDC
		makerAmount = sizeInt
		takerAmount = usdcInt
		sideInt = 1
		sideStr = "SELL"
	}

	// Generate random salt
	salt := generateSalt()

	// Zero address for taker (anyone can fill)
	takerAddress := "0x0000000000000000000000000000000000000000"

	// Expiration: 0 for GTC orders (no expiration)
	expiration := int64(0)

	// For Magic wallets: maker = funder (where funds are), signer = private key wallet
	// For EOA wallets: maker = signer = wallet address
	makerAddress := c.funder.Hex()
	signerAddress := c.auth.GetAddress().Hex()

	log.Printf("[CLOB] DEBUG Order: maker=%s, signer=%s, signatureType=%d", makerAddress, signerAddress, c.signatureType)

	// Build order struct
	order := &Order{
		Salt:          salt,
		Maker:         makerAddress,
		Signer:        signerAddress,
		Taker:         takerAddress,
		TokenID:       tokenID,
		MakerAmount:   makerAmount.String(),
		TakerAmount:   takerAmount.String(),
		Expiration:    strconv.FormatInt(expiration, 10),
		Nonce:         "0",
		FeeRateBps:    "0",
		Side:          sideStr,
		SignatureType: c.signatureType, // Use client's signature type
		SideInt:       sideInt,
	}

	// Sign the order using EIP-712
	signature, err := c.signOrder(order, negRisk)
	if err != nil {
		return nil, fmt.Errorf("failed to sign order: %w", err)
	}
	order.Signature = signature

	return order, nil
}

func (c *ClobClient) signOrder(order *Order, negRisk bool) (string, error) {
	// Choose the correct contract based on market type
	// - NegRiskCTFExchange: 0xC5d563A36AE78145C45a50134d48A1215220f80a (for neg_risk markets)
	// - CTFExchange: 0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E (for regular markets)
	var verifyingContract string
	if negRisk {
		verifyingContract = "0xC5d563A36AE78145C45a50134d48A1215220f80a" // NegRiskCTFExchange
	} else {
		verifyingContract = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E" // CTFExchange
	}
	log.Printf("[CLOB] DEBUG signOrder: negRisk=%v, contract=%s", negRisk, verifyingContract)

	chainID := math.NewHexOrDecimal256(c.chainID)
	domain := apitypes.TypedDataDomain{
		Name:              "Polymarket CTF Exchange",
		Version:           "1",
		ChainId:           chainID,
		VerifyingContract: verifyingContract,
	}

	// Convert values to big integers for EIP-712
	salt := big.NewInt(order.Salt)
	tokenId := new(big.Int)
	tokenId.SetString(order.TokenID, 10)
	makerAmount := new(big.Int)
	makerAmount.SetString(order.MakerAmount, 10)
	takerAmount := new(big.Int)
	takerAmount.SetString(order.TakerAmount, 10)
	expiration := new(big.Int)
	expiration.SetString(order.Expiration, 10)
	nonce := new(big.Int)
	nonce.SetString(order.Nonce, 10)
	feeRateBps := new(big.Int)
	feeRateBps.SetString(order.FeeRateBps, 10)

	// Ensure addresses are in checksum format
	makerAddr := common.HexToAddress(order.Maker).Hex()
	signerAddr := common.HexToAddress(order.Signer).Hex()
	takerAddr := common.HexToAddress(order.Taker).Hex()


	// Order message for EIP-712
	// Use *big.Int for uint256 types, and *big.Int for uint8 types too
	// (go-ethereum's apitypes handles the encoding)
	message := map[string]interface{}{
		"salt":          salt,
		"maker":         makerAddr,
		"signer":        signerAddr,
		"taker":         takerAddr,
		"tokenId":       tokenId,
		"makerAmount":   makerAmount,
		"takerAmount":   takerAmount,
		"expiration":    expiration,
		"nonce":         nonce,
		"feeRateBps":    feeRateBps,
		"side":          big.NewInt(int64(order.SideInt)),
		"signatureType": big.NewInt(int64(order.SignatureType)),
	}

	typedData := apitypes.TypedData{
		Types: apitypes.Types{
			"EIP712Domain": []apitypes.Type{
				{Name: "name", Type: "string"},
				{Name: "version", Type: "string"},
				{Name: "chainId", Type: "uint256"},
				{Name: "verifyingContract", Type: "address"},
			},
			"Order": []apitypes.Type{
				{Name: "salt", Type: "uint256"},
				{Name: "maker", Type: "address"},
				{Name: "signer", Type: "address"},
				{Name: "taker", Type: "address"},
				{Name: "tokenId", Type: "uint256"},
				{Name: "makerAmount", Type: "uint256"},
				{Name: "takerAmount", Type: "uint256"},
				{Name: "expiration", Type: "uint256"},
				{Name: "nonce", Type: "uint256"},
				{Name: "feeRateBps", Type: "uint256"},
				{Name: "side", Type: "uint8"},
				{Name: "signatureType", Type: "uint8"},
			},
		},
		PrimaryType: "Order",
		Domain:      domain,
		Message:     message,
	}

	// Hash the typed data using EIP-712
	hash, _, err := apitypes.TypedDataAndHash(typedData)
	if err != nil {
		return "", fmt.Errorf("failed to hash typed data: %w", err)
	}

	signature, err := crypto.Sign(hash, c.auth.privateKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign: %w", err)
	}

	// Adjust v value
	signature[64] += 27

	return "0x" + hex.EncodeToString(signature), nil
}

func (c *ClobClient) postOrder(ctx context.Context, order *Order, orderType OrderType) (*OrderResponse, error) {
	payload := OrderRequest{
		Order:     *order,
		Owner:     c.apiCreds.APIKey, // Owner is the API key
		OrderType: orderType,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/order", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	// Add browser-like headers to avoid Cloudflare blocking
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Origin", "https://polymarket.com")
	req.Header.Set("Referer", "https://polymarket.com/")

	// Add L2 headers
	c.addL2Headers(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("post order failed: %d %s", resp.StatusCode, string(respBody))
	}

	var orderResp OrderResponse
	if err := json.Unmarshal(respBody, &orderResp); err != nil {
		return nil, fmt.Errorf("failed to decode order response: %w", err)
	}

	return &orderResp, nil
}

func (c *ClobClient) addL2Headers(req *http.Request) {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)

	// Create signature for L2 auth
	// Format: timestamp + method + path + body
	message := timestamp + req.Method + req.URL.Path
	if req.Body != nil {
		// Read body for signature
		bodyBytes, _ := io.ReadAll(req.Body)
		req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		message += string(bodyBytes)
	}

	// HMAC signature using API secret
	signature := c.hmacSign(message, c.apiCreds.APISecret)

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("POLY_ADDRESS", c.auth.GetAddress().Hex())
	req.Header.Set("POLY_API_KEY", c.apiCreds.APIKey)
	req.Header.Set("POLY_PASSPHRASE", c.apiCreds.APIPassphrase)
	req.Header.Set("POLY_TIMESTAMP", timestamp)
	req.Header.Set("POLY_SIGNATURE", signature)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (c *ClobClient) hmacSign(message string, secret string) string {
	// Decode URL-safe base64 secret
	key, err := base64.URLEncoding.DecodeString(secret)
	if err != nil {
		// Try standard base64
		key, err = base64.StdEncoding.DecodeString(secret)
		if err != nil {
			// If not base64, use as-is
			key = []byte(secret)
		}
	}

	// HMAC-SHA256 signature
	h := hmac.New(sha256.New, key)
	h.Write([]byte(message))
	return base64.URLEncoding.EncodeToString(h.Sum(nil))
}

func generateSalt() int64 {
	// Generate random salt (smaller number like Python SDK uses)
	// Use current timestamp with some randomness
	return time.Now().UnixNano() % 1000000000
}

// GetPrivateKey returns the private key (needed for signing)
func (a *Auth) GetPrivateKey() *ecdsa.PrivateKey {
	return a.privateKey
}

// CalculateOptimalFill calculates how much can be bought/sold from order book
func CalculateOptimalFill(book *OrderBook, side Side, amountUSDC float64) (totalSize float64, avgPrice float64, filledUSDC float64) {
	var levels []OrderBookLevel
	if side == SideBuy {
		levels = book.Asks
	} else {
		levels = book.Bids
	}

	remainingUSDC := amountUSDC
	totalCost := 0.0

	for _, level := range levels {
		price, _ := strconv.ParseFloat(level.Price, 64)
		size, _ := strconv.ParseFloat(level.Size, 64)

		levelValue := size * price
		if levelValue <= remainingUSDC {
			totalSize += size
			totalCost += levelValue
			remainingUSDC -= levelValue
		} else {
			fillSize := remainingUSDC / price
			totalSize += fillSize
			totalCost += remainingUSDC
			remainingUSDC = 0
			break
		}

		if remainingUSDC <= 0 {
			break
		}
	}

	if totalSize > 0 {
		avgPrice = totalCost / totalSize
	}
	filledUSDC = amountUSDC - remainingUSDC

	return
}

// =============================================================================
// ORDER STATUS & CANCELLATION
// =============================================================================

// OpenOrder represents the status of an order from GET /data/order/{id}
// Note: Some fields come as numbers from the API, using json.Number for flexibility
type OpenOrder struct {
	ID              string      `json:"id"`
	Status          string      `json:"status"`
	Market          string      `json:"market"`
	OriginalSize    string      `json:"original_size"`
	SizeMatched     string      `json:"size_matched"`
	Outcome         string      `json:"outcome"`
	MakerAddress    string      `json:"maker_address"`
	Owner           string      `json:"owner"`
	Price           string      `json:"price"`
	Side            string      `json:"side"`
	AssetID         string      `json:"asset_id"`
	Expiration      json.Number `json:"expiration"`
	Type            string      `json:"type"`
	CreatedAt       json.Number `json:"created_at"`
	AssociateTrades []string    `json:"associate_trades"`
}

// GetOrder retrieves order status by order ID
// Returns the order with original_size, size_matched, status, etc.
func (c *ClobClient) GetOrder(ctx context.Context, orderID string) (*OpenOrder, error) {
	if c.apiCreds == nil {
		if _, err := c.DeriveAPICreds(ctx); err != nil {
			return nil, fmt.Errorf("failed to get API creds: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/data/order/"+orderID, nil)
	if err != nil {
		return nil, err
	}

	// Add browser-like headers
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json")

	// Add L2 headers
	c.addL2Headers(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get order failed: %d %s", resp.StatusCode, string(respBody))
	}

	// Debug: log raw response to see actual field names
	log.Printf("[GetOrder] raw response: %s", string(respBody))

	var order OpenOrder
	if err := json.Unmarshal(respBody, &order); err != nil {
		return nil, fmt.Errorf("failed to decode order: %w", err)
	}

	return &order, nil
}

// CancelOrder cancels an order by ID
// Returns nil on success
func (c *ClobClient) CancelOrder(ctx context.Context, orderID string) error {
	if c.apiCreds == nil {
		if _, err := c.DeriveAPICreds(ctx); err != nil {
			return fmt.Errorf("failed to get API creds: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, "DELETE", c.baseURL+"/order/"+orderID, nil)
	if err != nil {
		return err
	}

	// Add browser-like headers
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json")

	// Add L2 headers
	c.addL2Headers(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	// 200, 204 = success; 404 = order already filled/cancelled (treat as success)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("cancel order failed: %d %s", resp.StatusCode, string(respBody))
	}

	if resp.StatusCode == http.StatusNotFound {
		log.Printf("[CancelOrder] order %s not found (already filled/cancelled)", orderID)
	}

	return nil
}
