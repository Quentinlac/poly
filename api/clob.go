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
	ConditionID           string `json:"condition_id"`
	QuestionID            string `json:"question_id"`
	Tokens                []ClobTokenInfo `json:"tokens"`
	MinimumOrderSize      string `json:"minimum_order_size"`
	MinimumTickSize       string `json:"minimum_tick_size"`
	Description           string `json:"description"`
	Category              string `json:"category"`
	EndDateISO            string `json:"end_date_iso"`
	GameStartTime         string `json:"game_start_time"`
	QuestionHasScreenshot bool   `json:"question_has_screenshot"`
	Active                bool   `json:"active"`
	Closed                bool   `json:"closed"`
	MarketSlug            string `json:"market_slug"`
	Icon                  string `json:"icon"`
	Fpmm                  string `json:"fpmm"`
	NegRisk               bool   `json:"neg_risk"`
}

// ClobTokenInfo represents token information from CLOB
type ClobTokenInfo struct {
	TokenID  string `json:"token_id"`
	Outcome  string `json:"outcome"`
	Price    string `json:"price"`
	Winner   bool   `json:"winner"`
}

// OrderType represents the type of order
type OrderType string

const (
	OrderTypeFOK OrderType = "FOK" // Fill-Or-Kill (market order)
	OrderTypeGTC OrderType = "GTC" // Good-Til-Cancelled (limit order)
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

	client := &ClobClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
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

// GetOrderBook fetches the order book for a token
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

func (c *ClobClient) createSignedOrder(tokenID string, side Side, size float64, price float64, negRisk bool) (*Order, error) {
	// Round price to tick size (0.01 for most markets)
	tickSize := 0.01
	price = float64(int(price/tickSize+0.5)) * tickSize

	// Round size to 2 decimal places
	size = float64(int(size*100+0.5)) / 100

	// Enforce minimum order size
	if size < 0.01 {
		size = 0.01
	}

	log.Printf("[CLOB DEBUG] Rounded price: %.4f, size: %.4f", price, size)

	// Convert to base units
	// USDC: 6 decimals
	// Outcome tokens: 6 decimals (same as USDC in Polymarket)
	// MakerAmount: what we're giving (USDC for buy, tokens for sell)
	// TakerAmount: what we're getting (tokens for buy, USDC for sell)

	var makerAmount, takerAmount *big.Int
	sideInt := 0     // 0 = BUY, 1 = SELL (for EIP-712)
	sideStr := "BUY" // String for JSON payload

	// Token amounts in 6 decimals (same as USDC)
	sizeUnits := new(big.Float).Mul(big.NewFloat(size), big.NewFloat(1e6))
	sizeInt := new(big.Int)
	sizeUnits.Int(sizeInt)

	// USDC amount in 6 decimals
	usdcAmount := new(big.Float).Mul(big.NewFloat(size*price), big.NewFloat(1e6))
	usdcInt := new(big.Int)
	usdcAmount.Int(usdcInt)

	if side == SideBuy {
		makerAmount = usdcInt    // We give USDC (6 decimals)
		takerAmount = sizeInt    // We get tokens (6 decimals)
		sideInt = 0
		sideStr = "BUY"
	} else {
		makerAmount = sizeInt    // We give tokens (6 decimals)
		takerAmount = usdcInt    // We get USDC (6 decimals)
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
	// Polymarket uses different contract addresses for neg_risk markets
	var verifyingContract string
	if negRisk {
		verifyingContract = "0xC5d563A36AE78145C45a50134d48A1215220f80a" // NegRiskCTFExchange
	} else {
		verifyingContract = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E" // CTFExchange
	}

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

	// Order message for EIP-712 - pass *big.Int for uint256 types
	message := map[string]interface{}{
		"salt":          salt,
		"maker":         order.Maker,
		"signer":        order.Signer,
		"taker":         order.Taker,
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

	log.Printf("[CLOB DEBUG] EIP-712 hash: 0x%x", hash)

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

	log.Printf("[CLOB DEBUG] Order payload: %s", string(body))

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

	log.Printf("[CLOB DEBUG] Response status: %d", resp.StatusCode)
	log.Printf("[CLOB DEBUG] Response body: %s", string(respBody))

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

	log.Printf("[CLOB DEBUG] HMAC message: %s", message[:min(200, len(message))])

	// HMAC signature using API secret
	signature := c.hmacSign(message, c.apiCreds.APISecret)
	log.Printf("[CLOB DEBUG] HMAC signature: %s", signature)

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("POLY_ADDRESS", c.auth.GetAddress().Hex()) // Use signer address for L2 auth
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
