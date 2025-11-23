package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	// Public Polygon RPC endpoint (free, no API key needed, but slower)
	DefaultPolygonRPC = "https://polygon-rpc.com"
)

// PolygonscanClient queries Polygon RPC for transaction details
type PolygonscanClient struct {
	httpClient  *http.Client
	rpcURL      string
	cache       map[string]string // txHash -> from address
	cacheMu     sync.RWMutex
	rateLimiter *time.Ticker
}

// RPCRequest represents a JSON-RPC request
type RPCRequest struct {
	JsonRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

// RPCResponse represents a JSON-RPC response
type RPCResponse struct {
	JsonRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Result  *struct {
		Hash             string `json:"hash"`
		From             string `json:"from"`
		To               string `json:"to"`
		BlockNumber      string `json:"blockNumber"`
		TransactionIndex string `json:"transactionIndex"`
	} `json:"result"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// NewPolygonscanClient creates a new Polygon RPC client
func NewPolygonscanClient() *PolygonscanClient {
	rpcURL := os.Getenv("POLYGON_RPC_URL")
	if rpcURL == "" {
		rpcURL = DefaultPolygonRPC
		log.Printf("[PolygonRPC] Using public RPC endpoint: %s", rpcURL)
	} else {
		log.Printf("[PolygonRPC] Using custom RPC endpoint")
	}

	return &PolygonscanClient{
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		rpcURL:      rpcURL,
		cache:       make(map[string]string),
		rateLimiter: time.NewTicker(100 * time.Millisecond), // 10 req/sec max
	}
}

// GetTransactionSender fetches the 'from' address for a single transaction
func (c *PolygonscanClient) GetTransactionSender(ctx context.Context, txHash string) (string, error) {
	txHash = strings.ToLower(txHash)

	// Check cache first
	c.cacheMu.RLock()
	if fromAddr, ok := c.cache[txHash]; ok {
		c.cacheMu.RUnlock()
		return fromAddr, nil
	}
	c.cacheMu.RUnlock()

	// Rate limit
	select {
	case <-c.rateLimiter.C:
		// Proceed
	case <-ctx.Done():
		return "", ctx.Err()
	}

	// Build JSON-RPC request
	rpcReq := RPCRequest{
		JsonRPC: "2.0",
		Method:  "eth_getTransactionByHash",
		Params:  []interface{}{txHash},
		ID:      1,
	}

	reqBody, err := json.Marshal(rpcReq)
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.rpcURL, bytes.NewReader(reqBody))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("RPC returned status %d: %s", resp.StatusCode, string(body))
	}

	var rpcResp RPCResponse
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return "", fmt.Errorf("unmarshal response: %w", err)
	}

	if rpcResp.Error != nil {
		return "", fmt.Errorf("RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	if rpcResp.Result == nil {
		return "", fmt.Errorf("transaction not found")
	}

	fromAddr := strings.ToLower(rpcResp.Result.From)
	if fromAddr == "" {
		return "", fmt.Errorf("no 'from' field in response")
	}

	// Cache the result
	c.cacheMu.Lock()
	c.cache[txHash] = fromAddr
	c.cacheMu.Unlock()

	return fromAddr, nil
}

// GetTransactionSenders fetches 'from' addresses for multiple transactions in batch
func (c *PolygonscanClient) GetTransactionSenders(ctx context.Context, txHashes []string) map[string]string {
	result := make(map[string]string)
	var mu sync.Mutex

	// Check how many are already cached
	uncached := []string{}
	c.cacheMu.RLock()
	for _, txHash := range txHashes {
		txHash = strings.ToLower(txHash)
		if fromAddr, ok := c.cache[txHash]; ok {
			result[txHash] = fromAddr
		} else {
			uncached = append(uncached, txHash)
		}
	}
	c.cacheMu.RUnlock()

	if len(uncached) == 0 {
		return result
	}

	log.Printf("[PolygonRPC] Fetching %d transaction senders (%d from cache)", len(uncached), len(result))

	// Fetch uncached transactions concurrently (with rate limiting)
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10) // Max 10 concurrent requests

	for _, txHash := range uncached {
		wg.Add(1)
		go func(hash string) {
			defer wg.Done()
			sem <- struct{}{}        // Acquire
			defer func() { <-sem }() // Release

			fromAddr, err := c.GetTransactionSender(ctx, hash)
			if err != nil {
				log.Printf("[PolygonRPC] Failed to fetch tx %s: %v", hash[:10]+"...", err)
				return
			}

			mu.Lock()
			result[hash] = fromAddr
			mu.Unlock()
		}(txHash)
	}

	wg.Wait()

	log.Printf("[PolygonRPC] Successfully fetched %d/%d transaction senders", len(result), len(txHashes))
	return result
}

// Close stops the rate limiter
func (c *PolygonscanClient) Close() {
	if c.rateLimiter != nil {
		c.rateLimiter.Stop()
	}
}
