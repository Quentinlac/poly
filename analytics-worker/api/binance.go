package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"
)

// BinanceClient fetches kline (candlestick) data from Binance API
type BinanceClient struct {
	baseURL    string
	httpClient *http.Client
}

// BinanceKline represents a single candlestick/kline data point
type BinanceKline struct {
	OpenTime         int64   // Kline open time (ms)
	Open             float64 // Open price
	High             float64 // High price
	Low              float64 // Low price
	Close            float64 // Close price
	Volume           float64 // Volume
	CloseTime        int64   // Kline close time (ms)
	QuoteAssetVolume float64 // Quote asset volume
	NumTrades        int64   // Number of trades
}

// NewBinanceClient creates a new Binance API client
func NewBinanceClient() *BinanceClient {
	return &BinanceClient{
		baseURL: "https://api.binance.com",
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// GetKlines fetches kline/candlestick data from Binance
// symbol: e.g., "BTCUSDT"
// interval: e.g., "1s", "1m", "1h", "1d"
// startTime: start time in milliseconds
// endTime: end time in milliseconds
// limit: max 1000 per request
func (c *BinanceClient) GetKlines(ctx context.Context, symbol, interval string, startTime, endTime int64, limit int) ([]BinanceKline, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

	url := fmt.Sprintf("%s/api/v3/klines?symbol=%s&interval=%s&limit=%d",
		c.baseURL, symbol, interval, limit)

	if startTime > 0 {
		url += fmt.Sprintf("&startTime=%d", startTime)
	}
	if endTime > 0 {
		url += fmt.Sprintf("&endTime=%d", endTime)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := resp.Header.Get("Retry-After")
		return nil, fmt.Errorf("rate limited (retry after %s)", retryAfter)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("binance API error %d: %s", resp.StatusCode, string(body))
	}

	// Binance returns klines as array of arrays
	var rawKlines [][]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&rawKlines); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	klines := make([]BinanceKline, 0, len(rawKlines))
	for _, raw := range rawKlines {
		if len(raw) < 9 {
			continue
		}

		kline := BinanceKline{
			OpenTime:  int64(raw[0].(float64)),
			CloseTime: int64(raw[6].(float64)),
		}

		// Parse string values
		if s, ok := raw[1].(string); ok {
			kline.Open, _ = strconv.ParseFloat(s, 64)
		}
		if s, ok := raw[2].(string); ok {
			kline.High, _ = strconv.ParseFloat(s, 64)
		}
		if s, ok := raw[3].(string); ok {
			kline.Low, _ = strconv.ParseFloat(s, 64)
		}
		if s, ok := raw[4].(string); ok {
			kline.Close, _ = strconv.ParseFloat(s, 64)
		}
		if s, ok := raw[5].(string); ok {
			kline.Volume, _ = strconv.ParseFloat(s, 64)
		}
		if s, ok := raw[7].(string); ok {
			kline.QuoteAssetVolume, _ = strconv.ParseFloat(s, 64)
		}
		if n, ok := raw[8].(float64); ok {
			kline.NumTrades = int64(n)
		}

		klines = append(klines, kline)
	}

	return klines, nil
}

// FetchKlinesRange fetches all klines between startTime and endTime
// handling pagination automatically. Returns klines in chronological order.
func (c *BinanceClient) FetchKlinesRange(ctx context.Context, symbol, interval string, startTime, endTime time.Time) ([]BinanceKline, error) {
	var allKlines []BinanceKline

	startMs := startTime.UnixMilli()
	endMs := endTime.UnixMilli()

	// Calculate interval duration for progress logging
	totalDuration := endTime.Sub(startTime)
	fetchedDuration := time.Duration(0)
	lastLogTime := time.Now()

	for startMs < endMs {
		select {
		case <-ctx.Done():
			return allKlines, ctx.Err()
		default:
		}

		klines, err := c.GetKlines(ctx, symbol, interval, startMs, endMs, 1000)
		if err != nil {
			// On rate limit, wait and retry
			if isRateLimitError(err) {
				log.Printf("[Binance] Rate limited, waiting 60s...")
				time.Sleep(60 * time.Second)
				continue
			}
			return allKlines, err
		}

		if len(klines) == 0 {
			break
		}

		allKlines = append(allKlines, klines...)

		// Move start time to after the last kline
		lastKline := klines[len(klines)-1]
		startMs = lastKline.CloseTime + 1

		// Update progress
		fetchedDuration = time.Duration(startMs-startTime.UnixMilli()) * time.Millisecond

		// Log progress every 10 seconds
		if time.Since(lastLogTime) > 10*time.Second {
			progress := float64(fetchedDuration) / float64(totalDuration) * 100
			log.Printf("[Binance] Fetching %s %s: %.1f%% complete (%d klines)",
				symbol, interval, progress, len(allKlines))
			lastLogTime = time.Now()
		}

		// Small delay to avoid rate limits (Binance limit: 1200 req/min)
		time.Sleep(50 * time.Millisecond)

		// If we got less than 1000, we've reached the end
		if len(klines) < 1000 {
			break
		}
	}

	return allKlines, nil
}

func isRateLimitError(err error) bool {
	return err != nil && (contains(err.Error(), "rate limit") || contains(err.Error(), "429"))
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsImpl(s, substr))
}

func containsImpl(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
