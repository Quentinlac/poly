package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const GammaMarketsURL = "https://gamma-api.polymarket.com/markets"

type GammaMarket struct {
	ConditionID  string `json:"condition_id"`
	Question     string `json:"question"`
	Slug         string `json:"slug"`
	EventSlug    string `json:"groupItemTitle"`
	Outcomes     string `json:"outcomes"`
	ClobTokenIds string `json:"clob_token_ids"`
}

type TokenInfo struct {
	TokenID     string
	ConditionID string
	Outcome     string
	Title       string
	Slug        string
	EventSlug   string
}

func main() {
	ctx := context.Background()

	// Get database URL
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL required")
	}

	// Connect to database
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Read token IDs from stdin or file
	var tokenIDs []string

	if len(os.Args) > 1 {
		// Read from file
		file, err := os.Open(os.Args[1])
		if err != nil {
			log.Fatalf("Failed to open file: %v", err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line != "" {
				tokenIDs = append(tokenIDs, line)
			}
		}
	} else {
		// Read from stdin
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line != "" {
				tokenIDs = append(tokenIDs, line)
			}
		}
	}

	log.Printf("Fetching info for %d tokens...", len(tokenIDs))

	// Fetch tokens concurrently
	results := make(chan TokenInfo, len(tokenIDs))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10) // Limit to 10 concurrent requests
	var fetched, failed int64

	client := &http.Client{Timeout: 10 * time.Second}

	for _, tokenID := range tokenIDs {
		wg.Add(1)
		go func(tid string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			info, err := fetchTokenInfo(ctx, client, tid)
			if err != nil {
				atomic.AddInt64(&failed, 1)
				if atomic.LoadInt64(&failed) <= 5 {
					log.Printf("Failed to fetch %s: %v", tid[:20], err)
				}
				return
			}

			atomic.AddInt64(&fetched, 1)
			results <- info
		}(tokenID)
	}

	// Close results when done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var tokens []TokenInfo
	for info := range results {
		tokens = append(tokens, info)
	}

	log.Printf("Fetched %d tokens, %d failed", fetched, failed)

	if len(tokens) == 0 {
		log.Println("No tokens to insert")
		return
	}

	// Insert into database
	inserted := 0
	for _, t := range tokens {
		_, err := pool.Exec(ctx, `
			INSERT INTO token_map_cache (token_id, condition_id, outcome, title, slug, event_slug)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (token_id) DO UPDATE SET
				condition_id = EXCLUDED.condition_id,
				outcome = EXCLUDED.outcome,
				title = EXCLUDED.title,
				slug = EXCLUDED.slug,
				event_slug = EXCLUDED.event_slug
		`, t.TokenID, t.ConditionID, t.Outcome, t.Title, t.Slug, t.EventSlug)
		if err != nil {
			log.Printf("Failed to insert token %s: %v", t.TokenID[:20], err)
			continue
		}
		inserted++
	}

	log.Printf("Inserted %d tokens into cache", inserted)

	// Now backfill trades
	result, err := pool.Exec(ctx, `
		UPDATE user_trades t
		SET
			title = c.title,
			slug = c.slug,
			outcome = c.outcome
		FROM token_map_cache c
		WHERE t.market_id = c.token_id
		  AND (t.title IS NULL OR t.title = '')
	`)
	if err != nil {
		log.Printf("Failed to backfill trades: %v", err)
		return
	}

	log.Printf("Backfilled %d trades", result.RowsAffected())
}

func fetchTokenInfo(ctx context.Context, client *http.Client, tokenID string) (TokenInfo, error) {
	url := GammaMarketsURL + "?clob_token_ids=" + tokenID

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return TokenInfo{}, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return TokenInfo{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return TokenInfo{}, err
	}

	var markets []GammaMarket
	if err := json.Unmarshal(body, &markets); err != nil {
		return TokenInfo{}, err
	}

	if len(markets) == 0 {
		return TokenInfo{}, fmt.Errorf("no market found")
	}

	market := markets[0]

	// Parse outcomes
	var outcomes []string
	if err := json.Unmarshal([]byte(market.Outcomes), &outcomes); err != nil {
		outcomes = []string{"Yes", "No"}
	}

	// Parse token IDs
	var marketTokens []string
	if err := json.Unmarshal([]byte(market.ClobTokenIds), &marketTokens); err != nil {
		marketTokens = strings.Split(market.ClobTokenIds, ",")
	}

	// Find which outcome this token corresponds to
	outcome := ""
	for i, mt := range marketTokens {
		mt = strings.TrimSpace(mt)
		mt = strings.Trim(mt, "\"")
		if mt == tokenID && i < len(outcomes) {
			outcome = outcomes[i]
			break
		}
	}

	return TokenInfo{
		TokenID:     tokenID,
		ConditionID: market.ConditionID,
		Outcome:     outcome,
		Title:       market.Question,
		Slug:        market.Slug,
		EventSlug:   market.EventSlug,
	}, nil
}
