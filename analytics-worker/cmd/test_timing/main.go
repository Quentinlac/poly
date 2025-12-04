package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"polymarket-analyzer/api"
)

func main() {
	if os.Getenv("POLYMARKET_PRIVATE_KEY") == "" {
		log.Fatal("POLYMARKET_PRIVATE_KEY not set")
	}

	// Use a liquid BTC market token
	// "Will Bitcoin reach $130,000 by December 31, 2025?" - YES token (negRisk=false)
	tokenID := "70455106433105725093003807079135685186309776081399075740142938507518314484366"
	negRisk := false

	auth, err := api.NewAuth()
	if err != nil {
		log.Fatalf("Failed to create auth: %v", err)
	}

	client, err := api.NewClobClient("", auth)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	ctx := context.Background()

	// Derive API creds first (so it doesn't count in timing)
	log.Println("Deriving API credentials...")
	if _, err := client.DeriveAPICreds(ctx); err != nil {
		log.Fatalf("Failed to derive creds: %v", err)
	}
	log.Println("API credentials ready")

	// Warm up connection pool
	log.Println("Warming connection pool...")
	if err := client.WarmConnection(ctx); err != nil {
		log.Printf("Warning: warm connection failed: %v", err)
	}

	// First, get order book to find current best ask price
	log.Println("Fetching initial order book to find price...")
	book, err := client.GetOrderBook(ctx, tokenID)
	if err != nil {
		log.Fatalf("Failed to get order book: %v", err)
	}
	if len(book.Asks) == 0 {
		log.Fatal("No asks in order book")
	}

	// Use best ask price
	var bestAskPrice float64
	fmt.Sscanf(book.Asks[0].Price, "%f", &bestAskPrice)
	log.Printf("Best ask price: %.2f", bestAskPrice)

	// Calculate size for ~$1 order (minimum 5 shares)
	orderSize := 5.0
	orderPrice := bestAskPrice
	orderCost := orderSize * orderPrice
	log.Printf("Will place order: %.2f shares @ $%.2f = $%.2f", orderSize, orderPrice, orderCost)

	fmt.Println("\n========================================")
	fmt.Println("PARALLEL TIMING TEST")
	fmt.Println("========================================")

	var wg sync.WaitGroup
	var orderTime, bookTime time.Duration
	var orderResp *api.OrderResponse
	var orderErr error
	var bookResp *api.OrderBook
	var bookErr error

	startTotal := time.Now()

	// Start both in parallel
	wg.Add(2)

	// Order placement
	go func() {
		defer wg.Done()
		start := time.Now()
		orderResp, orderErr = client.PlaceOrderFast(ctx, tokenID, api.SideBuy, orderSize, orderPrice, negRisk)
		orderTime = time.Since(start)
	}()

	// Order book fetch
	go func() {
		defer wg.Done()
		start := time.Now()
		bookResp, bookErr = client.GetOrderBook(ctx, tokenID)
		bookTime = time.Since(start)
	}()

	wg.Wait()
	totalTime := time.Since(startTotal)

	fmt.Println("\n========================================")
	fmt.Println("RESULTS")
	fmt.Println("========================================")

	fmt.Printf("\n📊 ORDER BOOK FETCH: %dms\n", bookTime.Milliseconds())
	if bookErr != nil {
		fmt.Printf("   Error: %v\n", bookErr)
	} else {
		fmt.Printf("   Asks: %d, Bids: %d\n", len(bookResp.Asks), len(bookResp.Bids))
	}

	fmt.Printf("\n📦 ORDER PLACEMENT: %dms\n", orderTime.Milliseconds())
	if orderErr != nil {
		fmt.Printf("   Error: %v\n", orderErr)
	} else {
		fmt.Printf("   Success: %v\n", orderResp.Success)
		fmt.Printf("   Status: %s\n", orderResp.Status)
		fmt.Printf("   OrderID: %s\n", orderResp.OrderID)
	}

	fmt.Printf("\n⏱️  TOTAL PARALLEL TIME: %dms\n", totalTime.Milliseconds())
	fmt.Printf("   (Sequential would be: %dms)\n", orderTime.Milliseconds()+bookTime.Milliseconds())

	// If order was placed and is live, check fill status
	if orderResp != nil && orderResp.Success && orderResp.OrderID != "" {
		fmt.Println("\n========================================")
		fmt.Println("CHECKING ORDER STATUS")
		fmt.Println("========================================")

		start := time.Now()
		order, err := client.GetOrder(ctx, orderResp.OrderID)
		getOrderTime := time.Since(start)

		fmt.Printf("\n📋 GET ORDER: %dms\n", getOrderTime.Milliseconds())
		if err != nil {
			fmt.Printf("   Error: %v\n", err)
		} else {
			fmt.Printf("   Status: %s\n", order.Status)
			fmt.Printf("   Original Size: %s\n", order.OriginalSize)
			fmt.Printf("   Size Matched: %s\n", order.SizeMatched)
		}

		// Cancel order if still live
		if order != nil && (order.Status == "live" || order.Status == "open") {
			fmt.Println("\n🗑️  Cancelling order...")
			start := time.Now()
			err := client.CancelOrder(ctx, orderResp.OrderID)
			cancelTime := time.Since(start)
			fmt.Printf("   Cancel took: %dms\n", cancelTime.Milliseconds())
			if err != nil {
				fmt.Printf("   Error: %v\n", err)
			} else {
				fmt.Println("   ✅ Cancelled")
			}
		}
	}
}
