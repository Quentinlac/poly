package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"polymarket-analyzer/storage"
	"polymarket-analyzer/syncer"

	"github.com/joho/godotenv"
)

func main() {
	log.Println("Testing Binance Price Syncer (30 minute lookback)...")

	// Load env
	godotenv.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize storage
	store, err := storage.NewPostgres()
	if err != nil {
		log.Fatalf("Database error: %v", err)
	}
	defer store.Close()

	// Create syncer with 30-minute lookback for testing
	priceSyncer := syncer.NewBinancePriceSyncer(store, syncer.BinancePriceSyncerConfig{
		Symbol:          "BTCUSDT",
		InitialLookback: 30 * time.Minute, // Just 30 min for testing
		UpdateInterval:  1 * time.Hour,
	})

	// Start syncer
	log.Println("Starting syncer...")
	if err := priceSyncer.Start(ctx); err != nil {
		log.Fatalf("Failed to start syncer: %v", err)
	}
	defer priceSyncer.Stop()

	// Wait for initial sync to complete (check every second)
	log.Println("Waiting for initial sync...")
	for i := 0; i < 120; i++ { // Max 2 minutes
		time.Sleep(1 * time.Second)

		stats, _ := priceSyncer.GetStats(ctx)
		count := stats["total_klines"].(int64)

		if count >= 1800 { // 30 minutes = 1800 seconds
			log.Printf("✓ Initial sync complete! %d klines stored", count)
			break
		}

		if i%10 == 0 {
			log.Printf("  Syncing... %d klines so far", count)
		}
	}

	// Show final stats
	stats, _ := priceSyncer.GetStats(ctx)
	log.Printf("\n--- Final Stats ---")
	log.Printf("Symbol: %s", stats["symbol"])
	log.Printf("Total klines: %d", stats["total_klines"])
	log.Printf("Oldest: %v", stats["oldest_time"])
	log.Printf("Latest: %v", stats["latest_time"])

	// Test price lookup
	testTime := time.Now().UTC().Add(-15 * time.Minute)
	price, err := priceSyncer.GetPriceAt(ctx, testTime)
	if err != nil {
		log.Printf("Price lookup error: %v", err)
	} else {
		log.Printf("✓ Price at %s: $%.2f", testTime.Format("15:04:05"), price)
	}

	log.Println("\n✓ Syncer test completed!")

	// If running interactively, wait for Ctrl+C
	if os.Getenv("QUICK_TEST") == "" {
		log.Println("\nPress Ctrl+C to stop...")
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
		<-stop
	}
}
