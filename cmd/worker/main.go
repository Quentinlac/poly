package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"polymarket-analyzer/api"
	"polymarket-analyzer/storage"
	"polymarket-analyzer/syncer"

	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	// Initialize storage
	store, err := storage.NewPostgres()
	if err != nil {
		log.Fatalf("[worker] failed to init storage: %v", err)
	}
	defer store.Close()

	log.Println("[worker] PostgreSQL storage initialized")

	// Initialize API client
	baseURL := os.Getenv("POLYMARKET_API_URL")
	if baseURL == "" {
		baseURL = "https://clob.polymarket.com"
	}
	apiClient := api.NewClient(baseURL)

	// Configure copy trader
	copyConfig := syncer.CopyTraderConfig{
		Enabled:          true,
		Multiplier:       getEnvFloat("COPY_TRADER_MULTIPLIER", 0.05),
		MinOrderUSDC:     getEnvFloat("COPY_TRADER_MIN_USDC", 1.0),
		CheckIntervalSec: 2,
	}

	log.Printf("[worker] Copy trader config: multiplier=%.2f, minOrder=$%.2f, interval=%ds",
		copyConfig.Multiplier, copyConfig.MinOrderUSDC, copyConfig.CheckIntervalSec)

	// Create copy trader
	copyTrader, err := syncer.NewCopyTrader(store, apiClient, copyConfig)
	if err != nil {
		log.Fatalf("[worker] failed to create copy trader: %v", err)
	}

	// Start copy trader
	ctx := context.Background()
	if err := copyTrader.Start(ctx); err != nil {
		log.Fatalf("[worker] failed to start copy trader: %v", err)
	}
	defer copyTrader.Stop()

	log.Println("[worker] Copy trader started successfully")
	log.Println("[worker] Worker is running. Press Ctrl+C to stop.")

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("[worker] Received shutdown signal, stopping gracefully...")
}

// getEnvFloat retrieves a float from environment or returns default
func getEnvFloat(key string, defaultVal float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return defaultVal
}
