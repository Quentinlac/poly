package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/joho/godotenv"

	"polymarket-analyzer/api"
	"polymarket-analyzer/config"
	"polymarket-analyzer/service"
	"polymarket-analyzer/storage"
	"polymarket-analyzer/syncer"
)

func main() {
	log.Println("[Worker] Starting incremental sync + copy trader worker...")

	// Load environment variables
	if err := godotenv.Load(); err != nil {
		log.Println("[Worker] No .env file found, using environment variables")
	}

	ctx := context.Background()

	// Load config
	cfgPath := os.Getenv("POLYMARKET_CONFIG")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Printf("[Worker] Warning: Failed to load config: %v, using defaults", err)
		cfg = &config.Config{}
	}

	// Initialize storage
	store, err := storage.NewPostgres()
	if err != nil {
		log.Fatalf("[Worker] Failed to init storage: %v", err)
	}
	defer store.Close()

	// SPEED OPTIMIZATION: Pre-warm token cache at startup
	log.Println("[Worker] Pre-warming token cache...")
	tokenCount, err := store.PreWarmTokenCache(ctx)
	if err != nil {
		log.Printf("[Worker] Warning: Token cache pre-warm failed: %v", err)
	} else {
		log.Printf("[Worker] Token cache pre-warmed with %d tokens", tokenCount)
	}

	// Initialize API client
	baseURL := os.Getenv("POLYMARKET_API_URL")
	if baseURL == "" {
		baseURL = "https://clob.polymarket.com"
	}
	apiClient := api.NewClient(baseURL)

	// Create service for incremental worker
	svc := service.NewService(store, cfg, apiClient)

	// Start incremental worker (fetches new trades from tracked users every 2 seconds)
	incrementalWorker := syncer.NewIncrementalWorker(apiClient, store, cfg, svc)
	incrementalWorker.Start()
	defer incrementalWorker.Stop()
	log.Println("[Worker] Incremental sync started (2-second polling for tracked users)")

	// Check if copy trader is enabled (disabled in worker by default to avoid duplicates)
	copyTraderEnabled := os.Getenv("COPY_TRADER_ENABLED")
	if copyTraderEnabled == "true" || copyTraderEnabled == "1" {
		// Configure copy trader
		copyConfig := syncer.CopyTraderConfig{
			Enabled:          true,
			Multiplier:       getEnvFloat("COPY_TRADER_MULTIPLIER", 0.05),
			MinOrderUSDC:     getEnvFloat("COPY_TRADER_MIN_USDC", 1.0),
			MaxPriceSlippage: getEnvFloat("COPY_TRADER_MAX_SLIPPAGE", 0.20), // 20% max above trader's price
			CheckIntervalSec: 2,
		}

		log.Printf("[Worker] Copy trader config: multiplier=%.2f, minOrder=$%.2f, maxSlippage=%.0f%%, interval=%ds",
			copyConfig.Multiplier, copyConfig.MinOrderUSDC, copyConfig.MaxPriceSlippage*100, copyConfig.CheckIntervalSec)

		// Create copy trader
		copyTrader, err := syncer.NewCopyTrader(store, apiClient, copyConfig)
		if err != nil {
			log.Printf("[Worker] Warning: Failed to create copy trader: %v", err)
			log.Println("[Worker] Copy trader will be disabled")
		} else {
			// Start copy trader
			if err := copyTrader.Start(ctx); err != nil {
				log.Printf("[Worker] Warning: Failed to start copy trader: %v", err)
			} else {
				defer copyTrader.Stop()
				log.Println("[Worker] Copy trader started successfully")
			}
		}
	} else {
		log.Println("[Worker] Copy trader DISABLED (COPY_TRADER_ENABLED != true)")
		log.Println("[Worker] This is correct for analytics-worker to avoid duplicate orders")
	}

	// Setup signal handling for graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	log.Println("[Worker] Running... Press Ctrl+C to stop")

	// Wait for shutdown signal
	<-stop
	log.Println("[Worker] Shutting down...")
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
