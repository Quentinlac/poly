// Test program for mempool monitoring
// Run with: go run cmd/test_mempool/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"polymarket-analyzer/api"
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// Get user address from command line or use default test address
	userAddr := "0x05c1882212a41aa8d7df5b70eebe03d9319345b7" // Strategy 3 user from DB
	if len(os.Args) > 1 {
		userAddr = os.Args[1]
	}

	log.Printf("Testing Mempool WebSocket monitoring")
	log.Printf("Monitoring address: %s", userAddr)
	log.Printf("Press Ctrl+C to stop")
	log.Println("=" + string(make([]byte, 60)))

	// Create mempool client with handler
	client := api.NewMempoolWSClient(func(event api.MempoolTradeEvent) {
		log.Printf("🚀 PENDING TRADE DETECTED!")
		log.Printf("  TX Hash: %s", event.TxHash)
		log.Printf("  From: %s", event.From)
		log.Printf("  To: %s", event.To)
		log.Printf("  Detected at: %s", event.DetectedAt.Format("15:04:05.000"))
		log.Printf("  Nonce: %d", event.Nonce)
	})

	// Set the address to monitor
	client.SetFollowedAddresses([]string{userAddr})

	// Start monitoring
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := client.Start(ctx); err != nil {
		log.Fatalf("Failed to start mempool client: %v", err)
	}

	// Print stats every 30 seconds
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pending, polymarket, trades := client.GetStats()
				log.Printf("[Stats] Pending TX: %d, Polymarket TX cached: %d, Trades detected: %d", pending, polymarket, trades)
			}
		}
	}()

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nStopping...")
	client.Stop()
	log.Println("Done")
}
