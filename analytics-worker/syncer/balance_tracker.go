package syncer

import (
	"context"
	"log"
	"sync"
	"time"

	"polymarket-analyzer/api"
	"polymarket-analyzer/storage"
)

// BalanceTracker monitors wallet balance and stores history
type BalanceTracker struct {
	store         *storage.PostgresStore
	walletAddress string
	interval      time.Duration

	mu            sync.RWMutex
	latestBalance float64
	lastUpdated   time.Time

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewBalanceTracker creates a new balance tracker
func NewBalanceTracker(store *storage.PostgresStore, walletAddress string, interval time.Duration) *BalanceTracker {
	return &BalanceTracker{
		store:         store,
		walletAddress: walletAddress,
		interval:      interval,
		stopCh:        make(chan struct{}),
	}
}

// Start begins the background balance tracking
func (bt *BalanceTracker) Start() {
	bt.wg.Add(1)
	go bt.trackLoop()
	log.Printf("[BalanceTracker] Started tracking balance for %s every %v", bt.walletAddress, bt.interval)
}

// Stop gracefully stops the balance tracking
func (bt *BalanceTracker) Stop() {
	close(bt.stopCh)
	bt.wg.Wait()
	log.Printf("[BalanceTracker] Stopped")
}

// GetLatestBalance returns the most recently fetched balance
func (bt *BalanceTracker) GetLatestBalance() (float64, time.Time) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.latestBalance, bt.lastUpdated
}

func (bt *BalanceTracker) trackLoop() {
	defer bt.wg.Done()

	// Do an immediate check on start
	bt.checkAndSaveBalance()

	ticker := time.NewTicker(bt.interval)
	defer ticker.Stop()

	for {
		select {
		case <-bt.stopCh:
			return
		case <-ticker.C:
			bt.checkAndSaveBalance()
		}
	}
}

func (bt *BalanceTracker) checkAndSaveBalance() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	balance, err := api.GetOnChainUSDCBalance(ctx, bt.walletAddress)
	if err != nil {
		log.Printf("[BalanceTracker] Error fetching balance: %v", err)
		return
	}

	// Update in-memory cache
	bt.mu.Lock()
	bt.latestBalance = balance
	bt.lastUpdated = time.Now()
	bt.mu.Unlock()

	// Save to database
	if err := bt.store.SaveBalanceHistory(ctx, bt.walletAddress, balance); err != nil {
		log.Printf("[BalanceTracker] Error saving balance: %v", err)
		return
	}

	// Log only significant changes or every minute
	// To avoid spamming logs, we just log silently
	// log.Printf("[BalanceTracker] Balance: $%.2f", balance)
}
