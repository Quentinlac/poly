package syncer

import (
	"context"
	"log"
	"sync"
	"time"

	"polymarket-analyzer/storage"
)

// PrivilegedWorker computes privileged knowledge analysis in the background
type PrivilegedWorker struct {
	store    *storage.PostgresStore
	interval time.Duration
	stop     chan struct{}
	wg       sync.WaitGroup
}

// All combinations to compute
var privilegedCombinations = []struct {
	Window    int // minutes
	Threshold int // percentage
}{
	{5, 30}, {5, 50}, {5, 80},
	{10, 30}, {10, 50}, {10, 80},
	{30, 30}, {30, 50}, {30, 80},
	{120, 30}, {120, 50}, {120, 80},
	{360, 30}, {360, 50}, {360, 80},
}

// NewPrivilegedWorker creates a new privileged knowledge worker
func NewPrivilegedWorker(store *storage.PostgresStore) *PrivilegedWorker {
	return &PrivilegedWorker{
		store:    store,
		interval: 15 * time.Minute,
		stop:     make(chan struct{}),
	}
}

// Start begins the background computation
func (w *PrivilegedWorker) Start() {
	log.Printf("[Privileged] Starting worker with %v interval", w.interval)

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		// Run immediately on startup
		w.computeAll()

		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()

		for {
			select {
			case <-w.stop:
				log.Printf("[Privileged] Worker stopped")
				return
			case <-ticker.C:
				w.computeAll()
			}
		}
	}()
}

// Stop halts the worker
func (w *PrivilegedWorker) Stop() {
	close(w.stop)
	w.wg.Wait()
}

// computeAll computes all combinations sequentially
func (w *PrivilegedWorker) computeAll() {
	startTime := time.Now()
	log.Printf("[Privileged] Starting computation of %d combinations...", len(privilegedCombinations))

	successful := 0
	for _, combo := range privilegedCombinations {
		// Use a longer timeout for heavy queries (5 minutes per query)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

		err := w.store.ComputeAndSavePrivilegedAnalysis(ctx, combo.Window, combo.Threshold)
		cancel()

		if err != nil {
			log.Printf("[Privileged] Error computing %dmin/+%d%%: %v", combo.Window, combo.Threshold, err)
		} else {
			successful++
		}
	}

	duration := time.Since(startTime)
	log.Printf("[Privileged] Completed %d/%d combinations in %v", successful, len(privilegedCombinations), duration.Round(time.Second))
}
