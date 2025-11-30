package syncer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"polymarket-analyzer/api"
	"polymarket-analyzer/storage"
)

// BinancePriceSyncer fetches and stores BTC/USDT 1-second klines from Binance
type BinancePriceSyncer struct {
	client  *api.BinanceClient
	store   *storage.PostgresStore
	symbol  string
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	// Configuration
	initialLookback time.Duration // How far back to fetch on initial sync (default: 2 weeks)
	updateInterval  time.Duration // How often to fetch new data (default: 1 hour)
}

// BinancePriceSyncerConfig holds configuration for the syncer
type BinancePriceSyncerConfig struct {
	Symbol          string        // Trading pair (default: BTCUSDT)
	InitialLookback time.Duration // Initial data fetch lookback (default: 14 days)
	UpdateInterval  time.Duration // Update frequency (default: 1 hour)
}

// NewBinancePriceSyncer creates a new price syncer
func NewBinancePriceSyncer(store *storage.PostgresStore, cfg BinancePriceSyncerConfig) *BinancePriceSyncer {
	if cfg.Symbol == "" {
		cfg.Symbol = "BTCUSDT"
	}
	if cfg.InitialLookback == 0 {
		cfg.InitialLookback = 14 * 24 * time.Hour // 2 weeks
	}
	if cfg.UpdateInterval == 0 {
		cfg.UpdateInterval = 1 * time.Hour
	}

	return &BinancePriceSyncer{
		client:          api.NewBinanceClient(),
		store:           store,
		symbol:          cfg.Symbol,
		initialLookback: cfg.InitialLookback,
		updateInterval:  cfg.UpdateInterval,
		stopCh:          make(chan struct{}),
	}
}

// Start begins the price syncing process
func (s *BinancePriceSyncer) Start(ctx context.Context) error {
	if s.running {
		return fmt.Errorf("syncer already running")
	}

	// Ensure table exists
	if err := s.store.EnsureBinancePriceTable(ctx); err != nil {
		return fmt.Errorf("ensure price table: %w", err)
	}

	s.running = true

	// Run initial sync
	if err := s.initialSync(ctx); err != nil {
		log.Printf("[BinancePriceSyncer] Initial sync error: %v", err)
		// Continue anyway - we'll catch up on hourly updates
	}

	// Start hourly update loop
	s.wg.Add(1)
	go s.updateLoop(ctx)

	log.Printf("[BinancePriceSyncer] Started for %s (update interval: %s)", s.symbol, s.updateInterval)
	return nil
}

// Stop halts the syncer
func (s *BinancePriceSyncer) Stop() {
	if !s.running {
		return
	}
	s.running = false
	close(s.stopCh)
	s.wg.Wait()
	log.Printf("[BinancePriceSyncer] Stopped")
}

// initialSync performs the initial data fetch with backfill
func (s *BinancePriceSyncer) initialSync(ctx context.Context) error {
	log.Printf("[BinancePriceSyncer] Starting initial sync (lookback: %s)...", s.initialLookback)

	// Check what data we already have
	latestTime, err := s.store.GetLatestBinancePrice(ctx, s.symbol)
	if err != nil {
		log.Printf("[BinancePriceSyncer] No existing data, will fetch full range")
		latestTime = time.Time{}
	} else {
		log.Printf("[BinancePriceSyncer] Latest data: %s", latestTime.Format(time.RFC3339))
	}

	// Determine start time
	now := time.Now().UTC()
	startTime := now.Add(-s.initialLookback)

	// If we have recent data, start from there (with 1 minute overlap for safety)
	if !latestTime.IsZero() && latestTime.After(startTime) {
		startTime = latestTime.Add(-1 * time.Minute)
		log.Printf("[BinancePriceSyncer] Resuming from %s", startTime.Format(time.RFC3339))
	}

	// Also check for gaps and backfill the oldest data
	oldestTime, err := s.store.GetOldestBinancePrice(ctx, s.symbol)
	if err == nil && !oldestTime.IsZero() {
		targetStart := now.Add(-s.initialLookback)
		if oldestTime.After(targetStart) {
			// We have a gap at the beginning - backfill it
			log.Printf("[BinancePriceSyncer] Backfilling gap from %s to %s",
				targetStart.Format(time.RFC3339), oldestTime.Format(time.RFC3339))
			if err := s.fetchAndStore(ctx, targetStart, oldestTime.Add(-1*time.Second)); err != nil {
				log.Printf("[BinancePriceSyncer] Backfill error: %v", err)
			}
		}
	}

	// Fetch from start to now
	return s.fetchAndStore(ctx, startTime, now)
}

// updateLoop runs periodic updates
func (s *BinancePriceSyncer) updateLoop(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			if err := s.update(ctx); err != nil {
				log.Printf("[BinancePriceSyncer] Update error: %v", err)
			}
		}
	}
}

// update fetches new data since last update
func (s *BinancePriceSyncer) update(ctx context.Context) error {
	// Get latest stored time
	latestTime, err := s.store.GetLatestBinancePrice(ctx, s.symbol)
	if err != nil {
		// No data - do full sync
		return s.initialSync(ctx)
	}

	// Fetch from latest - 1 minute (overlap for safety) to now
	startTime := latestTime.Add(-1 * time.Minute)
	endTime := time.Now().UTC()

	log.Printf("[BinancePriceSyncer] Updating from %s to %s",
		startTime.Format("2006-01-02 15:04:05"),
		endTime.Format("2006-01-02 15:04:05"))

	return s.fetchAndStore(ctx, startTime, endTime)
}

// fetchAndStore fetches klines and stores them in the database
func (s *BinancePriceSyncer) fetchAndStore(ctx context.Context, startTime, endTime time.Time) error {
	startTotal := time.Now()

	// Fetch 1-second klines
	klines, err := s.client.FetchKlinesRange(ctx, s.symbol, "1s", startTime, endTime)
	if err != nil {
		return fmt.Errorf("fetch klines: %w", err)
	}

	if len(klines) == 0 {
		log.Printf("[BinancePriceSyncer] No new klines to store")
		return nil
	}

	// Convert to storage type
	storageKlines := make([]storage.BinanceKline, len(klines))
	for i, k := range klines {
		storageKlines[i] = storage.BinanceKline{
			OpenTime:         k.OpenTime,
			Open:             k.Open,
			High:             k.High,
			Low:              k.Low,
			Close:            k.Close,
			Volume:           k.Volume,
			CloseTime:        k.CloseTime,
			QuoteAssetVolume: k.QuoteAssetVolume,
			NumTrades:        k.NumTrades,
		}
	}

	// Store in batches
	const batchSize = 10000
	totalStored := 0

	for i := 0; i < len(storageKlines); i += batchSize {
		end := i + batchSize
		if end > len(storageKlines) {
			end = len(storageKlines)
		}

		batch := storageKlines[i:end]
		if err := s.store.SaveBinancePrices(ctx, s.symbol, batch); err != nil {
			return fmt.Errorf("save batch %d-%d: %w", i, end, err)
		}
		totalStored += len(batch)

		// Log progress for large syncs
		if len(storageKlines) > batchSize {
			log.Printf("[BinancePriceSyncer] Stored %d/%d klines (%.1f%%)",
				totalStored, len(storageKlines), float64(totalStored)/float64(len(storageKlines))*100)
		}
	}

	duration := time.Since(startTotal)
	log.Printf("[BinancePriceSyncer] Stored %d klines for %s in %s (%.0f klines/sec)",
		totalStored, s.symbol, duration.Round(time.Millisecond),
		float64(totalStored)/duration.Seconds())

	return nil
}

// ForceSyncRange forces a sync for a specific time range (useful for manual backfill)
func (s *BinancePriceSyncer) ForceSyncRange(ctx context.Context, startTime, endTime time.Time) error {
	log.Printf("[BinancePriceSyncer] Force sync from %s to %s",
		startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
	return s.fetchAndStore(ctx, startTime, endTime)
}

// GetPriceAt returns the price at a specific timestamp (or closest before)
func (s *BinancePriceSyncer) GetPriceAt(ctx context.Context, timestamp time.Time) (float64, error) {
	return s.store.GetBinancePriceAt(ctx, s.symbol, timestamp)
}

// GetPriceRange returns all prices in a time range
func (s *BinancePriceSyncer) GetPriceRange(ctx context.Context, startTime, endTime time.Time) ([]storage.BinancePrice, error) {
	return s.store.GetBinancePriceRange(ctx, s.symbol, startTime, endTime)
}

// GetStats returns sync statistics
func (s *BinancePriceSyncer) GetStats(ctx context.Context) (map[string]interface{}, error) {
	oldest, _ := s.store.GetOldestBinancePrice(ctx, s.symbol)
	latest, _ := s.store.GetLatestBinancePrice(ctx, s.symbol)
	count, _ := s.store.GetBinancePriceCount(ctx, s.symbol)

	return map[string]interface{}{
		"symbol":       s.symbol,
		"oldest_time":  oldest,
		"latest_time":  latest,
		"total_klines": count,
		"running":      s.running,
	}, nil
}
