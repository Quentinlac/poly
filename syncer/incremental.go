package syncer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"polymarket-analyzer/analyzer"
	"polymarket-analyzer/api"
	"polymarket-analyzer/config"
	"polymarket-analyzer/models"
	"polymarket-analyzer/service"
	"polymarket-analyzer/storage"
)

// Note: api import is still needed for NewIncrementalWorker signature

// IncrementalWorker polls users for new trades incrementally (only fetches new data).
// This is much more efficient than full refreshes and can run frequently (5 seconds).
type IncrementalWorker struct {
	apiClient *api.Client
	store     storage.DataStore
	cfg       *config.Config
	svc       *service.Service
	ranker    *analyzer.Ranker
	processor *analyzer.Processor

	stop       chan struct{}
	wg         sync.WaitGroup
	userQueue  []models.User
	queueMutex sync.RWMutex
}

// NewIncrementalWorker creates a new incremental polling worker.
func NewIncrementalWorker(apiClient *api.Client, store storage.DataStore, cfg *config.Config, svc *service.Service) *IncrementalWorker {
	ranker := analyzer.NewRanker(cfg.Scoring)
	processor := analyzer.NewProcessor(ranker)

	return &IncrementalWorker{
		apiClient: apiClient,
		store:     store,
		cfg:       cfg,
		svc:       svc,
		ranker:    ranker,
		processor: processor,
		stop:      make(chan struct{}),
	}
}

// Start launches the incremental polling loop.
func (w *IncrementalWorker) Start() {
	interval := 2 * time.Second // 2-second interval for faster trade detection
	log.Printf("[incremental] starting with %v interval", interval)

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Initial load of user queue
		if err := w.refreshUserQueue(context.Background()); err != nil {
			log.Printf("[incremental] failed to load user queue: %v", err)
		}

		for {
			select {
			case <-w.stop:
				return
			case <-ticker.C:
				// Use 30 second timeout - Goldsky subgraph can be slow from cloud environments
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				if err := w.tick(ctx); err != nil {
					log.Printf("[incremental] tick error: %v", err)
				}
				cancel()
			}
		}
	}()
}

// Stop gracefully shuts down the worker.
func (w *IncrementalWorker) Stop() {
	close(w.stop)
	w.wg.Wait()
}

// tick performs one incremental sync cycle.
func (w *IncrementalWorker) tick(ctx context.Context) error {
	w.queueMutex.RLock()
	users := w.userQueue
	w.queueMutex.RUnlock()

	if len(users) == 0 {
		// No users to sync, refresh queue
		if err := w.refreshUserQueue(ctx); err != nil {
			return fmt.Errorf("refresh user queue: %w", err)
		}
		return nil
	}

	// Stagger: Only sync a batch of users per tick (e.g., 20 users)
	batchSize := w.cfg.Sync.BatchSizeUsers
	if batchSize <= 0 || batchSize > 50 {
		batchSize = 20 // Default to 20 users per 5-second tick = 4 users/second
	}

	if batchSize > len(users) {
		batchSize = len(users)
	}

	batch := users[:batchSize]
	remaining := users[batchSize:]

	// Update queue (remove synced users)
	w.queueMutex.Lock()
	w.userQueue = remaining
	w.queueMutex.Unlock()

	// Sync the batch
	synced := 0
	for _, user := range batch {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := w.syncUser(ctx, user); err != nil {
			log.Printf("[incremental] failed to sync user %s: %v", user.Address, err)
			continue
		}
		synced++
	}

	if synced > 0 {
		log.Printf("[incremental] synced %d/%d users (%d remaining in queue)", synced, len(batch), len(remaining))
	}

	return nil
}

// syncUser fetches only NEW trades for a single user since their last sync.
func (w *IncrementalWorker) syncUser(ctx context.Context, user models.User) error {
	now := time.Now()

	// Determine the timestamp to fetch from
	var sinceTime time.Time
	if !user.LastSyncedAt.IsZero() {
		sinceTime = user.LastSyncedAt
	} else if !user.LastActive.IsZero() {
		// If never synced, start from last known activity
		sinceTime = user.LastActive.Add(-24 * time.Hour) // Go back 24h to catch recent trades
	} else {
		// Never synced, fetch last 7 days
		sinceTime = now.Add(-7 * 24 * time.Hour)
	}

	// Fetch new trades using shared service (fetches maker, taker, and redeems)
	trades, err := w.svc.FetchIncrementalTrades(ctx, user.Address, sinceTime.Unix())
	if err != nil {
		return fmt.Errorf("fetch trades: %w", err)
	}

	if len(trades) == 0 {
		// No new trades, just update last_synced_at
		user.LastSyncedAt = now
		if err := w.store.SaveUserSnapshot(ctx, user); err != nil {
			return fmt.Errorf("update sync time: %w", err)
		}
		return nil
	}

	// Process new trades - check for duplicates before inserting
	// Get existing trade IDs to avoid duplicates (lightweight query - only IDs)
	existingIDs, err := w.store.ListUserTradeIDs(ctx, user.Address, 10000)
	if err != nil {
		return fmt.Errorf("list existing trades: %w", err)
	}

	existingIDMap := make(map[string]bool, len(existingIDs))
	for _, id := range existingIDs {
		existingIDMap[id] = true
	}

	// Filter out duplicates
	newTradeDetails := make([]models.TradeDetail, 0, len(trades))
	for _, trade := range trades {
		if !existingIDMap[trade.ID] {
			newTradeDetails = append(newTradeDetails, trade)
		}
	}

	// Only save and update metrics if we have truly new trades
	if len(newTradeDetails) == 0 {
		// No new trades - just update sync time
		user.LastSyncedAt = now
		if err := w.store.SaveUserSnapshot(ctx, user); err != nil {
			return fmt.Errorf("update sync time: %w", err)
		}
		return nil
	}

	// Save new trades - do NOT mark as processed so copy trader can pick them up
	if err := w.store.SaveTrades(ctx, newTradeDetails, false); err != nil {
		return fmt.Errorf("save trades: %w", err)
	}

	// Invalidate analysis cache since trades changed
	if err := w.store.InvalidateAnalysisCache(ctx, user.Address); err != nil {
		log.Printf("[incremental] warning: failed to invalidate analysis cache: %v", err)
	}

	// Recalculate user metrics (only if we have new trades)
	allTrades, err := w.store.ListUserTrades(ctx, user.Address, 10000)
	if err != nil {
		return fmt.Errorf("list all trades: %w", err)
	}

	// Rebuild user from trades
	updatedUser := w.processor.BuildUserFromTrades(user.Address, allTrades)
	updatedUser.LastSyncedAt = now

	// Save updated user
	if err := w.store.SaveUserSnapshot(ctx, updatedUser); err != nil {
		return fmt.Errorf("save user snapshot: %w", err)
	}

	// Invalidate cache for this user
	if w.svc != nil {
		w.svc.InvalidateCaches()

		// Trigger background update of materialized positions
		go func() {
			if err := w.svc.UpdateUserPositions(context.Background(), user.Address); err != nil {
				log.Printf("[incremental] failed to update positions for %s: %v", user.Address, err)
			}
		}()
	}

	log.Printf("[incremental] synced %s: %d new trades (API returned %d, %d were duplicates)",
		user.Address[:8], len(newTradeDetails), len(trades), len(trades)-len(newTradeDetails))
	return nil
}

// refreshUserQueue reloads all tracked users from the database.
func (w *IncrementalWorker) refreshUserQueue(ctx context.Context) error {
	users, err := w.store.ListUsers(ctx, "", 10000)
	if err != nil {
		return fmt.Errorf("list users: %w", err)
	}

	// Sort by last_synced_at (oldest first = highest priority)
	// This ensures we cycle through all users evenly
	sortUsersByLastSynced(users)

	w.queueMutex.Lock()
	w.userQueue = users
	w.queueMutex.Unlock()

	log.Printf("[incremental] loaded %d users into sync queue", len(users))
	return nil
}

// sortUsersByLastSynced sorts users by last_synced_at ascending (oldest first).
func sortUsersByLastSynced(users []models.User) {
	// Simple bubble sort (fine for reasonable user counts)
	for i := 0; i < len(users); i++ {
		for j := i + 1; j < len(users); j++ {
			if users[i].LastSyncedAt.After(users[j].LastSyncedAt) {
				users[i], users[j] = users[j], users[i]
			}
		}
	}
}
