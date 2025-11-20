package syncer

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"polymarket-analyzer/analyzer"
	"polymarket-analyzer/api"
	"polymarket-analyzer/config"
	"polymarket-analyzer/service"
	"polymarket-analyzer/storage"
)

// Worker runs periodic background sync jobs (markets, users, trades).
type Worker struct {
	apiClient *api.Client
	store     *storage.Store
	cfg       *config.Config
	svc       *service.Service

	stop chan struct{}
	wg   sync.WaitGroup
}

// NewWorker builds a new sync worker.
func NewWorker(apiClient *api.Client, store *storage.Store, cfg *config.Config, svc *service.Service) *Worker {
	return &Worker{
		apiClient: apiClient,
		store:     store,
		cfg:       cfg,
		svc:       svc,
		stop:      make(chan struct{}),
	}
}

// Start launches background goroutines.
func (w *Worker) Start() {
	w.startLoop("leaderboard-refresh", time.Duration(w.cfg.Sync.UserRefreshMins)*time.Minute, w.syncLeaderboard)
}

// Stop waits for goroutines to exit.
func (w *Worker) Stop() {
	close(w.stop)
	w.wg.Wait()
}

func (w *Worker) startLoop(name string, interval time.Duration, fn func(context.Context) error) {
	if interval <= 0 {
		interval = 15 * time.Minute
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Run immediately at startup
		if err := fn(context.Background()); err != nil {
			log.Printf("[sync] %s initial run failed: %v", name, err)
		}

		for {
			select {
			case <-w.stop:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), interval/2)
				if err := fn(ctx); err != nil {
					log.Printf("[sync] %s tick failed: %v", name, err)
				}
				cancel()
			}
		}
	}()
}

func (w *Worker) syncLeaderboard(ctx context.Context) error {
	start := time.Now()

	ranker := analyzer.NewRanker(w.cfg.Scoring)
	processor := analyzer.NewProcessor(ranker)
	builder := NewLeaderboardBuilder(ranker, processor, w.cfg.Ingestion)

	if err := w.collectMarkets(ctx, builder); err != nil {
		return err
	}
	if err := w.collectTrades(ctx, builder); err != nil {
		return err
	}
	if err := w.collectClosedPositions(ctx, builder); err != nil {
		return err
	}

	users := builder.BuildUsers()
	if len(users) == 0 {
		log.Println("[sync] leaderboard refresh produced no users")
		return nil
	}

	if err := w.store.ReplaceAllUsers(ctx, users); err != nil {
		return fmt.Errorf("replace users: %w", err)
	}
	if err := w.store.ReplaceTrades(ctx, builder.TradesByUser()); err != nil {
		return fmt.Errorf("replace trades: %w", err)
	}
	if w.svc != nil {
		w.svc.InvalidateCaches()
	}

	log.Printf("[sync] refreshed leaderboard with %d users in %s", len(users), time.Since(start))
	return nil
}

func (w *Worker) collectMarkets(ctx context.Context, builder *LeaderboardBuilder) error {
	if len(builder.Subjects()) == 0 {
		return fmt.Errorf("no subjects configured for ingestion")
	}

	limit := w.cfg.Sync.BatchSizeMarkets
	if limit <= 0 {
		limit = builder.cfg.MaxMarketsPerSubject * len(builder.Subjects()) * 2
	}

	openOnly := false
	markets, err := w.apiClient.ListMarkets(ctx, api.MarketQueryParams{
		Limit:  limit,
		Order:  "volume",
		Closed: &openOnly,
	})
	if err != nil {
		return fmt.Errorf("list markets: %w", err)
	}

	added := 0
	for _, market := range markets {
		if market.Closed != nil && *market.Closed {
			continue
		}
		subject, ok := builder.ClassifyMarket(market.Question, market.Description)
		if !ok {
			continue
		}
		builder.AddMarket(market, subject)
		added++
	}
	if added == 0 {
		return fmt.Errorf("no markets matched configured subjects")
	}
	log.Printf("[sync] collected %d markets across %d subjects", added, len(builder.subjectMarkets))
	return nil
}

func (w *Worker) collectTrades(ctx context.Context, builder *LeaderboardBuilder) error {
	maxTradesPerMarket := builder.cfg.MaxTradesPerMarket
	if maxTradesPerMarket <= 0 {
		maxTradesPerMarket = 1000
	}
	returningLimit := builder.cfg.TradeRequestLimit
	if returningLimit <= 0 {
		returningLimit = 100
	}

	takerOnly := true
	totalTrades := 0
	subjectsWithTrades := 0
	delay := w.requestDelay()
	maxUsersReached := false

subjectLoop:
	for _, subject := range builder.Subjects() {
		markets := builder.TopMarkets(subject)
		if len(markets) == 0 {
			continue
		}
		subjectTrades := 0
		for _, market := range markets {
			fetched := 0
			for fetched < maxTradesPerMarket {
				batch := min(returningLimit, maxTradesPerMarket-fetched)
				trades, err := w.apiClient.GetTrades(ctx, api.TradeQuery{
					Markets:   []string{market.ConditionID},
					Limit:     batch,
					Offset:    fetched,
					TakerOnly: &takerOnly,
				})
				if err != nil {
					return fmt.Errorf("fetch trades for market %s: %w", market.ID, err)
				}
				if len(trades) == 0 {
					break
				}
				for _, trade := range trades {
					builder.RecordTrade(trade, subject)
				}
				subjectTrades += len(trades)
				fetched += len(trades)
				if builder.cfg.MaxUsers > 0 && builder.UserCount() >= builder.cfg.MaxUsers {
					maxUsersReached = true
					break subjectLoop
				}
				if len(trades) < batch {
					break
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				if delay > 0 {
					time.Sleep(delay)
				}
			}
		}
		if subjectTrades > 0 {
			subjectsWithTrades++
			log.Printf("[sync] fetched %d trades for subject %s (%d markets)", subjectTrades, subject, len(markets))
			totalTrades += subjectTrades
		}
	}
	if totalTrades == 0 {
		return fmt.Errorf("no trades fetched for any subject")
	}
	if maxUsersReached {
		log.Printf("[sync] trade ingestion halted after reaching %d tracked users", builder.cfg.MaxUsers)
	}
	log.Printf("[sync] aggregated %d trades across %d subjects (%d unique users)", totalTrades, subjectsWithTrades, builder.UserCount())
	return nil
}

func (w *Worker) collectClosedPositions(ctx context.Context, builder *LeaderboardBuilder) error {
	limit := builder.cfg.TradeRequestLimit
	if limit <= 0 || limit > 50 {
		limit = 50
	}
	maxPerUser := builder.cfg.MaxClosedPositionsPerUser
	if maxPerUser <= 0 {
		maxPerUser = 50
	}

	addressLimit := builder.cfg.MaxUsers
	if addressLimit <= 0 || addressLimit > 150 {
		if addressLimit <= 0 {
			addressLimit = len(builder.UserAddresses())
		}
		if addressLimit > 150 {
			addressLimit = 150
		}
	}

	addresses := builder.TopUserAddresses(addressLimit)
	if len(addresses) == 0 {
		log.Println("[sync] no users available for closed position sync")
		return nil
	}

	delay := w.requestDelay()
	totalPositions := 0

	for _, addr := range addresses {
		fetched := 0
		for fetched < maxPerUser {
			batch := min(limit, maxPerUser-fetched)
			positions, err := w.apiClient.GetClosedPositions(ctx, api.ClosedPositionsQuery{
				User:          addr,
				Limit:         batch,
				Offset:        fetched,
				SortBy:        "REALIZEDPNL",
				SortDirection: "DESC",
			})
			if err != nil {
				if isRateLimitErr(err) {
					log.Printf("[sync] closed positions throttled for %s: %v", addr, err)
					if delay > 0 {
						time.Sleep(delay * 4)
					}
					break
				}
				return fmt.Errorf("fetch closed positions for %s: %w", addr, err)
			}
			if len(positions) == 0 {
				break
			}
			for _, pos := range positions {
				builder.RecordClosedPosition(addr, pos)
			}
			totalPositions += len(positions)
			fetched += len(positions)
			if len(positions) < batch {
				break
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if delay > 0 {
				time.Sleep(delay)
			}
		}
	}
	log.Printf("[sync] fetched %d closed positions across %d users", totalPositions, len(addresses))
	return nil
}

func (w *Worker) requestDelay() time.Duration {
	delay := time.Duration(w.cfg.Sync.RequestDelayMS) * time.Millisecond
	if delay <= 0 {
		return 250 * time.Millisecond
	}
	return delay
}

func isRateLimitErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "429") || strings.Contains(msg, "rate limit")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
