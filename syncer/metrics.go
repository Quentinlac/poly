// Package syncer provides metrics storage for copy trading performance.
package syncer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

const metricsKey = "copytrader:metrics"

// SystemMetrics represents combined system metrics
type SystemMetrics struct {
	CopyTrader CopyTraderMetrics `json:"copy_trader"`
	Detector   DetectorMetrics   `json:"detector"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

// MetricsStore handles storing and retrieving metrics
type MetricsStore struct {
	redis *redis.Client
}

// NewMetricsStore creates a new metrics store
func NewMetricsStore(redisClient *redis.Client) *MetricsStore {
	return &MetricsStore{redis: redisClient}
}

// SaveCopyTraderMetrics saves copy trader metrics to Redis
func (m *MetricsStore) SaveCopyTraderMetrics(ctx context.Context, metrics CopyTraderMetrics) error {
	// Get existing metrics
	var system SystemMetrics
	existing, err := m.redis.Get(ctx, metricsKey).Result()
	if err == nil {
		json.Unmarshal([]byte(existing), &system)
	}

	system.CopyTrader = metrics
	system.UpdatedAt = time.Now()

	data, err := json.Marshal(system)
	if err != nil {
		return err
	}

	return m.redis.Set(ctx, metricsKey, data, 24*time.Hour).Err()
}

// SaveDetectorMetrics saves detector metrics to Redis
func (m *MetricsStore) SaveDetectorMetrics(ctx context.Context, metrics DetectorMetrics) error {
	// Get existing metrics
	var system SystemMetrics
	existing, err := m.redis.Get(ctx, metricsKey).Result()
	if err == nil {
		json.Unmarshal([]byte(existing), &system)
	}

	system.Detector = metrics
	system.UpdatedAt = time.Now()

	data, err := json.Marshal(system)
	if err != nil {
		return err
	}

	return m.redis.Set(ctx, metricsKey, data, 24*time.Hour).Err()
}

// GetMetrics retrieves all metrics from Redis
func (m *MetricsStore) GetMetrics(ctx context.Context) (*SystemMetrics, error) {
	data, err := m.redis.Get(ctx, metricsKey).Result()
	if err != nil {
		if err == redis.Nil {
			return &SystemMetrics{}, nil
		}
		return nil, err
	}

	var metrics SystemMetrics
	if err := json.Unmarshal([]byte(data), &metrics); err != nil {
		return nil, err
	}

	return &metrics, nil
}

// LatencyStats provides detailed latency statistics
type LatencyStats struct {
	DetectionAvg    time.Duration `json:"detection_avg_ms"`
	DetectionFast   time.Duration `json:"detection_fastest_ms"`
	DetectionSlow   time.Duration `json:"detection_slowest_ms"`
	CopyAvg         time.Duration `json:"copy_avg_ms"`
	CopyFast        time.Duration `json:"copy_fastest_ms"`
	CopySlow        time.Duration `json:"copy_slowest_ms"`
	TotalAvg        time.Duration `json:"total_avg_ms"`
	TotalFast       time.Duration `json:"total_fastest_ms"`
	TotalSlow       time.Duration `json:"total_slowest_ms"`
}

// GetLatencyStats computes latency statistics from metrics
func (m *MetricsStore) GetLatencyStats(ctx context.Context) (*LatencyStats, error) {
	metrics, err := m.GetMetrics(ctx)
	if err != nil {
		return nil, err
	}

	return &LatencyStats{
		DetectionAvg:  metrics.Detector.AvgDetectionLatency,
		DetectionFast: metrics.Detector.FastestDetection,
		DetectionSlow: metrics.Detector.SlowestDetection,
		CopyAvg:       metrics.CopyTrader.AvgCopyLatency,
		CopyFast:      metrics.CopyTrader.FastestCopy,
		CopySlow:      metrics.CopyTrader.SlowestCopy,
		TotalAvg:      metrics.Detector.AvgDetectionLatency + metrics.CopyTrader.AvgCopyLatency,
		TotalFast:     metrics.Detector.FastestDetection + metrics.CopyTrader.FastestCopy,
		TotalSlow:     metrics.Detector.SlowestDetection + metrics.CopyTrader.SlowestCopy,
	}, nil
}
