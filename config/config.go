package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// ServerConfig controls HTTP server behavior.
type ServerConfig struct {
	Port              int `yaml:"port"`
	ReadTimeoutMS     int `yaml:"read_timeout_ms"`
	WriteTimeoutMS    int `yaml:"write_timeout_ms"`
	ShutdownTimeoutMS int `yaml:"shutdown_timeout_ms"`
}

// ScoringConfig defines ranking parameters.
type ScoringConfig struct {
	MinTrades         int     `yaml:"min_trades"`
	MinWinRate        float64 `yaml:"min_win_rate"`
	TradeWeight       float64 `yaml:"trade_weight"`
	PNLWeight         float64 `yaml:"pnl_weight"`
	WinRateWeight     float64 `yaml:"win_rate_weight"`
	ConsistencyWeight float64 `yaml:"consistency_weight"`
	PNLScale          float64 `yaml:"pnl_scale"`
	MaxRankResults    int     `yaml:"max_rank_results"`
}

// RiskConfig controls learning-budget nudges.
type RiskConfig struct {
	MaxPositionPct       float64 `yaml:"max_position_pct"`
	LossCapMonthly       float64 `yaml:"loss_cap_monthly"`
	WarningRecentLossPct float64 `yaml:"warning_recent_loss_pct"`
}

// CacheConfig defines TTLs for cached data.
type CacheConfig struct {
	RankingTTLMins int `yaml:"ranking_ttl_minutes"`
	ProfileTTLMins int `yaml:"profile_ttl_minutes"`
}

// SyncConfig controls background refresh cadence.
type SyncConfig struct {
	MarketRefreshMins int `yaml:"market_refresh_minutes"`
	UserRefreshMins   int `yaml:"user_refresh_minutes"`
	BatchSizeMarkets  int `yaml:"batch_size_markets"`
	BatchSizeUsers    int `yaml:"batch_size_users"`
	RequestDelayMS    int `yaml:"request_delay_ms"`
}

// DataConfig contains persistence-related settings.
type DataConfig struct {
	DBPath string `yaml:"db_path"`
}

// IngestionConfig controls API aggregation limits.
type IngestionConfig struct {
	Subjects                 []string `yaml:"subjects"`
	MaxMarketsPerSubject     int      `yaml:"max_markets_per_subject"`
	MaxTradesPerMarket       int      `yaml:"max_trades_per_market"`
	TradeRequestLimit        int      `yaml:"trade_request_limit"`
	MaxUsers                 int      `yaml:"max_users"`
	MaxClosedPositionsPerUser int     `yaml:"max_closed_positions_per_user"`
}

// Config aggregates all app configuration knobs.
type Config struct {
	Server  ServerConfig  `yaml:"server"`
	Scoring ScoringConfig `yaml:"scoring"`
	Risk    RiskConfig    `yaml:"risk"`
	Cache   CacheConfig   `yaml:"cache"`
	Sync    SyncConfig    `yaml:"sync"`
	Data    DataConfig    `yaml:"data"`
	Ingestion IngestionConfig `yaml:"ingestion"`
}

// Load reads configuration from disk, falling back to defaults.
func Load(path string) (*Config, error) {
	cfg := Default()

	configPath := path
	if configPath == "" {
		configPath = filepath.Join("config", "default.yaml")
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &cfg, nil
		}
		return nil, fmt.Errorf("config: unable to read %s: %w", configPath, err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: unable to parse %s: %w", configPath, err)
	}

	cfg.applyDefaults()
	return &cfg, nil
}

// Default returns baseline configuration values.
func Default() Config {
	return Config{
		Server: ServerConfig{
			Port:              8081,
			ReadTimeoutMS:     10000,
			WriteTimeoutMS:    10000,
			ShutdownTimeoutMS: 5000,
		},
		Scoring: ScoringConfig{
			MinTrades:         100,
			MinWinRate:        0.55,
			TradeWeight:       0.25,
			PNLWeight:         0.30,
			WinRateWeight:     0.25,
			ConsistencyWeight: 0.20,
			PNLScale:          1000,
			MaxRankResults:    10000,
		},
		Risk: RiskConfig{
			MaxPositionPct:       0.10,
			LossCapMonthly:       300,
			WarningRecentLossPct: 0.25,
		},
		Cache: CacheConfig{
			RankingTTLMins: 10,
			ProfileTTLMins: 5,
		},
		Sync: SyncConfig{
			MarketRefreshMins: 60,
			UserRefreshMins:   30,
			BatchSizeMarkets:  200,
			BatchSizeUsers:    50, // 50 users per 2s tick for faster trade detection
			RequestDelayMS:    250,
		},
		Data: DataConfig{
			DBPath: "data/polymarket.db",
		},
		Ingestion: IngestionConfig{
			Subjects: []string{
				"politics",
				"sports",
				"finance",
				"geopolitics",
				"tech",
				"culture",
				"economy",
			},
			MaxMarketsPerSubject:      8,
			MaxTradesPerMarket:        100,
			TradeRequestLimit:         25,
			MaxUsers:                  300,
			MaxClosedPositionsPerUser: 25,
		},
	}
}

func (c *Config) applyDefaults() {
	def := Default()

	if c.Server.Port == 0 {
		c.Server.Port = def.Server.Port
	}
	if c.Server.ReadTimeoutMS == 0 {
		c.Server.ReadTimeoutMS = def.Server.ReadTimeoutMS
	}
	if c.Server.WriteTimeoutMS == 0 {
		c.Server.WriteTimeoutMS = def.Server.WriteTimeoutMS
	}
	if c.Server.ShutdownTimeoutMS == 0 {
		c.Server.ShutdownTimeoutMS = def.Server.ShutdownTimeoutMS
	}

	if c.Scoring.MinTrades == 0 {
		c.Scoring.MinTrades = def.Scoring.MinTrades
	}
	if c.Scoring.MinWinRate == 0 {
		c.Scoring.MinWinRate = def.Scoring.MinWinRate
	}
	if c.Scoring.TradeWeight == 0 {
		c.Scoring.TradeWeight = def.Scoring.TradeWeight
	}
	if c.Scoring.PNLWeight == 0 {
		c.Scoring.PNLWeight = def.Scoring.PNLWeight
	}
	if c.Scoring.WinRateWeight == 0 {
		c.Scoring.WinRateWeight = def.Scoring.WinRateWeight
	}
	if c.Scoring.ConsistencyWeight == 0 {
		c.Scoring.ConsistencyWeight = def.Scoring.ConsistencyWeight
	}
	if c.Scoring.PNLScale == 0 {
		c.Scoring.PNLScale = def.Scoring.PNLScale
	}
	if c.Scoring.MaxRankResults == 0 {
		c.Scoring.MaxRankResults = def.Scoring.MaxRankResults
	}

	if c.Risk.MaxPositionPct == 0 {
		c.Risk.MaxPositionPct = def.Risk.MaxPositionPct
	}
	if c.Risk.LossCapMonthly == 0 {
		c.Risk.LossCapMonthly = def.Risk.LossCapMonthly
	}
	if c.Risk.WarningRecentLossPct == 0 {
		c.Risk.WarningRecentLossPct = def.Risk.WarningRecentLossPct
	}

	if c.Cache.RankingTTLMins == 0 {
		c.Cache.RankingTTLMins = def.Cache.RankingTTLMins
	}
	if c.Cache.ProfileTTLMins == 0 {
		c.Cache.ProfileTTLMins = def.Cache.ProfileTTLMins
	}

	if c.Sync.MarketRefreshMins == 0 {
		c.Sync.MarketRefreshMins = def.Sync.MarketRefreshMins
	}
	if c.Sync.UserRefreshMins == 0 {
		c.Sync.UserRefreshMins = def.Sync.UserRefreshMins
	}
	if c.Sync.BatchSizeMarkets == 0 {
		c.Sync.BatchSizeMarkets = def.Sync.BatchSizeMarkets
	}
	if c.Sync.BatchSizeUsers == 0 {
		c.Sync.BatchSizeUsers = def.Sync.BatchSizeUsers
	}
	if c.Sync.RequestDelayMS == 0 {
		c.Sync.RequestDelayMS = def.Sync.RequestDelayMS
	}
	if c.Data.DBPath == "" {
		c.Data.DBPath = def.Data.DBPath
	}
	if len(c.Ingestion.Subjects) == 0 {
		c.Ingestion.Subjects = def.Ingestion.Subjects
	}
	if c.Ingestion.MaxMarketsPerSubject == 0 {
		c.Ingestion.MaxMarketsPerSubject = def.Ingestion.MaxMarketsPerSubject
	}
	if c.Ingestion.MaxTradesPerMarket == 0 {
		c.Ingestion.MaxTradesPerMarket = def.Ingestion.MaxTradesPerMarket
	}
	if c.Ingestion.TradeRequestLimit == 0 {
		c.Ingestion.TradeRequestLimit = def.Ingestion.TradeRequestLimit
	}
	if c.Ingestion.MaxUsers == 0 {
		c.Ingestion.MaxUsers = def.Ingestion.MaxUsers
	}
	if c.Ingestion.MaxClosedPositionsPerUser == 0 {
		c.Ingestion.MaxClosedPositionsPerUser = def.Ingestion.MaxClosedPositionsPerUser
	}
}
