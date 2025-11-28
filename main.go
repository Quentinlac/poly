package main

import (
	"context"
	"html/template"
	"log"
	"os"
	"strconv"
	"time"

	"polymarket-analyzer/api"
	"polymarket-analyzer/config"
	"polymarket-analyzer/handlers"
	"polymarket-analyzer/middleware"
	"polymarket-analyzer/service"
	"polymarket-analyzer/storage"
	"polymarket-analyzer/syncer"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using defaults")
	}

	cfgPath := os.Getenv("POLYMARKET_CONFIG")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	store, err := storage.NewPostgres()
	if err != nil {
		log.Fatalf("failed to init PostgreSQL storage: %v", err)
	}
	defer store.Close()

	log.Println("[main] PostgreSQL + Redis storage initialized")

	baseURL := os.Getenv("POLYMARKET_API_URL")
	if baseURL == "" {
		baseURL = "https://clob.polymarket.com"
	}
	apiClient := api.NewClient(baseURL)
	svc := service.NewService(store, cfg, apiClient)

	// Start incremental worker for tracked users (2-second polling)
	incrementalWorker := syncer.NewIncrementalWorker(apiClient, store, cfg, svc)
	incrementalWorker.Start()
	defer incrementalWorker.Stop()

	log.Println("[main] Incremental worker started (2-second polling for tracked users)")

	// Start copy trader (copies trades from tracked users to our account)
	copyConfig := syncer.CopyTraderConfig{
		Enabled:          true,
		Multiplier:       getEnvFloat("COPY_TRADER_MULTIPLIER", 0.05),
		MinOrderUSDC:     getEnvFloat("COPY_TRADER_MIN_USDC", 1.0),
		MaxPriceSlippage: getEnvFloat("COPY_TRADER_MAX_SLIPPAGE", 0.20), // 20% max above trader's price
		CheckIntervalSec: 1,                                             // 1 second for faster copy execution
	}

	log.Printf("[main] Copy trader config: multiplier=%.2f, minOrder=$%.2f, maxSlippage=%.0f%%, interval=%ds",
		copyConfig.Multiplier, copyConfig.MinOrderUSDC, copyConfig.MaxPriceSlippage*100, copyConfig.CheckIntervalSec)

	copyTrader, err := syncer.NewCopyTrader(store, apiClient, copyConfig)
	if err != nil {
		log.Printf("[main] Warning: Failed to create copy trader: %v", err)
		log.Println("[main] Copy trader will be disabled")
	} else {
		ctx := context.Background()
		if err := copyTrader.Start(ctx); err != nil {
			log.Printf("[main] Warning: Failed to start copy trader: %v", err)
		} else {
			defer copyTrader.Stop()
			log.Println("[main] Copy trader started successfully")
		}
	}

	// Set up router
	r := gin.Default()

	funcMap := template.FuncMap{
		"mul100": func(v float64) float64 {
			return v * 100
		},
		"mul": func(a, b float64) float64 {
			return a * b
		},
		"formatTime": func(t time.Time) string {
			if t.IsZero() {
				return ""
			}
			return t.Local().Format("2006-01-02 15:04")
		},
	}
	tmpl := template.Must(template.New("").Funcs(funcMap).ParseGlob("templates/*"))
	r.SetHTMLTemplate(tmpl)

	// Serve static files
	r.Static("/static", "./static")

	// Initialize handlers
	h := handlers.NewHandler(cfg, svc)

	// Apply global middleware
	r.Use(middleware.BasicAuth())

	// Public routes (no additional validation needed)
	r.GET("/", h.Index)
	r.GET("/api/subjects", h.GetSubjects)

	// API routes
	api := r.Group("/api")
	{
		api.GET("/top-users", h.GetTopUsers)
		api.GET("/users/imported", h.GetAllImportedUsers)
		api.POST("/import-top-users", h.ImportTopUsers)
		api.GET("/import-status/:id", h.GetImportStatus)
		api.GET("/analytics", h.GetAnalyticsList)
		api.GET("/copy-trade-metrics", h.GetCopyTradeMetrics) // Real-time copy trading performance metrics
	}

	// User-specific routes with ID validation
	userRoutes := r.Group("/api/users/:id")
	userRoutes.Use(middleware.ValidateUserID())
	{
		userRoutes.GET("", h.GetUserProfile)
		userRoutes.GET("/trades", h.GetUserTrades)
		userRoutes.GET("/positions", h.GetUserPositions)
		userRoutes.GET("/analysis", h.GetUserAnalysis)
		userRoutes.DELETE("", h.DeleteUser)
		userRoutes.GET("/copy-settings", h.GetUserCopySettings)
		userRoutes.PUT("/copy-settings", h.UpdateUserCopySettings)
	}

	// HTML user profile page with ID validation
	r.GET("/users/:id", middleware.ValidateUserID(), h.UserProfilePage)

	// Get port from environment or use default
	port := os.Getenv("PORT")
	if port == "" {
		port = strconv.Itoa(cfg.Server.Port)
	}

	log.Printf("Server starting on http://localhost:%s", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatal(err)
	}
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
