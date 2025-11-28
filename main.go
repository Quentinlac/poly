package main

import (
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

	// Incremental sync is handled by analytics-worker only
	// Main app focuses on: API serving + trade execution (critical path)
	log.Println("[main] Incremental sync disabled (handled by analytics-worker)")

	// Initialize trade executor for copy trading (critical path)
	tradeExecutor, err := syncer.NewTradeExecutor(store, apiClient)
	if err != nil {
		log.Printf("[main] Warning: Trade executor not available: %v", err)
		log.Println("[main] Copy trade execution will be disabled")
	} else {
		log.Println("[main] Trade executor initialized (critical path for copy trading)")
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
	apiGroup := r.Group("/api")
	{
		apiGroup.GET("/top-users", h.GetTopUsers)
		apiGroup.GET("/users/imported", h.GetAllImportedUsers)
		apiGroup.POST("/import-top-users", h.ImportTopUsers)
		apiGroup.GET("/import-status/:id", h.GetImportStatus)
		apiGroup.GET("/analytics", h.GetAnalyticsList)
		apiGroup.GET("/copy-trade-metrics", h.GetCopyTradeMetrics)
	}

	// Internal API for worker communication (critical path - trade execution)
	internalAPI := r.Group("/api/internal")
	{
		internalAPI.POST("/execute-copy-trade", func(c *gin.Context) {
			if tradeExecutor == nil {
				c.JSON(500, gin.H{"error": "trade executor not available"})
				return
			}
			tradeExecutor.HandleExecuteRequest(c)
		})
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
