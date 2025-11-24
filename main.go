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

	// Start incremental worker for tracked users (2-second polling)
	incrementalWorker := syncer.NewIncrementalWorker(apiClient, store, cfg, svc)
	incrementalWorker.Start()
	defer incrementalWorker.Stop()

	log.Println("[main] Incremental worker started (2-second polling for tracked users)")

	// Start global trade monitor for platform-wide monitoring
	subgraphClient := api.NewSubgraphClient()

	// Create The Graph client for redemptions (optional, requires API key)
	var theGraphClient *api.TheGraphClient
	theGraphAPIKey := os.Getenv("THEGRAPH_API_KEY")
	if theGraphAPIKey != "" {
		theGraphClient = api.NewTheGraphClient(theGraphAPIKey)
		log.Println("[main] The Graph client configured for redemptions (5-minute polling)")
	} else {
		log.Println("[main] THEGRAPH_API_KEY not set - redemptions disabled")
	}

	globalMonitor := syncer.NewGlobalTradeMonitor(subgraphClient, theGraphClient, store)
	globalMonitor.Start()
	defer globalMonitor.Stop()

	log.Println("[main] Global trade monitor started (trades: 2s, redemptions: 5min)")

	// Copy trader has been moved to cmd/worker/main.go
	// Run it separately: go run cmd/worker/main.go

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

	// API routes with query parameter validation
	api := r.Group("/api")
	api.Use(middleware.ValidateQueryParams())
	{
		api.GET("/users/top", h.GetTopUsers)
		api.GET("/users/imported", h.GetAllImportedUsers)
		api.GET("/analytics", h.GetAnalyticsList)
		api.POST("/import-top-users", h.ImportTopUsers)
		api.POST("/pre-populate-tokens", h.PrePopulateTokens)
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
