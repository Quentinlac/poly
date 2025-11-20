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

	store, err := storage.New(cfg.Data.DBPath)
	if err != nil {
		log.Fatalf("failed to init storage: %v", err)
	}
	defer store.Close()

	baseURL := os.Getenv("POLYMARKET_API_URL")
	if baseURL == "" {
		baseURL = "https://clob.polymarket.com"
	}
	apiClient := api.NewClient(baseURL)
	svc := service.NewService(store, cfg, apiClient)

	// Start incremental worker for 5-second polling
	incrementalWorker := syncer.NewIncrementalWorker(apiClient, store, cfg, svc)
	incrementalWorker.Start()
	defer incrementalWorker.Stop()

	log.Println("[main] Incremental worker started (5-second polling for new trades)")

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

	// Routes
	r.GET("/", h.Index)
	r.GET("/api/users/top", h.GetTopUsers)
	r.GET("/api/users/imported", h.GetAllImportedUsers)
	r.GET("/api/users/:id", h.GetUserProfile)
	r.GET("/api/users/:id/trades", h.GetUserTrades)
	r.GET("/api/users/:id/positions", h.GetUserPositions)
	r.POST("/api/import-top-users", h.ImportTopUsers)
	r.DELETE("/api/users/:id", h.DeleteUser)
	r.GET("/api/subjects", h.GetSubjects)
	r.GET("/users/:id", h.UserProfilePage)

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
