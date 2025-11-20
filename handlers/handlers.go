package handlers

import (
	"net/http"
	"strconv"
	"strings"

	"polymarket-analyzer/config"
	"polymarket-analyzer/models"
	"polymarket-analyzer/service"

	"github.com/gin-gonic/gin"
)

// Handler handles HTTP requests
type Handler struct {
	cfg     *config.Config
	service *service.Service
}

// NewHandler creates a new handler
func NewHandler(cfg *config.Config, svc *service.Service) *Handler {
	return &Handler{
		cfg:     cfg,
		service: svc,
	}
}

// GetTopUsers returns top users filtered by subject
func (h *Handler) GetTopUsers(c *gin.Context) {
	subjectStr := c.Query("subject")
	if subjectStr == "" {
		subjectStr = "all"
	}

	var subject models.Subject
	if subjectStr != "all" {
		subject = models.Subject(subjectStr)
	}

	limitStr := c.Query("limit")
	limit := h.cfg.Scoring.MaxRankResults
	if limit == 0 {
		limit = 10000
	}
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= limit {
			limit = l
		}
	}

	filters := parseFilters(c)

	rankings, err := h.service.GetTopUsersBySubject(c.Request.Context(), subject, limit, filters)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load traders"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"rankings": rankings,
		"subject":  subjectStr,
		"count":    len(rankings),
	})
}

// GetUserProfile returns detailed profile for a user
func (h *Handler) GetUserProfile(c *gin.Context) {
	userID := c.Param("id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user ID required"})
		return
	}

	user, err := h.service.GetUserProfile(c.Request.Context(), userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load user"})
		return
	}
	if user == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"user": user,
	})
}

// UserProfilePage renders a standalone profile view.
func (h *Handler) UserProfilePage(c *gin.Context) {
	userID := strings.ToLower(strings.TrimSpace(c.Param("id")))
	if userID == "" {
		c.HTML(http.StatusBadRequest, "error.html", gin.H{"error": "User ID required"})
		return
	}

	user, err := h.service.GetUserProfile(c.Request.Context(), userID)
	if err != nil {
		c.HTML(http.StatusInternalServerError, "error.html", gin.H{"error": "Failed to load user: " + err.Error()})
		return
	}
	if user == nil {
		c.HTML(http.StatusNotFound, "error.html", gin.H{"error": "User not found: " + userID})
		return
	}

	// Always use live fetch to get Type field and REDEEM activities
	// Use high limit to capture all trades (maker + taker) and all redemptions
	trades, _ := h.service.GetUserTradesLive(c.Request.Context(), userID, 100000)

	c.HTML(http.StatusOK, "profile.html", gin.H{
		"user":   user,
		"trades": trades,
	})
}

// GetUserTrades returns stored trades for a given user.
func (h *Handler) GetUserTrades(c *gin.Context) {
	userID := c.Param("id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user ID required"})
		return
	}

	limit := 200
	if limitStr := c.Query("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	live := strings.EqualFold(c.Query("live"), "true") || c.Query("live") == "1"

	var (
		trades []models.TradeDetail
		err    error
	)
	if live {
		trades, err = h.service.GetUserTradesLive(c.Request.Context(), userID, limit)
	} else {
		trades, err = h.service.GetUserTrades(c.Request.Context(), userID, limit)
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load trades"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"trades": trades,
	})
}

// GetUserPositions returns aggregated positions grouped by market+outcome
func (h *Handler) GetUserPositions(c *gin.Context) {
	userID := c.Param("id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user ID required"})
		return
	}

	positions, err := h.service.AggregateUserPositions(c.Request.Context(), userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to aggregate positions: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"positions": positions,
		"count":     len(positions),
	})
}

// GetSubjects returns list of available subjects
func (h *Handler) GetSubjects(c *gin.Context) {
	subjects := []gin.H{
		{"value": "all", "label": "All Subjects"},
		{"value": string(models.SubjectPolitics), "label": "Politics"},
		{"value": string(models.SubjectSports), "label": "Sports"},
		{"value": string(models.SubjectFinance), "label": "Finance"},
		{"value": string(models.SubjectGeopolitics), "label": "Geopolitics"},
		{"value": string(models.SubjectTech), "label": "Tech"},
		{"value": string(models.SubjectCulture), "label": "Culture"},
		{"value": string(models.SubjectEconomy), "label": "Economy"},
	}

	c.JSON(http.StatusOK, gin.H{
		"subjects": subjects,
	})
}

// GetAllImportedUsers returns all users in the database (typically manually imported)
func (h *Handler) GetAllImportedUsers(c *gin.Context) {
	// Get all users without subject filter
	rankings, err := h.service.GetTopUsersBySubject(c.Request.Context(), "", 10000, service.FilterOptions{})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load users"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"rankings": rankings,
		"count":    len(rankings),
	})
}

// Index serves the main page
func (h *Handler) Index(c *gin.Context) {
	c.HTML(http.StatusOK, "index.html", gin.H{
		"title": "Polymarket Top Traders",
	})
}

// ImportTopUsersRequest is the payload for importing a list of users.
type ImportTopUsersRequest struct {
	Addresses []string `json:"addresses"`
}

// ImportTopUsers handles bulk import of user trade history.
func (h *Handler) ImportTopUsers(c *gin.Context) {
	var req ImportTopUsersRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request: " + err.Error()})
		return
	}

	if len(req.Addresses) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no addresses provided"})
		return
	}

	// Clean up addresses (trim whitespace, remove empty strings)
	cleaned := make([]string, 0, len(req.Addresses))
	for _, addr := range req.Addresses {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			cleaned = append(cleaned, addr)
		}
	}

	if len(cleaned) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no valid addresses provided"})
		return
	}

	results, err := h.service.ImportTopUsers(c.Request.Context(), cleaned)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "import failed: " + err.Error()})
		return
	}

	successCount := 0
	for _, r := range results {
		if r.Success {
			successCount++
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"results":       results,
		"total":         len(results),
		"success_count": successCount,
		"failed_count":  len(results) - successCount,
	})
}

// DeleteUser removes a user and all their associated data.
func (h *Handler) DeleteUser(c *gin.Context) {
	userID := c.Param("id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user ID required"})
		return
	}

	if err := h.service.DeleteUser(c.Request.Context(), userID); err != nil {
		if strings.Contains(err.Error(), "not found") {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete user: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "User deleted successfully",
	})
}

func parseFilters(c *gin.Context) service.FilterOptions {
	var opts service.FilterOptions

	if v := parseIntPointer(c.Query("min_trades")); v != nil {
		opts.MinTrades = v
	}
	if v := parseIntPointer(c.Query("max_trades")); v != nil {
		opts.MaxTrades = v
	}
	if v := parsePercentPointer(c.Query("min_win_rate")); v != nil {
		opts.MinWinRate = v
	}
	if v := parsePercentPointer(c.Query("max_win_rate")); v != nil {
		opts.MaxWinRate = v
	}
	if v := parsePercentPointer(c.Query("min_consistency")); v != nil {
		opts.MinConsistency = v
	}
	if v := parsePercentPointer(c.Query("max_consistency")); v != nil {
		opts.MaxConsistency = v
	}
	if v := parseFloatPointer(c.Query("min_pnl")); v != nil {
		opts.MinPNL = v
	}
	if v := parseFloatPointer(c.Query("max_pnl")); v != nil {
		opts.MaxPNL = v
	}
	hide := strings.ToLower(strings.TrimSpace(c.Query("hide_red_flags")))
	opts.HideRedFlags = hide == "1" || hide == "true" || hide == "yes"

	return opts
}

func parseIntPointer(val string) *int {
	val = strings.TrimSpace(val)
	if val == "" {
		return nil
	}
	if i, err := strconv.Atoi(val); err == nil {
		return &i
	}
	return nil
}

func parseFloatPointer(val string) *float64 {
	val = strings.TrimSpace(val)
	if val == "" {
		return nil
	}
	if f, err := strconv.ParseFloat(val, 64); err == nil {
		return &f
	}
	return nil
}

func parsePercentPointer(val string) *float64 {
	ptr := parseFloatPointer(val)
	if ptr == nil {
		return nil
	}
	if *ptr > 1 {
		converted := *ptr / 100.0
		return &converted
	}
	return ptr
}
