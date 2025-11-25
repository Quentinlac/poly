package handlers

import (
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"polymarket-analyzer/config"
	"polymarket-analyzer/models"
	"polymarket-analyzer/service"
	"polymarket-analyzer/storage"

	"github.com/gin-gonic/gin"
)

var ethAddressRegex = regexp.MustCompile(`^0x[a-fA-F0-9]{40}$`)

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

// GetUserAnalysis returns decile analysis for all behavioral patterns
func (h *Handler) GetUserAnalysis(c *gin.Context) {
	userID := c.Param("id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user ID required"})
		return
	}

	analysis, err := h.service.GetUserAnalysis(c.Request.Context(), userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to analyze patterns: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, analysis)
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

	// Clean up and validate addresses
	cleaned := make([]string, 0, len(req.Addresses))
	invalidAddresses := make([]string, 0)
	for _, addr := range req.Addresses {
		addr = strings.ToLower(strings.TrimSpace(addr))
		if addr == "" {
			continue
		}
		if !ethAddressRegex.MatchString(addr) {
			invalidAddresses = append(invalidAddresses, addr)
			continue
		}
		cleaned = append(cleaned, addr)
	}

	if len(invalidAddresses) > 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":             "invalid Ethereum addresses provided",
			"invalid_addresses": invalidAddresses,
		})
		return
	}

	// Start async import job
	jobID := h.service.StartImportJob(c.Request.Context(), cleaned)

	c.JSON(http.StatusAccepted, gin.H{
		"message": "Import started",
		"job_id":  jobID,
		"count":   len(cleaned),
	})
}

// GetImportStatus returns the status of an import job
func (h *Handler) GetImportStatus(c *gin.Context) {
	jobID := c.Param("id")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "job ID required"})
		return
	}

	job := h.service.GetImportJob(jobID)
	if job == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}

	c.JSON(http.StatusOK, job)
}

// PrePopulateTokens fetches all tokens from CLOB API and saves them to database.
// This should be run once to pre-populate the token cache for fast lookups.
func (h *Handler) PrePopulateTokens(c *gin.Context) {
	count, err := h.service.PrePopulateTokens(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "pre-populate failed: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success":     true,
		"token_count": count,
		"message":     "Token pre-population complete",
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

// GetUserCopySettings returns copy trading settings for a user
func (h *Handler) GetUserCopySettings(c *gin.Context) {
	userID := c.Param("id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user ID required"})
		return
	}

	settings, err := h.service.GetUserCopySettings(c.Request.Context(), userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get copy settings: " + err.Error()})
		return
	}

	// Return default settings if none exist
	if settings == nil {
		settings = &storage.UserCopySettings{
			UserAddress: strings.ToLower(userID),
			Multiplier:  0.05, // Default 1/20th
			Enabled:     true,
			MinUSDC:     1.0,
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"settings": settings,
	})
}

// UpdateUserCopySettingsRequest is the payload for updating copy settings
type UpdateUserCopySettingsRequest struct {
	Multiplier *float64 `json:"multiplier"`
	Enabled    *bool    `json:"enabled"`
	MinUSDC    *float64 `json:"min_usdc"`
}

// UpdateUserCopySettings updates copy trading settings for a user
func (h *Handler) UpdateUserCopySettings(c *gin.Context) {
	userID := c.Param("id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user ID required"})
		return
	}

	var req UpdateUserCopySettingsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request: " + err.Error()})
		return
	}

	// Get existing settings or create defaults
	settings, err := h.service.GetUserCopySettings(c.Request.Context(), userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get settings: " + err.Error()})
		return
	}

	if settings == nil {
		settings = &storage.UserCopySettings{
			UserAddress: strings.ToLower(userID),
			Multiplier:  0.05,
			Enabled:     true,
			MinUSDC:     1.0,
		}
	}

	// Apply updates
	if req.Multiplier != nil {
		if *req.Multiplier <= 0 || *req.Multiplier > 1 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "multiplier must be between 0 and 1"})
			return
		}
		settings.Multiplier = *req.Multiplier
	}
	if req.Enabled != nil {
		settings.Enabled = *req.Enabled
	}
	if req.MinUSDC != nil {
		if *req.MinUSDC < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "min_usdc must be non-negative"})
			return
		}
		settings.MinUSDC = *req.MinUSDC
	}

	if err := h.service.SetUserCopySettings(c.Request.Context(), *settings); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update settings: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success":  true,
		"settings": settings,
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

// GetAnalyticsList returns filtered and sorted user analytics data
func (h *Handler) GetAnalyticsList(c *gin.Context) {
	filter := storage.UserAnalyticsFilter{
		SortBy:   c.DefaultQuery("sort", "pnl"),
		SortDesc: c.DefaultQuery("order", "desc") == "desc",
	}

	// Parse limit and offset
	if limitStr := c.Query("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			filter.Limit = l
		}
	}
	if filter.Limit == 0 {
		filter.Limit = 100
	}

	if offsetStr := c.Query("offset"); offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
			filter.Offset = o
		}
	}

	// Parse filters
	filter.MinPnL = parseFloatPointer(c.Query("min_pnl"))
	filter.MaxPnL = parseFloatPointer(c.Query("max_pnl"))
	filter.MinBets = parseIntPointer(c.Query("min_bets"))
	filter.MaxBets = parseIntPointer(c.Query("max_bets"))
	filter.MinPnLPercent = parseFloatPointer(c.Query("min_pnl_percent"))
	filter.MaxPnLPercent = parseFloatPointer(c.Query("max_pnl_percent"))

	// Boolean filters
	hideBots := strings.ToLower(strings.TrimSpace(c.Query("hide_bots")))
	filter.HideBots = hideBots == "1" || hideBots == "true"

	dataComplete := strings.ToLower(strings.TrimSpace(c.Query("data_complete")))
	if dataComplete == "1" || dataComplete == "true" {
		dc := true
		filter.DataComplete = &dc
	} else if dataComplete == "0" || dataComplete == "false" {
		dc := false
		filter.DataComplete = &dc
	}

	results, totalCount, err := h.service.GetUserAnalyticsList(c.Request.Context(), filter)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load analytics: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"analytics":   results,
		"count":       len(results),
		"total_count": totalCount,
		"offset":      filter.Offset,
		"limit":       filter.Limit,
	})
}

// PrivilegedKnowledgePage renders the privileged knowledge analysis page
func (h *Handler) PrivilegedKnowledgePage(c *gin.Context) {
	c.HTML(http.StatusOK, "privileged.html", gin.H{
		"title": "Privileged Knowledge Analysis",
	})
}

// GetPrivilegedKnowledgeAPI returns privileged knowledge analysis data
func (h *Handler) GetPrivilegedKnowledgeAPI(c *gin.Context) {
	// Parse time window (minutes)
	timeWindowStr := c.DefaultQuery("window", "5")
	timeWindow, err := strconv.Atoi(timeWindowStr)
	if err != nil || timeWindow <= 0 {
		timeWindow = 5
	}

	// Parse price threshold (percentage as decimal, e.g., 30 = 0.30)
	thresholdStr := c.DefaultQuery("threshold", "30")
	thresholdPct, err := strconv.ParseFloat(thresholdStr, 64)
	if err != nil || thresholdPct <= 0 {
		thresholdPct = 30
	}
	threshold := thresholdPct / 100.0 // Convert 30 to 0.30

	results, err := h.service.GetPrivilegedKnowledgeAnalysis(c.Request.Context(), timeWindow, threshold)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to analyze: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"users":         results,
		"count":         len(results),
		"time_window":   timeWindow,
		"threshold_pct": thresholdPct,
	})
}
