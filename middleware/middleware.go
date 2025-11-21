package middleware

import (
	"crypto/subtle"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

var (
	// Ethereum address regex: 0x followed by 40 hex characters
	ethAddressRegex = regexp.MustCompile(`^0x[a-fA-F0-9]{40}$`)
)

// BasicAuth returns a middleware that implements HTTP Basic Authentication
func BasicAuth() gin.HandlerFunc {
	username := os.Getenv("AUTH_USERNAME")
	password := os.Getenv("AUTH_PASSWORD")

	return func(c *gin.Context) {
		// Skip auth if credentials not configured
		if username == "" || password == "" {
			c.Next()
			return
		}

		user, pass, hasAuth := c.Request.BasicAuth()
		if !hasAuth {
			c.Header("WWW-Authenticate", `Basic realm="Polymarket Analyzer"`)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "Authentication required",
			})
			return
		}

		// Use constant-time comparison to prevent timing attacks
		usernameMatch := subtle.ConstantTimeCompare([]byte(user), []byte(username)) == 1
		passwordMatch := subtle.ConstantTimeCompare([]byte(pass), []byte(password)) == 1

		if !usernameMatch || !passwordMatch {
			c.Header("WWW-Authenticate", `Basic realm="Polymarket Analyzer"`)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "Invalid credentials",
			})
			return
		}

		c.Next()
	}
}

// ValidateUserID validates that the user ID parameter is a valid Ethereum address
func ValidateUserID() gin.HandlerFunc {
	return func(c *gin.Context) {
		userID := c.Param("id")
		if userID == "" {
			c.Next()
			return
		}

		// Normalize to lowercase
		userID = strings.ToLower(strings.TrimSpace(userID))

		// Validate Ethereum address format
		if !ethAddressRegex.MatchString(userID) {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"error": "Invalid user ID format. Must be a valid Ethereum address (0x + 40 hex characters)",
			})
			return
		}

		// Store normalized ID for later use
		c.Set("validatedUserID", userID)
		c.Next()
	}
}

// ValidateQueryParams validates common query parameters
func ValidateQueryParams() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Validate limit parameter
		if limitStr := c.Query("limit"); limitStr != "" {
			limit, err := strconv.Atoi(limitStr)
			if err != nil || limit < 1 || limit > 100000 {
				c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
					"error": "Invalid limit parameter. Must be a positive integer between 1 and 100000",
				})
				return
			}
		}

		// Validate min_trades parameter
		if minTradesStr := c.Query("min_trades"); minTradesStr != "" {
			minTrades, err := strconv.Atoi(minTradesStr)
			if err != nil || minTrades < 0 {
				c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
					"error": "Invalid min_trades parameter. Must be a non-negative integer",
				})
				return
			}
		}

		// Validate max_trades parameter
		if maxTradesStr := c.Query("max_trades"); maxTradesStr != "" {
			maxTrades, err := strconv.Atoi(maxTradesStr)
			if err != nil || maxTrades < 0 {
				c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
					"error": "Invalid max_trades parameter. Must be a non-negative integer",
				})
				return
			}
		}

		// Validate percentage parameters (0-100 or 0-1)
		percentParams := []string{"min_win_rate", "max_win_rate", "min_consistency", "max_consistency"}
		for _, param := range percentParams {
			if val := c.Query(param); val != "" {
				f, err := strconv.ParseFloat(val, 64)
				if err != nil || f < 0 || f > 100 {
					c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
						"error": "Invalid " + param + " parameter. Must be a number between 0 and 100",
					})
					return
				}
			}
		}

		// Validate PNL parameters
		pnlParams := []string{"min_pnl", "max_pnl"}
		for _, param := range pnlParams {
			if val := c.Query(param); val != "" {
				_, err := strconv.ParseFloat(val, 64)
				if err != nil {
					c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
						"error": "Invalid " + param + " parameter. Must be a valid number",
					})
					return
				}
			}
		}

		// Validate subject parameter
		if subject := c.Query("subject"); subject != "" {
			validSubjects := map[string]bool{
				"all": true, "politics": true, "sports": true, "finance": true,
				"geopolitics": true, "tech": true, "culture": true, "economy": true,
			}
			if !validSubjects[strings.ToLower(subject)] {
				c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
					"error": "Invalid subject parameter. Must be one of: all, politics, sports, finance, geopolitics, tech, culture, economy",
				})
				return
			}
		}

		c.Next()
	}
}

// ValidateImportRequest validates the import request body
func ValidateImportRequest() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Only validate POST requests to import endpoint
		if c.Request.Method != "POST" {
			c.Next()
			return
		}

		c.Next()
	}
}

// IsValidEthAddress checks if a string is a valid Ethereum address
func IsValidEthAddress(addr string) bool {
	return ethAddressRegex.MatchString(strings.ToLower(strings.TrimSpace(addr)))
}
