package syncer

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"polymarket-analyzer/api"
	"polymarket-analyzer/storage"

	"github.com/gin-gonic/gin"
)

// TradeExecutor handles trade execution for copy trading (critical path)
type TradeExecutor struct {
	store      *storage.PostgresStore
	client     *api.Client
	clobClient *api.ClobClient
	myAddress  string
}

// CopyTradeRequest is the request from the worker to execute a copy trade
type CopyTradeRequest struct {
	FollowingAddress string  `json:"following_address"`
	FollowingTradeID string  `json:"following_trade_id"`
	FollowingTime    int64   `json:"following_time"`
	TokenID          string  `json:"token_id"`
	Side             string  `json:"side"`
	FollowingPrice   float64 `json:"following_price"`
	FollowingShares  float64 `json:"following_shares"`
	MarketTitle      string  `json:"market_title"`
	Outcome          string  `json:"outcome"`
	Multiplier       float64 `json:"multiplier"`
	MinOrderUSDC     float64 `json:"min_order_usdc"`
}

// CopyTradeResponse is the response back to the worker
type CopyTradeResponse struct {
	Success        bool    `json:"success"`
	Status         string  `json:"status"`
	FailedReason   string  `json:"failed_reason,omitempty"`
	OrderID        string  `json:"order_id,omitempty"`
	FollowerPrice  float64 `json:"follower_price,omitempty"`
	FollowerShares float64 `json:"follower_shares,omitempty"`
	ExecutionMs    int64   `json:"execution_ms"`
	DebugLog       string  `json:"debug_log,omitempty"`
}

// NewTradeExecutor creates a new trade executor
func NewTradeExecutor(store *storage.PostgresStore, client *api.Client) (*TradeExecutor, error) {
	// Create CLOB client with auth
	auth, err := api.NewAuth()
	if err != nil {
		return nil, fmt.Errorf("failed to create auth (POLYMARKET_PRIVATE_KEY not set?): %w", err)
	}

	clobClient, err := api.NewClobClient("", auth)
	if err != nil {
		return nil, fmt.Errorf("failed to create CLOB client: %w", err)
	}

	// Configure for Magic/Email wallet if funder address is set
	funderAddress := strings.TrimSpace(os.Getenv("POLYMARKET_FUNDER_ADDRESS"))
	if funderAddress != "" {
		clobClient.SetFunder(funderAddress)
		clobClient.SetSignatureType(1)
		log.Printf("[TradeExecutor] Configured for Magic wallet")
	} else {
		log.Printf("[TradeExecutor] Using EOA wallet")
	}

	// Derive API credentials
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := clobClient.DeriveAPICreds(ctx); err != nil {
		return nil, fmt.Errorf("failed to derive API creds: %w", err)
	}

	// Start order book caching
	clobClient.StartOrderBookCaching()

	// Determine our wallet address
	myAddress := funderAddress
	if myAddress == "" {
		myAddress = auth.GetAddress().Hex()
	}

	log.Printf("[TradeExecutor] Initialized for wallet %s", myAddress)

	return &TradeExecutor{
		store:      store,
		client:     client,
		clobClient: clobClient,
		myAddress:  myAddress,
	}, nil
}

// HandleExecuteRequest handles the HTTP request from the worker
func (te *TradeExecutor) HandleExecuteRequest(c *gin.Context) {
	startTime := time.Now()

	var req CopyTradeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, CopyTradeResponse{
			Success:      false,
			Status:       "error",
			FailedReason: fmt.Sprintf("invalid request: %v", err),
			ExecutionMs:  time.Since(startTime).Milliseconds(),
		})
		return
	}

	// Execute the trade
	resp := te.executeCopyTrade(c.Request.Context(), req)
	resp.ExecutionMs = time.Since(startTime).Milliseconds()

	if resp.Success {
		c.JSON(200, resp)
	} else {
		c.JSON(200, resp) // Still 200, but success=false
	}
}

// executeCopyTrade performs the actual trade execution
func (te *TradeExecutor) executeCopyTrade(ctx context.Context, req CopyTradeRequest) CopyTradeResponse {
	debugLog := make(map[string]interface{})
	debugLog["timestamp"] = time.Now().UTC().Format(time.RFC3339)
	debugLog["request"] = req

	// Determine action based on side
	isBuy := strings.ToUpper(req.Side) == "BUY"
	action := "BUY"
	if !isBuy {
		action = "SELL"
	}
	debugLog["action"] = action

	// Calculate target USDC
	copiedTradeUSDC := math.Abs(req.FollowingShares) * req.FollowingPrice
	targetUSDC := copiedTradeUSDC * req.Multiplier
	if targetUSDC < req.MinOrderUSDC {
		targetUSDC = req.MinOrderUSDC
	}
	debugLog["calculation"] = map[string]interface{}{
		"copiedTradeUSDC":    copiedTradeUSDC,
		"originalTargetUSDC": copiedTradeUSDC * req.Multiplier,
		"finalTargetUSDC":    targetUSDC,
		"copiedPrice":        req.FollowingPrice,
	}

	// Get order book to find best price
	orderBook, err := te.clobClient.GetOrderBook(ctx, req.TokenID)
	if err != nil {
		debugLog["orderBook"] = map[string]interface{}{"error": err.Error()}
		// Check if market is closed
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "No orderbook") {
			return CopyTradeResponse{
				Success:      false,
				Status:       "skipped",
				FailedReason: "market closed/resolved",
				DebugLog:     fmt.Sprintf("%v", debugLog),
			}
		}
		return CopyTradeResponse{
			Success:      false,
			Status:       "failed",
			FailedReason: fmt.Sprintf("get order book failed: %v", err),
			DebugLog:     fmt.Sprintf("%v", debugLog),
		}
	}

	// Find best price
	var bestPrice float64
	if isBuy {
		if len(orderBook.Asks) == 0 {
			return CopyTradeResponse{
				Success:      false,
				Status:       "skipped",
				FailedReason: "no asks available",
				DebugLog:     fmt.Sprintf("%v", debugLog),
			}
		}
		bestPrice, _ = strconv.ParseFloat(orderBook.Asks[0].Price, 64)
	} else {
		if len(orderBook.Bids) == 0 {
			return CopyTradeResponse{
				Success:      false,
				Status:       "skipped",
				FailedReason: "no bids available",
				DebugLog:     fmt.Sprintf("%v", debugLog),
			}
		}
		bestPrice, _ = strconv.ParseFloat(orderBook.Bids[0].Price, 64)
	}

	// Check price slippage
	maxSlippage := getMaxSlippageForPrice(req.FollowingPrice)
	maxPrice := req.FollowingPrice * (1 + maxSlippage)
	debugLog["calculation"].(map[string]interface{})["maxPrice"] = maxPrice
	debugLog["calculation"].(map[string]interface{})["priceLimit"] = fmt.Sprintf("+%.0f%%", maxSlippage*100)

	if isBuy && bestPrice > maxPrice {
		return CopyTradeResponse{
			Success:      false,
			Status:       "skipped",
			FailedReason: fmt.Sprintf("price too high: %.4f > %.4f (max +%.0f%%)", bestPrice, maxPrice, maxSlippage*100),
			DebugLog:     fmt.Sprintf("%v", debugLog),
		}
	}

	// Calculate size
	size := targetUSDC / bestPrice

	log.Printf("[TradeExecutor] Placing %s order: %.2f shares @ $%.4f ($%.2f USDC) for %s",
		action, size, bestPrice, targetUSDC, req.MarketTitle)

	// Use PlaceMarketOrder for FOK execution
	var side api.Side
	if isBuy {
		side = api.SideBuy
	} else {
		side = api.SideSell
	}

	// Determine if this is a neg-risk market (check token ID format)
	negRisk := false // Default to non-neg-risk

	orderResp, err := te.clobClient.PlaceMarketOrder(ctx, req.TokenID, side, targetUSDC, negRisk)
	if err != nil {
		return CopyTradeResponse{
			Success:      false,
			Status:       "failed",
			FailedReason: fmt.Sprintf("order failed: %v", err),
			DebugLog:     fmt.Sprintf("%v", debugLog),
		}
	}

	log.Printf("[TradeExecutor] Order placed successfully: %s", orderResp.OrderID)

	return CopyTradeResponse{
		Success:        true,
		Status:         "executed",
		OrderID:        orderResp.OrderID,
		FollowerPrice:  bestPrice,
		FollowerShares: size,
		DebugLog:       fmt.Sprintf("%v", debugLog),
	}
}

// getMaxSlippageForPrice returns max allowed slippage based on price
func getMaxSlippageForPrice(price float64) float64 {
	switch {
	case price < 0.10:
		return 2.00 // 200%
	case price < 0.20:
		return 0.80 // 80%
	case price < 0.30:
		return 0.50 // 50%
	case price < 0.40:
		return 0.30 // 30%
	default:
		return 0.20 // 20%
	}
}
