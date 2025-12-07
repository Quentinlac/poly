package syncer

import (
	"fmt"

	"polymarket-analyzer/api"
)

// BotBuyAnalysis holds the analysis result for a bot buy operation
type BotBuyAnalysis struct {
	TotalSize      float64   // Total tokens to buy
	TotalCost      float64   // Total USDC cost
	AvgPrice       float64   // Weighted average price
	AffordableAsks []BotOrderLevel // Asks within price limit
	CanExecute     bool      // Whether we can execute the trade
	SkipReason     string    // Reason for skipping (if any)
}

// BotSellAnalysis holds the analysis result for a bot sell operation
type BotSellAnalysis struct {
	TotalSold       float64         // Total tokens that can be sold
	TotalUSDC       float64         // Total USDC to receive
	AvgPrice        float64         // Weighted average price
	AcceptableBids  []BotOrderLevel // Bids within price limit
	RemainingSize   float64         // Size that couldn't be filled
	CanExecute      bool            // Whether we can execute the trade
	NeedsLimitOrder bool            // Whether we need limit order fallback
	SkipReason      string          // Reason for skipping (if any)
}

// BotOrderLevel represents a price level in the order book
type BotOrderLevel struct {
	Price float64
	Size  float64
}

// LimitOrderPlan holds the plan for limit order fallback
type LimitOrderPlan struct {
	Orders []LimitOrderSpec
}

// LimitOrderSpec specifies a single limit order
type LimitOrderSpec struct {
	Size       float64
	Price      float64
	Percentage float64 // 20%, 40%, 40%
}

// AnalyzeBotBuy analyzes the order book for a bot buy operation.
// Returns analysis of what can be bought within the 10% price limit.
// This is a pure function for easy testing.
func AnalyzeBotBuy(book *api.OrderBook, copiedPrice, targetUSDC float64) BotBuyAnalysis {
	result := BotBuyAnalysis{}

	if book == nil || len(book.Asks) == 0 {
		result.SkipReason = "empty order book"
		return result
	}

	maxPrice := copiedPrice * 1.10 // 10% above copied price

	// Find all affordable asks (within 10% of copied price)
	for _, ask := range book.Asks {
		var askPrice, askSize float64
		fmt.Sscanf(ask.Price, "%f", &askPrice)
		fmt.Sscanf(ask.Size, "%f", &askSize)

		// Stop if price exceeds our max (10% above copied price)
		if askPrice > maxPrice {
			break
		}

		result.AffordableAsks = append(result.AffordableAsks, BotOrderLevel{
			Price: askPrice,
			Size:  askSize,
		})
	}

	if len(result.AffordableAsks) == 0 {
		var bestAsk float64
		fmt.Sscanf(book.Asks[0].Price, "%f", &bestAsk)
		result.SkipReason = fmt.Sprintf("no liquidity within 10%% (best=%.4f, max=%.4f)", bestAsk, maxPrice)
		return result
	}

	// Calculate fill
	remainingUSDC := targetUSDC
	for _, ask := range result.AffordableAsks {
		if remainingUSDC <= 0 {
			break
		}

		levelCost := ask.Price * ask.Size
		if levelCost <= remainingUSDC {
			result.TotalSize += ask.Size
			result.TotalCost += levelCost
			remainingUSDC -= levelCost
		} else {
			partialSize := remainingUSDC / ask.Price
			result.TotalSize += partialSize
			result.TotalCost += remainingUSDC
			remainingUSDC = 0
		}
	}

	if result.TotalSize < 0.01 {
		result.SkipReason = fmt.Sprintf("insufficient affordable liquidity (size=%.4f, cost=$%.4f)", result.TotalSize, result.TotalCost)
		return result
	}

	result.AvgPrice = result.TotalCost / result.TotalSize
	result.CanExecute = true
	return result
}

// AnalyzeBotSell analyzes the order book for a bot sell operation.
// Returns analysis of what can be sold within the 10% price limit.
// This is a pure function for easy testing.
func AnalyzeBotSell(book *api.OrderBook, copiedPrice, sellSize float64) BotSellAnalysis {
	result := BotSellAnalysis{
		RemainingSize: sellSize,
	}

	if book == nil || len(book.Bids) == 0 {
		result.SkipReason = "no bids in order book"
		result.NeedsLimitOrder = true
		return result
	}

	minPrice := copiedPrice * 0.90 // 10% below copied price

	// Find all acceptable bids (within 10% of copied price)
	for _, bid := range book.Bids {
		var bidPrice, bidSize float64
		fmt.Sscanf(bid.Price, "%f", &bidPrice)
		fmt.Sscanf(bid.Size, "%f", &bidSize)

		// Stop if price is below our minimum (10% below copied price)
		if bidPrice < minPrice {
			break
		}

		result.AcceptableBids = append(result.AcceptableBids, BotOrderLevel{
			Price: bidPrice,
			Size:  bidSize,
		})
	}

	// Calculate how much we can sell to acceptable bids
	remainingSize := sellSize
	for _, bid := range result.AcceptableBids {
		if remainingSize <= 0 {
			break
		}

		if bid.Size <= remainingSize {
			// Take entire level
			result.TotalSold += bid.Size
			result.TotalUSDC += bid.Price * bid.Size
			remainingSize -= bid.Size
		} else {
			// Partial fill at this level
			result.TotalSold += remainingSize
			result.TotalUSDC += bid.Price * remainingSize
			remainingSize = 0
		}
	}

	result.RemainingSize = remainingSize

	if result.TotalSold > 0.01 {
		result.AvgPrice = result.TotalUSDC / result.TotalSold
		result.CanExecute = true
	} else {
		result.NeedsLimitOrder = true
		result.SkipReason = "no bids within 10%"
	}

	return result
}

// PlanLimitOrderFallback creates a plan for limit order fallback when market sell fails.
// Returns 3 orders: 20% at copied price, 40% at -3%, 40% at -5%
func PlanLimitOrderFallback(sellSize, copiedPrice float64) LimitOrderPlan {
	return LimitOrderPlan{
		Orders: []LimitOrderSpec{
			{
				Size:       sellSize * 0.20,
				Price:      copiedPrice,
				Percentage: 20,
			},
			{
				Size:       sellSize * 0.40,
				Price:      copiedPrice * 0.97, // -3%
				Percentage: 40,
			},
			{
				Size:       sellSize * 0.40,
				Price:      copiedPrice * 0.95, // -5%
				Percentage: 40,
			},
		},
	}
}

// CalculateBotBuyAmount calculates the target USDC amount for a bot buy.
// Applies multiplier, min order, and max cap.
func CalculateBotBuyAmount(tradeUSDC, multiplier, minUSDC float64, maxUSD *float64) float64 {
	targetUSDC := tradeUSDC * multiplier

	// Enforce minimum
	if targetUSDC < minUSDC {
		targetUSDC = minUSDC
	}

	// Apply max cap if set
	if maxUSD != nil && targetUSDC > *maxUSD {
		targetUSDC = *maxUSD
	}

	return targetUSDC
}

// CalculateBotSellAmount calculates the target token amount for a bot sell.
// Returns the minimum of (tradeSize * multiplier) and ourPosition.
func CalculateBotSellAmount(tradeSize, multiplier, ourPosition float64) float64 {
	targetTokens := tradeSize * multiplier

	if targetTokens > ourPosition {
		return ourPosition
	}
	return targetTokens
}

// IsWithinBuyPriceLimit checks if an ask price is within the 10% limit
func IsWithinBuyPriceLimit(askPrice, copiedPrice float64) bool {
	maxPrice := copiedPrice * 1.10
	return askPrice <= maxPrice
}

// IsWithinSellPriceLimit checks if a bid price is within the 10% limit
func IsWithinSellPriceLimit(bidPrice, copiedPrice float64) bool {
	minPrice := copiedPrice * 0.90
	return bidPrice >= minPrice
}
