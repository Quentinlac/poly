package main

import (
	"database/sql"
	"fmt"
	"log"
	"math"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	// DB Connection String
	connStr := "postgresql://qoveryadmin:xP-R3PMRO0dNuFOgqDm5HYuwMV-kK3Lp@zd4409065-postgresql.crysaioqovvg.eu-west-1.rds.amazonaws.com:5432/polymarket?sslmode=require"

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping DB: %v", err)
	}

	fmt.Println("Successfully connected to DB")

	// 1. Check for negative follower_shares_remaining
	fmt.Println("\n--- Checking for negative follower_shares_remaining ---")
	rows, err := db.Query(`
		SELECT token_id, following_address, follower_shares_remaining, follower_shares_bought, follower_shares_sold
		FROM copy_trade_pnl
		WHERE follower_shares_remaining < 0
		LIMIT 5
	`)
	if err != nil {
		log.Printf("Error querying negative shares: %v", err)
	} else {
		defer rows.Close()
		found := false
		for rows.Next() {
			found = true
			var tokenID, followingAddr string
			var remaining, bought, sold float64
			if err := rows.Scan(&tokenID, &followingAddr, &remaining, &bought, &sold); err != nil {
				log.Printf("Error scanning row: %v", err)
				continue
			}
			fmt.Printf("Token: %s, Following: %s, Remaining: %.4f, Bought: %.4f, Sold: %.4f\n",
				tokenID, followingAddr, remaining, bought, sold)
		}
		if !found {
			fmt.Println("No negative follower_shares_remaining found.")
		}
	}

	// 2. Check for negative following_shares_remaining
	fmt.Println("\n--- Checking for negative following_shares_remaining ---")
	rows2, err := db.Query(`
		SELECT token_id, following_address, following_shares_remaining, following_shares_bought, following_shares_sold
		FROM copy_trade_pnl
		WHERE following_shares_remaining < 0
		LIMIT 5
	`)
	if err != nil {
		log.Printf("Error querying negative following shares: %v", err)
	} else {
		defer rows2.Close()
		found := false
		for rows2.Next() {
			found = true
			var tokenID, followingAddr string
			var remaining, bought, sold float64
			if err := rows2.Scan(&tokenID, &followingAddr, &remaining, &bought, &sold); err != nil {
				log.Printf("Error scanning row: %v", err)
				continue
			}
			fmt.Printf("Token: %s, Following: %s, Remaining: %.4f, Bought: %.4f, Sold: %.4f\n",
				tokenID, followingAddr, remaining, bought, sold)
		}
		if !found {
			fmt.Println("No negative following_shares_remaining found.")
		}
	}

	// 3. Inspect a specific token/user from copy_trade_log to see raw data
	// Use one of the negative tokens found above
	targetToken := "52519958173620056976467390023481082009578041875246013360839466594760857643027"
	targetUser := "0x05c1882212a41aa8d7df5b70eebe03d9319345b7"

	fmt.Printf("\n--- Inspecting raw copy_trade_log for Token=%s, User=%s ---\n", targetToken, targetUser)
	rows3, err := db.Query(`
		SELECT following_shares, follower_shares, status, following_time
		FROM copy_trade_log
		WHERE token_id = $1 AND following_address = $2
		ORDER BY following_time ASC
	`, targetToken, targetUser)
	if err != nil {
		log.Printf("Error querying log: %v", err)
	} else {
		defer rows3.Close()
		var totalBought, totalSold, netShares float64
		for rows3.Next() {
			var fShares, myShares sql.NullFloat64
			var status string
			var timeStr string
			rows3.Scan(&fShares, &myShares, &status, &timeStr)
			fmt.Printf("Time: %s, FollowingShares: %.4f, FollowerShares: %.4f, Status: %s\n",
				timeStr, fShares.Float64, myShares.Float64, status)

			if status == "executed" {
				if fShares.Float64 < 0 {
					totalBought += math.Abs(myShares.Float64)
				} else {
					totalSold += math.Abs(myShares.Float64)
				}
			}
		}
		netShares = totalBought - totalSold
		fmt.Printf("Calculated: Bought=%.4f, Sold=%.4f, Net=%.4f\n", totalBought, totalSold, netShares)
	}

	// 4. Check REDEEM trades in user_trades and compare with copy_trade_pnl
	fmt.Println("\n--- Checking REDEEM trades vs copy_trade_pnl ---")
	// Find a user who has redeems
	rows4, err := db.Query(`
		SELECT t.market_id, t.user_id, t.outcome, t.usdc_size, p.token_id, p.outcome
		FROM user_trades t
		LEFT JOIN copy_trade_pnl p ON t.market_id = p.token_id AND t.user_id = p.following_address
		WHERE t.type = 'REDEEM'
		LIMIT 10
	`)
	if err != nil {
		log.Printf("Error querying redeems join: %v", err)
	} else {
		defer rows4.Close()
		for rows4.Next() {
			var mID, uID, outcome string
			var size float64
			var pTokenID, pOutcome sql.NullString
			rows4.Scan(&mID, &uID, &outcome, &size, &pTokenID, &pOutcome)

			matchStatus := "MATCH"
			if !pTokenID.Valid {
				matchStatus = "NO MATCH (TokenID)"
			} else if pOutcome.String != outcome {
				matchStatus = fmt.Sprintf("MISMATCH (Outcome: '%s' vs '%s')", outcome, pOutcome.String)
			}

			fmt.Printf("Redeem: MarketID=%s, User=%s, Outcome=%s -> PnL: %s, Status: %s\n",
				mID, uID, outcome, matchStatus, matchStatus)
		}
	}

	// 5. Test RefreshCopyTradePnL aggregation logic for the problematic token
	fmt.Println("\n--- Testing RefreshCopyTradePnL Aggregation Logic ---")
	rows5, err := db.Query(`
		SELECT
			token_id,
			following_address,
			-- follower_shares_remaining calculation from RefreshCopyTradePnL
			GREATEST(
				COALESCE(SUM(CASE WHEN status = 'executed' AND following_shares < 0 THEN ABS(follower_shares) ELSE 0 END), 0) -
				COALESCE(SUM(CASE WHEN status = 'executed' AND following_shares > 0 THEN ABS(follower_shares) ELSE 0 END), 0),
				0
			) as calculated_remaining,
			-- Raw difference without GREATEST
			COALESCE(SUM(CASE WHEN status = 'executed' AND following_shares < 0 THEN ABS(follower_shares) ELSE 0 END), 0) -
			COALESCE(SUM(CASE WHEN status = 'executed' AND following_shares > 0 THEN ABS(follower_shares) ELSE 0 END), 0) as raw_diff
		FROM copy_trade_log
		WHERE token_id = $1 AND following_address = $2
		GROUP BY token_id, following_address
	`, targetToken, targetUser)
	if err != nil {
		log.Printf("Error querying aggregation: %v", err)
	} else {
		defer rows5.Close()
		for rows5.Next() {
			var tID, fAddr string
			var calcRem, rawDiff float64
			rows5.Scan(&tID, &fAddr, &calcRem, &rawDiff)
			fmt.Printf("Aggregation Result: Token=%s, User=%s, CalculatedRemaining=%.4f, RawDiff=%.4f\n",
				tID, fAddr, calcRem, rawDiff)
		}
	}

	// 6. Check token_map_cache for redemption market_id
	fmt.Println("\n--- Checking token_map_cache for redemption market_id ---")
	// Use the market_id from the first failed match in previous step
	// MarketID=96117158101491631934927536251889289739243567183997394364137781069452695793194
	redeemMarketID := "96117158101491631934927536251889289739243567183997394364137781069452695793194"
	rows6, err := db.Query(`
		SELECT token_id, condition_id, outcome
		FROM token_map_cache
		WHERE token_id = $1 OR condition_id = $1
	`, redeemMarketID)
	if err != nil {
		log.Printf("Error querying token_map: %v", err)
	} else {
		defer rows6.Close()
		found := false
		for rows6.Next() {
			found = true
			var tID, cID, out string
			rows6.Scan(&tID, &cID, &out)
			fmt.Printf("TokenMap: Input=%s matches -> TokenID=%s, ConditionID=%s, Outcome=%s\n",
				redeemMarketID, tID, cID, out)
		}
		if !found {
			fmt.Printf("TokenMap: No match found for %s\n", redeemMarketID)
		}
	}
}
