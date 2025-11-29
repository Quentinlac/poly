// Package syncer provides auto-redemption for resolved Polymarket positions.
// Uses Polymarket's Relayer API for gasless transactions via Safe wallets.
package syncer

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"
	"sync"
	"time"

	"polymarket-analyzer/api"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

const (
	// Redeem check interval - not too frequent to avoid rate limits
	redeemInterval = 2 * time.Minute
	// Delay between individual redeem calls - relayer limit is 15/min, so 5s = 12/min max (safe)
	redeemDelay = 5 * time.Second
	// Max redeems per check cycle to stay under rate limit
	maxRedeemsPerCycle = 10
)

// AutoRedeemer automatically redeems resolved positions via Polymarket Relayer
type AutoRedeemer struct {
	client        *api.Client
	relayerClient *api.RelayerClient
	safeAddr      common.Address

	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	// Track submitted redeems to avoid duplicates
	submittedRedeems   map[string]time.Time
	submittedRedeemsMu sync.RWMutex

	// Stats
	totalRedeemed  int
	totalUSDC      float64
	lastRedeemTime time.Time
	statsMu        sync.RWMutex
}

// NewAutoRedeemer creates a new auto-redeemer using the Relayer API
func NewAutoRedeemer(apiClient *api.Client) (*AutoRedeemer, error) {
	// Get private key from env
	pkHex := strings.TrimSpace(os.Getenv("POLYMARKET_PRIVATE_KEY"))
	if pkHex == "" {
		return nil, fmt.Errorf("POLYMARKET_PRIVATE_KEY not set")
	}
	pkHex = strings.TrimPrefix(pkHex, "0x")

	privateKey, err := crypto.HexToECDSA(pkHex)
	if err != nil {
		return nil, fmt.Errorf("invalid private key: %w", err)
	}

	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("error casting public key")
	}
	eoaAddr := crypto.PubkeyToAddress(*publicKeyECDSA)

	// Get Safe/funder address (Magic wallet)
	funderAddr := strings.TrimSpace(os.Getenv("POLYMARKET_FUNDER_ADDRESS"))
	var safeAddr common.Address
	if funderAddr != "" {
		safeAddr = common.HexToAddress(funderAddr)
	} else {
		// If no funder, use EOA as Safe (for EOA-only wallets)
		safeAddr = eoaAddr
	}

	// Get Builder credentials
	builderKey := strings.TrimSpace(os.Getenv("BUILDER_API_KEY"))
	builderSecret := strings.TrimSpace(os.Getenv("BUILDER_SECRET"))
	builderPassphrase := strings.TrimSpace(os.Getenv("BUILDER_PASS_PHRASE"))

	if builderKey == "" || builderSecret == "" || builderPassphrase == "" {
		return nil, fmt.Errorf("BUILDER_API_KEY, BUILDER_SECRET, and BUILDER_PASS_PHRASE are required for auto-redeem")
	}

	creds := &api.BuilderCreds{
		Key:        builderKey,
		Secret:     builderSecret,
		Passphrase: builderPassphrase,
	}

	relayerClient := api.NewRelayerClient(privateKey, safeAddr, creds)

	log.Printf("[AutoRedeemer] Initialized with Relayer API for Safe wallet %s (signer: %s)",
		safeAddr.Hex(), eoaAddr.Hex())

	return &AutoRedeemer{
		client:           apiClient,
		relayerClient:    relayerClient,
		safeAddr:         safeAddr,
		stopCh:           make(chan struct{}),
		submittedRedeems: make(map[string]time.Time),
	}, nil
}

// Start begins the auto-redeem loop
func (ar *AutoRedeemer) Start(ctx context.Context) error {
	if ar.running {
		return fmt.Errorf("auto-redeemer already running")
	}

	// Check if Safe is deployed
	deployed, err := ar.relayerClient.IsDeployed(ctx)
	if err != nil {
		log.Printf("[AutoRedeemer] Warning: could not check Safe deployment: %v", err)
	} else if !deployed {
		log.Printf("[AutoRedeemer] Warning: Safe wallet not deployed yet")
	}

	ar.running = true
	ar.wg.Add(1)
	go ar.redeemLoop(ctx)

	log.Printf("[AutoRedeemer] Started - checking every %v (gasless via Relayer)", redeemInterval)
	return nil
}

// Stop halts the auto-redeemer
func (ar *AutoRedeemer) Stop() {
	if !ar.running {
		return
	}
	ar.running = false
	close(ar.stopCh)
	ar.wg.Wait()
	log.Printf("[AutoRedeemer] Stopped - total redeemed: %d positions, $%.2f USDC", ar.totalRedeemed, ar.totalUSDC)
}

// GetStats returns redemption statistics
func (ar *AutoRedeemer) GetStats() (redeemed int, usdc float64, lastTime time.Time) {
	ar.statsMu.RLock()
	defer ar.statsMu.RUnlock()
	return ar.totalRedeemed, ar.totalUSDC, ar.lastRedeemTime
}

func (ar *AutoRedeemer) redeemLoop(ctx context.Context) {
	defer ar.wg.Done()

	// Run immediately on start
	ar.checkAndRedeem(ctx)

	ticker := time.NewTicker(redeemInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ar.stopCh:
			return
		case <-ticker.C:
			ar.checkAndRedeem(ctx)
		}
	}
}

func (ar *AutoRedeemer) checkAndRedeem(ctx context.Context) {
	// Clean up old submitted entries (older than 10 minutes)
	ar.submittedRedeemsMu.Lock()
	cutoff := time.Now().Add(-10 * time.Minute)
	for key, submittedAt := range ar.submittedRedeems {
		if submittedAt.Before(cutoff) {
			delete(ar.submittedRedeems, key)
		}
	}
	ar.submittedRedeemsMu.Unlock()

	// Get open positions
	positions, err := ar.client.GetOpenPositions(ctx, ar.safeAddr.Hex())
	if err != nil {
		log.Printf("[AutoRedeemer] Failed to get positions: %v", err)
		return
	}

	if len(positions) == 0 {
		return // No positions to check
	}

	// Find resolved positions (curPrice = 0 or 1)
	var redeemable []api.OpenPosition
	for _, pos := range positions {
		curPrice := pos.CurPrice.Float64()
		size := pos.Size.Float64()

		// Position is redeemable if:
		// 1. curPrice is exactly 0 or 1 (resolved)
		// 2. We have tokens to redeem
		// 3. Not already submitted
		if size > 0.001 && (curPrice < 0.001 || curPrice > 0.999) {
			// Check if already submitted
			key := pos.ConditionID + "-" + pos.Outcome
			ar.submittedRedeemsMu.RLock()
			_, alreadySubmitted := ar.submittedRedeems[key]
			ar.submittedRedeemsMu.RUnlock()

			if !alreadySubmitted {
				redeemable = append(redeemable, pos)
			}
		}
	}

	if len(redeemable) == 0 {
		return // No redeemable positions
	}

	// Limit to max redeems per cycle to stay under rate limit
	if len(redeemable) > maxRedeemsPerCycle {
		log.Printf("[AutoRedeemer] Found %d redeemable positions, processing %d this cycle (rate limit)", len(redeemable), maxRedeemsPerCycle)
		redeemable = redeemable[:maxRedeemsPerCycle]
	} else {
		log.Printf("[AutoRedeemer] Found %d redeemable positions to submit", len(redeemable))
	}

	// Redeem each position via Relayer (gasless!) with delay between calls
	for i, pos := range redeemable {
		// Add delay between calls to avoid rate limits (skip first)
		if i > 0 {
			time.Sleep(redeemDelay)
		}

		key := pos.ConditionID + "-" + pos.Outcome

		if err := ar.redeemPosition(ctx, pos); err != nil {
			log.Printf("[AutoRedeemer] Failed to redeem %s: %v", pos.Title, err)
			continue
		}

		// Mark as submitted
		ar.submittedRedeemsMu.Lock()
		ar.submittedRedeems[key] = time.Now()
		ar.submittedRedeemsMu.Unlock()

		// Track stats
		ar.statsMu.Lock()
		ar.totalRedeemed++
		if pos.CurPrice.Float64() > 0.5 {
			// Won - redeemed at $1.00
			ar.totalUSDC += pos.Size.Float64()
		}
		ar.lastRedeemTime = time.Now()
		ar.statsMu.Unlock()
	}
}

func (ar *AutoRedeemer) redeemPosition(ctx context.Context, pos api.OpenPosition) error {
	log.Printf("[AutoRedeemer] Redeeming via Relayer: %s - %s (condition: %s)",
		pos.Title, pos.Outcome, pos.ConditionID)

	// Convert condition ID to bytes32
	conditionID := common.HexToHash(pos.ConditionID)

	// Determine index set based on outcome (1 = first outcome, 2 = second outcome)
	indexSet := big.NewInt(1)
	if pos.OutcomeIndex == 1 {
		indexSet = big.NewInt(2)
	}

	// Build redeem transaction
	tx, err := api.BuildRedeemTransaction(conditionID, indexSet)
	if err != nil {
		return fmt.Errorf("failed to build redeem tx: %w", err)
	}

	// Execute via Relayer (gasless!)
	metadata := fmt.Sprintf("Redeem: %s - %s", pos.Title, pos.Outcome)
	resp, err := ar.relayerClient.Execute(ctx, []api.SafeTransaction{tx}, metadata)
	if err != nil {
		return fmt.Errorf("relayer execution failed: %w", err)
	}

	log.Printf("[AutoRedeemer] ✅ Redemption submitted via Relayer: txID=%s hash=%s state=%s",
		resp.ID, resp.TransactionHash, resp.State)

	return nil
}
