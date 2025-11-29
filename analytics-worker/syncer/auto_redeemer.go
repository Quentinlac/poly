// Package syncer provides auto-redemption for resolved Polymarket positions.
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

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

const (
	// Polygon RPC endpoints
	polygonRPCURL = "https://polygon-rpc.com"

	// Contract addresses on Polygon
	conditionalTokensAddress = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
	usdcAddress              = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

	// Polygon chain ID
	polygonChainID = 137

	// Redeem check interval
	redeemInterval = 5 * time.Second
)

// Conditional Tokens ABI (only redeemPositions function)
const conditionalTokensABI = `[{
	"inputs": [
		{"internalType": "contract IERC20", "name": "collateralToken", "type": "address"},
		{"internalType": "bytes32", "name": "parentCollectionId", "type": "bytes32"},
		{"internalType": "bytes32", "name": "conditionId", "type": "bytes32"},
		{"internalType": "uint256[]", "name": "indexSets", "type": "uint256[]"}
	],
	"name": "redeemPositions",
	"outputs": [],
	"stateMutability": "nonpayable",
	"type": "function"
}]`

// AutoRedeemer automatically redeems resolved positions
type AutoRedeemer struct {
	client     *api.Client
	ethClient  *ethclient.Client
	privateKey *ecdsa.PrivateKey
	address    common.Address
	ctABI      abi.ABI

	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	// Stats
	totalRedeemed   int
	totalUSDC       float64
	lastRedeemTime  time.Time
	statsMu         sync.RWMutex
}

// NewAutoRedeemer creates a new auto-redeemer
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
	address := crypto.PubkeyToAddress(*publicKeyECDSA)

	// Check for funder address (Magic wallet)
	funderAddr := strings.TrimSpace(os.Getenv("POLYMARKET_FUNDER_ADDRESS"))
	if funderAddr != "" {
		address = common.HexToAddress(funderAddr)
	}

	// Connect to Polygon RPC
	ethClient, err := ethclient.Dial(polygonRPCURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Polygon: %w", err)
	}

	// Parse ABI
	ctABI, err := abi.JSON(strings.NewReader(conditionalTokensABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse ABI: %w", err)
	}

	log.Printf("[AutoRedeemer] Initialized for wallet %s", address.Hex())

	return &AutoRedeemer{
		client:     apiClient,
		ethClient:  ethClient,
		privateKey: privateKey,
		address:    address,
		ctABI:      ctABI,
		stopCh:     make(chan struct{}),
	}, nil
}

// Start begins the auto-redeem loop
func (ar *AutoRedeemer) Start(ctx context.Context) error {
	if ar.running {
		return fmt.Errorf("auto-redeemer already running")
	}

	ar.running = true
	ar.wg.Add(1)
	go ar.redeemLoop(ctx)

	log.Printf("[AutoRedeemer] Started - checking every %v", redeemInterval)
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
	ar.ethClient.Close()
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
	log.Printf("[AutoRedeemer] Checking for redeemable positions...")

	// Get open positions
	positions, err := ar.client.GetOpenPositions(ctx, ar.address.Hex())
	if err != nil {
		log.Printf("[AutoRedeemer] Failed to get positions: %v", err)
		return
	}

	if len(positions) == 0 {
		log.Printf("[AutoRedeemer] No open positions")
		return
	}

	// Find resolved positions (curPrice = 0 or 1)
	var redeemable []api.OpenPosition
	for _, pos := range positions {
		curPrice := pos.CurPrice.Float64()
		size := pos.Size.Float64()

		// Position is redeemable if:
		// 1. curPrice is exactly 0 or 1 (resolved)
		// 2. We have tokens to redeem
		if size > 0.001 && (curPrice < 0.001 || curPrice > 0.999) {
			redeemable = append(redeemable, pos)
			log.Printf("[AutoRedeemer] Found redeemable: %s - %s @ $%.4f (size: %.4f)",
				pos.Title, pos.Outcome, curPrice, size)
		}
	}

	if len(redeemable) == 0 {
		log.Printf("[AutoRedeemer] No redeemable positions found (checked %d positions)", len(positions))
		return
	}

	log.Printf("[AutoRedeemer] Found %d redeemable positions", len(redeemable))

	// Redeem each position
	for _, pos := range redeemable {
		if err := ar.redeemPosition(ctx, pos); err != nil {
			log.Printf("[AutoRedeemer] Failed to redeem %s: %v", pos.Title, err)
			continue
		}

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
	log.Printf("[AutoRedeemer] Redeeming: %s - %s (condition: %s)", pos.Title, pos.Outcome, pos.ConditionID)

	// Convert condition ID to bytes32
	conditionID := common.HexToHash(pos.ConditionID)

	// Determine index set based on outcome (1 = first outcome, 2 = second outcome)
	indexSet := big.NewInt(1)
	if pos.OutcomeIndex == 1 {
		indexSet = big.NewInt(2)
	}

	// Build transaction data
	data, err := ar.ctABI.Pack("redeemPositions",
		common.HexToAddress(usdcAddress),     // collateralToken
		common.Hash{},                        // parentCollectionId (0x0 for top-level)
		conditionID,                          // conditionId
		[]*big.Int{indexSet},                 // indexSets
	)
	if err != nil {
		return fmt.Errorf("failed to pack tx data: %w", err)
	}

	// Get nonce
	nonce, err := ar.ethClient.PendingNonceAt(ctx, ar.address)
	if err != nil {
		return fmt.Errorf("failed to get nonce: %w", err)
	}

	// Get gas price
	gasPrice, err := ar.ethClient.SuggestGasPrice(ctx)
	if err != nil {
		return fmt.Errorf("failed to get gas price: %w", err)
	}
	// Add 20% buffer for faster confirmation
	gasPrice = new(big.Int).Mul(gasPrice, big.NewInt(120))
	gasPrice = new(big.Int).Div(gasPrice, big.NewInt(100))

	// Estimate gas
	ctAddress := common.HexToAddress(conditionalTokensAddress)
	gasLimit := uint64(200000) // Conservative estimate for redeem

	// Create transaction
	tx := types.NewTransaction(
		nonce,
		ctAddress,
		big.NewInt(0), // No value transfer
		gasLimit,
		gasPrice,
		data,
	)

	// Sign transaction
	chainID := big.NewInt(polygonChainID)
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), ar.privateKey)
	if err != nil {
		return fmt.Errorf("failed to sign tx: %w", err)
	}

	// Send transaction
	err = ar.ethClient.SendTransaction(ctx, signedTx)
	if err != nil {
		return fmt.Errorf("failed to send tx: %w", err)
	}

	log.Printf("[AutoRedeemer] ✅ Redemption tx sent: %s", signedTx.Hash().Hex())

	// Wait for confirmation (optional - don't block)
	go ar.waitForConfirmation(ctx, signedTx.Hash(), pos.Title)

	return nil
}

func (ar *AutoRedeemer) waitForConfirmation(ctx context.Context, txHash common.Hash, title string) {
	// Wait up to 2 minutes for confirmation
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	receipt, err := bind.WaitMined(ctx, ar.ethClient, &types.Transaction{})
	if err != nil {
		log.Printf("[AutoRedeemer] Tx %s: waiting for confirmation failed: %v", txHash.Hex()[:10], err)
		return
	}

	if receipt != nil && receipt.Status == 1 {
		log.Printf("[AutoRedeemer] ✅ Confirmed: %s - %s", title, txHash.Hex())
	} else if receipt != nil {
		log.Printf("[AutoRedeemer] ❌ Failed: %s - %s", title, txHash.Hex())
	}
}
