package api

import (
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
)

// Auth handles Polymarket L1 authentication with EIP-712 signing
type Auth struct {
	privateKey *ecdsa.PrivateKey
	address    common.Address
}

// NewAuth creates a new auth instance from the default POLYMARKET_PRIVATE_KEY env var
func NewAuth() (*Auth, error) {
	return NewAuthFromEnvVar("POLYMARKET_PRIVATE_KEY")
}

// NewAuthFromEnvVar creates a new auth instance from a specific environment variable
func NewAuthFromEnvVar(envVarName string) (*Auth, error) {
	privateKeyStr := strings.TrimSpace(os.Getenv(envVarName))
	if privateKeyStr == "" {
		return nil, fmt.Errorf("%s environment variable not set", envVarName)
	}
	return NewAuthFromKey(privateKeyStr)
}

// NewAuthFromKey creates a new auth instance from a private key string
func NewAuthFromKey(privateKeyStr string) (*Auth, error) {
	privateKeyStr = strings.TrimSpace(privateKeyStr)
	if privateKeyStr == "" {
		return nil, fmt.Errorf("private key is empty")
	}

	// Remove 0x prefix if present
	if len(privateKeyStr) > 2 && privateKeyStr[:2] == "0x" {
		privateKeyStr = privateKeyStr[2:]
	}
	// Trim again after removing prefix (in case of "0x " with space)
	privateKeyStr = strings.TrimSpace(privateKeyStr)

	privateKeyBytes, err := hex.DecodeString(privateKeyStr)
	if err != nil {
		return nil, fmt.Errorf("invalid private key format: %w", err)
	}

	privateKey, err := crypto.ToECDSA(privateKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("failed to cast public key to ECDSA")
	}

	address := crypto.PubkeyToAddress(*publicKeyECDSA)

	return &Auth{
		privateKey: privateKey,
		address:    address,
	}, nil
}

// GetAddress returns the Ethereum address derived from the private key
func (a *Auth) GetAddress() common.Address {
	return a.address
}

// SignRequest creates L1 authentication headers for Polymarket API
func (a *Auth) SignRequest() (map[string]string, error) {
	timestamp := time.Now().Unix()
	nonce := int64(0)

	// EIP-712 domain for Polymarket CLOB
	chainID := math.NewHexOrDecimal256(137) // Polygon mainnet
	domain := apitypes.TypedDataDomain{
		Name:    "ClobAuthDomain",
		Version: "1",
		ChainId: chainID,
	}

	// EIP-712 ClobAuth message structure
	message := map[string]interface{}{
		"address":   a.address.Hex(),
		"timestamp": strconv.FormatInt(timestamp, 10), // timestamp as string
		"nonce":     math.NewHexOrDecimal256(nonce),
		"message":   "This message attests that I control the given wallet",
	}

	typedData := apitypes.TypedData{
		Types: apitypes.Types{
			"EIP712Domain": []apitypes.Type{
				{Name: "name", Type: "string"},
				{Name: "version", Type: "string"},
				{Name: "chainId", Type: "uint256"},
			},
			"ClobAuth": []apitypes.Type{
				{Name: "address", Type: "address"},
				{Name: "timestamp", Type: "string"},
				{Name: "nonce", Type: "uint256"},
				{Name: "message", Type: "string"},
			},
		},
		PrimaryType: "ClobAuth",
		Domain:      domain,
		Message:     message,
	}

	// Sign the typed data using go-ethereum's signer
	domainSeparator, err := typedData.HashStruct("EIP712Domain", typedData.Domain.Map())
	if err != nil {
		return nil, fmt.Errorf("failed to hash domain: %w", err)
	}

	typedDataHash, err := typedData.HashStruct(typedData.PrimaryType, typedData.Message)
	if err != nil {
		return nil, fmt.Errorf("failed to hash message: %w", err)
	}

	rawData := []byte(fmt.Sprintf("\x19\x01%s%s", string(domainSeparator), string(typedDataHash)))
	hash := crypto.Keccak256Hash(rawData)

	signature, err := crypto.Sign(hash.Bytes(), a.privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign: %w", err)
	}

	// Adjust v value (recovery ID)
	signature[64] += 27

	headers := map[string]string{
		"POLY_ADDRESS":   a.address.Hex(),
		"POLY_SIGNATURE": "0x" + hex.EncodeToString(signature),
		"POLY_TIMESTAMP": strconv.FormatInt(timestamp, 10),
		"POLY_NONCE":     strconv.FormatInt(nonce, 10),
		"Content-Type":   "application/json",
	}

	return headers, nil
}

// SignMessage signs a simple message (alternative method)
func (a *Auth) SignMessage(message string) (string, error) {
	hash := accounts.TextHash([]byte(message))
	signature, err := crypto.Sign(hash, a.privateKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign message: %w", err)
	}

	// Adjust v value
	signature[64] += 27

	return "0x" + hex.EncodeToString(signature), nil
}
