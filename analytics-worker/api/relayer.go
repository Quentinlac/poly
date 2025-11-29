// Package api provides a client for the Polymarket Relayer API.
// This enables gasless transactions through Safe wallets for Magic/email accounts.
package api

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
)

const (
	// Relayer URLs
	RelayerURL        = "https://relayer-v2.polymarket.com"
	RelayerStagingURL = "https://relayer-v2-staging.polymarket.dev"

	// Contract addresses on Polygon
	ConditionalTokensAddr = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
	USDCAddr              = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
	MultiSendAddr         = "0xA238CBeb142c10Ef7Ad8442C6D1f9E89e07e7761" // Gnosis MultiSend

	// Chain
	PolygonChainID = 137
)

// BuilderCreds holds the Builder API credentials
type BuilderCreds struct {
	Key        string
	Secret     string
	Passphrase string
}

// RelayerClient interacts with Polymarket's relayer for gasless transactions
type RelayerClient struct {
	baseURL      string
	chainID      int
	privateKey   *ecdsa.PrivateKey
	signerAddr   common.Address
	safeAddr     common.Address // The Safe/proxy wallet address
	builderCreds *BuilderCreds
	httpClient   *http.Client
}

// SafeTransaction represents a single transaction to execute via Safe
type SafeTransaction struct {
	To        common.Address
	Operation uint8  // 0 = Call, 1 = DelegateCall
	Data      []byte
	Value     *big.Int
}

// TransactionRequest is the request body for submitting to relayer
type TransactionRequest struct {
	Type            string           `json:"type"`
	From            string           `json:"from"`
	To              string           `json:"to"`
	ProxyWallet     string           `json:"proxyWallet,omitempty"`
	Data            string           `json:"data"`
	Nonce           string           `json:"nonce,omitempty"`
	Signature       string           `json:"signature"`
	SignatureParams *SignatureParams `json:"signatureParams"`
	Metadata        string           `json:"metadata,omitempty"`
}

// SignatureParams contains Safe transaction parameters
type SignatureParams struct {
	GasPrice        string `json:"gasPrice"`
	SafeTxnGas      string `json:"safeTxnGas"`
	BaseGas         string `json:"baseGas"`
	PaymentToken    string `json:"paymentToken,omitempty"`
	Payment         string `json:"payment,omitempty"`
	PaymentReceiver string `json:"paymentReceiver,omitempty"`
}

// RelayerResponse is the response from submitting a transaction
type RelayerResponse struct {
	ID              string `json:"id"`
	TransactionHash string `json:"transactionHash"`
	State           string `json:"state"`
	Error           string `json:"error,omitempty"`
}

// NonceResponse from the nonce endpoint
type NonceResponse struct {
	Nonce string `json:"nonce"`
}

// NewRelayerClient creates a new relayer client
func NewRelayerClient(privateKey *ecdsa.PrivateKey, safeAddr common.Address, creds *BuilderCreds) *RelayerClient {
	publicKey := privateKey.Public()
	publicKeyECDSA, _ := publicKey.(*ecdsa.PublicKey)
	signerAddr := crypto.PubkeyToAddress(*publicKeyECDSA)

	return &RelayerClient{
		baseURL:      RelayerURL,
		chainID:      PolygonChainID,
		privateKey:   privateKey,
		signerAddr:   signerAddr,
		safeAddr:     safeAddr,
		builderCreds: creds,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// generateBuilderHeaders creates HMAC-signed headers for authentication
func (c *RelayerClient) generateBuilderHeaders(method, path string, body []byte) map[string]string {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)

	// Build message: timestamp + method + path + body
	message := timestamp + method + path
	if len(body) > 0 {
		// Normalize JSON (replace single quotes with double quotes for compatibility)
		bodyStr := strings.ReplaceAll(string(body), "'", "\"")
		message += bodyStr
	}

	// Decode base64 secret
	secretBytes, err := base64.URLEncoding.DecodeString(c.builderCreds.Secret)
	if err != nil {
		// Try standard base64 if URL encoding fails
		secretBytes, err = base64.StdEncoding.DecodeString(c.builderCreds.Secret)
		if err != nil {
			log.Printf("[Relayer] Warning: failed to decode secret: %v", err)
			return nil
		}
	}

	// Generate HMAC-SHA256 signature
	h := hmac.New(sha256.New, secretBytes)
	h.Write([]byte(message))
	signature := base64.URLEncoding.EncodeToString(h.Sum(nil))

	return map[string]string{
		"POLY_BUILDER_API_KEY":    c.builderCreds.Key,
		"POLY_BUILDER_PASSPHRASE": c.builderCreds.Passphrase,
		"POLY_BUILDER_SIGNATURE":  signature,
		"POLY_BUILDER_TIMESTAMP":  timestamp,
	}
}

// GetNonce retrieves the current nonce for the signer
func (c *RelayerClient) GetNonce(ctx context.Context) (string, error) {
	path := "/nonce?address=" + c.signerAddr.Hex()
	url := c.baseURL + path

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", err
	}

	// Add builder headers
	headers := c.generateBuilderHeaders("GET", path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("nonce request failed: %d %s", resp.StatusCode, string(body))
	}

	var nonceResp NonceResponse
	if err := json.Unmarshal(body, &nonceResp); err != nil {
		return "", err
	}

	return nonceResp.Nonce, nil
}

// IsDeployed checks if the Safe wallet is deployed
func (c *RelayerClient) IsDeployed(ctx context.Context) (bool, error) {
	path := "/deployed?address=" + c.safeAddr.Hex()
	url := c.baseURL + path

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return false, err
	}

	headers := c.generateBuilderHeaders("GET", path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return false, fmt.Errorf("deployed check failed: %d %s", resp.StatusCode, string(body))
	}

	var result struct {
		Deployed bool `json:"deployed"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return false, err
	}

	return result.Deployed, nil
}

// encodeMultiSend encodes multiple transactions into a single multiSend call
func encodeMultiSend(txns []SafeTransaction) (common.Address, []byte, error) {
	if len(txns) == 1 {
		// Single transaction - no need for multiSend
		return txns[0].To, txns[0].Data, nil
	}

	// Pack each transaction for multiSend
	// Format: operation (1 byte) + to (20 bytes) + value (32 bytes) + dataLength (32 bytes) + data
	var packed []byte
	for _, tx := range txns {
		value := tx.Value
		if value == nil {
			value = big.NewInt(0)
		}

		// Pack: uint8 operation + address to + uint256 value + uint256 dataLength + bytes data
		opByte := []byte{tx.Operation}
		addrBytes := tx.To.Bytes()
		valueBytes := common.LeftPadBytes(value.Bytes(), 32)
		dataLenBytes := common.LeftPadBytes(big.NewInt(int64(len(tx.Data))).Bytes(), 32)

		packed = append(packed, opByte...)
		packed = append(packed, addrBytes...)
		packed = append(packed, valueBytes...)
		packed = append(packed, dataLenBytes...)
		packed = append(packed, tx.Data...)
	}

	// Encode multiSend(bytes transactions)
	multiSendABI, _ := abi.JSON(strings.NewReader(`[{"inputs":[{"internalType":"bytes","name":"transactions","type":"bytes"}],"name":"multiSend","outputs":[],"stateMutability":"payable","type":"function"}]`))
	data, err := multiSendABI.Pack("multiSend", packed)
	if err != nil {
		return common.Address{}, nil, err
	}

	return common.HexToAddress(MultiSendAddr), data, nil
}

// createSafeTypedDataHash creates the EIP-712 typed data hash for Safe transaction
func (c *RelayerClient) createSafeTypedDataHash(to common.Address, data []byte, operation uint8, nonce *big.Int) ([]byte, error) {
	// Safe domain
	domain := apitypes.TypedDataDomain{
		ChainId:           math.NewHexOrDecimal256(int64(c.chainID)),
		VerifyingContract: c.safeAddr.Hex(),
	}

	// SafeTx type
	types := apitypes.Types{
		"EIP712Domain": []apitypes.Type{
			{Name: "chainId", Type: "uint256"},
			{Name: "verifyingContract", Type: "address"},
		},
		"SafeTx": []apitypes.Type{
			{Name: "to", Type: "address"},
			{Name: "value", Type: "uint256"},
			{Name: "data", Type: "bytes"},
			{Name: "operation", Type: "uint8"},
			{Name: "safeTxGas", Type: "uint256"},
			{Name: "baseGas", Type: "uint256"},
			{Name: "gasPrice", Type: "uint256"},
			{Name: "gasToken", Type: "address"},
			{Name: "refundReceiver", Type: "address"},
			{Name: "nonce", Type: "uint256"},
		},
	}

	// Message values
	message := apitypes.TypedDataMessage{
		"to":             to.Hex(),
		"value":          "0",
		"data":           data,
		"operation":      fmt.Sprintf("%d", operation),
		"safeTxGas":      "0",
		"baseGas":        "0",
		"gasPrice":       "0",
		"gasToken":       "0x0000000000000000000000000000000000000000",
		"refundReceiver": "0x0000000000000000000000000000000000000000",
		"nonce":          nonce.String(),
	}

	typedData := apitypes.TypedData{
		Types:       types,
		PrimaryType: "SafeTx",
		Domain:      domain,
		Message:     message,
	}

	// Hash the typed data
	domainSeparator, err := typedData.HashStruct("EIP712Domain", typedData.Domain.Map())
	if err != nil {
		return nil, fmt.Errorf("failed to hash domain: %w", err)
	}

	messageHash, err := typedData.HashStruct(typedData.PrimaryType, typedData.Message)
	if err != nil {
		return nil, fmt.Errorf("failed to hash message: %w", err)
	}

	// EIP-712 hash: keccak256("\x19\x01" + domainSeparator + messageHash)
	rawData := []byte{0x19, 0x01}
	rawData = append(rawData, domainSeparator...)
	rawData = append(rawData, messageHash...)
	hash := crypto.Keccak256(rawData)

	return hash, nil
}

// signSafeTransaction signs the Safe transaction hash
func (c *RelayerClient) signSafeTransaction(hash []byte) (string, error) {
	// Sign with private key
	sig, err := crypto.Sign(hash, c.privateKey)
	if err != nil {
		return "", err
	}

	// Adjust v value for Ethereum (add 27)
	if sig[64] < 27 {
		sig[64] += 27
	}

	// Pack signature as r + s + v (Gnosis Safe format)
	return "0x" + hex.EncodeToString(sig), nil
}

// Execute submits transactions to the relayer
func (c *RelayerClient) Execute(ctx context.Context, txns []SafeTransaction, metadata string) (*RelayerResponse, error) {
	if len(txns) == 0 {
		return nil, fmt.Errorf("no transactions to execute")
	}

	// Get nonce
	nonceStr, err := c.GetNonce(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get nonce: %w", err)
	}
	nonce, _ := new(big.Int).SetString(nonceStr, 10)

	// Encode transactions (multiSend if multiple)
	to, data, err := encodeMultiSend(txns)
	if err != nil {
		return nil, fmt.Errorf("failed to encode transactions: %w", err)
	}

	// Determine operation type
	operation := uint8(0) // Call
	if len(txns) > 1 {
		operation = 1 // DelegateCall for multiSend
	}

	// Create and sign EIP-712 hash
	hash, err := c.createSafeTypedDataHash(to, data, operation, nonce)
	if err != nil {
		return nil, fmt.Errorf("failed to create typed data hash: %w", err)
	}

	signature, err := c.signSafeTransaction(hash)
	if err != nil {
		return nil, fmt.Errorf("failed to sign transaction: %w", err)
	}

	// Build request
	request := TransactionRequest{
		Type:        "SAFE",
		From:        c.signerAddr.Hex(),
		To:          to.Hex(),
		ProxyWallet: c.safeAddr.Hex(),
		Data:        "0x" + hex.EncodeToString(data),
		Nonce:       nonceStr,
		Signature:   signature,
		SignatureParams: &SignatureParams{
			GasPrice:   "0",
			SafeTxnGas: "0",
			BaseGas:    "0",
		},
		Metadata: metadata,
	}

	// Submit to relayer
	return c.submitTransaction(ctx, request)
}

// submitTransaction posts the transaction request to the relayer
func (c *RelayerClient) submitTransaction(ctx context.Context, request TransactionRequest) (*RelayerResponse, error) {
	path := "/submit"
	url := c.baseURL + path

	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	// Add builder headers
	headers := c.generateBuilderHeaders("POST", path, body)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		return nil, fmt.Errorf("submit failed: %d %s", resp.StatusCode, string(respBody))
	}

	var result RelayerResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

// BuildRedeemTransaction creates a SafeTransaction for redeeming positions
func BuildRedeemTransaction(conditionID common.Hash, indexSet *big.Int) (SafeTransaction, error) {
	// ConditionalTokens.redeemPositions ABI
	ctfABI, _ := abi.JSON(strings.NewReader(`[{
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
	}]`))

	// Pack function call
	data, err := ctfABI.Pack(
		"redeemPositions",
		common.HexToAddress(USDCAddr),   // collateralToken
		common.Hash{},                    // parentCollectionId (0x0)
		conditionID,                      // conditionId
		[]*big.Int{indexSet},            // indexSets
	)
	if err != nil {
		return SafeTransaction{}, err
	}

	return SafeTransaction{
		To:        common.HexToAddress(ConditionalTokensAddr),
		Operation: 0, // Call
		Data:      data,
		Value:     big.NewInt(0),
	}, nil
}
