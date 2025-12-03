#!/bin/bash
# Polymarket User Transaction Decoder
# Usage: ./decode_user_tx.sh <transaction_hash> <user_address>
# Output: Tokens | Price | BUY/SELL + TOTALS

TX_HASH="${1:-0xa2139121ef112d55673f196e5e6c72118e2187e545be25a2d39bd3ee3da48ab1}"
USER_ADDRESS="${2:-0x05c1882212a41aa8d7df5b70eebe03d9319345b7}"
RPC_URL="https://polygon-rpc.com"
OUTPUT_FILE="user_trade_${TX_HASH:0:10}.txt"

# Normalize user address to lowercase
USER_ADDRESS=$(echo "$USER_ADDRESS" | tr '[:upper:]' '[:lower:]')

echo "Decoding tx: $TX_HASH"
echo "For user: $USER_ADDRESS"
echo ""

# Temp file for accumulating trades
TEMP_FILE=$(mktemp)

# Get transaction receipt
RECEIPT=$(curl -s -X POST $RPC_URL \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "eth_getTransactionReceipt",
    "params": ["'"$TX_HASH"'"],
    "id": 1
  }')

# Check if transaction exists
STATUS=$(echo "$RECEIPT" | jq -r '.result.status')
if [ "$STATUS" != "0x1" ]; then
  echo "ERROR: Transaction failed or not found"
  exit 1
fi

{
echo "TX: $TX_HASH"
echo "User: $USER_ADDRESS"
echo "Time: $(date)"
echo "----------------------------------------"

# Parse OrderFilled events
echo "$RECEIPT" | jq -r '.result.logs[] | select(.topics[0] == "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6") | "\(.topics[1])|\(.topics[2])|\(.topics[3])|\(.data)"' | while IFS='|' read -r orderHash topic2 topic3 data; do
  
  # Extract maker and taker addresses (lowercase)
  maker=$(echo "0x${topic2:26}" | tr '[:upper:]' '[:lower:]')
  taker=$(echo "0x${topic3:26}" | tr '[:upper:]' '[:lower:]')
  
  # Only count when user is MAKER (their order being filled)
  # This avoids double-counting when user's order is matched against multiple counterparties
  if [ "$maker" != "$USER_ADDRESS" ]; then
    continue
  fi
  
  # Decode data field
  makerAssetId=$(echo $data | cut -c3-66)
  takerAssetId=$(echo $data | cut -c67-130)
  makerAmount=$((16#$(echo $data | cut -c131-194)))
  takerAmount=$((16#$(echo $data | cut -c195-258)))
  
  # Determine trade direction (user is always maker now)
  if [ "$makerAssetId" = "0000000000000000000000000000000000000000000000000000000000000000" ]; then
    # Maker gives USDC → BUY tokens
    usdc_amount=$makerAmount
    token_amount=$takerAmount
    direction="BUY"
  else
    # Maker gives tokens → SELL tokens
    token_amount=$makerAmount
    usdc_amount=$takerAmount
    direction="SELL"
  fi
  
  # Calculate price
  if [ "$token_amount" -gt 0 ]; then
    price=$(echo "scale=6; $usdc_amount / $token_amount" | bc)
  else
    price="0"
  fi
  
  # Format amounts
  tokens_formatted=$(echo "scale=2; $token_amount / 1000000" | bc)
  usdc_formatted=$(echo "scale=6; $usdc_amount / 1000000" | bc)
  
  # Output individual trade
  echo ""
  echo "Direction: $direction"
  echo "Tokens: $tokens_formatted"
  echo "USDC: $usdc_formatted"
  echo "Price: $price USDC/token"
  echo "Order: ${orderHash:0:18}..."
  
  # Write to temp file for totals calculation
  echo "$direction $token_amount $usdc_amount" >> "$TEMP_FILE"

done

echo ""
echo "========================================"
echo "SUMMARY"
echo "========================================"

# Calculate totals from temp file
total_buy_tokens=0
total_buy_usdc=0
total_sell_tokens=0
total_sell_usdc=0
buy_count=0
sell_count=0

while read -r dir tokens usdc; do
  if [ "$dir" = "BUY" ]; then
    total_buy_tokens=$((total_buy_tokens + tokens))
    total_buy_usdc=$((total_buy_usdc + usdc))
    buy_count=$((buy_count + 1))
  elif [ "$dir" = "SELL" ]; then
    total_sell_tokens=$((total_sell_tokens + tokens))
    total_sell_usdc=$((total_sell_usdc + usdc))
    sell_count=$((sell_count + 1))
  fi
done < "$TEMP_FILE"

# Format and display totals
if [ "$buy_count" -gt 0 ]; then
  buy_tokens_fmt=$(echo "scale=2; $total_buy_tokens / 1000000" | bc)
  buy_usdc_fmt=$(echo "scale=6; $total_buy_usdc / 1000000" | bc)
  if [ "$total_buy_tokens" -gt 0 ]; then
    buy_avg_price=$(echo "scale=6; $total_buy_usdc / $total_buy_tokens" | bc)
  else
    buy_avg_price="0"
  fi
  echo ""
  echo "TOTAL BUY ($buy_count trades):"
  echo "  Tokens: $buy_tokens_fmt"
  echo "  USDC spent: $buy_usdc_fmt"
  echo "  Avg Price: $buy_avg_price USDC/token"
fi

if [ "$sell_count" -gt 0 ]; then
  sell_tokens_fmt=$(echo "scale=2; $total_sell_tokens / 1000000" | bc)
  sell_usdc_fmt=$(echo "scale=6; $total_sell_usdc / 1000000" | bc)
  if [ "$total_sell_tokens" -gt 0 ]; then
    sell_avg_price=$(echo "scale=6; $total_sell_usdc / $total_sell_tokens" | bc)
  else
    sell_avg_price="0"
  fi
  echo ""
  echo "TOTAL SELL ($sell_count trades):"
  echo "  Tokens: $sell_tokens_fmt"
  echo "  USDC received: $sell_usdc_fmt"
  echo "  Avg Price: $sell_avg_price USDC/token"
fi

if [ "$buy_count" -eq 0 ] && [ "$sell_count" -eq 0 ]; then
  echo ""
  echo "No trades found for this user in this transaction."
fi

echo ""
echo "----------------------------------------"

} | tee "$OUTPUT_FILE"

# Cleanup
rm -f "$TEMP_FILE"

echo ""
echo "Saved to: $OUTPUT_FILE"
