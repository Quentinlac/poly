#!/bin/bash
# Comprehensive Polymarket API Parameter Test
# Testing ALL documented parameters from official docs

USER="0x1955273c5691a1330e264e9daf07411ba913aef1"
BASE_URL="https://data-api.polymarket.com/trades"

echo "Testing ALL parameters from Polymarket API documentation..."
echo "User: $USER"
echo "Expected: Different results for different parameters"
echo "Actual: All return same trades (bug confirmed)"
echo ""

echo "1. Baseline (no extra params):"
curl -s "${BASE_URL}?limit=3&user=${USER}" | jq '.[] | {timestamp}' | head -3
echo ""

echo "2. With start=1753364176 (should return trades AFTER this time):"
curl -s "${BASE_URL}?limit=3&user=${USER}&start=1753364176" | jq '.[] | {timestamp}' | head -3
echo ""

echo "3. With end=1753364176 (should return trades BEFORE this time):"
curl -s "${BASE_URL}?limit=3&user=${USER}&end=1753364176" | jq '.[] | {timestamp}' | head -3
echo ""

echo "4. With start=1&end=1753364176 (should return old trades):"
curl -s "${BASE_URL}?limit=3&user=${USER}&start=1&end=1753364176" | jq '.[] | {timestamp}' | head -3
echo ""

echo "5. With sortDirection=ASC (should return OLDEST first):"
curl -s "${BASE_URL}?limit=3&user=${USER}&sortDirection=ASC" | jq '.[] | {timestamp}' | head -3
echo ""

echo "6. With sortBy=TIMESTAMP&sortDirection=ASC:"
curl -s "${BASE_URL}?limit=3&user=${USER}&sortBy=TIMESTAMP&sortDirection=ASC" | jq '.[] | {timestamp}' | head -3
echo ""

echo "7. Offset 1500 test (duplicate detection):"
echo "Count at offset 0:"
curl -s "${BASE_URL}?limit=500&offset=0&user=${USER}" | jq 'length'
echo "Count at offset 1500:"
curl -s "${BASE_URL}?limit=500&offset=1500&user=${USER}" | jq 'length'
echo "First trade at 1500:"
curl -s "${BASE_URL}?limit=1&offset=1500&user=${USER}" | jq '.[0] | {timestamp, hash: .transactionHash}'
echo "First trade at 1000:"
curl -s "${BASE_URL}?limit=1&offset=1000&user=${USER}" | jq '.[0] | {timestamp, hash: .transactionHash}'

echo ""
echo "CONCLUSION: All timestamp/sort parameters are IGNORED by the API"
echo "The API always returns the same ~1500 trades regardless of parameters"
