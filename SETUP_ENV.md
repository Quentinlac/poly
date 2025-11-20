# Environment Setup

## Create .env File

Since `.env` files are protected, you need to create it manually. Create a file named `.env` in the project root with the following content:

```bash
# Polymarket API Configuration
POLYMARKET_PRIVATE_KEY=0xeb72f0e1f9fb2309fa8e6eabe566b58ee2492eda23616b0ff8341c817d4121bb
POLYMARKET_API_URL=https://clob.polymarket.com
POLYMARKET_DATA_API_URL=https://data-api.polymarket.com

# Server Configuration
PORT=8081

# Environment
GIN_MODE=release
```

## Quick Setup Command

Run this in your terminal from the project root:

```bash
cat > .env << 'EOF'
POLYMARKET_PRIVATE_KEY=0xeb72f0e1f9fb2309fa8e6eabe566b58ee2492eda23616b0ff8341c817d4121bb
POLYMARKET_API_URL=https://clob.polymarket.com
POLYMARKET_DATA_API_URL=https://data-api.polymarket.com
PORT=8081
GIN_MODE=release
EOF
```

## Security Note

⚠️ **IMPORTANT**: The `.env` file is already in `.gitignore` and will NOT be committed to version control. Keep your private key secure and never share it.

## Verification

After creating the `.env` file, the application will automatically:
1. Load the private key from `POLYMARKET_PRIVATE_KEY`
2. Derive your Ethereum address from the private key
3. Use L1 authentication (EIP-712 signing) for Polymarket API requests
4. Fall back to unauthenticated requests if the private key is missing

You can verify the setup by running:
```bash
GOWORK=off go run main.go
```

If authentication is working, you'll see the server start without warnings. If there are issues, check the logs for authentication errors.

