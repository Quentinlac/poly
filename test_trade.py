#!/usr/bin/env python3
import os
import json
import hmac
import hashlib
import base64
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.constants import POLYGON

load_dotenv()

# Monkey-patch the HMAC signature function to use proper JSON serialization
def fixed_build_hmac_signature(secret, timestamp, method, requestPath, body=None):
    """Fixed version that uses json.dumps instead of str() for body"""
    base64_secret = base64.urlsafe_b64decode(secret)
    message = str(timestamp) + str(method) + str(requestPath)
    if body:
        # Use json.dumps for proper JSON serialization
        message += json.dumps(body, separators=(',', ':'))

    h = hmac.new(base64_secret, bytes(message, "utf-8"), hashlib.sha256)
    return (base64.urlsafe_b64encode(h.digest())).decode("utf-8")

# Apply the monkey-patch
import py_clob_client.signing.hmac as hmac_module
hmac_module.build_hmac_signature = fixed_build_hmac_signature

def main():
    private_key = os.getenv("POLYMARKET_PRIVATE_KEY")
    if not private_key:
        print("ERROR: POLYMARKET_PRIVATE_KEY not set")
        return

    print(f"Private key found: {private_key[:6]}...{private_key[-4:]}")

    # Get wallet address from private key (this is the signer)
    from eth_account import Account
    account = Account.from_key(private_key)
    signer_address = account.address
    print(f"Signer address: {signer_address}")

    # For Email/Magic login:
    # - signature_type=1
    # - funder = Polymarket profile address (where USDC is held)
    funder_address = "0xffe81fbf9b7babb9b6914b83f82f1931735c6fea"
    print(f"Funder address: {funder_address}")

    # Use Email/Magic wallet settings
    client = ClobClient(
        host="https://clob.polymarket.com",
        key=private_key,
        chain_id=POLYGON,
        signature_type=1,  # 1 for Email/Magic login
        funder=funder_address,  # This is where USDC is held
    )

    # Delete old and create new API credentials
    print("\nDeleting existing API credentials...")
    try:
        # Need creds to delete, so derive first
        old_creds = client.derive_api_key()
        client.set_api_creds(old_creds)
        client.delete_api_key()
        print("Old credentials deleted")
    except Exception as e:
        print(f"Could not delete: {e}")

    print("Creating new API credentials...")
    creds = client.create_api_key()
    client.set_api_creds(creds)
    print(f"API Key: {creds.api_key}")
    print(f"API Secret: {creds.api_secret[:20]}...")
    print(f"API Passphrase: {creds.api_passphrase}")

    # Test if we can access authenticated endpoints
    print("\nTesting authenticated access...")
    try:
        # Get all API keys for this wallet
        keys = client.get_api_keys()
        print(f"API keys for wallet: {keys}")
    except Exception as e:
        print(f"Could not get API keys: {e}")

    try:
        # Get open orders
        orders = client.get_orders()
        print(f"Open orders: {len(orders) if orders else 0}")
    except Exception as e:
        print(f"Could not get orders: {e}")

    # Test cancel all orders (authenticated mutation)
    print("\nTesting cancel all orders (mutation)...")
    try:
        result = client.cancel_all()
        print(f"Cancel all result: {result}")
    except Exception as e:
        print(f"Cancel all failed: {e}")

    # Test market - Fed interest rates
    token_id = "87769991026114894163580777793845523168226980076553814689875238288185044414090"
    market_name = "Fed decreases interest rates by 25 bps after December 2025"

    # Get order book
    print(f"\nGetting order book for: {market_name}")
    book = client.get_order_book(token_id)

    if not book.asks:
        print("No asks in order book!")
        return

    best_ask = float(book.asks[0].price)
    print(f"Best ask price: {best_ask}")

    # Calculate size for $1 order
    size = round(1.0 / best_ask, 2)
    print(f"Buying {size} shares at {best_ask} = ${size * best_ask:.2f}")

    # Create market order (FOK - Fill-Or-Kill)
    print("\nCreating and placing $1 market BUY order...")
    from py_clob_client.clob_types import MarketOrderArgs
    from py_clob_client.order_builder.constants import BUY

    market_order_args = MarketOrderArgs(
        token_id=token_id,
        amount=1.0,  # $1 USDC
        side=BUY,
    )

    try:
        # Create market order
        signed_order = client.create_market_order(market_order_args)
        print(f"Market order created with signature: {signed_order.signature[:20]}...")

        # Print order details for comparison
        import json
        order_dict = signed_order.dict()
        print(f"\nPython order:\n{json.dumps(order_dict, indent=2)}")

        # Debug: print the exact EIP-712 values used
        print(f"\nDEBUG EIP-712 values:")
        print(f"  salt: {order_dict['salt']}")
        print(f"  maker: {order_dict['maker']}")
        print(f"  signer: {order_dict['signer']}")
        print(f"  taker: {order_dict['taker']}")
        print(f"  tokenId: {order_dict['tokenId']}")
        print(f"  makerAmount: {order_dict['makerAmount']}")
        print(f"  takerAmount: {order_dict['takerAmount']}")
        print(f"  expiration: {order_dict['expiration']}")
        print(f"  nonce: {order_dict['nonce']}")
        print(f"  feeRateBps: {order_dict['feeRateBps']}")
        print(f"  side: {order_dict['side']}")
        print(f"  signatureType: {order_dict['signatureType']}")

        # Print the EIP-712 hash for comparison
        from poly_eip712_structs import make_domain
        from eth_hash.auto import keccak
        domain = make_domain(
            name="Polymarket CTF Exchange",
            version="1",
            chainId=137,
            verifyingContract="0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
        )
        signable_bytes = signed_order.order.signable_bytes(domain)
        final_hash = keccak(signable_bytes)
        print(f"\nPython signable_bytes: 0x{signable_bytes.hex()}")
        print(f"Python EIP-712 final hash: 0x{final_hash.hex()}")

        # Post as FOK (Fill-Or-Kill)
        resp = client.post_order(signed_order, OrderType.FOK)
        print(f"\nOrder response: {resp}")

        if resp.get('success'):
            print("ORDER SUCCESS!")
            print(f"Order ID: {resp.get('orderID', 'N/A')}")
        else:
            print(f"Order failed: {resp}")

    except Exception as e:
        print(f"\nOrder failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
