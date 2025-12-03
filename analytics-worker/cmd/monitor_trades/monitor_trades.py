#!/usr/bin/env python3
"""
Comprehensive Trade Detection Monitor
Monitors Mempool + Data API and exports detailed CSV with all columns
Includes the FIX: extracts Word 3 (takerFillAmountShares) for correct size

Usage:
    python3 monitor_trades.py <wallet_address> [duration_minutes] [output.csv]

Example:
    python3 monitor_trades.py 0x05c1882212a41aa8d7df5b70eebe03d9319345b7 20
"""
import asyncio
import json
import csv
import time
import sys
import requests
from datetime import datetime
from websockets import connect
from collections import defaultdict
import threading

# Config - can be overridden by command line args
TARGET_WALLET = "0x05c1882212a41aa8d7df5b70eebe03d9319345b7"
DURATION_MINUTES = 20
CSV_OUTPUT = None  # Set dynamically

# Endpoints
POLYGON_WS = "wss://polygon-bor-rpc.publicnode.com"
POLYGON_HTTP = "https://polygon-bor-rpc.publicnode.com"
LIVEDATA_WS = "wss://ws-live-data.polymarket.com/"
DATA_API = "https://data-api.polymarket.com"
RELAYER = "0xe3f18acc55091e2c48d883fc8c8413319d4ab7b0"

# Storage
detections = {}
lock = threading.Lock()
start_time = None


def decode_fill_amount(input_data):
    """
    Extract Word 3 (takerFillAmountShares) from calldata

    For fillOrders (0x2287e350), the ABI layout is:
    [0] offset to orders array (32 bytes)
    [1] offset to fillAmounts array (32 bytes)
    [2] takerFillAmountUsdc (32 bytes) - USDC amount
    [3] takerFillAmountShares (32 bytes) - THIS IS THE FILL SIZE IN SHARES
    """
    if not input_data or len(input_data) < 266:  # 0x + 8 selector + 128*2 hex chars
        return None, None

    try:
        # Skip 0x and 8 char selector
        data = input_data[10:]
        if len(data) < 256:
            return None, None

        # Word 2 (bytes 64-96) = takerFillAmountUsdc
        word2_hex = data[128:192]
        usdc_amount = int(word2_hex, 16)

        # Word 3 (bytes 96-128) = takerFillAmountShares
        word3_hex = data[192:256]
        shares_amount = int(word3_hex, 16)

        return usdc_amount, shares_amount
    except:
        return None, None


def decode_order_from_calldata(input_data, target_addr):
    """Find and decode Order struct from calldata"""
    if not input_data or len(input_data) < 10:
        return {}

    result = {}
    try:
        data = bytes.fromhex(input_data[10:])
        target_clean = target_addr.lower().replace("0x", "")

        # Scan for Order struct
        for offset in range(0, len(data) - 352, 32):
            # Check for address pattern at maker field (offset+32)
            if offset + 64 > len(data):
                continue
            maker_field = data[offset+32:offset+64]
            if not all(b == 0 for b in maker_field[:12]):
                continue

            # Extract addresses
            maker = maker_field[12:32].hex()
            signer = data[offset+64+12:offset+96].hex() if offset+96 <= len(data) else ""
            taker = data[offset+96+12:offset+128].hex() if offset+128 <= len(data) else ""

            # Check if target is involved
            if target_clean not in maker and target_clean not in signer and target_clean not in taker:
                continue

            # Found our order - extract all fields
            if offset + 352 > len(data):
                continue

            token_id = int.from_bytes(data[offset+128:offset+160], 'big')
            if token_id == 0:
                continue

            maker_amt = int.from_bytes(data[offset+160:offset+192], 'big')
            taker_amt = int.from_bytes(data[offset+192:offset+224], 'big')
            expiration = int.from_bytes(data[offset+224:offset+256], 'big')
            nonce = int.from_bytes(data[offset+256:offset+288], 'big')
            fee_bps = int.from_bytes(data[offset+288:offset+320], 'big')
            side_val = data[offset+320+31] if offset+352 <= len(data) else 0

            if side_val > 1:
                continue

            result = {
                "maker_address": f"0x{maker}",
                "signer_address": f"0x{signer}",
                "taker_address": f"0x{taker}",
                "token_id": str(token_id),
                "order_maker_amount": maker_amt,
                "order_taker_amount": taker_amt,
                "expiration": expiration,
                "order_nonce": nonce,
                "fee_rate_bps": fee_bps,
                "side": "BUY" if side_val == 0 else "SELL",
                "role": "MAKER" if target_clean in maker or target_clean in signer else "TAKER"
            }
            break
    except Exception as e:
        pass

    return result


def get_tx_details(tx_hash):
    """Fetch full transaction from RPC"""
    try:
        resp = requests.post(POLYGON_HTTP, json={
            "jsonrpc": "2.0",
            "method": "eth_getTransactionByHash",
            "params": [tx_hash],
            "id": 1
        }, timeout=3)
        result = resp.json().get("result")
        return result
    except:
        return None


async def monitor_mempool():
    """Monitor Polygon mempool for pending transactions"""
    global detections
    target_clean = TARGET_WALLET.lower().replace("0x", "")

    while time.time() - start_time < DURATION_MINUTES * 60:
        try:
            async with connect(POLYGON_WS) as ws:
                await ws.send(json.dumps({
                    "jsonrpc": "2.0",
                    "method": "eth_subscribe",
                    "params": ["newPendingTransactions"],
                    "id": 1
                }))
                resp = await ws.recv()
                print(f"[MEMPOOL] Subscribed: {resp}", flush=True)

                while time.time() - start_time < DURATION_MINUTES * 60:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=5)
                        data = json.loads(msg)
                        tx_hash = data.get("params", {}).get("result")
                        if not tx_hash:
                            continue

                        # Fetch TX details
                        tx = get_tx_details(tx_hash)
                        if not tx or tx.get("to", "").lower() != RELAYER:
                            continue

                        input_data = tx.get("input", "")
                        if target_clean not in input_data.lower():
                            continue

                        now = datetime.now()

                        # Decode fill amounts (THE FIX)
                        usdc_amt, shares_amt = decode_fill_amount(input_data)

                        # Decode order details
                        order = decode_order_from_calldata(input_data, TARGET_WALLET)

                        # Calculate size from shares (THE FIX)
                        fill_size = shares_amt / 1e6 if shares_amt else 0
                        fill_usdc = usdc_amt / 1e6 if usdc_amt else 0
                        fill_price = fill_usdc / fill_size if fill_size > 0 else 0

                        # Calculate old (wrong) size for comparison
                        old_size = 0
                        if order:
                            if order.get("side") == "BUY":
                                old_size = order.get("order_taker_amount", 0) / 1e6
                            else:
                                old_size = order.get("order_maker_amount", 0) / 1e6

                        with lock:
                            tx_lower = tx_hash.lower()
                            if tx_lower not in detections:
                                detections[tx_lower] = {"first_method": "MEMPOOL", "tx_hash": tx_hash}

                            det = detections[tx_lower]
                            det["mempool_detected_at"] = now.isoformat()
                            det["mempool_from"] = tx.get("from", "")
                            det["mempool_to"] = tx.get("to", "")
                            det["mempool_gas"] = int(tx.get("gas", "0x0"), 16)
                            det["mempool_gas_price"] = tx.get("gasPrice", "")
                            det["mempool_nonce"] = int(tx.get("nonce", "0x0"), 16)
                            det["mempool_input_len"] = (len(input_data) - 2) // 2
                            det["mempool_function"] = input_data[2:10] if len(input_data) > 10 else ""

                            # Fill amounts (THE FIX)
                            det["mempool_fill_usdc"] = fill_usdc
                            det["mempool_fill_shares"] = fill_size
                            det["mempool_fill_price"] = fill_price

                            # Order details
                            det["mempool_side"] = order.get("side", "")
                            det["mempool_role"] = order.get("role", "")
                            det["mempool_token_id"] = order.get("token_id", "")
                            det["mempool_maker_address"] = order.get("maker_address", "")
                            det["mempool_taker_address"] = order.get("taker_address", "")
                            det["mempool_order_maker_amt"] = order.get("order_maker_amount", 0)
                            det["mempool_order_taker_amt"] = order.get("order_taker_amount", 0)
                            det["mempool_expiration"] = order.get("expiration", 0)
                            det["mempool_fee_bps"] = order.get("fee_rate_bps", 0)

                            # Old vs new size comparison
                            det["mempool_old_size_bug"] = old_size
                            det["mempool_new_size_fixed"] = fill_size
                            det["size_difference"] = fill_size - old_size

                        print(f"[MEMPOOL] 🎯 {order.get('side','')} | NEW_SIZE={fill_size:.4f} | OLD_SIZE={old_size:.4f} | DIFF={fill_size-old_size:.4f} | TX={tx_hash[:16]}", flush=True)

                    except asyncio.TimeoutError:
                        continue
        except Exception as e:
            print(f"[MEMPOOL] Error: {e}, reconnecting...", flush=True)
            await asyncio.sleep(5)


async def monitor_livedata():
    """Monitor LiveData WebSocket"""
    global detections

    while time.time() - start_time < DURATION_MINUTES * 60:
        try:
            # Try without extra_headers first (for older websockets versions)
            try:
                ws = await connect(LIVEDATA_WS, additional_headers={"Origin": "https://polymarket.com"})
            except TypeError:
                try:
                    ws = await connect(LIVEDATA_WS, extra_headers={"Origin": "https://polymarket.com"})
                except TypeError:
                    ws = await connect(LIVEDATA_WS)

            async with ws:
                await ws.send(json.dumps({
                    "action": "subscribe",
                    "subscriptions": [{"topic": "activity", "type": "orders_matched"}]
                }))
                print(f"[LIVEDATA] Subscribed", flush=True)

                while time.time() - start_time < DURATION_MINUTES * 60:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=90)
                        data = json.loads(msg)

                        if data.get("type") != "orders_matched":
                            continue

                        payload = data.get("payload", {})
                        if payload.get("proxyWallet", "").lower() != TARGET_WALLET.lower():
                            continue

                        now = datetime.now()
                        tx_hash = payload.get("transactionHash", "")

                        with lock:
                            tx_lower = tx_hash.lower()
                            if tx_lower not in detections:
                                detections[tx_lower] = {"first_method": "LIVEDATA", "tx_hash": tx_hash}

                            det = detections[tx_lower]
                            det["livedata_detected_at"] = now.isoformat()
                            det["livedata_side"] = payload.get("side", "")
                            det["livedata_size"] = payload.get("size", 0)
                            det["livedata_price"] = payload.get("price", 0)
                            det["livedata_outcome"] = payload.get("outcome", "")
                            det["livedata_condition_id"] = payload.get("conditionId", "")

                            # Calculate latency
                            if "mempool_detected_at" in det:
                                mempool_time = datetime.fromisoformat(det["mempool_detected_at"])
                                det["mempool_to_livedata_ms"] = int((now - mempool_time).total_seconds() * 1000)

                        print(f"[LIVEDATA] 🎯 {payload.get('side','')} | SIZE={payload.get('size',0):.4f} | PRICE={payload.get('price',0):.4f} | TX={tx_hash[:16]}", flush=True)

                    except asyncio.TimeoutError:
                        continue
        except Exception as e:
            print(f"[LIVEDATA] Error: {e}, reconnecting...", flush=True)
            await asyncio.sleep(5)


def monitor_data_api():
    """Poll Data API every 2 seconds"""
    global detections

    while time.time() - start_time < DURATION_MINUTES * 60:
        try:
            resp = requests.get(f"{DATA_API}/activity?user={TARGET_WALLET}&type=TRADE&limit=10", timeout=5)
            trades = resp.json()

            for trade in trades:
                tx_hash = trade.get("transactionHash", "")
                trade_time = datetime.fromtimestamp(trade.get("timestamp", 0))

                if trade_time < datetime.fromtimestamp(start_time):
                    continue

                now = datetime.now()

                with lock:
                    tx_lower = tx_hash.lower()
                    if tx_lower not in detections:
                        detections[tx_lower] = {"first_method": "DATA_API", "tx_hash": tx_hash}

                    det = detections[tx_lower]
                    if "dataapi_detected_at" not in det:
                        det["dataapi_detected_at"] = now.isoformat()
                        det["dataapi_side"] = trade.get("side", "")
                        det["dataapi_size"] = trade.get("size", 0)
                        det["dataapi_price"] = trade.get("price", 0)
                        det["dataapi_outcome"] = trade.get("outcome", "")
                        det["dataapi_title"] = trade.get("title", "")

                        if "mempool_detected_at" in det:
                            mempool_time = datetime.fromisoformat(det["mempool_detected_at"])
                            det["mempool_to_dataapi_ms"] = int((now - mempool_time).total_seconds() * 1000)

                        print(f"[DATA_API] 🎯 {trade.get('side','')} | SIZE={trade.get('size',0):.4f} | TX={tx_hash[:16]}", flush=True)
        except Exception as e:
            pass

        time.sleep(2)


def write_csv():
    """Write all detections to CSV"""
    columns = [
        # Identification
        "tx_hash", "first_method",

        # Mempool data
        "mempool_detected_at", "mempool_from", "mempool_to", "mempool_gas", "mempool_gas_price",
        "mempool_nonce", "mempool_input_len", "mempool_function",

        # Mempool fill amounts (THE FIX)
        "mempool_fill_usdc", "mempool_fill_shares", "mempool_fill_price",

        # Mempool order details
        "mempool_side", "mempool_role", "mempool_token_id",
        "mempool_maker_address", "mempool_taker_address",
        "mempool_order_maker_amt", "mempool_order_taker_amt",
        "mempool_expiration", "mempool_fee_bps",

        # Size comparison (OLD vs NEW)
        "mempool_old_size_bug", "mempool_new_size_fixed", "size_difference",

        # LiveData
        "livedata_detected_at", "livedata_side", "livedata_size", "livedata_price",
        "livedata_outcome", "livedata_condition_id",

        # Data API
        "dataapi_detected_at", "dataapi_side", "dataapi_size", "dataapi_price",
        "dataapi_outcome", "dataapi_title",

        # Latencies
        "mempool_to_livedata_ms", "mempool_to_dataapi_ms"
    ]

    with open(CSV_OUTPUT, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()

        for det in sorted(detections.values(), key=lambda x: x.get("mempool_detected_at", x.get("livedata_detected_at", ""))):
            writer.writerow(det)

    print(f"\n✅ CSV written to {CSV_OUTPUT} ({len(detections)} trades)", flush=True)


async def main():
    global start_time
    start_time = time.time()

    print("="*70, flush=True)
    print("COMPREHENSIVE TRADE DETECTION MONITOR", flush=True)
    print("="*70, flush=True)
    print(f"Target: {TARGET_WALLET}", flush=True)
    print(f"Duration: {DURATION_MINUTES} minutes", flush=True)
    print(f"Output: {CSV_OUTPUT}", flush=True)
    print("="*70, flush=True)
    print(flush=True)

    # Start Data API polling in thread
    api_thread = threading.Thread(target=monitor_data_api, daemon=True)
    api_thread.start()

    # Run async monitors
    await asyncio.gather(
        monitor_mempool(),
        monitor_livedata()
    )

    write_csv()


if __name__ == "__main__":
    # Parse command line args
    if len(sys.argv) >= 2:
        TARGET_WALLET = sys.argv[1]

    if len(sys.argv) >= 3:
        DURATION_MINUTES = int(sys.argv[2])

    if len(sys.argv) >= 4:
        CSV_OUTPUT = sys.argv[3]
    else:
        CSV_OUTPUT = f"detection_comprehensive_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    asyncio.run(main())
