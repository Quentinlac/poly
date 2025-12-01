#!/usr/bin/env python3
"""
Compare detection latency between:
1. Polymarket LiveData WebSocket (ws-live-data.polymarket.com)
2. Polygon Blockchain WebSocket (confirmed OrderFilled events)
3. Polygon Mempool (pending transactions) - if available

This helps understand where time is spent in the detection pipeline.
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from collections import defaultdict
import websockets

# Configuration
TARGET_USER = "djangodja"  # Match by name or pseudonym
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E".lower()
ORDER_FILLED_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"

# WebSocket URLs
LIVEDATA_WS_URL = "wss://ws-live-data.polymarket.com/"
POLYGON_WS_URLS = [
    "wss://polygon-bor-rpc.publicnode.com",
    "wss://polygon.drpc.org",
    "wss://polygon-mainnet.g.alchemy.com/v2/demo",  # Alchemy demo
]

# Track detections by tx hash
detections = defaultdict(dict)
stats = {
    "livedata_count": 0,
    "polygon_confirmed_count": 0,
    "polygon_pending_count": 0,
}

async def monitor_livedata_ws():
    """Monitor Polymarket LiveData WebSocket for trades"""
    print(f"[LiveData] Connecting to {LIVEDATA_WS_URL}")

    headers = {"Origin": "https://polymarket.com"}

    try:
        async with websockets.connect(LIVEDATA_WS_URL, additional_headers=headers) as ws:
            print(f"[LiveData] Connected!")

            # Subscribe to all trades
            subscribe_msg = {
                "action": "subscribe",
                "subscriptions": [{"topic": "activity", "type": "orders_matched"}]
            }
            await ws.send(json.dumps(subscribe_msg))
            print(f"[LiveData] Subscribed to all orders_matched")

            while True:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    received_at = time.time()

                    data = json.loads(msg)
                    if data.get("type") != "orders_matched":
                        continue

                    payload = data.get("payload", {})
                    tx_hash = payload.get("transactionHash", "").lower()
                    name = payload.get("name", "").lower()
                    pseudonym = payload.get("pseudonym", "").lower()

                    # Check if target user
                    target = TARGET_USER.lower()
                    if target not in name and target not in pseudonym:
                        continue

                    stats["livedata_count"] += 1

                    # Record detection
                    if tx_hash not in detections:
                        detections[tx_hash] = {"first_seen": received_at}
                    detections[tx_hash]["livedata_at"] = received_at
                    detections[tx_hash]["livedata_payload"] = payload

                    # Calculate latency vs trade timestamp
                    trade_ts = payload.get("timestamp", 0)
                    latency_ms = (received_at - trade_ts) * 1000 if trade_ts else None

                    print(f"\n[LiveData] 🎯 TRADE DETECTED!")
                    print(f"  TX: {tx_hash[:20]}...")
                    print(f"  Side: {payload.get('side')} | Size: {payload.get('size')} | Price: {payload.get('price')}")

                    # Show exact timestamps
                    print(f"  LiveData received:    {datetime.fromtimestamp(received_at).strftime('%H:%M:%S.%f')[:-3]}")
                    if "polygon_confirmed_at" in detections[tx_hash]:
                        print(f"  Polygon confirmed at: {datetime.fromtimestamp(detections[tx_hash]['polygon_confirmed_at']).strftime('%H:%M:%S.%f')[:-3]}")
                    if "polygon_pending_at" in detections[tx_hash]:
                        print(f"  Polygon pending at:   {datetime.fromtimestamp(detections[tx_hash]['polygon_pending_at']).strftime('%H:%M:%S.%f')[:-3]}")

                    print(f"  Trade timestamp:      {datetime.fromtimestamp(payload.get('timestamp', 0)).strftime('%H:%M:%S.%f')[:-3] if payload.get('timestamp') else 'unknown'}")

                    # Compare with Polygon detection if available
                    if "polygon_pending_at" in detections[tx_hash]:
                        diff = (received_at - detections[tx_hash]["polygon_pending_at"]) * 1000
                        print(f"  Mempool → LiveData:   {diff:.0f}ms")

                except asyncio.TimeoutError:
                    continue
                except json.JSONDecodeError:
                    continue

    except Exception as e:
        print(f"[LiveData] Error: {e}")


async def monitor_polygon_confirmed():
    """Monitor Polygon blockchain for confirmed OrderFilled events"""

    for url in POLYGON_WS_URLS:
        try:
            print(f"[Polygon-Confirmed] Trying {url}")
            async with websockets.connect(url) as ws:
                print(f"[Polygon-Confirmed] Connected!")

                # Subscribe to CTF Exchange OrderFilled events
                subscribe_msg = {
                    "jsonrpc": "2.0",
                    "method": "eth_subscribe",
                    "params": [
                        "logs",
                        {
                            "address": CTF_EXCHANGE,
                            "topics": [ORDER_FILLED_TOPIC]
                        }
                    ],
                    "id": 1
                }
                await ws.send(json.dumps(subscribe_msg))

                # Get subscription ID
                response = await asyncio.wait_for(ws.recv(), timeout=10)
                resp_data = json.loads(response)
                if "error" in resp_data:
                    print(f"[Polygon-Confirmed] Subscription error: {resp_data['error']}")
                    continue

                sub_id = resp_data.get("result")
                print(f"[Polygon-Confirmed] Subscribed (id={sub_id})")

                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        received_at = time.time()

                        data = json.loads(msg)
                        if data.get("method") != "eth_subscription":
                            continue

                        result = data.get("params", {}).get("result", {})
                        tx_hash = result.get("transactionHash", "").lower()

                        if not tx_hash:
                            continue

                        # We see ALL OrderFilled events, not just target user
                        # Record first detection time
                        if tx_hash not in detections:
                            detections[tx_hash] = {"first_seen": received_at}

                        if "polygon_confirmed_at" not in detections[tx_hash]:
                            detections[tx_hash]["polygon_confirmed_at"] = received_at
                            stats["polygon_confirmed_count"] += 1

                            # Check if this was already seen via LiveData
                            if "livedata_at" in detections[tx_hash]:
                                diff = (received_at - detections[tx_hash]["livedata_at"]) * 1000
                                print(f"\n[Polygon-Confirmed] TX {tx_hash[:20]}... detected")
                                print(f"  vs LiveData: {diff:+.0f}ms {'(Polygon slower)' if diff > 0 else '(Polygon faster)'}")

                    except asyncio.TimeoutError:
                        continue
                    except json.JSONDecodeError:
                        continue

        except Exception as e:
            print(f"[Polygon-Confirmed] Error with {url}: {e}")
            continue

    print(f"[Polygon-Confirmed] All endpoints failed")


async def monitor_polygon_pending():
    """Monitor Polygon mempool for pending transactions"""

    # Try to find an RPC that supports newPendingTransactions
    pending_urls = [
        "wss://polygon-bor-rpc.publicnode.com",
        "wss://polygon.drpc.org",
    ]

    for url in pending_urls:
        try:
            print(f"[Polygon-Pending] Trying {url}")
            async with websockets.connect(url) as ws:
                print(f"[Polygon-Pending] Connected!")

                # Subscribe to pending transactions
                subscribe_msg = {
                    "jsonrpc": "2.0",
                    "method": "eth_subscribe",
                    "params": ["newPendingTransactions"],
                    "id": 1
                }
                await ws.send(json.dumps(subscribe_msg))

                # Get subscription ID
                response = await asyncio.wait_for(ws.recv(), timeout=10)
                resp_data = json.loads(response)

                if "error" in resp_data:
                    print(f"[Polygon-Pending] Subscription error: {resp_data['error']}")
                    print(f"[Polygon-Pending] This RPC doesn't support pending tx monitoring")
                    continue

                sub_id = resp_data.get("result")
                print(f"[Polygon-Pending] Subscribed to pending transactions (id={sub_id})")

                pending_count = 0
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        received_at = time.time()

                        data = json.loads(msg)
                        if data.get("method") != "eth_subscription":
                            continue

                        # This gives us just the tx hash
                        tx_hash = data.get("params", {}).get("result", "").lower()

                        if not tx_hash:
                            continue

                        pending_count += 1
                        if pending_count % 100 == 0:
                            print(f"[Polygon-Pending] Seen {pending_count} pending txs...")

                        # Record if we haven't seen this tx yet
                        if tx_hash not in detections:
                            detections[tx_hash] = {"first_seen": received_at}

                        if "polygon_pending_at" not in detections[tx_hash]:
                            detections[tx_hash]["polygon_pending_at"] = received_at
                            stats["polygon_pending_count"] += 1

                    except asyncio.TimeoutError:
                        continue
                    except json.JSONDecodeError:
                        continue

        except Exception as e:
            print(f"[Polygon-Pending] Error with {url}: {e}")
            continue

    print(f"[Polygon-Pending] No RPC with pending tx support found")


async def print_stats():
    """Periodically print statistics"""
    start_time = time.time()

    while True:
        await asyncio.sleep(30)
        elapsed = int(time.time() - start_time)

        print(f"\n{'='*60}")
        print(f"STATS after {elapsed}s:")
        print(f"  LiveData detections: {stats['livedata_count']}")
        print(f"  Polygon confirmed: {stats['polygon_confirmed_count']}")
        print(f"  Polygon pending: {stats['polygon_pending_count']}")

        # Calculate average latency differences
        livedata_faster = 0
        polygon_faster = 0

        for tx_hash, d in detections.items():
            if "livedata_at" in d and "polygon_confirmed_at" in d:
                if d["livedata_at"] < d["polygon_confirmed_at"]:
                    livedata_faster += 1
                else:
                    polygon_faster += 1

        if livedata_faster + polygon_faster > 0:
            print(f"  LiveData faster: {livedata_faster} times")
            print(f"  Polygon faster: {polygon_faster} times")

        print(f"{'='*60}\n")


async def main():
    print(f"Starting comparison monitor at {datetime.now().isoformat()}")
    print(f"Target user: {TARGET_USER}")
    print(f"Will run for 10 minutes...")
    print("="*60)

    # Run all monitors concurrently
    tasks = [
        asyncio.create_task(monitor_livedata_ws()),
        asyncio.create_task(monitor_polygon_confirmed()),
        asyncio.create_task(monitor_polygon_pending()),
        asyncio.create_task(print_stats()),
    ]

    # Run for 10 minutes
    try:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=600)
    except asyncio.TimeoutError:
        print("\n10 minutes elapsed, stopping...")
    except KeyboardInterrupt:
        print("\nInterrupted by user...")

    # Final summary
    print("\n" + "="*60)
    print("FINAL SUMMARY")
    print("="*60)
    print(f"Total LiveData detections: {stats['livedata_count']}")
    print(f"Total Polygon confirmed: {stats['polygon_confirmed_count']}")
    print(f"Total Polygon pending: {stats['polygon_pending_count']}")

    # Show all target user trades with timing comparison
    target_trades = [(h, d) for h, d in detections.items() if "livedata_payload" in d]
    if target_trades:
        print(f"\nTarget user trades ({len(target_trades)}):")
        print("-"*60)
        for tx_hash, d in target_trades:
            payload = d.get("livedata_payload", {})
            print(f"\nTX: {tx_hash[:30]}...")
            print(f"  Side: {payload.get('side')} | Size: {payload.get('size'):.4f} | Price: {payload.get('price'):.4f}")

            if "livedata_at" in d:
                print(f"  LiveData at: {datetime.fromtimestamp(d['livedata_at']).strftime('%H:%M:%S.%f')[:-3]}")
            if "polygon_confirmed_at" in d:
                print(f"  Polygon confirmed at: {datetime.fromtimestamp(d['polygon_confirmed_at']).strftime('%H:%M:%S.%f')[:-3]}")
                if "livedata_at" in d:
                    diff = (d["livedata_at"] - d["polygon_confirmed_at"]) * 1000
                    print(f"  Difference: {diff:+.0f}ms {'(LiveData slower)' if diff > 0 else '(LiveData faster)'}")
            if "polygon_pending_at" in d:
                print(f"  Polygon pending at: {datetime.fromtimestamp(d['polygon_pending_at']).strftime('%H:%M:%S.%f')[:-3]}")


if __name__ == "__main__":
    asyncio.run(main())
