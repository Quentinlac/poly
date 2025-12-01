#!/usr/bin/env python3
"""
Monitor ws-live-data.polymarket.com for trades from a specific user.
Logs exact timestamps for latency measurement.
"""

import asyncio
import json
import signal
import sys
from datetime import datetime, timezone
import websockets

WS_URL = "wss://ws-live-data.polymarket.com/"
TARGET_USER = "djangodja"  # Match by name or pseudonym (case-insensitive)

# Track if we should keep running
running = True

def signal_handler(sig, frame):
    global running
    print(f"\n[{datetime.now().isoformat()}] Stopping monitor...")
    running = False

async def monitor():
    signal.signal(signal.SIGINT, signal_handler)

    print(f"[{datetime.now().isoformat()}] Connecting to {WS_URL}")
    print(f"[{datetime.now().isoformat()}] Monitoring for user: {TARGET_USER}")
    print(f"[{datetime.now().isoformat()}] Will run for 10 minutes...")
    print("-" * 80)

    headers = {"Origin": "https://polymarket.com"}

    try:
        async with websockets.connect(WS_URL, additional_headers=headers) as ws:
            print(f"[{datetime.now().isoformat()}] Connected!")

            # Subscribe to ALL orders_matched events (no filter)
            subscribe_msg = {
                "action": "subscribe",
                "subscriptions": [
                    {"topic": "activity", "type": "orders_matched"},
                ]
            }
            await ws.send(json.dumps(subscribe_msg))
            print(f"[{datetime.now().isoformat()}] Subscribed to ALL orders_matched events")
            print("-" * 80)

            start_time = datetime.now()
            trade_count = 0
            target_trades = []

            while running:
                # Check if 10 minutes have passed
                elapsed = (datetime.now() - start_time).total_seconds()
                if elapsed > 600:  # 10 minutes
                    print(f"\n[{datetime.now().isoformat()}] 10 minutes elapsed, stopping...")
                    break

                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    received_at = datetime.now()

                    try:
                        data = json.loads(msg)
                    except json.JSONDecodeError:
                        continue

                    # Skip non-trade messages
                    if data.get("type") != "orders_matched":
                        continue

                    trade_count += 1
                    payload = data.get("payload", {})

                    name = payload.get("name", "").lower()
                    pseudonym = payload.get("pseudonym", "").lower()
                    target = TARGET_USER.lower()

                    # Check if it's our target user
                    is_target = target in name or target in pseudonym

                    # Parse trade timestamp (comes as Unix seconds in UTC)
                    trade_ts = payload.get("timestamp", 0)
                    trade_time = datetime.fromtimestamp(trade_ts, tz=timezone.utc) if trade_ts else None

                    # Calculate latency (convert received_at to UTC for proper comparison)
                    latency_ms = None
                    if trade_time:
                        received_at_utc = datetime.now(timezone.utc)
                        latency_ms = (received_at_utc - trade_time).total_seconds() * 1000

                    if is_target:
                        target_trades.append({
                            "received_at": received_at.isoformat(),
                            "trade_time": trade_time.isoformat() if trade_time else "unknown",
                            "latency_ms": latency_ms,
                            "payload": payload
                        })

                        print(f"\n{'='*80}")
                        print(f"🎯 TARGET USER TRADE DETECTED!")
                        print(f"{'='*80}")
                        print(f"  Received at:  {received_at.isoformat()}")
                        print(f"  Trade time:   {trade_time.isoformat() if trade_time else 'unknown'}")
                        print(f"  Latency:      {latency_ms:.0f}ms" if latency_ms else "  Latency:      unknown")
                        print(f"  Name:         {payload.get('name')}")
                        print(f"  Pseudonym:    {payload.get('pseudonym')}")
                        print(f"  Side:         {payload.get('side')}")
                        print(f"  Size:         {payload.get('size')}")
                        print(f"  Price:        {payload.get('price')}")
                        print(f"  Outcome:      {payload.get('outcome')}")
                        print(f"  Event:        {payload.get('eventSlug')}")
                        print(f"  TX Hash:      {payload.get('transactionHash')}")
                        print(f"{'='*80}\n")
                    else:
                        # Just show a dot for other trades to indicate activity
                        print(f".", end="", flush=True)
                        if trade_count % 50 == 0:
                            print(f" [{trade_count} trades seen, {int(elapsed)}s elapsed]")

                except asyncio.TimeoutError:
                    continue
                except websockets.exceptions.ConnectionClosed:
                    print(f"\n[{datetime.now().isoformat()}] Connection closed, reconnecting...")
                    break

            print(f"\n" + "=" * 80)
            print(f"SUMMARY")
            print(f"=" * 80)
            print(f"Total trades seen: {trade_count}")
            print(f"Target user trades: {len(target_trades)}")
            if target_trades:
                print(f"\n{'#':<3} {'Side':<5} {'Size':>10} {'Price':>8} {'USDC':>10} {'Trade Time (UTC)':<24} {'Received (Local)':<24} {'Latency':>10}")
                print("-" * 100)
                for i, t in enumerate(target_trades, 1):
                    p = t['payload']
                    size = p.get('size', 0)
                    price = p.get('price', 0)
                    usdc = size * price
                    side = p.get('side', '?')
                    latency_str = f"{t['latency_ms']:.0f}ms" if t['latency_ms'] else "?"
                    print(f"{i:<3} {side:<5} {size:>10.4f} {price:>8.4f} ${usdc:>9.2f} {t['trade_time']:<24} {t['received_at']:<24} {latency_str:>10}")

    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(monitor())
