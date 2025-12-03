#!/usr/bin/env python3
"""Fetch last 2000 trades for specified addresses and export to CSV."""

import requests
import csv
import time
from datetime import datetime

DATA_API = "https://data-api.polymarket.com"

def fetch_trades(address: str, total_limit: int = 2000) -> list:
    """Fetch trades for an address with pagination."""
    all_trades = []
    offset = 0
    batch_size = 500  # Max allowed by API

    while len(all_trades) < total_limit:
        remaining = total_limit - len(all_trades)
        limit = min(batch_size, remaining)

        url = f"{DATA_API}/activity"
        params = {
            "user": address,
            "type": "TRADE",
            "limit": limit,
            "offset": offset,
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC"
        }

        print(f"  Fetching offset={offset}, limit={limit}...")
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()

        trades = resp.json()
        if not trades:
            print(f"  No more trades at offset {offset}")
            break

        all_trades.extend(trades)
        print(f"  Got {len(trades)} trades (total: {len(all_trades)})")

        if len(trades) < limit:
            break

        offset += limit
        time.sleep(0.2)  # Rate limiting

    return all_trades

def trades_to_csv(trades: list, filename: str):
    """Write trades to CSV file."""
    if not trades:
        print(f"No trades to write to {filename}")
        return

    # Define columns
    columns = [
        "timestamp",
        "datetime",
        "type",
        "side",
        "size",
        "price",
        "usdcSize",
        "outcome",
        "title",
        "slug",
        "eventSlug",
        "conditionId",
        "asset",
        "transactionHash",
        "proxyWallet",
        "name",
        "pseudonym"
    ]

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()

        for trade in trades:
            # Add human-readable datetime
            ts = trade.get('timestamp', 0)
            trade['datetime'] = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S') if ts else ''
            writer.writerow(trade)

    print(f"Wrote {len(trades)} trades to {filename}")

def main():
    addresses = [
        ("0x0Fd419C486d1F53c92CD09c3C52eCA61d6367750", "trades_0x0Fd419C.csv"),
        ("0x05c1882212a41aa8d7df5b70eebe03d9319345b7", "trades_0x05c1882.csv"),
    ]

    for address, filename in addresses:
        print(f"\nFetching trades for {address}...")
        trades = fetch_trades(address, total_limit=2000)
        trades_to_csv(trades, filename)
        print(f"Done: {len(trades)} trades saved to {filename}")

if __name__ == "__main__":
    main()
