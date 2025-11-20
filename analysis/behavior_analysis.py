#!/usr/bin/env python3
"""
Behavioral Analysis Script for Polymarket Trades

Analyzes 12 behavioral hypotheses to find correlations between
successful and unsuccessful trades.
"""

import requests
import sys
from collections import defaultdict
from datetime import datetime
from statistics import mean, median, stdev
import json

BASE_URL = "http://localhost:8081"


def fetch_trades(address: str) -> list:
    """Fetch all trades for a user"""
    url = f"{BASE_URL}/api/users/{address}/trades"
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Error fetching trades: {resp.status_code}")
        return []
    data = resp.json()
    # API returns {"trades": [...]}
    if isinstance(data, dict) and "trades" in data:
        return data["trades"]
    return data


def fetch_positions(address: str) -> list:
    """Fetch aggregated positions for a user"""
    url = f"{BASE_URL}/api/users/{address}/positions"
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Error fetching positions: {resp.status_code}")
        return []
    data = resp.json()
    # API returns {"count": N, "positions": [...]}
    if isinstance(data, dict) and "positions" in data:
        return data["positions"]
    return data


def aggregate_position_data(trades: list, positions: list) -> dict:
    """
    Build position data structure with:
    - P&L from positions
    - All trades for each position
    - Behavioral metrics
    """
    # Map title+outcome -> position metrics
    pos_map = {}
    for pos in positions:
        key = f"{pos['title']}|{pos['outcome']}"
        pos_map[key] = {
            "title": pos["title"],
            "outcome": pos["outcome"],
            "gain_loss": pos["gain_loss"],
            "total_bought": pos["total_bought"],
            "total_sold": pos["total_sold"],
            "qty_bought": pos["qty_bought"],
            "qty_sold": pos["qty_sold"],
            "buy_count": pos["buy_count"],
            "sell_count": pos["sell_count"],
            "duration_mins": pos["duration_mins"],
            "first_buy_at": pos.get("first_buy_at", ""),
            "last_buy_at": pos.get("last_buy_at", ""),
            "first_sell_at": pos.get("first_sell_at", ""),
            "last_sell_at": pos.get("last_sell_at", ""),
            "trades": [],
        }

    # Add individual trades to positions
    for trade in trades:
        key = f"{trade['title']}|{trade['outcome']}"
        if key in pos_map:
            pos_map[key]["trades"].append(trade)

    return pos_map


def calculate_metrics(pos_data: dict) -> dict:
    """Calculate behavioral metrics for each position"""
    metrics = {}

    for key, pos in pos_data.items():
        trades = pos["trades"]
        if not trades:
            continue

        # Basic metrics
        total_value = pos["total_bought"]
        num_buys = pos["buy_count"]
        num_sells = pos["sell_count"]

        # Maker/Taker analysis
        maker_count = sum(1 for t in trades if t.get("is_maker", False))
        taker_count = len(trades) - maker_count
        maker_ratio = maker_count / len(trades) if trades else 0

        # Buy trades analysis
        buy_trades = [t for t in trades if t["side"] == "BUY"]

        # Average buy price
        avg_buy_price = 0
        if buy_trades:
            total_cost = sum(t["size"] * t["price"] for t in buy_trades)
            total_qty = sum(t["size"] for t in buy_trades)
            avg_buy_price = total_cost / total_qty if total_qty > 0 else 0

        # Time between trades (DCA pattern)
        timestamps = sorted([
            datetime.fromisoformat(t["timestamp"].replace("Z", "+00:00"))
            for t in buy_trades
        ]) if buy_trades else []

        time_gaps = []
        for i in range(1, len(timestamps)):
            gap = (timestamps[i] - timestamps[i-1]).total_seconds() / 60  # minutes
            time_gaps.append(gap)

        avg_time_gap = mean(time_gaps) if time_gaps else 0

        # Trade size analysis
        buy_sizes = [t["size"] * t["price"] for t in buy_trades]
        avg_trade_size = mean(buy_sizes) if buy_sizes else 0

        # Size progression (are later buys bigger? = scaling into position)
        size_progression = 0
        if len(buy_sizes) > 1:
            first_half = mean(buy_sizes[:len(buy_sizes)//2])
            second_half = mean(buy_sizes[len(buy_sizes)//2:])
            size_progression = (second_half - first_half) / first_half if first_half > 0 else 0

        # Time of day analysis
        hours = [
            datetime.fromisoformat(t["timestamp"].replace("Z", "+00:00")).hour
            for t in buy_trades
        ] if buy_trades else []
        avg_hour = mean(hours) if hours else 12

        # Burst buying analysis - count buys within 10 minutes of each other
        burst_buy_count = 0
        if time_gaps:
            burst_buy_count = sum(1 for gap in time_gaps if gap <= 10)  # 10 minute threshold

        # Time to exit - minutes between last buy and first sell
        time_to_exit_mins = 0
        sell_trades = [t for t in trades if t["side"] == "SELL" or t.get("type") == "REDEEM"]
        if buy_trades and sell_trades:
            last_buy_time = max(
                datetime.fromisoformat(t["timestamp"].replace("Z", "+00:00"))
                for t in buy_trades
            )
            first_sell_time = min(
                datetime.fromisoformat(t["timestamp"].replace("Z", "+00:00"))
                for t in sell_trades
            )
            if first_sell_time > last_buy_time:
                time_to_exit_mins = (first_sell_time - last_buy_time).total_seconds() / 60

        # Is fast exit (< 2 hours)?
        is_fast_exit = 0 < time_to_exit_mins < 120

        metrics[key] = {
            "gain_loss": pos["gain_loss"],
            "total_value": total_value,
            "num_buys": num_buys,
            "num_sells": num_sells,
            "maker_ratio": maker_ratio,
            "maker_count": maker_count,
            "taker_count": taker_count,
            "avg_buy_price": avg_buy_price,
            "avg_time_gap_mins": avg_time_gap,
            "avg_trade_size": avg_trade_size,
            "size_progression": size_progression,
            "duration_mins": pos["duration_mins"],
            "avg_hour": avg_hour,
            "total_trades": len(trades),
            "burst_buy_count": burst_buy_count,
            "time_to_exit_mins": time_to_exit_mins,
            "is_fast_exit": is_fast_exit,
        }

    return metrics


def analyze_hypothesis(metrics: dict, hypothesis_num: int) -> dict:
    """
    Analyze a specific hypothesis against the data.
    Returns correlation results.
    """
    # Split into winners and losers
    winners = {k: v for k, v in metrics.items() if v["gain_loss"] > 0}
    losers = {k: v for k, v in metrics.items() if v["gain_loss"] < 0}

    if not winners or not losers:
        return {"error": "Insufficient data (need both winners and losers)"}

    result = {
        "winner_count": len(winners),
        "loser_count": len(losers),
        "total_winner_pnl": sum(w["gain_loss"] for w in winners.values()),
        "total_loser_pnl": sum(l["gain_loss"] for l in losers.values()),
    }

    if hypothesis_num == 1:
        # Conviction Sizing - larger positions = more successful?
        winner_sizes = [w["total_value"] for w in winners.values()]
        loser_sizes = [l["total_value"] for l in losers.values()]
        result["metric_name"] = "Total Position Value (USDC)"
        result["winner_avg"] = mean(winner_sizes)
        result["loser_avg"] = mean(loser_sizes)
        result["winner_median"] = median(winner_sizes)
        result["loser_median"] = median(loser_sizes)
        result["correlation"] = "POSITIVE" if result["winner_avg"] > result["loser_avg"] else "NEGATIVE"

    elif hypothesis_num == 2:
        # Maker vs Taker - maker trades more successful?
        winner_maker_ratio = [w["maker_ratio"] for w in winners.values()]
        loser_maker_ratio = [l["maker_ratio"] for l in losers.values()]
        result["metric_name"] = "Maker Trade Ratio"
        result["winner_avg"] = mean(winner_maker_ratio)
        result["loser_avg"] = mean(loser_maker_ratio)
        result["correlation"] = "POSITIVE" if result["winner_avg"] > result["loser_avg"] else "NEGATIVE"

    elif hypothesis_num == 3:
        # DCA Pattern - more buys = better?
        winner_buys = [w["num_buys"] for w in winners.values()]
        loser_buys = [l["num_buys"] for l in losers.values()]
        result["metric_name"] = "Number of Buy Trades (DCA)"
        result["winner_avg"] = mean(winner_buys)
        result["loser_avg"] = mean(loser_buys)
        result["winner_median"] = median(winner_buys)
        result["loser_median"] = median(loser_buys)
        result["correlation"] = "POSITIVE" if result["winner_avg"] > result["loser_avg"] else "NEGATIVE"

    elif hypothesis_num == 4:
        # Quick Exit Discipline - shorter holding = better?
        winner_duration = [w["duration_mins"] for w in winners.values() if w["duration_mins"] > 0]
        loser_duration = [l["duration_mins"] for l in losers.values() if l["duration_mins"] > 0]
        result["metric_name"] = "Holding Duration (minutes)"
        result["winner_avg"] = mean(winner_duration) if winner_duration else 0
        result["loser_avg"] = mean(loser_duration) if loser_duration else 0
        result["winner_median"] = median(winner_duration) if winner_duration else 0
        result["loser_median"] = median(loser_duration) if loser_duration else 0
        # Shorter = better for this hypothesis
        result["correlation"] = "POSITIVE" if result["winner_avg"] < result["loser_avg"] else "NEGATIVE"

    elif hypothesis_num == 5:
        # Asymmetric Position Building - more buys than sells = conviction
        winner_ratio = [w["num_buys"] / (w["num_sells"] + 1) for w in winners.values()]
        loser_ratio = [l["num_buys"] / (l["num_sells"] + 1) for l in losers.values()]
        result["metric_name"] = "Buy/Sell Ratio"
        result["winner_avg"] = mean(winner_ratio)
        result["loser_avg"] = mean(loser_ratio)
        result["correlation"] = "POSITIVE" if result["winner_avg"] > result["loser_avg"] else "NEGATIVE"

    elif hypothesis_num == 6:
        # Time of Day - specific hours better?
        winner_hours = [w["avg_hour"] for w in winners.values()]
        loser_hours = [l["avg_hour"] for l in losers.values()]
        result["metric_name"] = "Average Trading Hour (UTC)"
        result["winner_avg"] = mean(winner_hours)
        result["loser_avg"] = mean(loser_hours)
        if winner_hours:
            result["winner_stdev"] = stdev(winner_hours) if len(winner_hours) > 1 else 0
        if loser_hours:
            result["loser_stdev"] = stdev(loser_hours) if len(loser_hours) > 1 else 0
        result["correlation"] = "INCONCLUSIVE"  # Time is cyclical

    elif hypothesis_num == 7:
        # Price Improvement - lower avg buy price = better?
        winner_prices = [w["avg_buy_price"] for w in winners.values() if w["avg_buy_price"] > 0]
        loser_prices = [l["avg_buy_price"] for l in losers.values() if l["avg_buy_price"] > 0]
        result["metric_name"] = "Average Buy Price"
        result["winner_avg"] = mean(winner_prices) if winner_prices else 0
        result["loser_avg"] = mean(loser_prices) if loser_prices else 0
        result["winner_median"] = median(winner_prices) if winner_prices else 0
        result["loser_median"] = median(loser_prices) if loser_prices else 0
        # Lower buy price = better profit potential
        result["correlation"] = "POSITIVE" if result["winner_avg"] < result["loser_avg"] else "NEGATIVE"

    elif hypothesis_num == 8:
        # Position Scaling - adding to winners (size progression)
        winner_prog = [w["size_progression"] for w in winners.values()]
        loser_prog = [l["size_progression"] for l in losers.values()]
        result["metric_name"] = "Size Progression (2nd half vs 1st half)"
        result["winner_avg"] = mean(winner_prog)
        result["loser_avg"] = mean(loser_prog)
        result["correlation"] = "POSITIVE" if result["winner_avg"] > result["loser_avg"] else "NEGATIVE"

    elif hypothesis_num == 9:
        # Early Entry - check if we can infer this
        # Use avg_time_gap as proxy (wider gaps = spread out, possibly early)
        winner_gaps = [w["avg_time_gap_mins"] for w in winners.values() if w["avg_time_gap_mins"] > 0]
        loser_gaps = [l["avg_time_gap_mins"] for l in losers.values() if l["avg_time_gap_mins"] > 0]
        result["metric_name"] = "Avg Time Gap Between Buys (mins)"
        result["winner_avg"] = mean(winner_gaps) if winner_gaps else 0
        result["loser_avg"] = mean(loser_gaps) if loser_gaps else 0
        result["correlation"] = "INCONCLUSIVE"

    elif hypothesis_num == 10:
        # Low Frequency Trading - fewer total trades = better?
        winner_trades = [w["total_trades"] for w in winners.values()]
        loser_trades = [l["total_trades"] for l in losers.values()]
        result["metric_name"] = "Total Trades per Position"
        result["winner_avg"] = mean(winner_trades)
        result["loser_avg"] = mean(loser_trades)
        result["winner_median"] = median(winner_trades)
        result["loser_median"] = median(loser_trades)
        # Fewer trades = more deliberate
        result["correlation"] = "POSITIVE" if result["winner_avg"] < result["loser_avg"] else "NEGATIVE"

    elif hypothesis_num == 11:
        # Burst Buying - multiple buys in short period (< 10 mins apart)
        winner_bursts = [w["burst_buy_count"] for w in winners.values()]
        loser_bursts = [l["burst_buy_count"] for l in losers.values()]
        result["metric_name"] = "Burst Buy Count (buys < 10 mins apart)"
        result["winner_avg"] = mean(winner_bursts)
        result["loser_avg"] = mean(loser_bursts)
        result["winner_median"] = median(winner_bursts)
        result["loser_median"] = median(loser_bursts)
        result["correlation"] = "POSITIVE" if result["winner_avg"] > result["loser_avg"] else "NEGATIVE"

    elif hypothesis_num == 12:
        # Fast Exit - selling within 2 hours of last buy
        winner_exits = [w["time_to_exit_mins"] for w in winners.values() if w["time_to_exit_mins"] > 0]
        loser_exits = [l["time_to_exit_mins"] for l in losers.values() if l["time_to_exit_mins"] > 0]
        result["metric_name"] = "Time to Exit (mins from last buy to first sell)"
        result["winner_avg"] = mean(winner_exits) if winner_exits else 0
        result["loser_avg"] = mean(loser_exits) if loser_exits else 0
        result["winner_median"] = median(winner_exits) if winner_exits else 0
        result["loser_median"] = median(loser_exits) if loser_exits else 0

        # Also show % of fast exits (< 2h)
        winner_fast = sum(1 for w in winners.values() if w["is_fast_exit"])
        loser_fast = sum(1 for l in losers.values() if l["is_fast_exit"])
        result["winner_fast_exit_pct"] = (winner_fast / len(winners) * 100) if winners else 0
        result["loser_fast_exit_pct"] = (loser_fast / len(losers) * 100) if losers else 0

        # Faster exit = better for this hypothesis
        result["correlation"] = "POSITIVE" if result["winner_avg"] < result["loser_avg"] else "NEGATIVE"

    return result


def print_analysis_report(metrics: dict, address: str):
    """Print the full analysis report"""
    print("\n" + "="*70)
    print(f"BEHAVIORAL ANALYSIS REPORT")
    print(f"User: {address}")
    print("="*70)

    hypotheses = [
        (1, "Conviction Sizing", "Larger position sizes indicate stronger conviction → better outcomes"),
        (2, "Maker vs Taker", "Using maker orders (limit) more often → better prices → better outcomes"),
        (3, "DCA Pattern", "Multiple buy entries over time → lower avg cost → better outcomes"),
        (4, "Quick Exit Discipline", "Shorter holding periods → disciplined exits → better outcomes"),
        (5, "Asymmetric Position Building", "More buys than sells → conviction → better outcomes"),
        (6, "Time of Day", "Trading at specific hours → better liquidity → better outcomes"),
        (7, "Price Improvement", "Lower average buy price → better entry → better outcomes"),
        (8, "Position Scaling", "Adding to winners (size progression) → better outcomes"),
        (9, "Early Entry", "Entering positions early → better prices → better outcomes"),
        (10, "Low Frequency Trading", "Fewer, more deliberate trades → better outcomes"),
        (11, "Burst Buying", "Multiple buys in short period (<10 mins) → aggressive entry → better outcomes"),
        (12, "Fast Exit (<2h)", "Selling within 2 hours of last buy → quick profits → better outcomes"),
    ]

    results = []

    for num, name, description in hypotheses:
        print(f"\n{'─'*70}")
        print(f"Hypothesis {num}: {name}")
        print(f"{'─'*70}")
        print(f"Theory: {description}")

        analysis = analyze_hypothesis(metrics, num)
        results.append((num, name, analysis))

        if "error" in analysis:
            print(f"Result: {analysis['error']}")
            continue

        print(f"\nData Summary:")
        print(f"  Winners: {analysis['winner_count']} positions, Total P&L: ${analysis['total_winner_pnl']:.2f}")
        print(f"  Losers:  {analysis['loser_count']} positions, Total P&L: ${analysis['total_loser_pnl']:.2f}")

        print(f"\nMetric: {analysis['metric_name']}")
        print(f"  Winner Avg: {analysis['winner_avg']:.4f}")
        print(f"  Loser Avg:  {analysis['loser_avg']:.4f}")

        if "winner_median" in analysis:
            print(f"  Winner Median: {analysis['winner_median']:.4f}")
            print(f"  Loser Median:  {analysis['loser_median']:.4f}")

        # Special case for Fast Exit hypothesis
        if "winner_fast_exit_pct" in analysis:
            print(f"  Winner Fast Exit (<2h): {analysis['winner_fast_exit_pct']:.1f}%")
            print(f"  Loser Fast Exit (<2h):  {analysis['loser_fast_exit_pct']:.1f}%")

        correlation = analysis.get("correlation", "UNKNOWN")
        if correlation == "POSITIVE":
            print(f"\n  ✓ SUPPORTS hypothesis - correlation found")
        elif correlation == "NEGATIVE":
            print(f"\n  ✗ REFUTES hypothesis - opposite correlation found")
        else:
            print(f"\n  ? INCONCLUSIVE - no clear correlation")

    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)

    supported = []
    refuted = []
    inconclusive = []

    for num, name, analysis in results:
        if "error" not in analysis:
            corr = analysis.get("correlation", "")
            avg_win = analysis.get("total_winner_pnl", 0) / analysis.get("winner_count", 1)
            avg_loss = analysis.get("total_loser_pnl", 0) / analysis.get("loser_count", 1)

            if corr == "POSITIVE":
                supported.append((num, name, avg_win, avg_loss, analysis))
            elif corr == "NEGATIVE":
                refuted.append((num, name, avg_win, avg_loss, analysis))
            else:
                inconclusive.append((num, name, avg_win, avg_loss, analysis))

    print("\nSupported Hypotheses (✓):")
    for h in supported:
        print(f"  • {h[0]}. {h[1]}")

    print("\nRefuted Hypotheses (✗):")
    for h in refuted:
        print(f"  • {h[0]}. {h[1]}")

    print("\nInconclusive:")
    for h in inconclusive:
        print(f"  • {h[0]}. {h[1]}")

    # Cluster Analysis with Dollar Amounts
    print("\n" + "="*70)
    print("CLUSTER ANALYSIS - P&L IMPACT")
    print("="*70)

    # Calculate overall averages
    all_results = supported + refuted + inconclusive
    if all_results:
        avg_win_overall = all_results[0][2]
        avg_loss_overall = all_results[0][3]
    else:
        avg_win_overall = 0
        avg_loss_overall = 0

    print(f"\nOverall: Avg Win ${avg_win_overall:.2f} | Avg Loss ${avg_loss_overall:.2f}")

    # Cluster 1: WIN PATTERNS (Supported hypotheses)
    print("\n" + "─"*70)
    print("CLUSTER 1: WIN PATTERNS (use these behaviors)")
    print("─"*70)

    if supported:
        for num, name, avg_win, avg_loss, analysis in supported:
            metric_name = analysis.get("metric_name", "")
            winner_avg = analysis.get("winner_avg", 0)
            loser_avg = analysis.get("loser_avg", 0)

            print(f"\n  {num}. {name}")
            print(f"     Avg Win: ${avg_win:.2f} | Metric: {winner_avg:.2f} (winners) vs {loser_avg:.2f} (losers)")

            # Provide actionable threshold
            if winner_avg > loser_avg:
                print(f"     → Target: {metric_name} > {loser_avg:.2f}")
            else:
                print(f"     → Target: {metric_name} < {loser_avg:.2f}")
    else:
        print("  No winning patterns identified")

    # Cluster 2: LOSS PATTERNS (Refuted hypotheses - opposite behavior causes losses)
    print("\n" + "─"*70)
    print("CLUSTER 2: LOSS PATTERNS (avoid these behaviors)")
    print("─"*70)

    if refuted:
        for num, name, avg_win, avg_loss, analysis in refuted:
            metric_name = analysis.get("metric_name", "")
            winner_avg = analysis.get("winner_avg", 0)
            loser_avg = analysis.get("loser_avg", 0)

            print(f"\n  {num}. {name}")
            print(f"     Avg Loss: ${avg_loss:.2f} | Metric: {loser_avg:.2f} (losers) vs {winner_avg:.2f} (winners)")

            # Provide actionable avoidance threshold
            if loser_avg > winner_avg:
                print(f"     → Avoid: {metric_name} > {winner_avg:.2f}")
            else:
                print(f"     → Avoid: {metric_name} < {winner_avg:.2f}")
    else:
        print("  No loss patterns identified")

    # Net Impact Summary
    print("\n" + "─"*70)
    print("NET IMPACT SUMMARY")
    print("─"*70)

    total_win_impact = len(supported) * avg_win_overall if supported else 0
    total_loss_impact = len(refuted) * abs(avg_loss_overall) if refuted else 0

    print(f"\n  Win patterns identified: {len(supported)}")
    print(f"  Loss patterns identified: {len(refuted)}")
    print(f"  Estimated impact per trade following all patterns: ${avg_win_overall - abs(avg_loss_overall):.2f}")

    print("\n" + "="*70)


def main():
    if len(sys.argv) < 2:
        print("Usage: python behavior_analysis.py <user_address>")
        print("Example: python behavior_analysis.py 0x1234...")
        sys.exit(1)

    address = sys.argv[1]
    print(f"Fetching data for user: {address}")

    # Fetch data
    trades = fetch_trades(address)
    if not trades:
        print("No trades found. Make sure the server is running and the user exists.")
        sys.exit(1)

    positions = fetch_positions(address)
    if not positions:
        print("No positions found.")
        sys.exit(1)

    print(f"Found {len(trades)} trades and {len(positions)} positions")

    # Aggregate and analyze
    pos_data = aggregate_position_data(trades, positions)
    metrics = calculate_metrics(pos_data)

    # Print report
    print_analysis_report(metrics, address)


if __name__ == "__main__":
    main()
