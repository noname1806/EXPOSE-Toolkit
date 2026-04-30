#!/usr/bin/env python3
"""
SIG Streaming Hub Detection and Evaluation
============================================
Simulates the SIG running as a real-time detection system.
Processes complaints chronologically and flags shadow callback
hubs when their fan-in crosses a threshold.

Uses a temporal split to evaluate: train on old data (2007-2023),
test on new data (2024-2026). Measures detection latency,
precision, and source consistency.

Input:
    results.jsonl (or multiple part files)

Output:
    streaming_hub_eval.csv       - Flagged hubs for manual labeling
    streaming_hub_report.txt     - Full evaluation report

Usage:
    python3 sig_streaming.py results.jsonl
    python3 sig_streaming.py results_part_1.jsonl results_part_2.jsonl results_part_3.jsonl
"""

import json
import csv
import sys
import os
from collections import defaultdict, Counter
from datetime import datetime


# ================================================================
# CONFIGURATION
# ================================================================

# Temporal split date: everything before this is "training"
# (we pretend we already have it), everything after is "test"
# (new complaints arriving in real time)
SPLIT_DATE = datetime(2024, 1, 1)

# Fan-in thresholds to evaluate
THRESHOLDS = [2, 3, 5]


# ================================================================
# STEP 1: EXTRACT DATED CALLBACK EDGES
# ================================================================

def extract_dated_edges(records, main_set):
    """
    Extract callback edges with dates from complaint data.
    Only keeps edges where:
      - The target is a shadow node (not in corpus)
      - The context is 'callback number'
      - The comment has a parseable date

    Returns list of {date, source, target} dicts, sorted by date.
    """
    edges = []

    for r in records:
        source = r['e164']
        for c in r.get('comments', []):
            # Parse date
            d = c.get('date', '')
            if len(d) != 10 or d[4] != '-':
                continue
            try:
                dt = datetime.strptime(d, '%Y-%m-%d')
            except ValueError:
                continue

            # Extract callback edges to shadow nodes
            for mn in c.get('mentioned_numbers', []):
                tgt = mn.get('e164', '')
                if not tgt or tgt == source:
                    continue
                if mn.get('context') != 'callback number':
                    continue
                if tgt in main_set:
                    continue  # Only shadow nodes

                edges.append({
                    'date': dt,
                    'source': source,
                    'target': tgt,
                })

    # Sort chronologically (this is how a streaming system would see them)
    edges.sort(key=lambda x: x['date'])
    return edges


# ================================================================
# STEP 2: SIMULATE STREAMING DETECTION
# ================================================================

def simulate_streaming(edges, split_date, thresholds):
    """
    Process edges chronologically, simulating a streaming system.

    For each shadow callback hub, track:
    - When it was first mentioned (first_seen)
    - Which source numbers mention it (fan-in set)
    - When it crosses each fan-in threshold (threshold_date)

    Returns:
    - hub_data: dict of hub -> {first_seen, sources, threshold_dates}
    - train_edges: count of edges in train period
    - test_edges: count of edges in test period
    """
    hub_data = {}
    train_count = 0
    test_count = 0

    for e in edges:
        hub = e['target']
        source = e['source']
        date = e['date']

        # Initialize hub if first time seeing it
        if hub not in hub_data:
            hub_data[hub] = {
                'first_seen': date,
                'sources': set(),
                'threshold_dates': {},
            }

        # Track which period this edge belongs to
        if date < split_date:
            train_count += 1
        else:
            test_count += 1

        # Record fan-in BEFORE adding new source
        old_fanin = len(hub_data[hub]['sources'])

        # Add source to fan-in set
        hub_data[hub]['sources'].add(source)

        # Record fan-in AFTER adding new source
        new_fanin = len(hub_data[hub]['sources'])

        # Check if any threshold was crossed IN THE TEST PERIOD
        if date >= split_date:
            for threshold in thresholds:
                if old_fanin < threshold and new_fanin >= threshold:
                    # This hub just crossed the threshold!
                    if threshold not in hub_data[hub]['threshold_dates']:
                        hub_data[hub]['threshold_dates'][threshold] = date

    return hub_data, train_count, test_count


# ================================================================
# STEP 3: EVALUATE FLAGGED HUBS
# ================================================================

def evaluate_flagged_hubs(hub_data, main_lookup, thresholds, split_date):
    """
    For each threshold, compute:
    - How many hubs were flagged in the test period
    - Detection latency (days from first mention to threshold crossing)
    - Source call-type consistency (do all sources share a scam type?)
    """
    results = {}

    for threshold in thresholds:
        # Get hubs that crossed this threshold during test period
        flagged = []
        for hub, data in hub_data.items():
            if threshold in data['threshold_dates']:
                crossed_date = data['threshold_dates'][threshold]
                first_seen = data['first_seen']
                latency_days = (crossed_date - first_seen).days

                # Source call-type analysis
                source_types = Counter()
                for src in data['sources']:
                    if src in main_lookup:
                        source_types[main_lookup[src]['dominant_call_type']] += 1

                if source_types:
                    top_type, top_count = source_types.most_common(1)[0]
                    consistency = top_count / sum(source_types.values())
                else:
                    top_type = 'Unknown'
                    consistency = 0

                flagged.append({
                    'hub': hub,
                    'fan_in': len(data['sources']),
                    'first_seen': first_seen,
                    'crossed_date': crossed_date,
                    'latency_days': latency_days,
                    'top_source_type': top_type,
                    'type_consistency': round(consistency, 2),
                    'source_count': len(data['sources']),
                    'sources': sorted(data['sources']),
                })

        # Sort by fan-in (most connected first)
        flagged.sort(key=lambda x: -x['fan_in'])

        # Compute aggregate metrics
        if flagged:
            latencies = [f['latency_days'] for f in flagged]
            latencies.sort()
            consistent_count = sum(1 for f in flagged if f['type_consistency'] >= 0.5)

            metrics = {
                'hubs_flagged': len(flagged),
                'latency_min': min(latencies),
                'latency_median': latencies[len(latencies) // 2],
                'latency_max': max(latencies),
                'latency_mean': round(sum(latencies) / len(latencies), 1),
                'same_day_detection': sum(1 for l in latencies if l == 0),
                'within_7_days': sum(1 for l in latencies if l <= 7),
                'type_consistent': consistent_count,
                'flagged_hubs': flagged,
            }
        else:
            metrics = {
                'hubs_flagged': 0,
                'flagged_hubs': [],
            }

        results[threshold] = metrics

    return results


# ================================================================
# MAIN
# ================================================================

def main():
    # Determine input files
    if len(sys.argv) > 1:
        jsonl_files = sys.argv[1:]
    else:
        jsonl_files = ['results.jsonl']

    for f in jsonl_files:
        if not os.path.exists(f):
            print(f"ERROR: {f} not found")
            sys.exit(1)

    # Load data
    print("Loading data...")
    records = []
    for f in jsonl_files:
        with open(f, 'r', encoding='utf-8') as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))

    main_set = set(r['e164'] for r in records)
    main_lookup = {r['e164']: r for r in records}
    print(f"  Records: {len(records):,}")

    # Step 1: Extract dated callback edges to shadow nodes
    print("\nExtracting dated callback edges to shadow nodes...")
    edges = extract_dated_edges(records, main_set)
    print(f"  Total callback edges to shadow nodes: {len(edges):,}")

    if not edges:
        print("  No callback edges found. Exiting.")
        return

    print(f"  Date range: {edges[0]['date'].strftime('%Y-%m-%d')} to {edges[-1]['date'].strftime('%Y-%m-%d')}")

    # Step 2: Simulate streaming
    print(f"\nSimulating streaming detection (split: {SPLIT_DATE.strftime('%Y-%m-%d')})...")
    hub_data, train_count, test_count = simulate_streaming(edges, SPLIT_DATE, THRESHOLDS)
    print(f"  Train period edges: {train_count:,}")
    print(f"  Test period edges:  {test_count:,}")
    print(f"  Total shadow hubs seen: {len(hub_data):,}")

    # Step 3: Evaluate
    print("\nEvaluating flagged hubs...")
    eval_results = evaluate_flagged_hubs(hub_data, main_lookup, THRESHOLDS, SPLIT_DATE)

    # ================================================================
    # SAVE OUTPUTS
    # ================================================================

    # 1. Evaluation CSV for manual labeling (use fan-in >= 3 hubs)
    target_threshold = 3
    flagged = eval_results[target_threshold].get('flagged_hubs', [])

    tf_codes = {'800', '888', '877', '866', '855', '844', '833'}

    with open('streaming_hub_eval.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'hub_number', 'fan_in', 'area_code', 'toll_free',
            'first_seen', 'threshold_crossed', 'detection_latency_days',
            'source_count', 'top_source_type', 'type_consistency',
            'source_phones',
            'sample_text_1', 'sample_text_2', 'sample_text_3',
            'is_real_scam_hub',  # USER FILLS: yes / no
        ])

        for hub_info in flagged:
            hub = hub_info['hub']
            ac = hub[2:5] if hub.startswith('+1') and len(hub) >= 5 else '?'

            # Get sample complaint texts from sources
            sample_texts = []
            for src in hub_info['sources'][:3]:
                if src in main_lookup:
                    for c in main_lookup[src].get('comments', []):
                        text = c.get('text', '')
                        if text:
                            sample_texts.append(f"[{src}]: {text[:200]}")
                            break

            writer.writerow([
                hub,
                hub_info['fan_in'],
                ac,
                'Yes' if ac in tf_codes else 'No',
                hub_info['first_seen'].strftime('%Y-%m-%d'),
                hub_info['crossed_date'].strftime('%Y-%m-%d'),
                hub_info['latency_days'],
                hub_info['source_count'],
                hub_info['top_source_type'],
                hub_info['type_consistency'],
                ';'.join(hub_info['sources']),
                sample_texts[0] if len(sample_texts) > 0 else '',
                sample_texts[1] if len(sample_texts) > 1 else '',
                sample_texts[2] if len(sample_texts) > 2 else '',
                '',  # User fills this
            ])

    print(f"\nSaved: streaming_hub_eval.csv ({len(flagged)} hubs at fan-in >= {target_threshold})")

    # 2. Full report
    report = []
    report.append("=" * 70)
    report.append("SIG STREAMING HUB DETECTION REPORT")
    report.append("=" * 70)
    report.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report.append(f"Input: {len(records):,} records")
    report.append(f"Callback edges to shadow nodes: {len(edges):,}")
    report.append(f"Split date: {SPLIT_DATE.strftime('%Y-%m-%d')}")
    report.append(f"Train edges: {train_count:,}")
    report.append(f"Test edges: {test_count:,}")
    report.append("")

    for threshold in THRESHOLDS:
        r = eval_results[threshold]
        report.append(f"THRESHOLD: fan-in >= {threshold}")
        report.append("-" * 40)
        report.append(f"  Hubs flagged in test period: {r['hubs_flagged']}")

        if r['hubs_flagged'] > 0:
            report.append(f"  Detection latency (days):")
            report.append(f"    Min: {r['latency_min']}")
            report.append(f"    Median: {r['latency_median']}")
            report.append(f"    Max: {r['latency_max']}")
            report.append(f"    Mean: {r['latency_mean']}")
            report.append(f"  Same-day detection: {r['same_day_detection']}/{r['hubs_flagged']} ({r['same_day_detection']/r['hubs_flagged']*100:.0f}%)")
            report.append(f"  Within 7 days: {r['within_7_days']}/{r['hubs_flagged']} ({r['within_7_days']/r['hubs_flagged']*100:.0f}%)")
            report.append(f"  Source type consistent: {r['type_consistent']}/{r['hubs_flagged']} ({r['type_consistent']/r['hubs_flagged']*100:.0f}%)")

            report.append(f"\n  Flagged hubs:")
            for h in r['flagged_hubs']:
                report.append(f"    {h['hub']} fan-in={h['fan_in']} "
                              f"latency={h['latency_days']}d "
                              f"type={h['top_source_type']} "
                              f"consistency={h['type_consistency']}")
        report.append("")

    report.append("=" * 70)
    report.append("NEXT STEPS")
    report.append("=" * 70)
    report.append("1. Open streaming_hub_eval.csv")
    report.append("2. For each hub, read the sample texts from source complaints")
    report.append("3. In 'is_real_scam_hub' column, write: yes / no")
    report.append("4. Report precision = yes_count / total")
    report.append("=" * 70)

    report_text = '\n'.join(report)
    with open('streaming_hub_report.txt', 'w') as f:
        f.write(report_text)
    print(f"Saved: streaming_hub_report.txt")

    print("\n" + report_text)


if __name__ == '__main__':
    main()
