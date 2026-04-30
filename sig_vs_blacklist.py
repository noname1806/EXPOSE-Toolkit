#!/usr/bin/env python3
"""
SIG vs Complaint-Volume Blacklist Comparison
=============================================
Head-to-head comparison: can a traditional blacklist find
the shadow callback hubs that the SIG discovers?

The blacklist approach: flag phone numbers with many complaints.
The SIG approach: flag shadow callback hubs with high fan-in.

Input:
    results.jsonl (or multiple part files)

Output:
    sig_vs_blacklist_report.txt  - Full comparison report
    sig_vs_blacklist.csv         - Per-hub comparison data

Usage:
    python3 sig_vs_blacklist.py results.jsonl
    python3 sig_vs_blacklist.py results_part_1.jsonl results_part_2.jsonl results_part_3.jsonl
"""

import json
import csv
import sys
import os
from collections import defaultdict, Counter
from datetime import datetime


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

    # ================================================================
    # STEP 1: LOAD DATA
    # ================================================================
    print("Step 1: Loading data...")
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

    # ================================================================
    # STEP 2: COUNT COMPLAINTS PER NUMBER
    # ================================================================
    # This is what a blacklist uses: how many times was this
    # number reported? More complaints = more likely to be scam.
    print("\nStep 2: Counting complaints per number...")
    complaint_counts = {}
    for r in records:
        complaint_counts[r['e164']] = len(r.get('comments', []))

    print(f"  Numbers with >= 1 complaint:  {sum(1 for c in complaint_counts.values() if c >= 1):,}")
    print(f"  Numbers with >= 5 complaints: {sum(1 for c in complaint_counts.values() if c >= 5):,}")
    print(f"  Numbers with >= 10 complaints: {sum(1 for c in complaint_counts.values() if c >= 10):,}")
    print(f"  Numbers with >= 20 complaints: {sum(1 for c in complaint_counts.values() if c >= 20):,}")

    # ================================================================
    # STEP 3: FIND ALL SHADOW CALLBACK HUBS (SIG METHOD)
    # ================================================================
    # A shadow callback hub is a phone number that:
    #   1. Is NOT in the corpus (no complaint page = shadow)
    #   2. Is mentioned as a "callback number" by at least one
    #      corpus number
    # Fan-in = how many different corpus numbers point to it
    print("\nStep 3: Finding shadow callback hubs (SIG method)...")

    # For each number mentioned as a callback target,
    # track which source numbers mention it
    callback_targets = defaultdict(set)

    for r in records:
        source = r['e164']
        for c in r.get('comments', []):
            for mn in c.get('mentioned_numbers', []):
                tgt = mn.get('e164', '')
                if tgt and tgt != source and mn.get('context') == 'callback number':
                    callback_targets[tgt].add(source)

    # Split into shadow (not in corpus) and bridge (in corpus)
    shadow_hubs = {
        tgt: sources
        for tgt, sources in callback_targets.items()
        if tgt not in main_set
    }
    bridge_hubs = {
        tgt: sources
        for tgt, sources in callback_targets.items()
        if tgt in main_set
    }

    print(f"  Total callback targets: {len(callback_targets):,}")
    print(f"  Shadow callback hubs (not in corpus): {len(shadow_hubs):,}")
    print(f"  Bridge callback hubs (in corpus): {len(bridge_hubs):,}")

    # ================================================================
    # STEP 4: THE KEY QUESTION - CAN THE BLACKLIST FIND SHADOW HUBS?
    # ================================================================
    print(f"\n{'='*60}")
    print("Step 4: CAN THE BLACKLIST FIND SHADOW HUBS?")
    print(f"{'='*60}")

    print(f"""
  A shadow hub has ZERO complaints filed against it.
  It is not in your corpus. No victim reported it directly.
  It exists ONLY because victims wrote it in their comments.

  A complaint-volume blacklist flags numbers with many
  complaints. But shadow hubs have ZERO complaints.

  Therefore: the blacklist finds ZERO shadow hubs.
  At ANY threshold. Always.
""")

    print(f"  {'Blacklist threshold':<30} {'Numbers flagged':>15} {'Shadow hubs found':>18}")
    print(f"  {'-'*65}")
    for threshold in [1, 2, 5, 10, 20, 50]:
        flagged = sum(1 for c in complaint_counts.values() if c >= threshold)
        # How many shadow hubs does the blacklist find?
        # ZERO. Because shadow hubs have 0 complaints.
        # They are not in complaint_counts at all.
        shadow_found = 0
        for hub in shadow_hubs:
            if hub in complaint_counts and complaint_counts[hub] >= threshold:
                shadow_found += 1
        print(f"  {'complaints >= ' + str(threshold):<30} {flagged:>15,} {shadow_found:>15} / {len(shadow_hubs)}")

    print(f"\n  SIG method:")
    for k in [1, 2, 3, 5]:
        found = sum(1 for sources in shadow_hubs.values() if len(sources) >= k)
        print(f"  {'fan-in >= ' + str(k):<30} {'N/A':>15} {found:>15} / {len(shadow_hubs)}")

    # ================================================================
    # STEP 5: DISRUPTION LEVERAGE COMPARISON
    # ================================================================
    print(f"\n{'='*60}")
    print("Step 5: DISRUPTION LEVERAGE")
    print(f"{'='*60}")

    print(f"""
  Disruption leverage = how many frontline scam numbers
  are disrupted when you block one target.

  BLACKLIST approach:
    You block the most-complained number. That blocks 1 number.
    You block the top 10. That blocks 10 numbers.
    Leverage = 1.0x (you block N numbers, you disrupt N numbers)

  SIG approach:
    You block a shadow callback hub. Every scam number that
    was directing victims to that hub loses its backend.
    If 15 scam numbers point to one hub, blocking the hub
    disrupts all 15.
    Leverage = 15x for that one hub.
""")

    # Blacklist: rank by complaint count, block top-K
    ranked_by_complaints = sorted(
        complaint_counts.items(), key=lambda x: -x[1]
    )

    print(f"  {'Method':<35} {'Blocked':>8} {'Disrupted':>10} {'Leverage':>10}")
    print(f"  {'-'*65}")

    # Blacklist results
    for k in [10, 50, 100, 541]:
        # Blocking top-K most-complained numbers
        # Each blocked number disrupts exactly itself
        print(f"  {'Blacklist top-' + str(k):<35} {k:>8} {k:>10} {'1.0x':>10}")

    print(f"  {'-'*65}")

    # SIG results
    shadow_ranked = sorted(shadow_hubs.items(), key=lambda x: -len(x[1]))

    for label, k in [('top-10', 10), ('top-50', 50), ('all 541', len(shadow_ranked))]:
        hubs_to_block = shadow_ranked[:k]
        # Disrupted = all unique source numbers across blocked hubs
        disrupted = set()
        for hub, sources in hubs_to_block:
            disrupted |= sources
        actual_k = min(k, len(shadow_ranked))
        leverage = len(disrupted) / actual_k if actual_k > 0 else 0
        print(f"  {'SIG shadow hubs ' + label:<35} {actual_k:>8} {len(disrupted):>10} {f'{leverage:.1f}x':>10}")

    # ================================================================
    # STEP 6: BRIDGE HUBS - DOES THE BLACKLIST KNOW THEY ARE HUBS?
    # ================================================================
    print(f"\n{'='*60}")
    print("Step 6: BRIDGE HUBS IN THE BLACKLIST")
    print(f"{'='*60}")

    print(f"""
  Bridge hubs ARE in your corpus. The blacklist CAN see them.
  But does it rank them as important?

  A bridge hub might have only 2 complaints against it.
  The blacklist ranks it below thousands of other numbers.
  It does not know the bridge hub is infrastructure that
  connects multiple scam operations.
""")

    rank_map = {n: i + 1 for i, (n, _) in enumerate(ranked_by_complaints)}

    print(f"  Bridge hubs in corpus: {len(bridge_hubs)}")
    print(f"\n  How many bridge hubs appear in the blacklist's top-N?")
    for n in [100, 500, 1000, 5000, 10000]:
        top_n = set(num for num, _ in ranked_by_complaints[:n])
        found = sum(1 for hub in bridge_hubs if hub in top_n)
        print(f"    Top {n:>6,}: {found:>3} / {len(bridge_hubs)} bridge hubs ({found/len(bridge_hubs)*100:.1f}%)")

    # Show top-5 bridge hubs with their blacklist rank
    print(f"\n  Top-5 bridge hubs by fan-in:")
    print(f"  {'Hub':<18} {'Fan-in':>7} {'Complaints':>10} {'Blacklist rank':>15}")
    print(f"  {'-'*55}")
    for hub, sources in sorted(bridge_hubs.items(), key=lambda x: -len(x[1]))[:5]:
        complaints = complaint_counts.get(hub, 0)
        bl_rank = rank_map.get(hub, 'N/A')
        print(f"  {hub:<18} {len(sources):>7} {complaints:>10} {bl_rank:>15,}")

    # ================================================================
    # SAVE OUTPUTS
    # ================================================================

    # Report
    report = []
    report.append("=" * 70)
    report.append("SIG vs BLACKLIST COMPARISON REPORT")
    report.append("=" * 70)
    report.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report.append(f"Input: {len(records):,} records")
    report.append(f"Shadow callback hubs: {len(shadow_hubs)}")
    report.append(f"Bridge callback hubs: {len(bridge_hubs)}")
    report.append("")
    report.append("KEY FINDING:")
    report.append(f"  Blacklist detects: 0 / {len(shadow_hubs)} shadow hubs (0%)")
    report.append(f"  SIG detects:       {len(shadow_hubs)} / {len(shadow_hubs)} shadow hubs (100%)")
    report.append("")
    report.append("DISRUPTION LEVERAGE:")
    top10_disrupted = set()
    for hub, sources in shadow_ranked[:10]:
        top10_disrupted |= sources
    report.append(f"  Blacklist top-10: disrupts 10 frontlines (1.0x)")
    report.append(f"  SIG top-10 hubs:  disrupts {len(top10_disrupted)} frontlines ({len(top10_disrupted)/10:.1f}x)")
    report.append("")
    report.append("BRIDGE HUBS:")
    top1000 = set(n for n, _ in ranked_by_complaints[:1000])
    bridge_in_1000 = sum(1 for h in bridge_hubs if h in top1000)
    report.append(f"  In blacklist top-1000: {bridge_in_1000} / {len(bridge_hubs)} ({bridge_in_1000/len(bridge_hubs)*100:.1f}%)")

    with open('sig_vs_blacklist_report.txt', 'w') as f:
        f.write('\n'.join(report))
    print(f"\nSaved: sig_vs_blacklist_report.txt")

    # CSV with all shadow hubs
    with open('sig_vs_blacklist.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['hub', 'fan_in', 'complaints', 'in_corpus', 'blacklist_finds'])
        for hub, sources in shadow_ranked:
            writer.writerow([hub, len(sources), 0, 'No', 'No'])
        for hub, sources in sorted(bridge_hubs.items(), key=lambda x: -len(x[1])):
            complaints = complaint_counts.get(hub, 0)
            writer.writerow([hub, len(sources), complaints, 'Yes', 'Maybe'])
    print(f"Saved: sig_vs_blacklist.csv")


if __name__ == '__main__':
    main()
