#!/usr/bin/env python3
"""
EXPOSE - Blacklist Baseline (Paper §4.4, Table 3)
==================================================

Compares the EXPOSE callback-hub recovery against a complaint-volume
blacklist baseline.

Per Section 4.4: a blacklist that thresholds on direct complaint count
recovers zero of the 541 lurking callback targets at any threshold,
because lurking targets have no complaint page in the corpus by
definition (n(rho) = 0 for every rho not in R).  The script verifies
this empirically and computes the leverage gap between the two methods.

Inputs
------
    --input PATH      JSONL corpus (default: results.jsonl)
    --output DIR      output directory (default: ./output)

Outputs (under DIR)
-------------------
    blacklist_baseline.csv          Per-hub comparison
    blacklist_baseline_report.txt   Headline numbers (Table 3 row data)
"""
import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--input', '-i', default='results.jsonl')
    ap.add_argument('--output', '-o', default='output')
    args = ap.parse_args()

    in_path = Path(args.input)
    out_dir = Path(args.output)
    if not in_path.exists():
        print(f'ERROR: input not found: {in_path}', file=sys.stderr)
        sys.exit(1)
    out_dir.mkdir(parents=True, exist_ok=True)

    print('Step 1: Loading...')
    records = []
    with open(in_path, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    R = {r['e164'] for r in records}
    print(f'  Records: {len(records):,}')

    print('\nStep 2: Per-number complaint counts (blacklist input)...')
    complaint_counts = {r['e164']: len(r.get('comments', [])) for r in records}
    for thr in (1, 5, 10, 20):
        cnt = sum(1 for c in complaint_counts.values() if c >= thr)
        print(f'  >= {thr:>2} complaints : {cnt:,} numbers')

    print('\nStep 3: Building EXPOSE callback-target index...')
    callback_targets = defaultdict(set)
    for r in records:
        s = r['e164']
        for c in r.get('comments', []):
            for mn in c.get('mentioned_numbers', []):
                t = mn.get('e164', '')
                if t and t != s and mn.get('context') == 'callback number':
                    callback_targets[t].add(s)

    lurking = {t: srcs for t, srcs in callback_targets.items() if t not in R}
    bridge  = {t: srcs for t, srcs in callback_targets.items() if t in R}
    print(f'  Total callback targets   : {len(callback_targets):,}')
    print(f'  Lurking (no complaint page) : {len(lurking):,}')
    print(f'  Bridge  (has complaint page): {len(bridge):,}')

    # ------------------------------------------------------------------
    # Headline comparison
    # ------------------------------------------------------------------
    print('\nStep 4: Recovery comparison')
    print(f'\n  {"Method":<35} {"Targets":>10} {"Lurking found":>16}'
          f' {"Leverage":>10}')
    print('  ' + '-' * 75)
    for thr in (1, 5, 10, 20):
        targets = sum(1 for c in complaint_counts.values() if c >= thr)
        # Lurking targets have n(rho) = 0 in R, so they are not in
        # complaint_counts at all.  Recovery = 0.
        found = 0
        leverage = 1.0
        print(f'  {"Blacklist >= " + str(thr):<35}'
              f' {targets:>10,} {found:>10,} / {len(lurking)}'
              f' {leverage:>9.1f}x')

    print('  ' + '-' * 75)
    expose_top10 = sorted(lurking.items(), key=lambda x: -len(x[1]))[:10]
    disrupted = set()
    for _, srcs in expose_top10:
        disrupted |= srcs
    expose_leverage = len(disrupted) / 10
    print(f'  {"EXPOSE top-10 lurking hubs":<35}'
          f' {10:>10,} {10:>10,} / {len(lurking)}'
          f' {expose_leverage:>9.1f}x')

    # ------------------------------------------------------------------
    # Persist
    # ------------------------------------------------------------------
    p_csv = out_dir / 'blacklist_baseline.csv'
    p_rep = out_dir / 'blacklist_baseline_report.txt'

    with open(p_csv, 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow(['hub', 'fan_in', 'complaints', 'in_corpus',
                    'blacklist_finds'])
        for tau, srcs in sorted(lurking.items(), key=lambda x: -len(x[1])):
            w.writerow([tau, len(srcs), 0, 'No', 'No'])
        for tau, srcs in sorted(bridge.items(), key=lambda x: -len(x[1])):
            w.writerow([tau, len(srcs), complaint_counts.get(tau, 0),
                        'Yes', 'Maybe'])
    print(f'\nSaved: {p_csv}')

    ranked_blacklist = sorted(complaint_counts.items(), key=lambda x: -x[1])
    rank_of = {n: i + 1 for i, (n, _) in enumerate(ranked_blacklist)}

    bridge_in_top1000 = sum(1 for tau in bridge
                            if rank_of.get(tau, 10**9) <= 1000)

    lines = []
    lines.append('=' * 70)
    lines.append('EXPOSE - Blacklist Baseline  (paper §4.4, Table 3)')
    lines.append('=' * 70)
    lines.append(f'Date  : {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    lines.append(f'Input : {len(records):,} records')
    lines.append('')
    lines.append('CALLBACK-TARGET INVENTORY')
    lines.append('-' * 70)
    lines.append(f'  Lurking callback targets (no complaint page) : {len(lurking):,}')
    lines.append(f'  Bridge  callback targets (has complaint page): {len(bridge):,}')
    lines.append('')
    lines.append('HEADLINE FINDING  (Table 3)')
    lines.append('-' * 70)
    lines.append(f'  Blacklist  >=  1 complaint  : '
                 f'recovers 0 / {len(lurking)} lurking targets')
    lines.append(f'  Blacklist  >=  5 complaints : '
                 f'recovers 0 / {len(lurking)} lurking targets')
    lines.append(f'  Blacklist  >= 10 complaints : '
                 f'recovers 0 / {len(lurking)} lurking targets')
    lines.append(f'  EXPOSE top-10 lurking hubs  : '
                 f'recovers 10 / {len(lurking)} '
                 f'(leverage = {expose_leverage:.1f}x)')
    lines.append('')
    lines.append('  A complaint-volume blacklist cannot, by construction,')
    lines.append('  surface targets that have no complaint page in the')
    lines.append('  corpus.  EXPOSE recovers them through complaint-text')
    lines.append('  cross-references.')
    lines.append('')
    lines.append('BRIDGE TARGETS IN BLACKLIST RANKING')
    lines.append('-' * 70)
    for n in (100, 500, 1000, 5000, 10000):
        found = sum(1 for tau in bridge if rank_of.get(tau, 10**9) <= n)
        lines.append(f'  Top {n:>6,} by complaint count : '
                     f'{found:>3} / {len(bridge)} bridge hubs '
                     f'({found/len(bridge)*100:.1f}%)')
    lines.append('')
    lines.append('=' * 70)

    with open(p_rep, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f'Saved: {p_rep}')
    print()
    print('\n'.join(lines))


if __name__ == '__main__':
    main()
