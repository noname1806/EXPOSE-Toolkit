#!/usr/bin/env python3
"""
EXPOSE - Stage 1 (helper): Score Manual Adjudications
=====================================================

Run this AFTER reviewers have populated the `manual_label` column in
output/stage1_disagreements.csv (or its renamed copy
labeled_stage1_disagreements.csv).  It computes who was correct on each
disagreement (the EXPOSE extractor or the platform parser) and writes
a short scoring file.

Usage
-----
    python3 stage1_score_manual.py
    python3 stage1_score_manual.py --input labeled_stage1_disagreements.csv
"""
import argparse
import csv
import sys
from collections import Counter
from pathlib import Path


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--input', '-i', default='labeled_stage1_disagreements.csv')
    ap.add_argument('--output', '-o', default='output/stage1_manual_scores.txt')
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        print(f'ERROR: {in_path} not found', file=sys.stderr)
        print(f'Open the disagreement CSV from Stage 1, fill in the '
              f'manual_label column, save as {args.input}, and re-run.',
              file=sys.stderr)
        sys.exit(1)

    rows = []
    with open(in_path, 'r', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            if r.get('manual_label', '').strip():
                rows.append(r)
    if not rows:
        print('No labeled rows found.', file=sys.stderr)
        sys.exit(1)

    print(f'Loaded {len(rows)} labeled disagreements from {in_path}')
    out_dir = Path(args.output).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    extractor_correct = 0
    platform_correct = 0
    neither = 0
    by_pair = Counter()
    for r in rows:
        manual = r['manual_label'].strip().lower()
        ours   = r['our_context'].strip().lower()
        plat   = r['platform_context'].strip().lower()
        by_pair[f'{plat}->{ours}'] += 1
        if manual == ours:
            extractor_correct += 1
        elif manual == plat:
            platform_correct += 1
        else:
            neither += 1

    n = len(rows)
    lines = []
    lines.append('Stage 1 manual adjudication scores')
    lines.append('=' * 50)
    lines.append(f'Total adjudicated rows : {n}')
    lines.append(f'EXPOSE extractor right : '
                 f'{extractor_correct}/{n} ({extractor_correct/n*100:.1f}%)')
    lines.append(f'Platform parser right  : '
                 f'{platform_correct}/{n} ({platform_correct/n*100:.1f}%)')
    lines.append(f'Neither                : {neither}/{n} ({neither/n*100:.1f}%)')
    lines.append('')
    lines.append('Disagreement patterns (platform_label -> our_label) :')
    for k, v in by_pair.most_common():
        lines.append(f'  {k:<35} : {v}')
    text = '\n'.join(lines)
    print('\n' + text)

    with open(args.output, 'w', encoding='utf-8') as f:
        f.write(text)
    print(f'\nSaved: {args.output}')


if __name__ == '__main__':
    main()
