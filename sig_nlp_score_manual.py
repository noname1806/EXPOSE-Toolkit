#!/usr/bin/env python3
"""
Score Manual Labels from Disagreement Review
=============================================
Run this AFTER you have filled in the 'manual_label' column
in sig_nlp_disagreements.csv.

Input:  sig_nlp_disagreements.csv (with manual_label column filled)
Output: sig_nlp_manual_scores.txt (final metrics for the paper)

Usage:
    python3 sig_nlp_score_manual.py
"""

import csv
import json
from collections import Counter


def main():
    # Load manually labeled disagreements
    rows = []
    with open('labeled_sig_nlp_disagreements.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            if r.get('manual_label', '').strip():
                rows.append(r)

    if not rows:
        print("ERROR: No manual labels found in sig_nlp_disagreements.csv")
        print("Open the CSV, fill in the 'manual_label' column, then re-run.")
        return

    print(f"Loaded {len(rows)} manually labeled disagreements")
    print("=" * 60)

    # For each disagreement, who was right: our pipeline or the platform?
    our_correct = 0
    platform_correct = 0
    neither = 0

    for r in rows:
        manual = r['manual_label'].strip().lower()
        ours = r['our_context'].strip().lower()
        platform = r['platform_context'].strip().lower()

        if manual == ours:
            our_correct += 1
        elif manual == platform:
            platform_correct += 1
        else:
            neither += 1

    total = len(rows)
    print(f"\nWho was correct on the disagreements?")
    print(f"  Our pipeline:    {our_correct}/{total} ({our_correct/total*100:.1f}%)")
    print(f"  Platform parser: {platform_correct}/{total} ({platform_correct/total*100:.1f}%)")
    print(f"  Neither:         {neither}/{total} ({neither/total*100:.1f}%)")

    # Break down by disagreement type
    print(f"\nBy disagreement type:")
    types = Counter((r['platform_context'], r['our_context']) for r in rows)
    for (plat, ours), cnt in types.most_common():
        subset = [r for r in rows if r['platform_context'] == plat and r['our_context'] == ours]
        our_wins = sum(1 for r in subset if r['manual_label'].strip().lower() == ours)
        plat_wins = sum(1 for r in subset if r['manual_label'].strip().lower() == plat)
        print(f"  {plat} -> {ours}: {cnt} cases (ours correct: {our_wins}, platform correct: {plat_wins})")

    # The key question for the paper: how many "mentioned" numbers
    # are actually callbacks that the platform missed?
    cb_disagree = [r for r in rows
                   if r['our_context'] == 'callback'
                   and r['platform_context'] == 'mentioned']
    if cb_disagree:
        actually_cb = sum(1 for r in cb_disagree
                         if r['manual_label'].strip().lower() == 'callback')
        print(f"\n  KEY FINDING: callback vs mentioned disagreements")
        print(f"    Total: {len(cb_disagree)}")
        print(f"    Actually callbacks (our pipeline was right): {actually_cb} ({actually_cb/len(cb_disagree)*100:.1f}%)")
        print(f"    Actually mentions (platform was right): {len(cb_disagree)-actually_cb} ({(len(cb_disagree)-actually_cb)/len(cb_disagree)*100:.1f}%)")

        if actually_cb > 0:
            # Load the original evaluation to recompute adjusted metrics
            try:
                with open('sig_nlp_evaluation.json', 'r') as f:
                    orig = json.load(f)

                orig_cb = orig['context_classification']['per_context']['callback']
                # Our original callback FPs included these disagreements.
                # If X of them are actually callbacks, our true FP count decreases.
                adjusted_tp = orig_cb['tp'] + actually_cb
                adjusted_fp = orig_cb['fp'] - actually_cb
                adjusted_fn = orig_cb['fn']
                adj_p = adjusted_tp / (adjusted_tp + adjusted_fp) if (adjusted_tp + adjusted_fp) > 0 else 0
                adj_r = adjusted_tp / (adjusted_tp + adjusted_fn) if (adjusted_tp + adjusted_fn) > 0 else 0
                adj_f1 = 2 * adj_p * adj_r / (adj_p + adj_r) if (adj_p + adj_r) > 0 else 0

                print(f"\n  ADJUSTED CALLBACK METRICS (after manual correction):")
                print(f"    Original:  P={orig_cb['precision']:.3f}  R={orig_cb['recall']:.3f}  F1={orig_cb['f1']:.3f}")
                print(f"    Adjusted:  P={adj_p:.3f}  R={adj_r:.3f}  F1={adj_f1:.3f}")
                print(f"    (Extrapolated from {len(cb_disagree)} labeled / {orig_cb['fp']} total FPs)")
            except FileNotFoundError:
                print("  (sig_nlp_evaluation.json not found, skipping adjusted metrics)")

    # Save scores
    with open('sig_nlp_manual_scores.txt', 'w') as f:
        f.write(f"Manual labeling results ({len(rows)} disagreements)\n")
        f.write(f"Our pipeline correct: {our_correct}/{total} ({our_correct/total*100:.1f}%)\n")
        f.write(f"Platform correct: {platform_correct}/{total} ({platform_correct/total*100:.1f}%)\n")

    print(f"\nSaved: sig_nlp_manual_scores.txt")


if __name__ == '__main__':
    main()
