#!/usr/bin/env python3
"""
SIG NLP Extraction Pipeline
============================
Independent phone number extraction and context classification
from raw complaint text. Evaluates against platform parser as
ground truth.

Input:
    results.jsonl
    (Place in same directory as this script, or pass paths as arguments)

Output:
    sig_nlp_evaluation.json      - Full evaluation metrics
    sig_nlp_extractions.jsonl    - Every extraction from our pipeline
    sig_nlp_disagreements.csv    - Cases where our pipeline disagrees with platform (for manual labeling)
    sig_nlp_report.txt           - Human-readable evaluation report

Usage:
    python3 sig_nlp_pipeline.py
    python3 sig_nlp_pipeline.py path/to/results_part_1.jsonl path/to/results_part_2.jsonl path/to/results_part_3.jsonl

The pipeline has two stages:
  Stage 1: Phone number extraction (regex-based)
  Stage 2: Context classification (keyword + window analysis)

Context labels:
  callback    - Victim told to call this number back
  spoofed_cid - Number appeared as spoofed caller ID
  sms         - Number appeared in text/SMS context
  mentioned   - General mention (default)
"""

import json
import re
import csv
import sys
import os
from collections import defaultdict, Counter
from datetime import datetime


# ================================================================
# STAGE 1: PHONE NUMBER EXTRACTION
# ================================================================

# Matches US/Canadian phone numbers in common formats:
# (XXX) XXX-XXXX, XXX-XXX-XXXX, XXX.XXX.XXXX, XXXXXXXXXX,
# 1-XXX-XXX-XXXX, +1XXXXXXXXXX, etc.
PHONE_REGEX = re.compile(
    r'(?:'
    r'(?:\+?1[-.\s]?)?'               # optional +1 or 1- prefix
    r'(?:\(?\d{3}\)?[-.\s]?)'         # area code with optional parens
    r'\d{3}[-.\s]?\d{4}'              # subscriber number
    r')'
)


def normalize_to_e164(raw_match):
    """
    Convert a raw phone string to E.164 format (+1XXXXXXXXXX).
    Returns None if the string does not yield a valid 10 or 11 digit number.
    """
    digits = re.sub(r'[^\d]', '', raw_match)

    if len(digits) == 10:
        return '+1' + digits
    elif len(digits) == 11 and digits[0] == '1':
        return '+' + digits
    else:
        return None


def extract_phone_numbers(text):
    """
    Extract all phone number matches from raw text.
    Returns list of (e164, start_pos, end_pos, raw_match).
    """
    results = []
    for match in PHONE_REGEX.finditer(text):
        raw = match.group()
        e164 = normalize_to_e164(raw)
        if e164 is None:
            continue
        # Basic validity: area code cannot start with 0 or 1
        ac = e164[2:5]
        if ac[0] in ('0', '1'):
            continue
        results.append((e164, match.start(), match.end(), raw))
    return results


CALLBACK_PATTERNS = re.compile(
    r'call(?:ed)?\s*(?:them\s+)?back\s*(?:at|to|on)?\s*'
    r'|call\s+(?:this|that|the)\s+(?:number|#)\s*'
    r'|(?:call|contact|reach|dial)\s+(?:us|them|me)\s+(?:at|on)\s+'
    r'|press\s*(?:1|one)\s*(?:to|for)\s+'
    r'|return\s+(?:call|my call)\s*(?:to|at)?\s*'
    r'|(?:their|the|this)\s+(?:real|actual|callback|call[\s-]?back)\s+(?:number|#|phone)'
    r'|(?:gave|left|provided)\s+(?:a\s+)?(?:number|#|phone)\s*(?:to\s+call)?'
    r'|(?:asked|told|want|said)\s+(?:me\s+)?to\s+call\s*(?:back)?'
    r'|(?:if\s+you\s+)?(?:wish|want|like)\s+to\s+(?:call|speak|talk)'
    r'|(?:number|#)\s+(?:to|for)\s+(?:call(?:ing)?|reach(?:ing)?)\s+(?:back|them|us)',
    re.IGNORECASE
)

SPOOFING_PATTERNS = re.compile(
    r'spoof|fake\s+(?:number|caller|id|#)'
    r'|pretend|imitat|impersonat'
    r'|(?:caller\s+id|display|show)\s+(?:said|showed|displayed|read|was)\s+'
    r'|(?:came\s+up|showed\s+up)\s+(?:as|on)'
    r'|disguised\s+as'
    r'|not\s+(?:the\s+)?(?:real|actual)\s+(?:number|#)',
    re.IGNORECASE
)

SMS_PATTERNS = re.compile(
    r'(?:text|sms)\s+(?:message|msg|from|saying|me)'
    r'|(?:got|received|sent|sends?|sending)\s+(?:a\s+)?(?:text|sms)'
    r'|(?:link|url|click|tap)\s+(?:in|from)\s+(?:the\s+)?(?:text|sms|message)'
    r'|(?:text(?:ed)?|sms(?:ed)?)\s+(?:me|us|you|this)',
    re.IGNORECASE
)

# Context window size (characters before and after the phone match)
WINDOW_SIZE = 120


def classify_context(text, match_start, match_end):
    """
    Classify the context of a phone number mention based on
    surrounding text within WINDOW_SIZE characters.

    Returns one of: 'callback', 'spoofed_cid', 'sms', 'mentioned'
    """
    window_start = max(0, match_start - WINDOW_SIZE)
    window_end = min(len(text), match_end + WINDOW_SIZE)
    window = text[window_start:window_end]

    if CALLBACK_PATTERNS.search(window):
        return 'callback'
    elif SPOOFING_PATTERNS.search(window):
        return 'spoofed_cid'
    elif SMS_PATTERNS.search(window):
        return 'sms'
    else:
        return 'mentioned'


# ================================================================
# FULL PIPELINE: Extract + Classify for one comment
# ================================================================

def process_comment(source_e164, comment_text):
    """
    Run the full pipeline on one comment.
    Returns list of extractions, excluding self-mentions.
    """
    phone_matches = extract_phone_numbers(comment_text)
    extractions = []

    for e164, start, end, raw in phone_matches:
        # Skip self-mentions
        if e164 == source_e164:
            continue

        context = classify_context(comment_text, start, end)

        # Capture the window for inspection
        ws = max(0, start - WINDOW_SIZE)
        we = min(len(comment_text), end + WINDOW_SIZE)

        extractions.append({
            'target_e164': e164,
            'context': context,
            'raw_match': raw,
            'window': comment_text[ws:we],
            'char_start': start,
            'char_end': end,
        })

    return extractions


# ================================================================
# EVALUATION: Compare against platform parser
# ================================================================

# Map platform context labels to our labels
PLATFORM_CONTEXT_MAP = {
    'callback number': 'callback',
    'spoofed caller id': 'spoofed_cid',
    'mentioned in text message': 'sms',
    'text/sms number': 'sms',
    'mentioned in comment': 'mentioned',
}


def evaluate(our_results, platform_results):
    """
    Compare our pipeline's extractions against the platform parser.
    Returns a dict of metrics.
    """
    # Build pair sets: (source_e164, target_e164)
    our_pairs = set()
    our_by_pair = {}
    for r in our_results:
        pair = (r['source_e164'], r['target_e164'])
        our_pairs.add(pair)
        our_by_pair[pair] = r['context']

    platform_pairs = set()
    platform_by_pair = {}
    for r in platform_results:
        pair = (r['source_e164'], r['target_e164'])
        platform_pairs.add(pair)
        platform_by_pair[pair] = PLATFORM_CONTEXT_MAP.get(
            r['platform_context'], 'mentioned'
        )

    # Number extraction metrics
    tp = len(our_pairs & platform_pairs)
    fp = len(our_pairs - platform_pairs)
    fn = len(platform_pairs - our_pairs)
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

    # Context classification on overlapping pairs
    overlap = our_pairs & platform_pairs
    correct = 0
    confusion = Counter()
    for pair in overlap:
        ours = our_by_pair[pair]
        plat = platform_by_pair[pair]
        confusion[(plat, ours)] += 1
        if ours == plat:
            correct += 1

    context_accuracy = correct / len(overlap) if overlap else 0

    # Per-context precision/recall
    per_context = {}
    for ctx in ['callback', 'spoofed_cid', 'sms', 'mentioned']:
        plat_set = {p for p in overlap if platform_by_pair[p] == ctx}
        our_set = {p for p in overlap if our_by_pair[p] == ctx}
        ctx_tp = len(plat_set & our_set)
        ctx_fp = len(our_set - plat_set)
        ctx_fn = len(plat_set - our_set)
        ctx_p = ctx_tp / (ctx_tp + ctx_fp) if (ctx_tp + ctx_fp) > 0 else 0
        ctx_r = ctx_tp / (ctx_tp + ctx_fn) if (ctx_tp + ctx_fn) > 0 else 0
        ctx_f1 = 2 * ctx_p * ctx_r / (ctx_p + ctx_r) if (ctx_p + ctx_r) > 0 else 0
        per_context[ctx] = {
            'precision': round(ctx_p, 4),
            'recall': round(ctx_r, 4),
            'f1': round(ctx_f1, 4),
            'tp': ctx_tp, 'fp': ctx_fp, 'fn': ctx_fn,
        }

    return {
        'extraction': {
            'platform_pairs': len(platform_pairs),
            'our_pairs': len(our_pairs),
            'true_positives': tp,
            'false_positives': fp,
            'false_negatives': fn,
            'precision': round(precision, 4),
            'recall': round(recall, 4),
            'f1': round(f1, 4),
        },
        'context_classification': {
            'overlap_pairs': len(overlap),
            'correct': correct,
            'accuracy': round(context_accuracy, 4),
            'per_context': per_context,
            'confusion_matrix': {f'{p}->{o}': c for (p, o), c in confusion.most_common()},
        },
    }


# ================================================================
# MAIN
# ================================================================

def main():
    # Determine input files
    if len(sys.argv) > 1:
        jsonl_files = sys.argv[1:]
    else:
        # Default: look in current directory
        jsonl_files = [
            'results.jsonl'
        ]

    # Check files exist
    for f in jsonl_files:
        if not os.path.exists(f):
            print(f"ERROR: File not found: {f}")
            print(f"Usage: python3 {sys.argv[0]} <jsonl_file1> <jsonl_file2> ...")
            sys.exit(1)

    # Load records
    print("Loading data...")
    records = []
    for f in jsonl_files:
        with open(f, 'r', encoding='utf-8') as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))

    print(f"Loaded {len(records):,} records from {len(jsonl_files)} files")

    corpus = set(r['e164'] for r in records)
    print(f"Corpus size: {len(corpus):,} unique numbers")

    # ── Run our pipeline ──
    print("\nRunning NLP extraction pipeline...")
    our_results = []
    platform_results = []
    all_extractions = []  # for JSONL output

    for idx, r in enumerate(records):
        source = r['e164']

        for c in r.get('comments', []):
            text = c.get('text', '')
            date = c.get('date', '')
            comment_idx = c.get('index', 0)

            # Our pipeline
            extractions = process_comment(source, text)
            for ex in extractions:
                our_results.append({
                    'source_e164': source,
                    'target_e164': ex['target_e164'],
                    'context': ex['context'],
                })
                all_extractions.append({
                    'source_e164': source,
                    'target_e164': ex['target_e164'],
                    'our_context': ex['context'],
                    'raw_match': ex['raw_match'],
                    'window': ex['window'][:200],
                    'date': date,
                    'comment_index': comment_idx,
                })

            # Platform parser (ground truth)
            for mn in c.get('mentioned_numbers', []):
                tgt = mn.get('e164', '')
                if tgt and tgt != source:
                    platform_results.append({
                        'source_e164': source,
                        'target_e164': tgt,
                        'platform_context': mn.get('context', ''),
                    })

        if (idx + 1) % 20000 == 0:
            print(f"  Processed {idx+1:,}/{len(records):,} records...")

    print(f"  Our pipeline: {len(our_results):,} extractions")
    print(f"  Platform parser: {len(platform_results):,} extractions")

    # ── Evaluate ──
    print("\nEvaluating...")
    metrics = evaluate(our_results, platform_results)

    # ── Build disagreement list for manual labeling ──
    print("Building disagreement list...")

    our_pair_ctx = {}
    for r in our_results:
        pair = (r['source_e164'], r['target_e164'])
        our_pair_ctx[pair] = r['context']

    plat_pair_ctx = {}
    for r in platform_results:
        pair = (r['source_e164'], r['target_e164'])
        plat_pair_ctx[pair] = PLATFORM_CONTEXT_MAP.get(
            r['platform_context'], 'mentioned'
        )

    # Find the extraction details for disagreements
    extraction_lookup = {}
    for ex in all_extractions:
        pair = (ex['source_e164'], ex['target_e164'])
        extraction_lookup[pair] = ex

    disagreements = []
    overlap = set(our_pair_ctx.keys()) & set(plat_pair_ctx.keys())
    for pair in overlap:
        if our_pair_ctx[pair] != plat_pair_ctx[pair]:
            ex = extraction_lookup.get(pair, {})
            disagreements.append({
                'source_e164': pair[0],
                'target_e164': pair[1],
                'our_context': our_pair_ctx[pair],
                'platform_context': plat_pair_ctx[pair],
                'window': ex.get('window', ''),
                'date': ex.get('date', ''),
                'manual_label': '',  # FOR YOU TO FILL IN
            })

    # Sort: callback disagreements first (most important)
    disagreements.sort(key=lambda x: (
        0 if x['our_context'] == 'callback' and x['platform_context'] == 'mentioned' else 1,
        x['source_e164'],
    ))

    # ── Save outputs ──
    # 1. Evaluation metrics (JSON)
    with open('sig_nlp_evaluation.json', 'w') as f:
        json.dump(metrics, f, indent=2)
    print(f"Saved: sig_nlp_evaluation.json")

    # 2. All extractions (JSONL)
    with open('sig_nlp_extractions.jsonl', 'w') as f:
        for ex in all_extractions:
            f.write(json.dumps(ex) + '\n')
    print(f"Saved: sig_nlp_extractions.jsonl ({len(all_extractions):,} extractions)")

    # 3. Disagreements for manual labeling (CSV)
    with open('sig_nlp_disagreements.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'source_e164', 'target_e164', 'our_context',
            'platform_context', 'window', 'date', 'manual_label',
        ])
        writer.writeheader()
        writer.writerows(disagreements)
    print(f"Saved: sig_nlp_disagreements.csv ({len(disagreements):,} rows)")

    # 4. Human-readable report
    report = []
    report.append("=" * 70)
    report.append("SIG NLP PIPELINE EVALUATION REPORT")
    report.append("=" * 70)
    report.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report.append(f"Input: {len(records):,} records, {len(corpus):,} unique numbers")
    report.append("")

    report.append("STAGE 1: PHONE NUMBER EXTRACTION")
    report.append("-" * 40)
    e = metrics['extraction']
    report.append(f"  Platform found:   {e['platform_pairs']:,} (source, target) pairs")
    report.append(f"  Our pipeline:     {e['our_pairs']:,} pairs")
    report.append(f"  True positives:   {e['true_positives']:,}")
    report.append(f"  False positives:  {e['false_positives']:,}")
    report.append(f"  False negatives:  {e['false_negatives']:,}")
    report.append(f"  Precision:        {e['precision']:.4f}")
    report.append(f"  Recall:           {e['recall']:.4f}")
    report.append(f"  F1:               {e['f1']:.4f}")
    report.append("")

    report.append("STAGE 2: CONTEXT CLASSIFICATION")
    report.append("-" * 40)
    cc = metrics['context_classification']
    report.append(f"  Evaluated on:     {cc['overlap_pairs']:,} overlapping pairs")
    report.append(f"  Correct:          {cc['correct']:,}")
    report.append(f"  Accuracy:         {cc['accuracy']:.4f}")
    report.append("")
    report.append(f"  {'Context':15s} {'Precision':>10s} {'Recall':>10s} {'F1':>10s} {'TP':>6s} {'FP':>6s} {'FN':>6s}")
    report.append(f"  {'-'*65}")
    for ctx in ['callback', 'spoofed_cid', 'sms', 'mentioned']:
        m = cc['per_context'][ctx]
        report.append(f"  {ctx:15s} {m['precision']:>10.4f} {m['recall']:>10.4f} {m['f1']:>10.4f} {m['tp']:>6} {m['fp']:>6} {m['fn']:>6}")

    report.append("")
    report.append("CONFUSION MATRIX (platform_label -> our_label)")
    report.append("-" * 40)
    for key, count in sorted(cc['confusion_matrix'].items(), key=lambda x: -x[1]):
        report.append(f"  {key:35s}: {count:,}")

    report.append("")
    report.append("DISAGREEMENTS FOR MANUAL LABELING")
    report.append("-" * 40)
    disagree_types = Counter(
        f"{d['platform_context']}->{d['our_context']}" for d in disagreements
    )
    for dt, cnt in disagree_types.most_common():
        report.append(f"  {dt:35s}: {cnt:,}")
    report.append(f"  Total: {len(disagreements):,}")
    report.append(f"  Saved to: sig_nlp_disagreements.csv")
    report.append(f"  >> Open this CSV and fill in the 'manual_label' column <<")

    report.append("")
    report.append("=" * 70)
    report.append("NEXT STEPS")
    report.append("=" * 70)
    report.append("1. Open sig_nlp_disagreements.csv in Excel or Google Sheets")
    report.append("2. For each row, read the 'window' column")
    report.append("3. In the 'manual_label' column, write the correct label:")
    report.append("   callback, spoofed_cid, sms, or mentioned")
    report.append("4. Focus on the first ~100 rows (callback vs mentioned)")
    report.append("5. Save the file and run: python3 sig_nlp_score_manual.py")
    report.append("=" * 70)

    report_text = '\n'.join(report)
    with open('sig_nlp_report.txt', 'w') as f:
        f.write(report_text)
    print(f"Saved: sig_nlp_report.txt")

    # Print report to console
    print("\n" + report_text)


if __name__ == '__main__':
    main()
