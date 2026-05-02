#!/usr/bin/env python3
"""
EXPOSE - Stage 1: Extract and Label (Paper §3.1)
================================================

Parser-independent extraction of phone-number cross-references from
raw complaint text.  For every comment attached to a reported phone
number, the stage:

    1. scans the comment text for phone-number strings,
    2. normalizes each match to E.164,
    3. drops self-references,
    4. labels every retained mention with one of
           callback / spoofed / sms / mention
       by inspecting a 120-character window around the match.

The labels reproduce the four context categories used in Section 3.1
of the paper.  A fifth "provided" category is carried forward only
when it appears as a label on the platform-supplied `mentioned_numbers`
ground truth (it is not produced by the regex classifier itself).

The stage compares its output to the platform-parser ground truth
shipped with the corpus and writes a disagreement file for human
adjudication.  The headline F1 = 0.9894 reported in the paper comes
from compute_iaa.py against two independent human annotators; this
stage produces the platform-comparison numbers and the disagreement
sample that fed the manual review.

Inputs
------
    --input PATH           JSONL corpus (default: results.jsonl)
    --output DIR           output directory (default: ./output)

Outputs (all under DIR)
-----------------------
    stage1_evaluation.json     Per-context precision / recall / F1
    stage1_extractions.jsonl   Every extraction (one per mention)
    stage1_disagreements.csv   Disagreements vs. platform parser,
                               for manual adjudication
    stage1_report.txt          Human-readable summary
"""
import argparse
import csv
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


# ----------------------------------------------------------------------
# Phone-number extraction (NANP)
# ----------------------------------------------------------------------

PHONE_REGEX = re.compile(
    r'(?:'
    r'(?:\+?1[-.\s]?)?'
    r'(?:\(?\d{3}\)?[-.\s]?)'
    r'\d{3}[-.\s]?\d{4}'
    r')'
)


def normalize_to_e164(raw_match):
    """Return E.164 (+1XXXXXXXXXX) for a NANP-shaped digit string, or None."""
    digits = re.sub(r'[^\d]', '', raw_match)
    if len(digits) == 10:
        return '+1' + digits
    if len(digits) == 11 and digits[0] == '1':
        return '+' + digits
    return None


def extract_phone_numbers(text):
    """Return [(e164, start, end, raw)] for all valid NANP matches in text."""
    out = []
    for m in PHONE_REGEX.finditer(text):
        raw = m.group()
        e164 = normalize_to_e164(raw)
        if e164 is None:
            continue
        # Reject NPA starting with 0 or 1 (invalid under NANP).
        if e164[2] in ('0', '1'):
            continue
        out.append((e164, m.start(), m.end(), raw))
    return out


# ----------------------------------------------------------------------
# Context classification (Table 6 of the paper, condensed)
# ----------------------------------------------------------------------

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
    re.IGNORECASE,
)

SPOOFED_PATTERNS = re.compile(
    r'spoof|fake\s+(?:number|caller|id|#)'
    r'|pretend|imitat|impersonat'
    r'|(?:caller\s+id|display|show)\s+(?:said|showed|displayed|read|was)\s+'
    r'|(?:came\s+up|showed\s+up)\s+(?:as|on)'
    r'|disguised\s+as'
    r'|not\s+(?:the\s+)?(?:real|actual)\s+(?:number|#)',
    re.IGNORECASE,
)

SMS_PATTERNS = re.compile(
    r'(?:text|sms)\s+(?:message|msg|from|saying|me)'
    r'|(?:got|received|sent|sends?|sending)\s+(?:a\s+)?(?:text|sms)'
    r'|(?:link|url|click|tap)\s+(?:in|from)\s+(?:the\s+)?(?:text|sms|message)'
    r'|(?:text(?:ed)?|sms(?:ed)?)\s+(?:me|us|you|this)',
    re.IGNORECASE,
)

WINDOW_SIZE = 120


def classify_context(text, match_start, match_end):
    """Return one of: 'callback', 'spoofed', 'sms', 'mention'."""
    ws = max(0, match_start - WINDOW_SIZE)
    we = min(len(text), match_end + WINDOW_SIZE)
    window = text[ws:we]
    if CALLBACK_PATTERNS.search(window):
        return 'callback'
    if SPOOFED_PATTERNS.search(window):
        return 'spoofed'
    if SMS_PATTERNS.search(window):
        return 'sms'
    return 'mention'


def process_comment(source_e164, comment_text):
    """Run extract+classify on a single comment; drop self-references."""
    out = []
    for e164, s, e, raw in extract_phone_numbers(comment_text):
        if e164 == source_e164:
            continue
        ctx = classify_context(comment_text, s, e)
        ws = max(0, s - WINDOW_SIZE)
        we = min(len(comment_text), e + WINDOW_SIZE)
        out.append({
            'target_e164': e164,
            'context': ctx,
            'raw_match': raw,
            'window': comment_text[ws:we],
            'char_start': s,
            'char_end': e,
        })
    return out


# ----------------------------------------------------------------------
# Platform-parser ground truth: map their labels to ours
# ----------------------------------------------------------------------

PLATFORM_CONTEXT_MAP = {
    'callback number':           'callback',
    'spoofed caller id':         'spoofed',
    'mentioned in text message': 'sms',
    'text/sms number':           'sms',
    'number they provided':      'provided',
    'mentioned in comment':      'mention',
}


def evaluate(our_results, platform_results):
    our_pairs, our_by_pair = set(), {}
    for r in our_results:
        pair = (r['source_e164'], r['target_e164'])
        our_pairs.add(pair)
        our_by_pair[pair] = r['context']

    platform_pairs, platform_by_pair = set(), {}
    for r in platform_results:
        pair = (r['source_e164'], r['target_e164'])
        platform_pairs.add(pair)
        platform_by_pair[pair] = PLATFORM_CONTEXT_MAP.get(
            r['platform_context'], 'mention'
        )

    tp = len(our_pairs & platform_pairs)
    fp = len(our_pairs - platform_pairs)
    fn = len(platform_pairs - our_pairs)
    precision = tp / (tp + fp) if (tp + fp) else 0
    recall = tp / (tp + fn) if (tp + fn) else 0
    f1 = (2 * precision * recall / (precision + recall)
          if (precision + recall) else 0)

    overlap = our_pairs & platform_pairs
    correct = 0
    confusion = Counter()
    for pair in overlap:
        ours, plat = our_by_pair[pair], platform_by_pair[pair]
        confusion[(plat, ours)] += 1
        if ours == plat:
            correct += 1
    context_accuracy = correct / len(overlap) if overlap else 0

    per_context = {}
    for ctx in ['callback', 'spoofed', 'sms', 'mention']:
        plat_set = {p for p in overlap if platform_by_pair[p] == ctx}
        our_set = {p for p in overlap if our_by_pair[p] == ctx}
        ctp = len(plat_set & our_set)
        cfp = len(our_set - plat_set)
        cfn = len(plat_set - our_set)
        cp = ctp / (ctp + cfp) if (ctp + cfp) else 0
        cr = ctp / (ctp + cfn) if (ctp + cfn) else 0
        cf1 = 2 * cp * cr / (cp + cr) if (cp + cr) else 0
        per_context[ctx] = {
            'precision': round(cp, 4),
            'recall': round(cr, 4),
            'f1': round(cf1, 4),
            'tp': ctp, 'fp': cfp, 'fn': cfn,
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
            'confusion_matrix': {
                f'{p}->{o}': c for (p, o), c in confusion.most_common()
            },
        },
    }


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--input', '-i', default='results.jsonl',
                    help='Input JSONL corpus (default: results.jsonl)')
    ap.add_argument('--output', '-o', default='output',
                    help='Output directory (default: ./output)')
    args = ap.parse_args()

    in_path = Path(args.input)
    out_dir = Path(args.output)
    if not in_path.exists():
        print(f'ERROR: input not found: {in_path}', file=sys.stderr)
        sys.exit(1)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f'Loading {in_path}...')
    records = []
    with open(in_path, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    print(f'Loaded {len(records):,} records')

    corpus = {r['e164'] for r in records}
    print(f'Corpus size: {len(corpus):,} unique numbers')

    print('\nRunning extractor on every comment...')
    our_results, platform_results, all_extractions = [], [], []

    for idx, r in enumerate(records):
        source = r['e164']
        for c in r.get('comments', []):
            text = c.get('text', '')
            date = c.get('date', '')
            comment_idx = c.get('index', 0)

            for ex in process_comment(source, text):
                our_results.append({
                    'source_e164': source,
                    'target_e164': ex['target_e164'],
                    'context':     ex['context'],
                })
                all_extractions.append({
                    'source_e164':   source,
                    'target_e164':   ex['target_e164'],
                    'our_context':   ex['context'],
                    'raw_match':     ex['raw_match'],
                    'window':        ex['window'][:200],
                    'date':          date,
                    'comment_index': comment_idx,
                })

            for mn in c.get('mentioned_numbers', []):
                tgt = mn.get('e164', '')
                if tgt and tgt != source:
                    platform_results.append({
                        'source_e164':       source,
                        'target_e164':       tgt,
                        'platform_context':  mn.get('context', ''),
                    })

        if (idx + 1) % 20000 == 0:
            print(f'  ...{idx + 1:,} / {len(records):,} records')

    print(f'  Extractor:        {len(our_results):,} mentions')
    print(f'  Platform parser:  {len(platform_results):,} mentions')

    print('\nEvaluating against platform parser...')
    metrics = evaluate(our_results, platform_results)

    # ------------------------------------------------------------------
    # Disagreement table for manual review
    # ------------------------------------------------------------------
    our_pair_ctx = {(r['source_e164'], r['target_e164']): r['context']
                    for r in our_results}
    plat_pair_ctx = {
        (r['source_e164'], r['target_e164']):
            PLATFORM_CONTEXT_MAP.get(r['platform_context'], 'mention')
        for r in platform_results
    }
    extraction_lookup = {(ex['source_e164'], ex['target_e164']): ex
                         for ex in all_extractions}

    disagreements = []
    for pair in set(our_pair_ctx) & set(plat_pair_ctx):
        if our_pair_ctx[pair] != plat_pair_ctx[pair]:
            ex = extraction_lookup.get(pair, {})
            disagreements.append({
                'source_e164':       pair[0],
                'target_e164':       pair[1],
                'our_context':       our_pair_ctx[pair],
                'platform_context':  plat_pair_ctx[pair],
                'window':            ex.get('window', ''),
                'date':              ex.get('date', ''),
                'manual_label':      '',
            })
    disagreements.sort(key=lambda x: (
        0 if x['our_context'] == 'callback'
             and x['platform_context'] == 'mention' else 1,
        x['source_e164'],
    ))

    # ------------------------------------------------------------------
    # Persist
    # ------------------------------------------------------------------
    p_eval  = out_dir / 'stage1_evaluation.json'
    p_jsonl = out_dir / 'stage1_extractions.jsonl'
    p_dis   = out_dir / 'stage1_disagreements.csv'
    p_rep   = out_dir / 'stage1_report.txt'

    with open(p_eval, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, indent=2)
    print(f'Saved: {p_eval}')

    with open(p_jsonl, 'w', encoding='utf-8') as f:
        for ex in all_extractions:
            f.write(json.dumps(ex) + '\n')
    print(f'Saved: {p_jsonl}  ({len(all_extractions):,} extractions)')

    with open(p_dis, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'source_e164', 'target_e164', 'our_context',
            'platform_context', 'window', 'date', 'manual_label',
        ])
        writer.writeheader()
        writer.writerows(disagreements)
    print(f'Saved: {p_dis}  ({len(disagreements):,} rows for human adjudication)')

    # Human-readable report
    e  = metrics['extraction']
    cc = metrics['context_classification']
    lines = []
    lines.append('=' * 70)
    lines.append('EXPOSE - Stage 1: Extract and Label  (paper §3.1)')
    lines.append('=' * 70)
    lines.append(f'Date  : {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    lines.append(f'Input : {len(records):,} records ({len(corpus):,} unique numbers)')
    lines.append('')
    lines.append('PHONE-NUMBER EXTRACTION (vs. platform parser)')
    lines.append('-' * 70)
    lines.append(f'  Platform pairs    : {e["platform_pairs"]:,}')
    lines.append(f'  Extractor pairs   : {e["our_pairs"]:,}')
    lines.append(f'  True positives    : {e["true_positives"]:,}')
    lines.append(f'  False positives   : {e["false_positives"]:,}')
    lines.append(f'  False negatives   : {e["false_negatives"]:,}')
    lines.append(f'  Precision         : {e["precision"]:.4f}')
    lines.append(f'  Recall            : {e["recall"]:.4f}')
    lines.append(f'  F1                : {e["f1"]:.4f}')
    lines.append('')
    lines.append('CONTEXT CLASSIFICATION (overlap with platform)')
    lines.append('-' * 70)
    lines.append(f'  Evaluated on      : {cc["overlap_pairs"]:,} pairs')
    lines.append(f'  Correct           : {cc["correct"]:,}')
    lines.append(f'  Accuracy          : {cc["accuracy"]:.4f}')
    lines.append('')
    lines.append(f'  {"Label":10s} {"Prec":>8s} {"Rec":>8s} {"F1":>8s}'
                 f' {"TP":>6s} {"FP":>6s} {"FN":>6s}')
    lines.append(f'  {"-"*60}')
    for ctx in ['callback', 'spoofed', 'sms', 'mention']:
        m = cc['per_context'][ctx]
        lines.append(f'  {ctx:10s} {m["precision"]:>8.4f} {m["recall"]:>8.4f}'
                     f' {m["f1"]:>8.4f} {m["tp"]:>6} {m["fp"]:>6} {m["fn"]:>6}')
    lines.append('')
    lines.append('CONFUSION MATRIX  (platform_label -> our_label)')
    lines.append('-' * 70)
    for k, v in sorted(cc['confusion_matrix'].items(), key=lambda x: -x[1]):
        lines.append(f'  {k:35s}: {v:,}')
    lines.append('')
    lines.append('NOTE: the headline F1 = 0.9894 reported in the paper is the')
    lines.append('manual-annotation F1 produced by extraction_validation/compute_iaa.py.')
    lines.append('The numbers above are the platform-parser comparison; they bound')
    lines.append('the disagreement set used as input to the human review.')
    lines.append('=' * 70)

    text = '\n'.join(lines)
    with open(p_rep, 'w', encoding='utf-8') as f:
        f.write(text)
    print(f'Saved: {p_rep}')
    print()
    print(text)


if __name__ == '__main__':
    main()
