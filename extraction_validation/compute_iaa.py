#!/usr/bin/env python3
"""
EXPOSE - Stage 1 Validation: Inter-Annotator Agreement and Pipeline Score
=========================================================================

Reproduces the manual-validation numbers reported in paper §3.1
("Label validation").  Two annotators independently labelled 200
stratified mentions sampled by sample_for_annotation.py.  This script:

    1. computes Cohen's kappa between the two annotators
       (paper: kappa = 0.63 on the three functional labels
       callback / spoofed / sms),
    2. builds the consensus set (rows where both annotators agree),
    3. scores the EXPOSE Stage 1 classifier against that consensus
       (paper: callback P=0.87 R=0.70; spoofed and sms F1 > 0.95).

Note that the headline extraction F1 = 0.9894 in the paper abstract
is a *different* metric (does the regex extractor find the same
phone-number pairs as the platform parser); that one is produced by
stage1_extract.py and reported in output/stage1_report.txt.

Inputs (under extraction_validation/)
-------------------------------------
    annoter_validation/labeled_annotation_sheet.csv             annotator 1
    annoter_validation/annotation_sheet_annotator2_labeled.csv  annotator 2

Both share the schema produced by sample_for_annotation.py:
    sample_id, page_phone, mentioned_phone, comment_text,
    annotator1_label, annotator2_label

Pipeline predictions are produced inline by running the Stage 1
classifier (stage1_extract.process_comment) on each row's
comment_text -- no separate predictions file is required.

Usage
-----
    cd extraction_validation
    python compute_iaa.py
"""
import csv
import os
import sys
from collections import Counter

# Make the Stage 1 classifier importable regardless of cwd.
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from stage1_extract import process_comment, normalize_to_e164  # noqa: E402


# ----------------------------------------------------------------------
# Label normalization
# ----------------------------------------------------------------------

ALL_LABELS       = ['callback', 'spoofed', 'sms', 'mention', 'provided']
FUNCTIONAL_LABELS = ['callback', 'spoofed', 'sms']
PIPELINE_LABELS   = ['callback', 'spoofed', 'sms', 'mention']

ANNOTATOR_NORMALIZE = {
    'callback':    'callback',
    'spoofed_cid': 'spoofed',
    'spoofed':     'spoofed',
    'sms':         'sms',
    'mention':     'mention',
    'mentioned':   'mention',
    'provided':    'provided',
    '':            '',
}

PIPELINE_NORMALIZE = {
    'callback': 'callback',
    'spoofed':  'spoofed',
    'sms':      'sms',
    'mention':  'mention',
}


# ----------------------------------------------------------------------
# Loaders
# ----------------------------------------------------------------------

def load_labels(path, column):
    """Return {sample_id: normalized_label} for non-empty cells in column."""
    out = {}
    with open(path, encoding='utf-8', newline='') as f:
        for row in csv.DictReader(f):
            v = row.get(column, '').strip()
            if not v:
                continue
            out[int(row['sample_id'])] = ANNOTATOR_NORMALIZE.get(v, v)
    return out


def load_rows(path):
    """Return {sample_id: row_dict} -- needed to regenerate predictions."""
    out = {}
    with open(path, encoding='utf-8', newline='') as f:
        for row in csv.DictReader(f):
            out[int(row['sample_id'])] = row
    return out


def predict(rows):
    """
    Run the Stage 1 classifier on each row's comment_text and return
    {sample_id: pipeline_label}.  When the classifier emits no
    extraction matching mentioned_phone, default to 'mention'.
    """
    out = {}
    for sid, row in rows.items():
        target = row['mentioned_phone'].strip()
        target_e164 = (target if target.startswith('+') else
                       normalize_to_e164(target) or target)
        page = row['page_phone'].strip()
        page_e164 = (page if page.startswith('+') else
                     normalize_to_e164(page) or page)

        label = 'mention'
        for ex in process_comment(page_e164, row['comment_text']):
            if ex['target_e164'] == target_e164:
                label = ex['context']
                break
        out[sid] = PIPELINE_NORMALIZE.get(label, label)
    return out


# ----------------------------------------------------------------------
# Metrics
# ----------------------------------------------------------------------

def cohen_kappa(y1, y2, labels):
    n = len(y1)
    if n == 0:
        return 0.0, 0.0, 0.0
    po = sum(1 for a, b in zip(y1, y2) if a == b) / n
    c1 = Counter(y1)
    c2 = Counter(y2)
    pe = sum((c1[c] / n) * (c2[c] / n) for c in labels)
    return po, pe, ((po - pe) / (1 - pe) if pe < 1 else 1.0)


def confusion(y_true, y_pred, labels):
    M = {t: Counter() for t in labels}
    for t, p in zip(y_true, y_pred):
        if t in M:
            M[t][p] += 1
    return M


def print_matrix(M, labels, row_name, col_name):
    cw = max(max(len(c) for c in labels), 7)
    rw = max(max(len(r) for r in labels), len(row_name)) + 2
    head = '  '.join(c.rjust(cw) for c in labels)
    print(f'\n{row_name + " \\ " + col_name:{rw}}  {head}  | total')
    for r in labels:
        total = sum(M[r].values())
        cells = '  '.join(str(M[r][c]).rjust(cw) for c in labels)
        print(f'{r:{rw}}  {cells}  | {total}')
    col_totals = [sum(M[r][c] for r in labels) for c in labels]
    print(f'{"total":{rw}}  ' + '  '.join(str(t).rjust(cw) for t in col_totals))


def per_class_prf(y_true, y_pred, labels):
    """Per-label precision / recall / F1, plus macro-averaged F1."""
    out = {}
    for c in labels:
        tp = sum(1 for t, p in zip(y_true, y_pred) if t == c and p == c)
        fp = sum(1 for t, p in zip(y_true, y_pred) if t != c and p == c)
        fn = sum(1 for t, p in zip(y_true, y_pred) if t == c and p != c)
        prec = tp / (tp + fp) if (tp + fp) else 0.0
        rec = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
        out[c] = {'precision': prec, 'recall': rec, 'f1': f1,
                  'tp': tp, 'fp': fp, 'fn': fn}
    macro_f1 = sum(out[c]['f1'] for c in labels) / len(labels)
    return out, macro_f1


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main():
    a1_path = os.path.join(HERE, 'annoter_validation',
                           'labeled_annotation_sheet.csv')
    a2_path = os.path.join(HERE, 'annoter_validation',
                           'annotation_sheet_annotator2_labeled.csv')

    if not os.path.exists(a1_path) or not os.path.exists(a2_path):
        print('ERROR: annotator-labelled sheets not found under '
              'extraction_validation/annoter_validation/.', file=sys.stderr)
        sys.exit(1)

    a1 = load_labels(a1_path, 'annotator1_label')
    a2 = load_labels(a2_path, 'annotator2_label')
    rows = load_rows(a1_path)
    pipe = predict(rows)

    common = sorted(set(a1) & set(a2) & set(pipe))
    Y1 = [a1[i] for i in common]
    Y2 = [a2[i] for i in common]

    # ------------------------------------------------------------------
    # Cohen's kappa
    # ------------------------------------------------------------------
    print('=' * 70)
    print("COHEN'S KAPPA  (annotator 1 vs. annotator 2)")
    print('=' * 70)

    po, pe, kappa = cohen_kappa(Y1, Y2, ALL_LABELS)
    raw = sum(1 for a, b in zip(Y1, Y2) if a == b)
    print(f'  All labels       : N={len(common)}  raw={raw}/{len(common)}'
          f' = {po:.4f}  kappa={kappa:.4f}')

    # Paper §3.1 restricts to the three functional labels.
    func_idx = [i for i, (a, b) in enumerate(zip(Y1, Y2))
                if a in FUNCTIONAL_LABELS and b in FUNCTIONAL_LABELS]
    Y1_f = [Y1[i] for i in func_idx]
    Y2_f = [Y2[i] for i in func_idx]
    po_f, pe_f, kappa_f = cohen_kappa(Y1_f, Y2_f, FUNCTIONAL_LABELS)
    raw_f = sum(1 for a, b in zip(Y1_f, Y2_f) if a == b)
    print(f'  Functional only  : N={len(func_idx)}  raw={raw_f}/{len(func_idx)}'
          f' = {po_f:.4f}  kappa={kappa_f:.4f}')
    print('  (paper §3.1: 103/137 raw agreement, kappa = 0.63)')

    print_matrix(confusion(Y1, Y2, ALL_LABELS), ALL_LABELS,
                 'annotator1', 'annotator2')

    # ------------------------------------------------------------------
    # Pipeline classifier vs. consensus
    # ------------------------------------------------------------------
    consensus = [i for i in common if a1[i] == a2[i]]
    Y_cons = [a1[i] for i in consensus]
    Y_pipe = [pipe[i] for i in consensus]

    print()
    print('=' * 70)
    print('PIPELINE CLASSIFIER vs. CONSENSUS  (rows where annotators agree)')
    print('=' * 70)
    print(f'  Consensus rows   : {len(consensus)} / {len(common)}')

    # Pipeline emits four labels (no "provided"); restrict to those.
    eval_pairs = [(t, p) for t, p in zip(Y_cons, Y_pipe)
                  if t in PIPELINE_LABELS]
    Y_eval = [t for t, _ in eval_pairs]
    P_eval = [p for _, p in eval_pairs]

    prf, macro_f1 = per_class_prf(Y_eval, P_eval, PIPELINE_LABELS)
    print(f'\n  {"label":10s} {"prec":>8s} {"rec":>8s} {"F1":>8s}'
          f' {"TP":>5s} {"FP":>5s} {"FN":>5s}')
    print('  ' + '-' * 55)
    for c in PIPELINE_LABELS:
        m = prf[c]
        print(f'  {c:10s} {m["precision"]:>8.4f} {m["recall"]:>8.4f}'
              f' {m["f1"]:>8.4f} {m["tp"]:>5} {m["fp"]:>5} {m["fn"]:>5}')

    tp = sum(prf[c]['tp'] for c in PIPELINE_LABELS)
    fp = sum(prf[c]['fp'] for c in PIPELINE_LABELS)
    fn = sum(prf[c]['fn'] for c in PIPELINE_LABELS)
    micro_p = tp / (tp + fp) if (tp + fp) else 0.0
    micro_r = tp / (tp + fn) if (tp + fn) else 0.0
    micro_f1 = (2 * micro_p * micro_r / (micro_p + micro_r)
                if (micro_p + micro_r) else 0.0)
    print('  ' + '-' * 55)
    print(f'  {"macro F1":10s} {macro_f1:>26.4f}')
    print(f'  {"micro F1":10s} {micro_f1:>26.4f}')

    print_matrix(confusion(Y_eval, P_eval, PIPELINE_LABELS),
                 PIPELINE_LABELS, 'consensus', 'pipeline')

    print()
    print('Paper §3.1 reports (against consensus):')
    print('  callback : precision 0.87, recall 0.70')
    print('  spoofed  : F1 > 0.95')
    print('  sms      : F1 > 0.95')


if __name__ == '__main__':
    main()
