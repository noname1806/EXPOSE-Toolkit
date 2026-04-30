"""
Inter-annotator agreement + pipeline validation.
- abdur   = annoter_validation/labeled_annotation_sheet.csv annotator1_label
- roohana = roohanacsv.csv annotator2_label
- pipeline (sample_for_annotation) = abdur.csv predicted_label
"""
import csv
from collections import Counter, defaultdict

SHORT2FULL = {
    'callback':    'callback number',
    'spoofed_cid': 'spoofed caller id',
    'mention':     'mentioned in comment',
    'sms':         'text/sms number',
    'provided':    'number they provided',
}
LABELS = ['callback number', 'spoofed caller id', 'text/sms number',
          'number they provided', 'mentioned in comment']

def load(fn, col):
    with open(fn, encoding='utf-8', newline='') as f:
        rows = list(csv.reader(f))
    hdr = rows[0]
    ci = hdr.index(col)
    si = hdr.index('sample_id')
    out = {}
    for r in rows[1:]:
        v = r[ci].strip()
        out[int(r[si])] = SHORT2FULL.get(v, v)
    return out

abdur    = load('annoter_validation/labeled_annotation_sheet.csv', 'annotator1_label')
roohana  = load('roohanacsv.csv', 'annotator2_label')
pipeline = load('abdur.csv', 'predicted_label')

ids = sorted(set(abdur) & set(roohana) & set(pipeline))
A = [abdur[i] for i in ids]
R = [roohana[i] for i in ids]
P = [pipeline[i] for i in ids]
N = len(ids)

# ---------- Cohen's kappa ----------
def cohen_kappa(y1, y2, labels):
    n = len(y1)
    cats = labels
    conf = {a: Counter() for a in cats}
    for x, y in zip(y1, y2):
        conf[x][y] += 1
    po = sum(conf[c][c] for c in cats) / n
    m1 = Counter(y1); m2 = Counter(y2)
    pe = sum((m1[c]/n) * (m2[c]/n) for c in cats)
    return po, pe, (po - pe) / (1 - pe) if pe < 1 else 1.0

po, pe, kappa = cohen_kappa(A, R, LABELS)
agree = sum(1 for a, r in zip(A, R) if a == r)
print("="*70)
print("COHEN'S KAPPA — abdur vs roohana")
print("="*70)
print(f"N = {N}")
print(f"raw agreement : {agree}/{N} = {po:.4f}")
print(f"expected agr. : {pe:.4f}")
print(f"Cohen's kappa : {kappa:.4f}")

# ---------- Confusion matrix: abdur vs roohana ----------
def confusion(y_true, y_pred, labels):
    M = {t: Counter() for t in labels}
    for t, p in zip(y_true, y_pred):
        M[t][p] += 1
    return M

def print_matrix(M, labels, row_name, col_name):
    col_w = max(len(c) for c in labels)
    cw = max(col_w, 7)
    row_w = max(len(r) for r in labels) + 2
    print(f"\n{'':{row_w}}  " + "  ".join(c[:cw].rjust(cw) for c in labels) + "  | total")
    for r in labels:
        total = sum(M[r].values())
        cells = "  ".join(str(M[r][c]).rjust(cw) for c in labels)
        print(f"{r:{row_w}}  {cells}  | {total}")
    col_totals = [sum(M[r][c] for r in labels) for c in labels]
    print(f"{'total':{row_w}}  " + "  ".join(str(t).rjust(cw) for t in col_totals))

print("\n" + "="*70)
print("CONFUSION MATRIX — abdur (rows) vs roohana (cols)")
print("="*70)
M_ar = confusion(A, R, LABELS)
print_matrix(M_ar, LABELS, 'abdur', 'roohana')

# ---------- Consensus and pipeline precision/recall ----------
consensus_ids = [i for i, a, r in zip(ids, A, R) if a == r]
cons_true = [abdur[i] for i in consensus_ids]
cons_pred = [pipeline[i] for i in consensus_ids]
print("\n" + "="*70)
print("CONSENSUS (where abdur == roohana)")
print("="*70)
print(f"consensus N = {len(consensus_ids)} / {N}")
print("consensus label distribution:", dict(Counter(cons_true)))

# Per-label P / R / F1 for the pipeline against consensus
def prf(y_true, y_pred, labels):
    per = {}
    for c in labels:
        tp = sum(1 for t, p in zip(y_true, y_pred) if t == c and p == c)
        fp = sum(1 for t, p in zip(y_true, y_pred) if t != c and p == c)
        fn = sum(1 for t, p in zip(y_true, y_pred) if t == c and p != c)
        prec = tp / (tp + fp) if tp + fp else 0.0
        rec  = tp / (tp + fn) if tp + fn else 0.0
        f1   = 2*prec*rec / (prec+rec) if prec+rec else 0.0
        per[c] = (tp, fp, fn, prec, rec, f1)
    return per

print("\n" + "="*70)
print("PIPELINE precision / recall / F1 vs CONSENSUS")
print("="*70)
per = prf(cons_true, cons_pred, LABELS)
print(f"{'label':25}  {'TP':>4}  {'FP':>4}  {'FN':>4}  {'prec':>6}  {'rec':>6}  {'F1':>6}")
for c in LABELS:
    tp, fp, fn, p, r, f = per[c]
    print(f"{c:25}  {tp:4d}  {fp:4d}  {fn:4d}  {p:6.3f}  {r:6.3f}  {f:6.3f}")

# Accuracy overall
acc = sum(1 for t, p in zip(cons_true, cons_pred) if t == p) / len(consensus_ids)
macro_f1 = sum(per[c][5] for c in LABELS) / len(LABELS)
print(f"\nOverall accuracy (pipeline vs consensus): {acc:.4f}")
print(f"Macro-F1 (pipeline vs consensus)        : {macro_f1:.4f}")

# ---------- Confusion matrix: pipeline vs consensus ----------
print("\n" + "="*70)
print("CONFUSION MATRIX — consensus (rows) vs pipeline prediction (cols)")
print("="*70)
M_cp = confusion(cons_true, cons_pred, LABELS)
print_matrix(M_cp, LABELS, 'consensus', 'pipeline')
