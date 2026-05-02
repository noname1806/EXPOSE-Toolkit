#!/usr/bin/env python3
"""
EXPOSE - Stage 4 (cont.): FTC Do-Not-Call Cross-Validation (Paper §6.8)
========================================================================

Cross-references the 666 EXPOSE callback targets and the 541 lurking
callback targets against public Federal Trade Commission Do-Not-Call
complaint data (FY 2021-2025).  Reproduces the "65.1% of shadow callback
targets are invisible to the federal complaint system as well" claim.

The FTC bulk files are large (~4 GB merged) and not redistributed with
this artifact.  Two reviewer paths are supported:

(1) Use a pre-fetched merged CSV (default: ftc_dnc_complaints.csv).
(2) Run with --download to fetch the daily FTC CSVs and merge them.
    This calls https://www.ftc.gov/sites/default/files/DNC_Complaint_Numbers_*.csv
    and is rate-limited; expect 10-20 minutes for two years of data.

Inputs
------
    --input PATH         JSONL corpus (default: results.jsonl)
    --output DIR         output directory (default: ./output)
    --ftc PATH           merged FTC CSV (default: ftc_dnc_complaints.csv)
    --download           fetch and merge daily files first
    --days N             when downloading, days of history (default: 1200)

Outputs (under DIR)
-------------------
    ftc_cross_check.csv          per-target match counts
    ftc_cross_check_report.txt   §6.8 headline numbers
"""
import argparse
import csv
import json
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from concurrent.futures import ThreadPoolExecutor, as_completed


CSV_URL_TEMPLATE = (
    'https://www.ftc.gov/sites/default/files/DNC_Complaint_Numbers_{date}.csv'
)
DEFAULT_DAYS = 1200
DOWNLOAD_DIR = 'ftc_daily_csv'
MAX_WORKERS = 5
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5
TF_NPAS = {'800', '888', '877', '866', '855', '844', '833'}


# ----------------------------------------------------------------------
# Optional: download FTC daily files
# ----------------------------------------------------------------------

def _download_one(date_str, out_dir):
    url = CSV_URL_TEMPLATE.format(date=date_str)
    path = os.path.join(out_dir, f'DNC_{date_str}.csv')
    if os.path.exists(path) and os.path.getsize(path) > 100:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            n = sum(1 for _ in f) - 1
        return date_str, True, max(n, 0)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            req = Request(url, headers={'User-Agent': 'EXPOSE-Toolkit/1.0'})
            with urlopen(req, timeout=30) as resp:
                data = resp.read()
            with open(path, 'wb') as f:
                f.write(data)
            text = data.decode('utf-8', errors='replace')
            return date_str, True, max(text.count('\n') - 1, 0)
        except HTTPError as e:
            if e.code == 404:
                return date_str, False, 0
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY * attempt)
        except (URLError, TimeoutError, OSError):
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY * attempt)
    return date_str, False, 0


def _generate_weekday_dates(days):
    end = datetime.now()
    start = end - timedelta(days=days)
    out = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            out.append(cur.strftime('%Y-%m-%d'))
        cur += timedelta(days=1)
    return out


def download_ftc(days, merged_path, workers=MAX_WORKERS):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    dates = _generate_weekday_dates(days)
    print(f'Downloading {len(dates)} weekday FTC files...')
    ok = fail = 0
    rows_total = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_download_one, d, DOWNLOAD_DIR): d for d in dates}
        for i, fut in enumerate(as_completed(futures), 1):
            d, success, rows = fut.result()
            if success:
                ok += 1
                rows_total += rows
            else:
                fail += 1
            if i % 50 == 0:
                print(f'  {i}/{len(dates)}  ok={ok} fail={fail}')
    print(f'  Done: ok={ok} fail={fail} (total rows ~{rows_total:,})')

    print(f'Merging into {merged_path}...')
    files = sorted(f for f in os.listdir(DOWNLOAD_DIR)
                   if f.startswith('DNC_') and f.endswith('.csv'))
    if not files:
        print('  No daily files found.')
        return 0
    with open(os.path.join(DOWNLOAD_DIR, files[0]),
              'r', encoding='utf-8', errors='replace') as f:
        header = next(csv.reader(f), [])
    written = 0
    with open(merged_path, 'w', encoding='utf-8', newline='') as out_f:
        w = csv.writer(out_f)
        w.writerow(header)
        for fname in files:
            with open(os.path.join(DOWNLOAD_DIR, fname),
                      'r', encoding='utf-8', errors='replace') as in_f:
                rd = csv.reader(in_f)
                next(rd, None)
                for row in rd:
                    if row:
                        w.writerow(row)
                        written += 1
    print(f'  Merged rows: {written:,}')
    return written


# ----------------------------------------------------------------------
# Load callback targets and their roles
# ----------------------------------------------------------------------

def callback_targets_and_roles(records):
    R = {r['e164'] for r in records}
    out = defaultdict(set)
    for r in records:
        s = r['e164']
        for c in r.get('comments', []):
            for mn in c.get('mentioned_numbers', []):
                t = mn.get('e164', '')
                if t and t != s and mn.get('context') == 'callback number':
                    out[t].add(s)
    role = {t: ('bridge' if t in R else 'lurking') for t in out}
    return out, role


def normalize_e164(raw):
    digits = ''.join(c for c in str(raw) if c.isdigit())
    if len(digits) == 10:
        return '+1' + digits
    if len(digits) == 11 and digits[0] == '1':
        return '+' + digits
    return None


# ----------------------------------------------------------------------
# Core comparison
# ----------------------------------------------------------------------

def count_ftc_matches(ftc_path, targets):
    """Return {e164 -> count} for every target appearing in the FTC file."""
    print(f'  Streaming FTC CSV: {ftc_path}')
    counts = Counter()
    total_rows = 0
    matched_rows = 0
    targets_set = set(targets)
    candidate_cols = ('Phone', 'PhoneNumber', 'phone',
                      'Phone Number', 'Number')

    with open(ftc_path, 'r', encoding='utf-8', errors='replace', newline='') as f:
        rd = csv.reader(f)
        header = next(rd, None)
        if header is None:
            return counts, 0, 0
        col_idx = None
        for guess in candidate_cols:
            if guess in header:
                col_idx = header.index(guess)
                break
        if col_idx is None:
            for i, h in enumerate(header):
                if 'phone' in h.lower() or 'number' in h.lower():
                    col_idx = i
                    break
        if col_idx is None:
            print('  WARN: could not find phone column; got header:', header[:8])
            return counts, 0, 0
        for row in rd:
            total_rows += 1
            if col_idx >= len(row):
                continue
            e164 = normalize_e164(row[col_idx])
            if e164 and e164 in targets_set:
                counts[e164] += 1
                matched_rows += 1
            if total_rows % 500000 == 0:
                print(f'    scanned {total_rows:,}  matched {matched_rows:,}')
    return counts, total_rows, matched_rows


def is_toll_free(e164):
    return e164.startswith('+1') and e164[2:5] in TF_NPAS


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--input', '-i', default='results.jsonl')
    ap.add_argument('--output', '-o', default='output')
    ap.add_argument('--ftc', default='ftc_dnc_complaints.csv',
                    help='merged FTC CSV (default: ftc_dnc_complaints.csv)')
    ap.add_argument('--download', action='store_true',
                    help='fetch and merge FTC daily files into --ftc first')
    ap.add_argument('--days', type=int, default=DEFAULT_DAYS,
                    help='days of FTC history to fetch (default: 1200)')
    args = ap.parse_args()

    in_path = Path(args.input)
    out_dir = Path(args.output)
    if not in_path.exists():
        print(f'ERROR: input not found: {in_path}', file=sys.stderr)
        sys.exit(1)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.download:
        download_ftc(args.days, args.ftc)

    if not Path(args.ftc).exists():
        print(f'\nFTC merged CSV not found: {args.ftc}', file=sys.stderr)
        print('  Re-run with --download, or provide --ftc /path/to/merged.csv',
              file=sys.stderr)
        # Still write a stub report so downstream tooling does not break.
        p_rep = out_dir / 'ftc_cross_check_report.txt'
        with open(p_rep, 'w', encoding='utf-8') as f:
            f.write('FTC cross-check skipped: merged CSV not found.\n'
                    'Run stage4_ftc_cross_check.py --download to fetch.\n')
        sys.exit(0)

    print('Loading corpus and re-deriving callback targets...')
    records = []
    with open(in_path, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    targets, role = callback_targets_and_roles(records)
    print(f'  Callback targets : {len(targets)}')
    print(f'  Lurking          : {sum(1 for v in role.values() if v == "lurking")}')
    print(f'  Bridge           : {sum(1 for v in role.values() if v == "bridge")}')

    print('\nMatching against FTC corpus...')
    counts, total_rows, matched_rows = count_ftc_matches(args.ftc, list(targets))
    print(f'  FTC rows scanned : {total_rows:,}')
    print(f'  FTC rows matched : {matched_rows:,}')

    # ------------------------------------------------------------------
    # Per-target table + headline numbers
    # ------------------------------------------------------------------
    p_csv = out_dir / 'ftc_cross_check.csv'
    p_rep = out_dir / 'ftc_cross_check_report.txt'

    by_role_zero = Counter()
    by_role_total = Counter()
    by_role_tf_zero = Counter()
    by_role_tf_total = Counter()

    with open(p_csv, 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow(['e164', 'role', 'toll_free', 'fan_in',
                    'ftc_match_count', 'ftc_first_seen', 'ftc_invisible'])
        for tau in sorted(targets):
            r = role[tau]
            tf = is_toll_free(tau)
            cnt = counts.get(tau, 0)
            invisible = cnt == 0
            by_role_total[r] += 1
            if invisible:
                by_role_zero[r] += 1
            if tf:
                by_role_tf_total[r] += 1
                if invisible:
                    by_role_tf_zero[r] += 1
            w.writerow([tau, r, 'Y' if tf else 'N',
                        len(targets[tau]), cnt, '',
                        'Y' if invisible else 'N'])
    print(f'Saved: {p_csv}')

    def pct(a, b):
        return 0 if not b else 100 * a / b

    lines = []
    lines.append('=' * 70)
    lines.append('EXPOSE - FTC DNC Cross-Validation  (paper §6.8)')
    lines.append('=' * 70)
    lines.append(f'Date  : {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    lines.append(f'FTC corpus rows scanned : {total_rows:,}')
    lines.append('')
    lines.append('FTC INVISIBILITY BY ROLE')
    lines.append('-' * 70)
    for r in ('lurking', 'bridge'):
        z = by_role_zero[r]
        t = by_role_total[r]
        lines.append(f'  {r:<10} : {z}/{t} have zero FTC matches '
                     f'({pct(z, t):.1f}%)')
    lines.append('')
    lines.append('FTC INVISIBILITY AMONG TOLL-FREE TARGETS')
    lines.append('-' * 70)
    for r in ('lurking', 'bridge'):
        z = by_role_tf_zero[r]
        t = by_role_tf_total[r]
        lines.append(f'  {r:<10} : {z}/{t} have zero FTC matches '
                     f'({pct(z, t):.1f}%)')
    lines.append('')
    total_zero = sum(by_role_zero.values())
    total_n = sum(by_role_total.values())
    lines.append(f'OVERALL : {total_zero}/{total_n} '
                 f'callback targets are FTC-invisible '
                 f'({pct(total_zero, total_n):.1f}%)')
    lines.append('=' * 70)

    with open(p_rep, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f'Saved: {p_rep}')
    print()
    print('\n'.join(lines))


if __name__ == '__main__':
    main()
