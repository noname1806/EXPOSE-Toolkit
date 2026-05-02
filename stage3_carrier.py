#!/usr/bin/env python3
"""
EXPOSE - Stage 3: Carrier Metadata Enrichment (Paper §3.3, §5)
===============================================================

For every callback target tau extracted in Stage 2, query the Twilio
Lookup v2 API for line type, carrier name, and burner / virtual flag.
The script then computes the Section 5 statistics: toll-free vs.
non-toll-free share, dominant wholesale providers, and the null-carrier
rate that produces the "carrier-opacity cliff" of Figure 5.

Two reviewer modes
------------------

(a) Cached mode (default).  We ship a frozen lookup file covering all
    666 callback targets as of March 2026 (data/carrier_lookup_cached.jsonl)
    so reviewers can reproduce §5 without a Twilio account.

(b) Refresh mode.  Pass --refresh and supply the two environment
    variables:

        TWILIO_ACCOUNT_SID
        TWILIO_AUTH_TOKEN

    The script then queries the Twilio Lookup v2 endpoint
    /v2/PhoneNumbers/{e164} with fields=line_type_intelligence at
    1 request per second and writes a fresh lookup file.  Already-
    looked-up numbers in the cache are not re-queried unless
    --force-all is passed.

Inputs
------
    --input PATH         JSONL corpus (default: results.jsonl)
    --output DIR         output directory (default: ./output)
    --cache PATH         cached lookup JSONL
                         (default: data/carrier_lookup_cached.jsonl)
    --refresh            attempt Twilio API call for any missing targets
    --force-all          re-query every target (ignored unless --refresh)

Outputs (under DIR)
-------------------
    carrier_lookup.jsonl         merged cache + new results, one row
                                 per callback target
    stage3_carrier_report.txt    §5 statistics (Findings 1-3, Figure 5)
    stage3_carrier_breakdown.csv role x line_type x carrier table
"""
import argparse
import csv
import json
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


TF_NPAS = {'800', '888', '877', '866', '855', '844', '833'}


# ----------------------------------------------------------------------
# Pull callback targets and their role (bridge vs lurking) from corpus
# ----------------------------------------------------------------------

def callback_targets_and_roles(records):
    R = {r['e164'] for r in records}
    target_to_sources = defaultdict(set)
    for r in records:
        s = r['e164']
        for c in r.get('comments', []):
            for mn in c.get('mentioned_numbers', []):
                t = mn.get('e164', '')
                if t and t != s and mn.get('context') == 'callback number':
                    target_to_sources[t].add(s)
    role = {}
    for t in target_to_sources:
        role[t] = 'bridge' if t in R else 'lurking'
    return target_to_sources, role


# ----------------------------------------------------------------------
# Carrier lookup (cached + optional API refresh)
# ----------------------------------------------------------------------

def load_cache(path):
    cache = {}
    if not path or not Path(path).exists():
        return cache
    with open(path, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get('e164'):
                cache[rec['e164']] = rec
    return cache


def classify_line_type(line_type):
    if not line_type:
        return 'UNKNOWN'
    mapping = {
        'tollFree':     'TOLL FREE',
        'nonFixedVoip': 'BURNER/VIRTUAL',
        'mobile':       'REAL MOBILE',
        'fixedVoip':    'FIXED VOIP',
        'landline':     'LANDLINE',
        'voip':         'VOIP',
    }
    return mapping.get(line_type, line_type.upper())


def twilio_query(sid, token, e164):
    """Single Twilio Lookup v2 call.  Returns a result dict."""
    from twilio.rest import Client
    client = Client(sid, token)
    try:
        resp = (client.lookups.v2
                .phone_numbers(e164)
                .fetch(fields='line_type_intelligence'))
        intel = resp.line_type_intelligence or {}
        line_type = intel.get('type', 'unknown')
        return {
            'e164':           e164,
            'phone_number':   e164,
            'checked_at':     datetime.utcnow().isoformat(),
            'carrier_name':   intel.get('carrier_name'),
            'line_type':      line_type,
            'classification': classify_line_type(line_type),
            'is_burner':      line_type == 'nonFixedVoip',
            'mobile_country_code': intel.get('mobile_country_code'),
            'mobile_network_code': intel.get('mobile_network_code'),
            'lookup_success': True,
        }
    except Exception as e:
        return {
            'e164':           e164,
            'phone_number':   e164,
            'checked_at':     datetime.utcnow().isoformat(),
            'carrier_name':   None,
            'line_type':      None,
            'classification': 'ERROR',
            'is_burner':      None,
            'lookup_success': False,
            'error':          str(e),
        }


def refresh(targets, cache, force_all):
    sid = os.environ.get('TWILIO_ACCOUNT_SID')
    tok = os.environ.get('TWILIO_AUTH_TOKEN')
    if not sid or not tok:
        print('  --refresh requested but TWILIO_ACCOUNT_SID / '
              'TWILIO_AUTH_TOKEN not set in environment.',
              file=sys.stderr)
        sys.exit(2)

    todo = [t for t in targets if force_all or t not in cache]
    print(f'  Querying Twilio for {len(todo)} targets'
          f' (rate limit: 1 req/sec)...')
    for i, t in enumerate(todo, 1):
        cache[t] = twilio_query(sid, tok, t)
        if i % 25 == 0:
            print(f'    {i}/{len(todo)}  last={t}')
        time.sleep(1.0)
    return cache


# ----------------------------------------------------------------------
# §5 report
# ----------------------------------------------------------------------

def is_toll_free(e164):
    return e164.startswith('+1') and e164[2:5] in TF_NPAS


def report(results, role, R_size, R_tf_count):
    """Compute headline numbers from §5 and the carrier-opacity cliff."""
    total = len(results)
    by_lt = Counter(r.get('line_type') or 'unknown' for r in results.values())
    by_class = Counter(r.get('classification') or 'UNKNOWN'
                       for r in results.values())
    tf_count = sum(1 for t in results if is_toll_free(t))
    geo_count = total - tf_count

    null_total = sum(1 for r in results.values()
                     if r.get('lookup_success') and not r.get('carrier_name'))

    # role x line-type cross-tab
    cells = defaultdict(lambda: {'count': 0, 'null': 0})
    for tau, r in results.items():
        line_type = r.get('line_type') or 'unknown'
        is_tf = 'toll_free' if is_toll_free(tau) else 'geographic'
        cells[(role.get(tau, 'lurking'), is_tf)]['count'] += 1
        if r.get('lookup_success') and not r.get('carrier_name'):
            cells[(role.get(tau, 'lurking'), is_tf)]['null'] += 1

    # carrier dominance among shadow-geographic targets
    geo_shadow_carriers = Counter()
    for tau, r in results.items():
        if role.get(tau) == 'lurking' and not is_toll_free(tau):
            cn = r.get('carrier_name')
            if cn:
                geo_shadow_carriers[cn] += 1

    return {
        'total':             total,
        'tf_count':          tf_count,
        'tf_share':          tf_count / total if total else 0,
        'geo_count':         geo_count,
        'by_line_type':      dict(by_lt),
        'by_classification': dict(by_class),
        'null_total':        null_total,
        'null_rate':         null_total / total if total else 0,
        'cells':             {f'{k[0]}|{k[1]}': v for k, v in cells.items()},
        'geo_shadow_top10':  geo_shadow_carriers.most_common(10),
        'R_size':            R_size,
        'R_tf_count':        R_tf_count,
        'R_tf_share':        R_tf_count / R_size if R_size else 0,
    }


def write_outputs(results, role, summary, out_dir):
    p_jsonl = out_dir / 'carrier_lookup.jsonl'
    p_csv   = out_dir / 'stage3_carrier_breakdown.csv'
    p_rep   = out_dir / 'stage3_carrier_report.txt'

    with open(p_jsonl, 'w', encoding='utf-8') as f:
        for tau in sorted(results):
            row = dict(results[tau])
            row['role'] = role.get(tau, 'lurking')
            f.write(json.dumps(row) + '\n')
    print(f'Saved: {p_jsonl}  ({len(results)} targets)')

    with open(p_csv, 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow(['role', 'line_kind', 'count', 'null_carrier_count',
                    'null_carrier_rate'])
        for k in ('bridge|toll_free', 'bridge|geographic',
                  'lurking|toll_free', 'lurking|geographic'):
            d = summary['cells'].get(k, {'count': 0, 'null': 0})
            rate = d['null'] / d['count'] if d['count'] else 0
            w.writerow([*k.split('|'), d['count'], d['null'], f'{rate:.4f}'])
    print(f'Saved: {p_csv}')

    s = summary
    geo_shadow_n = s['cells'].get('lurking|geographic', {'count': 0})['count']
    geo_shadow_null = s['cells'].get('lurking|geographic', {'null': 0})['null']
    tf_shadow_n = s['cells'].get('lurking|toll_free', {'count': 0})['count']
    tf_shadow_null = s['cells'].get('lurking|toll_free', {'null': 0})['null']
    bridge_tf_n = s['cells'].get('bridge|toll_free', {'count': 0})['count']
    bridge_tf_null = s['cells'].get('bridge|toll_free', {'null': 0})['null']

    enrichment = (s['tf_share'] / s['R_tf_share']) if s['R_tf_share'] else 0

    lines = []
    lines.append('=' * 70)
    lines.append('EXPOSE - Stage 3: Carrier Metadata Enrichment  (paper §5)')
    lines.append('=' * 70)
    lines.append(f'Date  : {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    lines.append(f'Targets queried : {s["total"]}')
    lines.append('')
    lines.append('FINDING 1: Toll-free and VoIP dominate')
    lines.append('-' * 70)
    lines.append(f'  Toll-free callback targets : '
                 f'{s["tf_count"]}/{s["total"]} '
                 f'({s["tf_share"]*100:.1f}%)')
    lines.append(f'  Toll-free share in R       : '
                 f'{s["R_tf_count"]}/{s["R_size"]} '
                 f'({s["R_tf_share"]*100:.1f}%)')
    lines.append(f'  Toll-free enrichment factor: {enrichment:.1f}x')
    nfv = s['by_line_type'].get('nonFixedVoip', 0)
    lines.append(f'  nonFixedVoIP (burner)      : '
                 f'{nfv}/{s["total"]} '
                 f'({nfv/s["total"]*100:.1f}%)')
    mob = s['by_line_type'].get('mobile', 0)
    ll  = s['by_line_type'].get('landline', 0)
    lines.append(f'  Mobile                     : '
                 f'{mob}/{s["total"]} ({mob/s["total"]*100:.1f}%)')
    lines.append(f'  Landline                   : '
                 f'{ll}/{s["total"]} ({ll/s["total"]*100:.1f}%)')
    lines.append('')
    lines.append('FINDING 2: No major consumer carrier provisions shadow infra')
    lines.append('-' * 70)
    lines.append(f'  Top-10 carriers among {geo_shadow_n} shadow geographic targets:')
    for name, cnt in s['geo_shadow_top10']:
        pct = cnt / geo_shadow_n * 100 if geo_shadow_n else 0
        lines.append(f'    {name[:40]:<40} {cnt:>4} ({pct:.1f}%)')
    lines.append('')
    lines.append('FINDING 3: Carrier-identification opacity for toll-free')
    lines.append('-' * 70)
    if tf_shadow_n:
        lines.append(f'  Shadow toll-free   : null carrier '
                     f'{tf_shadow_null}/{tf_shadow_n} '
                     f'({tf_shadow_null/tf_shadow_n*100:.1f}%)')
    if bridge_tf_n:
        lines.append(f'  Bridge toll-free   : null carrier '
                     f'{bridge_tf_null}/{bridge_tf_n} '
                     f'({bridge_tf_null/bridge_tf_n*100:.1f}%)')
    if geo_shadow_n:
        lines.append(f'  Shadow geographic  : null carrier '
                     f'{geo_shadow_null}/{geo_shadow_n} '
                     f'({geo_shadow_null/geo_shadow_n*100:.1f}%)')
    lines.append('')
    lines.append('  The gradient (toll-free >> geographic) is the carrier-')
    lines.append('  opacity cliff of Figure 5: U.S. toll-free numbers are')
    lines.append('  managed through the Somos RespOrg system, whose identity')
    lines.append('  is not exposed through standard telecom lookup APIs.')
    lines.append('')
    lines.append('LINE-TYPE BREAKDOWN')
    lines.append('-' * 70)
    for lt, cnt in sorted(s['by_line_type'].items(), key=lambda x: -x[1]):
        lines.append(f'  {lt:<20} {cnt:>5}'
                     f' ({cnt/s["total"]*100:.1f}%)')
    lines.append('=' * 70)

    with open(p_rep, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f'Saved: {p_rep}')
    print()
    print('\n'.join(lines))


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--input', '-i', default='results.jsonl')
    ap.add_argument('--output', '-o', default='output')
    ap.add_argument('--cache', default='data/carrier_lookup_cached.jsonl')
    ap.add_argument('--refresh', action='store_true',
                    help='attempt Twilio API call for missing targets')
    ap.add_argument('--force-all', action='store_true',
                    help='re-query every target (ignored unless --refresh)')
    args = ap.parse_args()

    in_path = Path(args.input)
    out_dir = Path(args.output)
    cache_path = Path(args.cache)
    if not in_path.exists():
        print(f'ERROR: input not found: {in_path}', file=sys.stderr)
        sys.exit(1)
    out_dir.mkdir(parents=True, exist_ok=True)

    print('Loading corpus and re-deriving callback targets...')
    records = []
    with open(in_path, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    R = {r['e164'] for r in records}
    R_tf = sum(1 for n in R if is_toll_free(n))
    targets, role = callback_targets_and_roles(records)
    print(f'  Records: {len(records):,}')
    print(f'  Callback targets: {len(targets)}')
    print(f'  Lurking : {sum(1 for v in role.values() if v == "lurking")}')
    print(f'  Bridge  : {sum(1 for v in role.values() if v == "bridge")}')

    print(f'\nCache: {cache_path}')
    cache = load_cache(cache_path)
    print(f'  Cached lookups : {len(cache)}')
    missing = [t for t in targets if t not in cache]
    print(f'  Missing        : {len(missing)}')

    if args.refresh:
        print('\nRefreshing from Twilio Lookup v2...')
        cache = refresh(list(targets), cache, args.force_all)
    elif missing:
        print('  Missing entries will appear with classification=NOT_QUERIED.')
        for t in missing:
            cache[t] = {
                'e164':           t,
                'phone_number':   t,
                'checked_at':     None,
                'carrier_name':   None,
                'line_type':      None,
                'classification': 'NOT_QUERIED',
                'is_burner':      None,
                'lookup_success': False,
            }

    # Filter cache down to current callback targets only
    results = {t: cache[t] for t in targets if t in cache}

    print('\nGenerating §5 report...')
    summary = report(results, role, len(R), R_tf)
    write_outputs(results, role, summary, out_dir)


if __name__ == '__main__':
    main()
