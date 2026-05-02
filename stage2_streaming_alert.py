#!/usr/bin/env python3
"""
EXPOSE - Stage 2 (cont.): Streaming Alert for Lurking Callback Hubs (Paper §4.4)
=================================================================================

Replays callback observations chronologically to test whether shadow
callback hubs can be flagged as they emerge, without access to future
data.  The detector maintains, for every shadow callback target tau,
a running count

        d(tau) = | { rho : (rho, tau, callback, t) in Omega up to time t } |

and fires an alert the first time d(tau) crosses the threshold during
the evaluation window.  Following the paper, history runs through
2007-2023 and the evaluation window is 2024-01-01 onward.

Inputs
------
    --input PATH         JSONL corpus (default: results.jsonl)
    --output DIR         output directory (default: ./output)

Outputs (under DIR)
-------------------
    streaming_alert_eval.csv     Flagged hubs at d(tau) >= 3, with
                                 per-hub source list + sample text,
                                 ready for analyst review.
    streaming_alert_report.txt   Latency distribution at thresholds
                                 d(tau) in {2, 3, 5}.
"""
import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


SPLIT_DATE = datetime(2024, 1, 1)
THRESHOLDS = [2, 3, 5]
TF_NPAS = {'800', '888', '877', '866', '855', '844', '833'}


def load_records(path):
    records = []
    with open(path, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def extract_dated_callback_to_shadow(records, R):
    """
    Yield (date, source, target) for callback observations whose target
    is shadow (not in R) and whose date parses cleanly.
    """
    out = []
    for r in records:
        source = r['e164']
        for c in r.get('comments', []):
            d = c.get('date', '')
            if len(d) != 10 or d[4] != '-':
                continue
            try:
                dt = datetime.strptime(d, '%Y-%m-%d')
            except ValueError:
                continue
            for mn in c.get('mentioned_numbers', []):
                tgt = mn.get('e164', '')
                if not tgt or tgt == source:
                    continue
                if mn.get('context') != 'callback number':
                    continue
                if tgt in R:
                    continue
                out.append({'date': dt, 'source': source, 'target': tgt})
    out.sort(key=lambda x: x['date'])
    return out


def simulate(edges, split_date, thresholds):
    hubs = {}
    train = test = 0
    for e in edges:
        tau = e['target']
        rho = e['source']
        dt = e['date']
        if tau not in hubs:
            hubs[tau] = {
                'first_seen':       dt,
                'sources':          set(),
                'threshold_dates':  {},
            }
        if dt < split_date:
            train += 1
        else:
            test += 1
        old = len(hubs[tau]['sources'])
        hubs[tau]['sources'].add(rho)
        new = len(hubs[tau]['sources'])
        if dt >= split_date:
            for k in thresholds:
                if old < k <= new and k not in hubs[tau]['threshold_dates']:
                    hubs[tau]['threshold_dates'][k] = dt
    return hubs, train, test


def evaluate(hubs, lookup, thresholds):
    res = {}
    for k in thresholds:
        flagged = []
        for tau, d in hubs.items():
            if k not in d['threshold_dates']:
                continue
            crossed = d['threshold_dates'][k]
            first = d['first_seen']
            lat = (crossed - first).days
            t_dist = Counter()
            for s in d['sources']:
                if s in lookup:
                    t_dist[lookup[s]['dominant_call_type']] += 1
            if t_dist:
                top_t, top_c = t_dist.most_common(1)[0]
                cons = top_c / sum(t_dist.values())
            else:
                top_t, cons = 'Unknown', 0
            flagged.append({
                'hub':              tau,
                'fan_in':           len(d['sources']),
                'first_seen':       first,
                'crossed_date':     crossed,
                'latency_days':     lat,
                'top_source_type':  top_t,
                'type_consistency': round(cons, 2),
                'source_count':     len(d['sources']),
                'sources':          sorted(d['sources']),
            })
        flagged.sort(key=lambda x: -x['fan_in'])
        if flagged:
            lats = sorted(f['latency_days'] for f in flagged)
            res[k] = {
                'hubs_flagged':       len(flagged),
                'latency_min':        lats[0],
                'latency_median':     lats[len(lats) // 2],
                'latency_max':        lats[-1],
                'latency_mean':       round(sum(lats) / len(lats), 1),
                'same_day':           sum(1 for l in lats if l == 0),
                'within_7_days':      sum(1 for l in lats if l <= 7),
                'type_consistent':    sum(1 for f in flagged
                                          if f['type_consistency'] >= 0.5),
                'flagged_hubs':       flagged,
            }
        else:
            res[k] = {'hubs_flagged': 0, 'flagged_hubs': []}
    return res


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

    print('Loading...')
    records = load_records(in_path)
    R = {r['e164'] for r in records}
    lookup = {r['e164']: r for r in records}
    print(f'  Records: {len(records):,}')

    print('\nExtracting dated callback observations to shadow targets...')
    edges = extract_dated_callback_to_shadow(records, R)
    print(f'  Edges: {len(edges):,}')
    if not edges:
        print('  No callback edges found; aborting.')
        return
    print(f'  Range: {edges[0]["date"].strftime("%Y-%m-%d")} -> '
          f'{edges[-1]["date"].strftime("%Y-%m-%d")}')

    print(f'\nSimulating streaming detector  (split = {SPLIT_DATE.strftime("%Y-%m-%d")})')
    hubs, train, test = simulate(edges, SPLIT_DATE, THRESHOLDS)
    print(f'  History edges    : {train:,}')
    print(f'  Evaluation edges : {test:,}')
    print(f'  Distinct shadow hubs seen : {len(hubs):,}')

    print('\nEvaluating thresholds...')
    res = evaluate(hubs, lookup, THRESHOLDS)

    # ------------------------------------------------------------------
    # Persist
    # ------------------------------------------------------------------
    p_eval = out_dir / 'streaming_alert_eval.csv'
    p_rep  = out_dir / 'streaming_alert_report.txt'

    flagged = res[3].get('flagged_hubs', [])
    with open(p_eval, 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow([
            'hub_number', 'fan_in', 'area_code', 'toll_free',
            'first_seen', 'threshold_crossed', 'detection_latency_days',
            'source_count', 'top_source_type', 'type_consistency',
            'source_phones',
            'sample_text_1', 'sample_text_2', 'sample_text_3',
            'is_real_callback_hub',
        ])
        for h in flagged:
            tau = h['hub']
            ac = tau[2:5] if tau.startswith('+1') and len(tau) >= 5 else '?'
            samples = []
            for s in h['sources'][:3]:
                if s in lookup:
                    for c in lookup[s].get('comments', []):
                        t = c.get('text', '')
                        if t:
                            samples.append(f'[{s}]: {t[:200]}')
                            break
            w.writerow([
                tau, h['fan_in'], ac, 'Yes' if ac in TF_NPAS else 'No',
                h['first_seen'].strftime('%Y-%m-%d'),
                h['crossed_date'].strftime('%Y-%m-%d'),
                h['latency_days'], h['source_count'], h['top_source_type'],
                h['type_consistency'], ';'.join(h['sources']),
                samples[0] if len(samples) > 0 else '',
                samples[1] if len(samples) > 1 else '',
                samples[2] if len(samples) > 2 else '',
                '',
            ])
    print(f'\nSaved: {p_eval}  ({len(flagged)} hubs at d(tau) >= 3)')

    lines = []
    lines.append('=' * 70)
    lines.append('EXPOSE - Streaming Alert Report  (paper §4.4)')
    lines.append('=' * 70)
    lines.append(f'Date  : {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    lines.append(f'Input : {len(records):,} records')
    lines.append(f'Callback observations to shadow targets : {len(edges):,}')
    lines.append(f'Split date : {SPLIT_DATE.strftime("%Y-%m-%d")}')
    lines.append(f'History edges    : {train:,}')
    lines.append(f'Evaluation edges : {test:,}')
    lines.append('')
    for k in THRESHOLDS:
        r = res[k]
        lines.append(f'THRESHOLD  d(tau) >= {k}')
        lines.append('-' * 70)
        lines.append(f'  Hubs flagged in evaluation window : {r["hubs_flagged"]}')
        if r['hubs_flagged']:
            lines.append(f'  Detection latency (days)')
            lines.append(f'    min    = {r["latency_min"]}')
            lines.append(f'    median = {r["latency_median"]}')
            lines.append(f'    max    = {r["latency_max"]}')
            lines.append(f'    mean   = {r["latency_mean"]}')
            lines.append(f'  Same-day detection : '
                         f'{r["same_day"]}/{r["hubs_flagged"]}')
            lines.append(f'  Within 7 days      : '
                         f'{r["within_7_days"]}/{r["hubs_flagged"]}')
            lines.append(f'  Source-type consistent (>= 0.5) : '
                         f'{r["type_consistent"]}/{r["hubs_flagged"]}')
            lines.append('')
            for h in r['flagged_hubs']:
                lines.append(
                    f'    {h["hub"]:<18} fan_in={h["fan_in"]:<3} '
                    f'latency={h["latency_days"]:>4}d  '
                    f'type={h["top_source_type"]:<14} '
                    f'cons={h["type_consistency"]}'
                )
        lines.append('')
    lines.append('=' * 70)

    with open(p_rep, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f'Saved: {p_rep}')
    print()
    print('\n'.join(lines))


if __name__ == '__main__':
    main()
