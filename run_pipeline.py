#!/usr/bin/env python3
"""
EXPOSE-Toolkit: end-to-end orchestrator.

Drives every stage of the EXPOSE pipeline on any complaint corpus that
satisfies the input contract documented in docs/data_schema.md.  This
is the single entry point referenced from scripts/reproduce_all.{sh,bat}
and from the artifact's Dockerfile.

Stages
------
    1   Extract and Label                       stage1_extract.py
    2a  Cross-Reference Graph                   stage2_xref_graph.py
    2b  Streaming Alert (lurking callback hubs) stage2_streaming_alert.py
    2c  Blacklist Baseline                      stage2_blacklist_baseline.py
    3   Carrier Metadata Enrichment             stage3_carrier.py
    4a  Campaigns and Shadow Scam Ecosystems    stage4_campaigns_ecosystems.py
    4b  FTC DNC Cross-Validation                stage4_ftc_cross_check.py

Usage
-----
    python run_pipeline.py                       # uses ./results.jsonl
    python run_pipeline.py --input my_corpus.jsonl
    python run_pipeline.py --skip 4b             # skip FTC cross-check
    python run_pipeline.py --refresh-carrier     # call Twilio for missing
                                                 # callback targets

All outputs land under --output (default: ./output).  Each stage's
stdout and stderr are tee'd to output/_logs/<stage>.{stdout,stderr}.log.
The final summary table is printed to stdout.
"""
import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


HERE = Path(__file__).resolve().parent


def _stage(label, script, args_factory, produces, skip_id):
    return {
        'id':       skip_id,
        'label':    label,
        'script':   script,
        'args':     args_factory,
        'produces': produces,
    }


STAGES = [
    _stage(
        '1   Extract and Label',
        'stage1_extract.py',
        lambda inp, out, ctx: ['--input', str(inp), '--output', str(out)],
        ['stage1_evaluation.json', 'stage1_extractions.jsonl',
         'stage1_disagreements.csv', 'stage1_report.txt'],
        '1',
    ),
    _stage(
        '2a  Cross-Reference Graph',
        'stage2_xref_graph.py',
        lambda inp, out, ctx: ['--input', str(inp), '--output', str(out)],
        ['xref_edges.csv', 'xref_components.csv', 'xref_component_eval.csv',
         'stage2_baseline_comparison.txt', 'stage2_report.txt'],
        '2a',
    ),
    _stage(
        '2b  Streaming Alert',
        'stage2_streaming_alert.py',
        lambda inp, out, ctx: ['--input', str(inp), '--output', str(out)],
        ['streaming_alert_eval.csv', 'streaming_alert_report.txt'],
        '2b',
    ),
    _stage(
        '2c  Blacklist Baseline',
        'stage2_blacklist_baseline.py',
        lambda inp, out, ctx: ['--input', str(inp), '--output', str(out)],
        ['blacklist_baseline.csv', 'blacklist_baseline_report.txt'],
        '2c',
    ),
    _stage(
        '3   Carrier Metadata',
        'stage3_carrier.py',
        lambda inp, out, ctx: (
            ['--input', str(inp), '--output', str(out),
             '--cache', str(ctx['carrier_cache'])]
            + (['--refresh'] if ctx['refresh_carrier'] else [])
        ),
        ['carrier_lookup.jsonl', 'stage3_carrier_breakdown.csv',
         'stage3_carrier_report.txt'],
        '3',
    ),
    _stage(
        '4a  Campaigns and Ecosystems',
        'stage4_campaigns_ecosystems.py',
        lambda inp, out, ctx: ['--input', str(inp), '--output', str(out),
                               '--carrier', str(out / 'carrier_lookup.jsonl')],
        ['campaigns.csv', 'ecosystem_links.csv',
         'ecosystems_primary.csv', 'ecosystems_augmented.csv',
         'stage4_eval.csv', 'stage4_report.txt'],
        '4a',
    ),
    _stage(
        '4b  FTC DNC Cross-Validation',
        'stage4_ftc_cross_check.py',
        lambda inp, out, ctx: (
            ['--input', str(inp), '--output', str(out),
             '--ftc', str(ctx['ftc_csv'])]
            + (['--download'] if ctx['download_ftc'] else [])
        ),
        ['ftc_cross_check_report.txt'],
        '4b',
    ),
]


def log(msg):
    print(f'[run_pipeline] {msg}', flush=True)


def run_one(stage, inp, out_dir, log_dir, ctx):
    script = HERE / stage['script']
    if not script.exists():
        raise FileNotFoundError(f'Stage script missing: {script}')

    args = stage['args'](inp, out_dir, ctx)
    log(f'-> {stage["label"]}')
    log(f'    cmd: python {stage["script"]} {" ".join(args)}')

    name = stage['script'].replace('.py', '')
    p_out = log_dir / f'{name}.stdout.log'
    p_err = log_dir / f'{name}.stderr.log'

    t0 = time.time()
    with open(p_out, 'w', encoding='utf-8') as fo, \
         open(p_err, 'w', encoding='utf-8') as fe:
        result = subprocess.run(
            [sys.executable, str(script), *args],
            stdout=fo, stderr=fe,
        )
    elapsed = time.time() - t0

    if result.returncode != 0:
        log(f'   [FAIL] after {elapsed:.1f}s (exit={result.returncode})')
        log(f'   tail of {p_err}:')
        try:
            for line in p_err.read_text(encoding='utf-8').splitlines()[-15:]:
                log(f'   | {line}')
        except Exception:
            pass
        return False

    log(f'   [ok] {elapsed:.1f}s')
    missing = [p for p in stage['produces'] if not (out_dir / p).exists()]
    if missing:
        log(f'   ! expected outputs missing: {missing}')
        return False
    return True


def main():
    ap = argparse.ArgumentParser(
        description='Run the full EXPOSE-Toolkit pipeline on any '
                    'complaint corpus.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument('--input', '-i',
                    default=str(HERE / 'results.jsonl'),
                    help='Input JSONL corpus (default: ./results.jsonl). '
                         'See docs/data_schema.md.')
    ap.add_argument('--output', '-o',
                    default=str(HERE / 'output'),
                    help='Output directory (default: ./output).')
    ap.add_argument('--carrier-cache',
                    default=str(HERE / 'data' / 'carrier_lookup_cached.jsonl'),
                    help='Cached Twilio Lookup v2 results.')
    ap.add_argument('--refresh-carrier', action='store_true',
                    help='Call Twilio Lookup for any callback target not in '
                         'the cache.  Requires TWILIO_ACCOUNT_SID and '
                         'TWILIO_AUTH_TOKEN in the environment.')
    ap.add_argument('--ftc',
                    default=str(HERE / 'ftc_dnc_complaints.csv'),
                    help='Merged FTC DNC CSV for §6.8 cross-validation.')
    ap.add_argument('--download-ftc', action='store_true',
                    help='Fetch and merge FTC daily CSV files first.')
    ap.add_argument('--skip',
                    default='',
                    help='Comma-separated stage IDs to skip '
                         '(e.g. "2b,4b").  IDs: 1, 2a, 2b, 2c, 3, 4a, 4b.')
    args = ap.parse_args()

    inp = Path(args.input).resolve()
    out_dir = Path(args.output).resolve()
    log_dir = out_dir / '_logs'

    if not inp.exists():
        log(f'ERROR: input corpus not found: {inp}')
        log(f'  Expected JSONL.  See docs/data_schema.md.')
        sys.exit(1)

    out_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    skip = {s.strip() for s in args.skip.split(',') if s.strip()}

    ctx = {
        'carrier_cache':   Path(args.carrier_cache),
        'refresh_carrier': args.refresh_carrier,
        'ftc_csv':         Path(args.ftc),
        'download_ftc':    args.download_ftc,
    }

    log('=' * 68)
    log(f'Input    : {inp}')
    log(f'Output   : {out_dir}')
    log(f'Logs     : {log_dir}')
    log(f'Carrier  : cache={ctx["carrier_cache"]}'
        f' refresh={ctx["refresh_carrier"]}')
    log(f'FTC      : csv={ctx["ftc_csv"]}'
        f' download={ctx["download_ftc"]}')
    log('=' * 68)

    failures = []
    t_start = time.time()
    for stage in STAGES:
        if stage['id'] in skip:
            log(f'-> {stage["label"]} -- SKIPPED (--skip {stage["id"]})')
            continue
        ok = run_one(stage, inp, out_dir, log_dir, ctx)
        if not ok:
            failures.append(stage['label'])

    elapsed = time.time() - t_start
    log('=' * 68)
    log(f'Total wall time: {elapsed:.1f}s')
    if failures:
        log(f'FAILED stages ({len(failures)}):')
        for f in failures:
            log(f'  - {f}')
        sys.exit(2)

    log('All stages succeeded.  Final artifacts under:')
    log(f'  {out_dir}')
    log('Headline reports for ACM CCS reviewers:')
    log('  stage1_report.txt                -> §3.1 extraction quality')
    log('  stage2_report.txt                -> §4 graph + role partition')
    log('  stage2_baseline_comparison.txt   -> §4 H_T baselines')
    log('  streaming_alert_report.txt       -> §4.4 streaming alert')
    log('  blacklist_baseline_report.txt    -> Table 3 leverage gap')
    log('  stage3_carrier_report.txt        -> §5 Findings 1-3')
    log('  stage4_report.txt                -> §6 campaigns + ecosystems')
    log('  ftc_cross_check_report.txt       -> §6.8 FTC invisibility')


if __name__ == '__main__':
    main()
