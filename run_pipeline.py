#!/usr/bin/env python3
"""
SIG-Toolkit: end-to-end orchestrator.

Runs every stage of the complaint-to-infrastructure pipeline on any
corpus that satisfies the input contract documented in docs/data_schema.md.

Single-command usage (reviewer path):

    python run_pipeline.py                       # uses ./results.jsonl
    python run_pipeline.py --input my_corpus.jsonl
    python run_pipeline.py --input my_corpus.jsonl --output ./out

All outputs (CSVs, JSONLs, reports) land under the --output directory.
Exit code is non-zero if any stage fails.
"""
import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


HERE = Path(__file__).resolve().parent

STAGES = [
    # (label, script, extra_args_factory, produces)
    (
        "1/5  NLP extraction + evaluation",
        "sig_nlp_pipeline.py",
        lambda inp: [str(inp)],
        [
            "sig_nlp_evaluation.json",
            "sig_nlp_extractions.jsonl",
            "sig_nlp_disagreements.csv",
            "sig_nlp_report.txt",
        ],
    ),
    (
        "2/5  SIG construction + component evaluation",
        "sig_construction.py",
        lambda inp: [str(inp)],
        [
            "sig_edges.csv",
            "sig_components.csv",
            "sig_component_eval.csv",
            "sig_baseline_comparison.txt",
            "sig_construction_report.txt",
        ],
    ),
    (
        "3/5  Streaming hub detection",
        "sig_streaming.py",
        lambda inp: [str(inp)],
        [
            "streaming_hub_eval.csv",
            "streaming_hub_report.txt",
        ],
    ),
    (
        "4/5  SIG vs. complaint-volume blacklist",
        "sig_vs_blacklist.py",
        lambda inp: [str(inp)],
        [
            "sig_vs_blacklist.csv",
            "sig_vs_blacklist_report.txt",
        ],
    ),
    (
        "5/5  Campaign + operation discovery",
        "campaign_linking.py",
        lambda inp: [str(inp)],
        [
            "campaign_clusters.csv",
            "operation_graph.csv",
            "operations_summary.csv",
            "campaign_linking_eval.csv",
            "campaign_linking_report.txt",
        ],
    ),
]


def log(msg: str) -> None:
    print(f"[run_pipeline] {msg}", flush=True)


def run_stage(label, script, args, out_dir, log_dir):
    script_path = HERE / script
    if not script_path.exists():
        raise FileNotFoundError(f"Pipeline script missing: {script_path}")

    log(f"-> {label}")
    log(f"    cmd: python {script} {' '.join(args)}")
    log(f"    cwd: {out_dir}")

    stage_name = script.replace(".py", "")
    stdout_path = log_dir / f"{stage_name}.stdout.log"
    stderr_path = log_dir / f"{stage_name}.stderr.log"

    t0 = time.time()
    with open(stdout_path, "w", encoding="utf-8") as stdout_f, \
         open(stderr_path, "w", encoding="utf-8") as stderr_f:
        result = subprocess.run(
            [sys.executable, str(script_path), *args],
            cwd=str(out_dir),
            stdout=stdout_f,
            stderr=stderr_f,
        )
    elapsed = time.time() - t0

    if result.returncode != 0:
        log(f"  [FAIL] after {elapsed:.1f}s (exit={result.returncode})")
        log(f"    see: {stderr_path}")
        try:
            tail = stderr_path.read_text(encoding="utf-8").splitlines()[-20:]
            for line in tail:
                log(f"    | {line}")
        except Exception:
            pass
        return False

    log(f"  [ok] {elapsed:.1f}s")
    return True


def main():
    ap = argparse.ArgumentParser(
        description="Run the full SIG-Toolkit pipeline on any complaint corpus.",
    )
    ap.add_argument(
        "--input", "-i",
        default=str(HERE / "results.jsonl"),
        help="Input JSONL corpus (default: ./results.jsonl). "
             "See docs/data_schema.md for the required schema.",
    )
    ap.add_argument(
        "--output", "-o",
        default=str(HERE / "output"),
        help="Output directory (default: ./output). Created if missing.",
    )
    ap.add_argument(
        "--twilio",
        default=None,
        help="Optional Twilio Lookup v2 results JSONL. If provided, it is "
             "copied next to the campaign-linking script so Layer C (shared "
             "carrier) activates. Without it, Layer C is silently skipped.",
    )
    ap.add_argument(
        "--skip",
        default="",
        help="Comma-separated stage numbers to skip (e.g. '3,4').",
    )
    args = ap.parse_args()

    input_path = Path(args.input).resolve()
    output_dir = Path(args.output).resolve()
    log_dir = output_dir / "_logs"

    if not input_path.exists():
        log(f"ERROR: input corpus not found: {input_path}")
        log(f"  Expected JSONL format. See docs/data_schema.md.")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    if args.twilio:
        twilio_src = Path(args.twilio).resolve()
        if not twilio_src.exists():
            log(f"ERROR: --twilio file not found: {twilio_src}")
            sys.exit(1)
        twilio_dst = output_dir / "twilio_lookup_results.jsonl"
        shutil.copyfile(twilio_src, twilio_dst)
        log(f"Twilio enrichment: {twilio_src} -> {twilio_dst}")
    else:
        log("Twilio enrichment: not provided (Layer C will be skipped)")

    skip_set = {s.strip() for s in args.skip.split(",") if s.strip()}

    log("=" * 68)
    log(f"Input  : {input_path}")
    log(f"Output : {output_dir}")
    log(f"Logs   : {log_dir}")
    log("=" * 68)

    failures = []
    t_start = time.time()
    for idx, (label, script, arg_fn, produces) in enumerate(STAGES, 1):
        if str(idx) in skip_set:
            log(f"-> {label} -- SKIPPED (per --skip)")
            continue
        ok = run_stage(label, script, arg_fn(input_path), output_dir, log_dir)
        if not ok:
            failures.append(label)
            # Stages are ordered but mostly independent; keep going so the
            # reviewer sees every failure, not just the first.
        else:
            missing = [p for p in produces if not (output_dir / p).exists()]
            if missing:
                log(f"  ! expected output(s) missing: {missing}")
                failures.append(label + " (missing outputs)")

    elapsed = time.time() - t_start
    log("=" * 68)
    log(f"Total wall time: {elapsed:.1f}s")
    if failures:
        log(f"FAILED stages ({len(failures)}):")
        for f in failures:
            log(f"  - {f}")
        sys.exit(2)

    log("All stages succeeded. Final artifacts are in:")
    log(f"  {output_dir}")
    log("Key tables to inspect:")
    log("  sig_nlp_report.txt              -> Paper Table 1, sec.3.1 F1")
    log("  sig_construction_report.txt     -> Paper sec.4.1 graph stats")
    log("  sig_baseline_comparison.txt     -> Paper sec.4.3 homogeneity")
    log("  streaming_hub_report.txt        -> Paper sec.4.3 streaming")
    log("  sig_vs_blacklist_report.txt     -> Paper Table 3, sec.4.3 leverage")
    log("  campaign_linking_report.txt     -> Paper Tables 6-8, sec.6")


if __name__ == "__main__":
    main()
