# Badges claimed

This artifact is submitted for ACM CCS 2026 Artifact Evaluation.
We claim four badges.

## Artifacts Available

- Released under a permanent, DOI-minted archive with a pinned commit.
- Code: Apache-2.0. Data: CC-BY-4.0.
- See §Availability in `README.md` for the archive URL and DOI.

## Artifacts Functional

A reviewer can reproduce every table and figure with one command:

```bash
docker build -t sig-toolkit .
docker run --rm -v "$PWD/output:/artifact/output" sig-toolkit
```

- Full run on the packaged 800notes corpus: ~8 minutes wall time.
- No GPU. No network access required at run time.
- All five pipeline stages succeed end-to-end on a fresh container.
- Outputs are deterministic for a fixed Python + library pinning
  (see `requirements.txt`).

## Artifacts Reusable

The pipeline is written around a minimal input contract
(`docs/data_schema.md`):

- reported identifier (E.164)
- per-complaint free-text narratives
- per-complaint timestamps (YYYY-MM-DD)

Any telephony complaint corpus exposing these three fields can be
swapped in with a single command:

```bash
python run_pipeline.py --input your_corpus.jsonl --output ./out
```

`docs/reuse.md` documents the 20-line CSV→JSONL adapter and expected
behaviour on sparse corpora.

Reusability evidence:

- No corpus-specific constants hard-coded in the orchestrator.
- Each stage accepts positional input paths.
- Non-portable assumptions (NANP, English regexes, US toll-free NPAs)
  are explicitly listed in `docs/reuse.md` §6.

## Results Reproduced

The packaged corpus reproduces these paper claims:

| Paper claim                             | Where reproduced              |
| --------------------------------------- | ----------------------------- |
| F1 = 0.9894 (extraction)                | `sig_nlp_report.txt`          |
| 9,756 observations, 8,542 unique pairs  | `sig_construction_report.txt` |
| 5,673 shadow, 943 bridge, 92,065 front  | `sig_construction_report.txt` |
| H_T = 0.787 vs. 0.651 random            | `sig_baseline_comparison.txt` |
| 541/541 shadow hubs vs. 0 for blacklist | `sig_vs_blacklist_report.txt` |
| 6.2× leverage                           | `sig_vs_blacklist_report.txt` |
| 15 flagged hubs at fan-in ≥ 3           | `streaming_hub_report.txt`    |
| 203 campaigns, 608 phones               | `campaign_linking_report.txt` |
| 12 operations, 101 phones               | `operations_summary.csv`      |

Claims requiring external services are reproduced conditionally:

- §5 carrier enrichment (Tables 4, 5) requires Twilio credentials.
  We ship a cached lookup file and re-run on `--twilio <file>`.
- §6.8 FTC DNC cross-match requires ~4 GB of FTC bulk data. A
  `fetch_ftc_dnc.sh` script is provided for reviewers who wish to
  reproduce it; the result is not required for Tables 1–9.
