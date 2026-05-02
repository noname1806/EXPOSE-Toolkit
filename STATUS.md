# Badges claimed and supporting evidence

This artifact is submitted for ACM CCS 2026 Artifact Evaluation in
support of the paper

> Call Me Maybe? Exposing Patterns of Shadow Scam Ecosystems via
> Open-Source Victim Complaints

We claim **all four** AE badges. The evidence for each is below.

---

## Artifacts Available

- The artifact is released under a permanent, DOI-minted archive
  pinned to a tagged Git commit; see `README.md` §Availability.
- License: Apache-2.0 for code, CC-BY-4.0 for the released corpus.
- The packaged 800notes corpus, the cached Twilio Lookup v2 results,
  and every script needed to reproduce the paper are included in this
  bundle and in the released archive.

## Artifacts Functional

A reviewer can reproduce the local stages with one command:

```bash
docker build -t expose-toolkit .
docker run --rm -v "$PWD/output:/artifact/output" expose-toolkit
```

- Wall time on the packaged corpus: **~8 minutes** on a 2023 commodity
  laptop.
- No GPU; no network access required at run time for the local stages.
- All seven pipeline stages succeed end-to-end on a fresh container.
- Output filenames are deterministic for a fixed Python and library
  pinning (`requirements.txt`).

§5 carrier enrichment runs against the cached Twilio Lookup v2 file
shipped in `data/carrier_lookup_cached.jsonl`. Reviewers with a
Twilio account can refresh it (see `README.md` §"Optional external
services").

§6.8 FTC cross-validation requires the FTC Do-Not-Call bulk files
(~4 GB merged). They are not redistributed; pass `--download-ftc` to
fetch them directly from <https://www.ftc.gov/policy-notices/open-government/data-sets/do-not-call-data>.

## Artifacts Reusable

The pipeline is written around a minimal input contract documented in
`docs/data_schema.md`:

- a reported identifier in E.164 form,
- per-complaint free-text narratives,
- per-complaint timestamps (YYYY-MM-DD).

Any telephony complaint corpus exposing these three fields can be
swapped in:

```bash
python run_pipeline.py --input your_corpus.jsonl --output ./out
```

Evidence of reusability:

- No corpus-specific constants are hard-coded in the orchestrator.
- Every stage script accepts `--input` and `--output`.
- The non-portable assumptions (NANP regexes, U.S. toll-free area
  codes, English-language context cues) are listed explicitly in
  `docs/reuse.md` §6.
- `docs/reuse.md` ships a 20-line CSV-to-JSONL adapter for the most
  common input shape.

## Results Reproduced

The packaged corpus reproduces these paper claims:

| Paper claim                                              | Where reproduced                             |
| -------------------------------------------------------- | -------------------------------------------- |
| Extraction precision/recall vs. platform parser          | `output/stage1_report.txt`                   |
| 9,756 observations, 8,542 unique pairs                   | `output/stage2_report.txt`                   |
| frontline 92,065, bridge 943, shadow 5,673               | `output/stage2_report.txt`                   |
| Component homogeneity vs. random and area-code baselines | `output/stage2_baseline_comparison.txt`      |
| 15 hubs flagged at d(tau) >= 3, latency distribution     | `output/streaming_alert_report.txt`          |
| 0 / 541 lurking targets recovered by blacklist           | `output/blacklist_baseline_report.txt`       |
| Toll-free 54.5%, nonFixedVoIP 22.4% (Finding 1)          | `output/stage3_carrier_report.txt`           |
| No major consumer carrier in top-10 (Finding 2)          | `output/stage3_carrier_report.txt`           |
| 85.3% null on shadow toll-free (Finding 3)               | `output/stage3_carrier_report.txt`           |
| 203 campaigns, 608 phones                                | `output/stage4_report.txt`                   |
| 10 primary / 12 augmented ecosystems                     | `output/stage4_report.txt`, `ecosystems_*.csv` |
| 65.1% of lurking targets FTC-invisible                   | `output/ftc_cross_check_report.txt`          |

The headline F1 = 0.9894 in the paper abstract is computed against
two independent human annotators by `extraction_validation/compute_iaa.py`.
The platform-parser comparison numbers in `stage1_report.txt` are a
separate sanity check that produces the disagreement set fed into the
manual review.
