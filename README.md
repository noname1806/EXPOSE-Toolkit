# EXPOSE-Toolkit

Artifact accompanying

> **Call Me Maybe? Exposing Patterns of Shadow Scam Ecosystems via
> Open-Source Victim Complaints** (ACM CCS 2026, under review)

This repository reproduces every quantitative claim in the paper
(Tables 1-9, Figures 1-6, Findings 1-5) from the packaged 800notes
victim-complaint corpus, and is portable to any complaint corpus that
satisfies the input contract in [`docs/data_schema.md`](docs/data_schema.md).

The framework, EXPOSE (*Exposing Patterns of Shadow Scam Ecosystems*),
is described in Sections 3-6 of the paper. The toolkit is the
reference implementation and is released to support open science and
independent verification (see paper §A "Open Policy").

---

## Badges claimed

- [x] **Artifacts Available**   - DOI-minted snapshot (see §Availability).
- [x] **Artifacts Functional**  - one-command reproduction of every local stage.
- [x] **Artifacts Reusable**    - the input contract is minimal and the
                                  pipeline runs on any JSONL complaint
                                  corpus matching `docs/data_schema.md`.
- [x] **Results Reproduced**    - all paper tables regenerate from the
                                  packaged corpus.

See [`STATUS.md`](STATUS.md) for the badge-by-badge evidence.

---

## One-command reproduction

### Option A - Docker (recommended for AE reviewers)

```bash
docker build -t expose-toolkit .
docker run --rm -v "$PWD/output:/artifact/output" expose-toolkit
```

Wall time on a 2023 commodity laptop: **~8 min** for the local stages
(Stage 1 -> 4a). Stage 4b (FTC cross-validation) requires the public
FTC bulk data and runs separately; see below.

### Option B - Local Python

```bash
pip install -r requirements.txt
./scripts/reproduce_all.sh
```

On Windows:

```bat
pip install -r requirements.txt
scripts\reproduce_all.bat
```

### Option C - Any other complaint corpus

```bash
python run_pipeline.py --input /path/to/your_corpus.jsonl --output ./out
```

The corpus must satisfy [`docs/data_schema.md`](docs/data_schema.md).
See [`docs/reuse.md`](docs/reuse.md) for a 20-line CSV-to-JSONL
adapter.

---

## What the pipeline produces

All outputs land under `output/` (or whatever directory `--output`
specifies). Cross-reference each artifact with the paper:

| Output file                             | Paper reference        | Claim it reproduces                                              |
| --------------------------------------- | ---------------------- | ---------------------------------------------------------------- |
| `stage1_report.txt`                     | §3.1, Figure 1 V1      | Extraction precision/recall against the platform parser          |
| `stage1_extractions.jsonl`              | §3.1                   | Every (source, target, context) tuple our extractor produces     |
| `stage1_disagreements.csv`              | §3.1                   | Disagreement set fed into the manual adjudication                |
| `xref_edges.csv`                        | §3.2, §4.1             | The 9,756 complaint-derived observations / 8,542 unique pairs    |
| `xref_components.csv`                   | §4.1                   | Connected components of the cross-reference graph                |
| `stage2_report.txt`                     | §4.1, §4.2             | Frontline / bridge / shadow role partition                        |
| `stage2_baseline_comparison.txt`        | §4.3                   | Component homogeneity vs. random and area-code baselines          |
| `streaming_alert_report.txt`            | §4.4                   | 15 callback hubs flagged at d(tau) >= 3, latency distribution     |
| `streaming_alert_eval.csv`              | §4.4                   | Per-hub data for the 100% precision claim                         |
| `blacklist_baseline_report.txt`         | §4.4, Table 3          | 0 / 541 lurking targets recovered by complaint-volume blacklist   |
| `blacklist_baseline.csv`                | Table 3                | Per-hub blacklist comparison                                      |
| `stage3_carrier_report.txt`             | §5, Figure 4, Figure 5 | Findings 1-3 (toll-free dominance, wholesale carriers, opacity)   |
| `stage3_carrier_breakdown.csv`          | §5                     | Role x line-type cross-tab                                        |
| `carrier_lookup.jsonl`                  | §3.3, §5               | Twilio Lookup v2 results for all 666 callback targets             |
| `campaigns.csv`                         | §6.2                   | 203 non-singleton persona campaigns                               |
| `ecosystem_links.csv`                   | §6.3                   | Every campaign-pair link with its indicator                       |
| `ecosystems_primary.csv`                | §6.3, Table 4          | 10 ecosystems from the primary graph (sigma_hub or sigma_edge)    |
| `ecosystems_augmented.csv`              | §6.3, Table 4          | 12 ecosystems from the augmented graph (+ sigma_carrier)          |
| `stage4_report.txt`                     | §6, Tables 4-5         | Indicator coverage and ecosystem statistics                       |
| `ftc_cross_check_report.txt`            | §6.8                   | 65.1% of lurking callback targets are FTC-invisible               |

---

## Pipeline stages

```
results.jsonl (any corpus matching docs/data_schema.md)
   |
   |-- [1]   stage1_extract.py               extraction + labeling      (§3.1)
   |-- [2a]  stage2_xref_graph.py            cross-reference graph      (§3.2, §4)
   |-- [2b]  stage2_streaming_alert.py       lurking-callback alert     (§4.4)
   |-- [2c]  stage2_blacklist_baseline.py    leverage gap (Table 3)     (§4.4)
   |-- [3]   stage3_carrier.py               carrier metadata + §5      (§3.3, §5)
   |-- [4a]  stage4_campaigns_ecosystems.py  campaigns + ecosystems     (§3.4, §6)
   `-- [4b]  stage4_ftc_cross_check.py       FTC DNC cross-validation   (§6.8)
```

Each stage is independent and idempotent and accepts `--input`
and `--output`. To skip stages from the orchestrator:

```bash
python run_pipeline.py --skip 4b              # skip FTC cross-check
python run_pipeline.py --skip 2b,2c           # skip streaming + blacklist
```

---

## Environment

- Python 3.11
- scikit-learn 1.4.2, numpy 1.26.4, scipy 1.13.0 (pinned)
- twilio 9.x (only needed if you want to refresh the carrier lookup)
- No GPU
- ~16 GB RAM comfortable; 8 GB works for the local stages
- Full local run: ~8 min wall time on the packaged corpus

The Dockerfile pins everything; if you reproduce inside the container
no host configuration matters.

---

## Packaged data

- `results.jsonl` - 800notes corpus, 93,008 reported numbers,
  97,093 comments, February 2007 -- March 2026 (~72 MB).
  All text is publicly posted on 800notes.com. Reporter screen names
  appear only when the poster chose them publicly; no private
  identifiers are included. See paper §A and §B for ethics.

- `data/carrier_lookup_cached.jsonl` - cached Twilio Lookup v2 results
  for all 666 callback targets as of March 2026, so reviewers can
  reproduce §5 without a Twilio account. See paper §A.

---

## Optional external services

### Twilio Lookup v2 (paper §3.3, §5)

The default reviewer path uses the cached lookup file shipped in
`data/carrier_lookup_cached.jsonl`. To re-query Twilio for the current
provisioning state of each callback target:

```bash
export TWILIO_ACCOUNT_SID=ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
export TWILIO_AUTH_TOKEN=...
python run_pipeline.py --refresh-carrier
```

Rate is fixed at 1 request per second to stay within Twilio's
default Lookup throughput. All 666 queries take about 12 minutes.

### FTC DNC cross-match (paper §6.8)

The Federal Trade Commission publishes daily Do-Not-Call complaint
files at <https://www.ftc.gov/policy-notices/open-government/data-sets/do-not-call-data>.
The merged corpus is ~4 GB and is not redistributed with this
artifact.

To fetch and merge the last 1,200 weekdays' files, then run §6.8:

```bash
python run_pipeline.py --download-ftc
```

`stage4_ftc_cross_check.py` can also be invoked directly with a
pre-downloaded merged CSV via `--ftc /path/to/ftc_dnc_complaints.csv`.

---

## Reviewer checklist

1. `docker build -t expose-toolkit .`
2. `docker run --rm -v "$PWD/output:/artifact/output" expose-toolkit`
3. Open `output/stage2_report.txt` -> confirm 9,756 observations,
   |frontline| / |bridge| / |shadow|, and the role-partition counts.
4. Open `output/stage3_carrier_report.txt` -> confirm Findings 1-3
   (toll-free 54.5%, no major consumer carrier, 85.3% null on shadow
   toll-free).
5. Open `output/blacklist_baseline_report.txt` -> confirm Table 3
   (0 / 541 vs. 10 / 541 with leverage > 5).
6. Open `output/stage4_report.txt` -> confirm 203 campaigns and 10
   primary / 12 augmented ecosystems.
7. (Optional) Re-run with your own corpus per `docs/reuse.md`.

If any number disagrees with the paper, file an issue on the
anonymous repository or contact the corresponding author.

---

## Repository layout

```
artifact_evaluation/
|-- README.md                            # this file
|-- STATUS.md                            # badges and evidence
|-- LICENSE                              # Apache-2.0 (code)
|-- LICENSE-DATA                         # CC-BY-4.0 (released corpus)
|-- Dockerfile                           # reproducible container
|-- requirements.txt                     # pinned Python deps
|-- run_pipeline.py                      # ORCHESTRATOR - single entry point
|-- scripts/
|   |-- reproduce_all.sh                 # Linux/macOS shim
|   `-- reproduce_all.bat                # Windows shim
|-- stage1_extract.py                    # §3.1
|-- stage1_score_manual.py               # §3.1 manual-review scorer
|-- stage2_xref_graph.py                 # §3.2, §4
|-- stage2_streaming_alert.py            # §4.4 streaming detector
|-- stage2_blacklist_baseline.py         # §4.4 blacklist comparison
|-- stage3_carrier.py                    # §3.3, §5
|-- stage4_campaigns_ecosystems.py       # §3.4, §6
|-- stage4_ftc_cross_check.py            # §6.8
|-- results.jsonl                        # packaged 800notes corpus
|-- data/
|   `-- carrier_lookup_cached.jsonl      # cached Twilio results (March 2026)
|-- docs/
|   |-- data_schema.md                   # input contract
|   `-- reuse.md                         # porting guide
|-- extraction_validation/
|   |-- compute_iaa.py                   # Cohen's kappa, F1 against 2 humans
|   |-- sample_for_annotation.py         # stratified-sample draw
|   `-- (annotation sheets)
`-- output/                              # populated by run_pipeline.py
```

---

## Availability

- GitHub  : <add repo URL before camera-ready>
- Anonymous mirror: <https://anonymous.4open.science/r/EXPOSE-Toolkit-87E3/>
- Zenodo DOI: <minted on release; https://doi.org/10.5281/zenodo.XXXXXXX>
- Software Heritage: <SWH ID pinned to the tagged commit>

---

## License

- Code: Apache-2.0 (see `LICENSE`)
- Data: CC-BY-4.0 (see `LICENSE-DATA`)

---

## Contact

Corresponding author: anonymized for review.

---

## Ethics

This study uses publicly accessible complaint data from 800notes.com
and public FTC Do-Not-Call complaint files. No calls were placed and
no messages were sent; we did not interact with suspected operators or
trigger scam infrastructure. We did not collect, infer, or analyze the
real-world identities of complainants. Carrier metadata is queried
through Twilio Lookup v2, which draws from the same number-portability
databases that law enforcement accesses by subpoena, and is used here
strictly for infrastructure characterization rather than attribution.
The full statement is in §B of the paper. This research was reviewed
by our IRB and determined to be non-human-subjects research; the
determination letter is on file.
