# EXPOSE-Toolkit: Artifact for "Recovering Scam Infrastructure Graphs from Victim Complaint Intelligence"

Artifact accompanying the ACM CCS 2026 submission. This toolkit
reproduces every quantitative claim in the paper (Tables 1–10,
Figure 1) from the packaged 800notes complaint corpus, and can be
re-run on any complaint corpus that satisfies the input contract in
[`docs/data_schema.md`](docs/data_schema.md).

## Badges claimed

- [x] **Artifacts Available** — DOI-minted snapshot (see §Availability).
- [x] **Artifacts Functional** — one-command reproduction.
- [x] **Artifacts Reusable** — minimal input contract; runs on any
      JSONL complaint corpus matching `docs/data_schema.md`.
- [x] **Results Reproduced** — all paper tables regenerate from the
      packaged corpus.

## One-command reproduction

### Option A — Docker (recommended for reviewers)

```bash
docker build -t expose-toolkit .
docker run --rm -v "$PWD/output:/artifact/output" expose-toolkit
```

Full run: ~8 minutes on a 2023 commodity laptop. No GPU. No network.

### Option B — Local Python

```bash
pip install -r requirements.txt
python run_pipeline.py                       # uses ./results.jsonl
```

Windows:

```bat
pip install -r requirements.txt
reproduce_all.bat
```

### Option C — Any other complaint corpus

```bash
python run_pipeline.py --input /path/to/your_corpus.jsonl --output ./out_mycorpus
```

The corpus must satisfy [`docs/data_schema.md`](docs/data_schema.md).
See [`docs/reuse.md`](docs/reuse.md) for a 20-line CSV→JSONL adapter.

## What the pipeline produces

All outputs land under `output/` (or the `--output` directory you
pass). Cross-reference each file with the paper:

| Output file                      | Paper reference        | Claim it reproduces                                    |
| -------------------------------- | ---------------------- | ------------------------------------------------------ |
| `sig_nlp_report.txt`             | §3.1, Abstract         | Parser-independent extraction F1 = 0.9894              |
| `sig_nlp_evaluation.json`        | §3.1                   | Per-context precision / recall                         |
| `sig_nlp_disagreements.csv`      | §3.1                   | 100-case manual review input                           |
| `sig_edges.csv`                  | §4.1                   | 8,542 unique directed pairs                            |
| `sig_components.csv`             | §4.1                   | 2,449 connected components                             |
| `sig_construction_report.txt`    | §4.1, Table 1          | Role decomposition, shadow ratio 81.1%                 |
| `sig_baseline_comparison.txt`    | §4.3                   | H_T vs. random + area-code baselines                   |
| `sig_component_eval.csv`         | §4.3                   | Top-50 components for manual coherence review          |
| `streaming_hub_report.txt`       | §4.3                   | 15 hubs flagged at fan-in ≥ 3, 100% precision          |
| `streaming_hub_eval.csv`         | §4.3                   | Per-hub streaming detection data                       |
| `sig_vs_blacklist_report.txt`    | §4.3, Table 3          | 541/541 shadow hubs recovered, 6.2× leverage           |
| `sig_vs_blacklist.csv`           | Table 3                | Per-hub blacklist comparison                           |
| `campaign_clusters.csv`          | §6.2                   | 203 non-singleton campaigns                            |
| `operation_graph.csv`            | §6.3                   | Cross-campaign infrastructure links                    |
| `operations_summary.csv`         | Table 6                | 12 recovered operations                                |
| `campaign_linking_report.txt`    | §6, Tables 6–8         | Full operation discovery report                        |

## Pipeline stages

```
results.jsonl                     (any corpus matching docs/data_schema.md)
    │
    ├─[1] sig_nlp_pipeline.py          parser-independent extraction (§3.1)
    ├─[2] sig_construction.py          SIG + component / baseline eval  (§4.1, §4.3)
    ├─[3] sig_streaming.py             temporal-split hub detection     (§4.3)
    ├─[4] sig_vs_blacklist.py          complaint-volume comparison      (§4.3, Tab 3)
    └─[5] campaign_linking.py          campaigns + operations           (§6)
                 ├─ Layer A (σ_hub)    shared callback hub
                 ├─ Layer B            shared scam domain
                 ├─ Layer C (σ_carrier) shared carrier  (needs --twilio)
                 └─ Layer D (σ_edge)   SIG cross-reference
```

Each stage is independent and idempotent; skip stages with
`python run_pipeline.py --skip 3,4`.

## Environment

- Python 3.11
- scikit-learn 1.4.2, numpy 1.26.4 (pinned in `requirements.txt`)
- No GPU
- ~16 GB RAM comfortable; 8 GB works
- Full run on the packaged corpus: ~8 minutes wall time

## Packaged data

- `results.jsonl` — 800notes corpus, 93,008 reported numbers,
  97,093 comments, February 2007 – March 2026. ~72 MB. All text is
  already publicly posted on 800notes.com. Reporter display names
  are retained only when the poster chose them publicly; no private
  identifiers are included.

## Optional external data

**Twilio Lookup v2** (paper §5). The pipeline activates Layer C
(shared carrier) if `twilio_lookup_results.jsonl` is provided:

```bash
python run_pipeline.py --input results.jsonl --twilio twilio_lookup_results.jsonl
```

Without it, three of four linking layers still run, and 10 of 12
operations in Table 6 are still recovered (σ_carrier contributes the
remaining 2; see paper Table 7).

**FTC DNC cross-match** (paper §6.8) uses public FTC Do-Not-Call
bulk data. It is not required to reproduce any of the twelve
numbered tables; it provides the external-validation column in §6.8
and is run separately because the FTC files are large (~4 GB) and
not redistributable from this artifact.

## Reviewer checklist

1. `docker build -t expose-toolkit .`
2. `docker run --rm -v "$PWD/output:/artifact/output" expose-toolkit`
3. Open `output/sig_construction_report.txt` → confirm Table 1 numbers.
4. Open `output/sig_vs_blacklist_report.txt` → confirm Table 3.
5. Open `output/campaign_linking_report.txt` → confirm Tables 6–8.
6. (Optional) Re-run with your own corpus per `docs/reuse.md`.

## Files in this artifact

```
artifact_evaluation/
├── README.md                   # this file
├── STATUS.md                   # badges claimed + evidence
├── LICENSE                     # Apache-2.0 for code
├── LICENSE-DATA                # CC-BY-4.0 for the released corpus
├── Dockerfile                  # reproducible container
├── .dockerignore
├── requirements.txt            # pinned Python deps
├── run_pipeline.py             # ORCHESTRATOR — single entry point
├── reproduce_all.sh            # Linux/macOS shim
├── reproduce_all.bat           # Windows shim
├── results.jsonl               # packaged 800notes corpus (72 MB)
├── sig_nlp_pipeline.py         # stage 1 (§3.1)
├── sig_nlp_score_manual.py     # post-labeling scoring helper
├── sig_construction.py         # stage 2 (§4.1, §4.3)
├── sig_streaming.py            # stage 3 (§4.3)
├── sig_vs_blacklist.py         # stage 4 (§4.3, Table 3)
├── campaign_linking.py         # stage 5 (§6)
├── docs/
│   ├── data_schema.md          # INPUT CONTRACT — swap-in guide
│   └── reuse.md                # porting the toolkit to a new corpus
└── output/                     # populated by run_pipeline.py
```

## Availability

- GitHub    : <add repo URL before camera-ready>
- Zenodo DOI: <mint on release; https://doi.org/10.5281/zenodo.XXXXXXX>
- Software Heritage: <SWH ID pinned to the tagged commit>

## License

- Code: Apache-2.0 (see `LICENSE`)
- Data: CC-BY-4.0 (see `LICENSE-DATA`)

## Contact

Corresponding author: <anonymized for review>

## Ethics

See the Open-Science and Ethics statement in §8 of the paper. All
complaint text is drawn from publicly posted 800notes.com pages; no
interaction with complainants or scam operators occurred. This
research was reviewed by our IRB and determined to be non-human-
subjects (determination letter on file).
