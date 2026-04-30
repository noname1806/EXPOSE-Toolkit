# Porting the toolkit to a new complaint corpus

The framework's input contract is documented in
[`data_schema.md`](data_schema.md). If your corpus already matches
that schema, skip straight to step 3.

## 1. Convert your corpus to the JSONL schema

Write a small adapter that groups your source into one JSON record per
reported identifier. Minimum fields:

- `e164`          — normalized `+1XXXXXXXXXX`
- `comments[]`    — at least one object with `date` (YYYY-MM-DD) and `text`

A reference adapter for a simple CSV (`e164,date,text`) is under 20 lines:

```python
import csv, json
from collections import defaultdict

recs = defaultdict(list)
for row in csv.DictReader(open("my_corpus.csv", encoding="utf-8")):
    recs[row["e164"]].append({"date": row["date"], "text": row["text"]})

with open("my_corpus.jsonl", "w", encoding="utf-8") as f:
    for e164, comments in recs.items():
        f.write(json.dumps({"e164": e164, "comments": comments}) + "\n")
```

## 2. Validate

Run the validator from the schema doc. If it reports 0 invalid
records, you are ready.

## 3. Run

```bash
python run_pipeline.py --input my_corpus.jsonl --output ./out_mycorpus
```

or via Docker:

```bash
docker run --rm \
  -v "$PWD/my_corpus.jsonl:/artifact/input.jsonl:ro" \
  -v "$PWD/out_mycorpus:/artifact/output" \
  sig-toolkit \
  python run_pipeline.py --input /artifact/input.jsonl --output /artifact/output
```

## 4. What to read first

After the pipeline completes, inspect these files in the output
directory in this order:

| File                              | Answers                                      |
| --------------------------------- | -------------------------------------------- |
| `sig_nlp_report.txt`              | Is extraction precision / recall sane?       |
| `sig_construction_report.txt`     | How large is your SIG? Shadow ratio?         |
| `sig_baseline_comparison.txt`     | Does H_T beat random and area-code grouping? |
| `sig_vs_blacklist_report.txt`     | Is the shadow layer non-trivial?             |
| `streaming_hub_report.txt`        | Can a streaming detector flag hubs early?    |
| `campaign_linking_report.txt`     | Do you recover multi-campaign operations?    |

## 5. Expected behavior on sparse corpora

Small or sparse corpora may produce:

- **Zero campaigns** — raise `TEXT_SIM_THRESHOLD` search: edit the
  constant in `campaign_linking.py` or pre-filter to complaints with
  ≥50 characters of text.
- **Zero shadow hubs** — if victims in your corpus do not quote
  callback numbers in their narratives, the shadow layer will be
  empty. This is a corpus property, not a failure.
- **Zero operations** — operations require ≥2 linked campaigns. On
  small corpora with few cross-campaign infrastructure links, this is
  expected and reported honestly in the pipeline output.

The pipeline will not crash on these cases; it will report the empty
result and continue to the next stage.

## 6. Known non-portable assumptions

- **NANP (North American Numbering Plan)** — the E.164 normalizer
  assumes 10- or 11-digit US/Canada/Caribbean numbers. To support
  other numbering plans, swap `normalize_to_e164` in
  `sig_nlp_pipeline.py` for a library such as `phonenumbers`.
- **English-language context classifier** — `CALLBACK_PATTERNS`,
  `SPOOFING_PATTERNS`, and `SMS_PATTERNS` are English regexes. For
  non-English corpora, translate the regex idioms or replace with a
  multilingual classifier.
- **Toll-free NPA list** — the list `{800,888,877,866,855,844,833}`
  is US toll-free. Adjust for other jurisdictions.

None of these are load-bearing for the graph-construction,
streaming, or campaign-linking logic; they are localization-level
concerns.
