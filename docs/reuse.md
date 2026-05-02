# Porting EXPOSE-Toolkit to a new complaint corpus

The framework's input contract is documented in
[`data_schema.md`](data_schema.md).  If your corpus already matches
that schema, skip straight to step 3.

## 1. Convert your corpus to the JSONL schema

Write a small adapter that groups your source into one JSON record per
reported identifier.  Minimum fields:

- `e164`          - normalized `+1XXXXXXXXXX`
- `comments[]`    - at least one object with `date` (YYYY-MM-DD) and `text`

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

Run the validator from the schema doc.  If it reports 0 invalid
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
  expose-toolkit \
  python run_pipeline.py --input /artifact/input.jsonl --output /artifact/output
```

## 4. What to read first

After the pipeline completes, inspect these files in the output
directory in this order:

| File                                  | Answers                                                      |
| ------------------------------------- | ------------------------------------------------------------ |
| `stage1_report.txt`                   | Is extraction precision / recall sane?                       |
| `stage2_report.txt`                   | What does the role partition look like?  Shadow ratio?       |
| `stage2_baseline_comparison.txt`      | Do components beat random and area-code homogeneity?         |
| `blacklist_baseline_report.txt`       | Is there a non-trivial lurking-callback layer?               |
| `streaming_alert_report.txt`          | Can a streaming detector flag callback hubs early?           |
| `stage3_carrier_report.txt`           | What does the carrier line-type breakdown look like?         |
| `stage4_report.txt`                   | Do you recover multi-campaign ecosystems?                    |

## 5. Expected behaviour on sparse corpora

Small or sparse corpora may produce:

- **Zero campaigns** - raise `--threshold` higher or lower depending
  on how lexical your corpus is, or pre-filter to complaints with
  >= 50 characters of text.
- **Zero shadow callback targets** - if victims in your corpus do not
  quote callback numbers in their narratives, the shadow layer will
  be empty.  That is a corpus property, not a pipeline failure.
- **Zero ecosystems** - ecosystems require >= 2 campaigns linked by
  at least one indicator.  On small corpora with few cross-campaign
  infrastructure links, this is expected and reported honestly.

The pipeline will not crash on these cases; it will report the empty
result and continue to the next stage.

## 6. Known non-portable assumptions

- **NANP (North American Numbering Plan)** - the E.164 normalizer
  assumes 10- or 11-digit U.S./Canada/Caribbean numbers.  For other
  numbering plans, swap `normalize_to_e164` in `stage1_extract.py`
  for a library such as `phonenumbers`.
- **English-language context classifier** - `CALLBACK_PATTERNS`,
  `SPOOFED_PATTERNS`, and `SMS_PATTERNS` in `stage1_extract.py` are
  English regexes.  For non-English corpora, translate the idioms or
  replace with a multilingual classifier.
- **Toll-free NPA list** - the set `{800, 888, 877, 866, 855, 844, 833}`
  is U.S. toll-free.  Adjust for other jurisdictions.

None of these are load-bearing for the graph-construction, streaming,
or ecosystem-linking logic; they are localization-level concerns.
