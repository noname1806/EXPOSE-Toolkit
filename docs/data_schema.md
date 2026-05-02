# Input data schema (`results.jsonl`)

The EXPOSE-Toolkit operates over any telephony-complaint corpus that
exposes **three fields per complaint**:

1. a reported telephony identifier (normalized to E.164),
2. one or more free-text narratives associated with that identifier, and
3. a timestamp per narrative.

This is the "minimal input contract" referenced in the paper (§3).
To run the pipeline on a different complaint source (Reddit threads,
FTC CSN extracts, consumeraffairs, etc.), convert it to the JSONL
schema below.  No other change is needed.

## File format

A newline-delimited JSON file.  One record per line.  One record per
reported identifier.  UTF-8.

## Record schema

```jsonc
{
  "phone_number": "1-225-230-5021",      // display form, informational
  "e164":         "+12252305021",        // REQUIRED. Primary key. E.164, no spaces.
  "area_code":    "225",                 // optional, informational
  "url":          "https://...",         // optional, provenance
  "final_url":    "https://...",         // optional, after redirects
  "http_status":  200,                   // optional, fetch status
  "total_comments": 1,                   // optional, equals len(comments)
  "total_pages":  1,                     // optional
  "dominant_call_type": "Debt collector",// OPTIONAL weak platform label.
                                         //   Used only for descriptive reports
                                         //   and streaming-alert source-type
                                         //   consistency; if absent, pass
                                         //   "Unknown".
  "call_type_breakdown": {"Debt collector": 1}, // optional, informational
  "comments": [                          // REQUIRED. Array of complaint narratives.
    {
      "index":           1,              // optional
      "author":          "0",            // optional (redact for release)
      "date":            "2026-02-11",   // REQUIRED. ISO YYYY-MM-DD.
      "caller_identity": "scam",         // optional weak label
      "call_type":       "Debt collector", // optional weak label
      "text":            "Noelle claiming to be Lisa ... call 855.865.1878 or text 920.717.4451.",
                                         // REQUIRED. Free-text narrative.
      "mentioned_numbers": [             // OPTIONAL but recommended.
                                         //   If absent, Stage 1 will extract
                                         //   mentions directly from `text`.
        {
          "number":  "855-865-1878",     // display form
          "e164":    "+18558651878",     // normalized E.164
          "context": "text/sms number"   // one of the context labels below
        }
      ]
    }
  ],
  "scraped_at": "2026-03-07T20:03:38"    // optional, collection time
}
```

## Context labels (`mentioned_numbers[*].context`)

The platform parser used to produce the released corpus emits the
strings below.  EXPOSE Stage 1 maps them to the four-label scheme of
paper §3.1, plus a `provided` carry-through:

| Platform string                | Internal label | Meaning                                                  |
| ------------------------------ | -------------- | -------------------------------------------------------- |
| `callback number`              | `callback`     | Victim was told to call this number.                     |
| `spoofed caller id`            | `spoofed`      | Number appeared as caller-ID but is not the real source. |
| `text/sms number`              | `sms`          | Text/SMS channel number.                                 |
| `mentioned in text message`    | `sms`          | (older platform variant) same as above.                  |
| `number they provided`         | `provided`     | Number given during the interaction.                     |
| `mentioned in comment`         | `mention`      | Generic co-mention in narrative.                         |
| *(any other string)*           | `mention`      | Safe default; treated as generic mention.                |

If your corpus does not pre-extract `mentioned_numbers`, omit the
field entirely.  Stage 1 (`stage1_extract.py`) will extract mentions
from the `text` field using the regex + 120-character windowed
classifier described in paper §3.1 and Appendix C.  The platform
parser is treated as a weak label, not ground truth.

## Required vs. optional fields

| Field                              | Required? | Notes                                                  |
| ---------------------------------- | --------- | ------------------------------------------------------ |
| `e164`                             | yes       | Primary key.  Must be `+1XXXXXXXXXX` (11 digits).      |
| `comments`                         | yes       | Array; empty is allowed but the record is then unused. |
| `comments[*].date`                 | yes       | `YYYY-MM-DD`.  Needed for the streaming alert.         |
| `comments[*].text`                 | yes       | Free-text narrative.                                   |
| `comments[*].mentioned_numbers`    | no        | If absent, Stage 1 extracts from `text`.               |
| `dominant_call_type`               | no        | Used only for report tables and source-type consistency.|

All other fields are informational and may be omitted.

## Optional external enrichment

Stage 4 (`stage4_campaigns_ecosystems.py`) reads
`output/carrier_lookup.jsonl` (produced by Stage 3) to activate the
`sigma_carrier` indicator described in paper §6.3.  Each line is:

```jsonc
{
  "e164":           "+18772000760",
  "lookup_success": true,
  "line_type":      "tollFree",       // tollFree | nonFixedVoip | mobile | landline | fixedVoip
  "carrier_name":   "Bandwidth/Zipwhip",   // null allowed (RespOrg opacity)
  "mobile_country_code": "310",       // optional
  "mobile_network_code": "150"        // optional
}
```

If the file is not present, `sigma_carrier` is silently skipped; the
two stronger indicators (`sigma_hub`, `sigma_edge`) still run and
produce the primary ecosystem graph.

## Validating a new corpus

Before running the full pipeline on a new corpus, run a cheap sanity
check:

```bash
python -c "
import json, sys
req_rec  = ['e164', 'comments']
req_com  = ['date', 'text']
n, bad = 0, 0
for line in open(sys.argv[1], encoding='utf-8'):
    r = json.loads(line)
    n += 1
    missing = [f for f in req_rec if f not in r]
    for c in r.get('comments', []):
        missing += [f'comments.{f}' for f in req_com if f not in c]
    if missing:
        bad += 1
        if bad <= 3:
            print(f'[{r.get(\"e164\",\"?\")}] missing: {missing}')
print(f'{n} records, {bad} invalid')
" your_corpus.jsonl
```

If the validator reports 0 invalid records, the corpus is compatible
and you can run `python run_pipeline.py --input your_corpus.jsonl`.
