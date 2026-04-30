#!/usr/bin/env bash
# One-command reproduction entry point.
# Usage:
#   ./reproduce_all.sh                         # packaged 800notes corpus
#   ./reproduce_all.sh /path/to/corpus.jsonl   # any other corpus
set -euo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

INPUT="${1:-$HERE/results.jsonl}"
OUTPUT="${2:-$HERE/output}"

echo "SIG-Toolkit reproduction"
echo "  input : $INPUT"
echo "  output: $OUTPUT"

python3 run_pipeline.py --input "$INPUT" --output "$OUTPUT"
