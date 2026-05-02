#!/usr/bin/env bash
# EXPOSE-Toolkit: single-command reproduction (Linux / macOS / WSL).
#
# Reproduces every quantitative claim in
#   "Call Me Maybe? Exposing Patterns of Shadow Scam Ecosystems
#    via Open-Source Victim Complaints"
# from the packaged 800notes corpus.
#
# Usage:
#   ./scripts/reproduce_all.sh
#   ./scripts/reproduce_all.sh --refresh-carrier   # call Twilio API
#   ./scripts/reproduce_all.sh --download-ftc      # fetch FTC files
#
# Wall time on a 2023 commodity laptop: ~8 min for the local stages.
# §6.8 FTC cross-validation needs the merged FTC CSV (~4 GB);
# pass --download-ftc to fetch it (10-20 min).

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$HERE"

if ! command -v python >/dev/null 2>&1; then
    echo "ERROR: python not found in PATH" >&2
    exit 1
fi

echo "[reproduce_all] working directory : $HERE"
echo "[reproduce_all] python            : $(python --version 2>&1)"
echo "[reproduce_all] running EXPOSE-Toolkit pipeline..."
python run_pipeline.py "$@"

echo ""
echo "[reproduce_all] Done."
echo "[reproduce_all] Headline reports under output/:"
ls -1 output/*report*.txt 2>/dev/null || true
