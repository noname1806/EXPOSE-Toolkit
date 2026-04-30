# SIG-Toolkit reproduction image.
# Single-command reproduction of every quantitative claim in the paper
# "SIG: Recovering Scam Infrastructure Graphs from Victim Complaint
# Intelligence".
#
# Build:
#   docker build -t sig-toolkit .
#
# Run with the packaged 800notes corpus (default):
#   docker run --rm -v "$PWD/output:/artifact/output" sig-toolkit
#
# Run on a reviewer-supplied corpus:
#   docker run --rm \
#     -v "/abs/path/to/your_corpus.jsonl:/artifact/input.jsonl:ro" \
#     -v "$PWD/output:/artifact/output" \
#     sig-toolkit \
#     python run_pipeline.py --input /artifact/input.jsonl --output /artifact/output
#
# The image is deterministic: Python 3.11, pinned sklearn/numpy, no
# network access required at run time.

FROM python:3.11.9-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /artifact

COPY requirements.txt /artifact/requirements.txt
RUN pip install --no-cache-dir -r /artifact/requirements.txt

COPY . /artifact

# Output directory is the conventional mount point for reviewers.
RUN mkdir -p /artifact/output
VOLUME ["/artifact/output"]

# Default: run the full pipeline on the packaged corpus.
CMD ["python", "run_pipeline.py", "--input", "/artifact/results.jsonl", "--output", "/artifact/output"]
