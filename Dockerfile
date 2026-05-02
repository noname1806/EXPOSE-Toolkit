# EXPOSE-Toolkit reproduction image.
#
# Single-command reproduction of every quantitative claim in
#   "Call Me Maybe? Exposing Patterns of Shadow Scam Ecosystems
#    via Open-Source Victim Complaints"
#
# Build:
#   docker build -t expose-toolkit .
#
# Run with the packaged 800notes corpus (default):
#   docker run --rm -v "$PWD/output:/artifact/output" expose-toolkit
#
# Run on a reviewer-supplied corpus:
#   docker run --rm \
#     -v "/abs/path/to/your_corpus.jsonl:/artifact/input.jsonl:ro" \
#     -v "$PWD/output:/artifact/output" \
#     expose-toolkit \
#     python run_pipeline.py --input /artifact/input.jsonl --output /artifact/output
#
# The image is deterministic: Python 3.11, pinned scikit-learn / numpy /
# scipy.  No network access is required at run time for the local
# stages (Stages 1, 2a, 2b, 2c, 3, 4a).  Stage 4b (FTC cross-validation)
# fetches public FTC bulk files only when --download-ftc is passed.

FROM python:3.11.9-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /artifact

COPY requirements.txt /artifact/requirements.txt
RUN pip install --no-cache-dir -r /artifact/requirements.txt

COPY . /artifact

RUN mkdir -p /artifact/output
VOLUME ["/artifact/output"]

# Default: run the full local pipeline on the packaged corpus.
# Stage 4b (FTC) is skipped by default because the FTC corpus is not
# redistributed with this image.  Pass `--download-ftc` to fetch it.
CMD ["python", "run_pipeline.py", \
     "--input", "/artifact/results.jsonl", \
     "--output", "/artifact/output", \
     "--skip", "4b"]
