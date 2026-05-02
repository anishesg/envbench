#!/usr/bin/env bash
# run_local.sh - Run the envbench pipeline locally for testing.
#
# Usage:
#   ./scripts/run_local.sh              # run full pipeline
#   ./scripts/run_local.sh ingest       # run only ingest stage
#   ./scripts/run_local.sh join         # run only join stage
#   ./scripts/run_local.sh generate     # run only generate stage
#   ./scripts/run_local.sh export       # run only export stage
#
# Requirements:
#   - Python 3.12+
#   - uv (https://docs.astral.sh/uv/)
#   - GDAL/GEOS system libraries (for geopandas spatial joins)
#   - ~8 GB free RAM for the join stage
#   - .env file at project root (created automatically if missing)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

STAGE="${1:-all}"

# ── Validate stage argument ─────────────────────────────────────────────────
case "$STAGE" in
    all|ingest|join|generate|export) ;;
    *)
        echo "ERROR: Unknown stage '$STAGE'"
        echo "Valid stages: all, ingest, join, generate, export"
        exit 1
        ;;
esac

# ── Check prerequisites ─────────────────────────────────────────────────────
if ! command -v uv &>/dev/null; then
    echo "ERROR: uv is not installed. Install with: curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

if ! python3 --version 2>/dev/null | grep -q "3.1[2-9]"; then
    echo "WARNING: Python 3.12+ recommended. Current: $(python3 --version 2>/dev/null || echo 'not found')"
fi

# ── Create .env if missing ──────────────────────────────────────────────────
if [[ ! -f .env ]]; then
    echo "Creating default .env file..."
    cat > .env << 'EOF'
CENSUS_API_KEY=
NOAA_API_KEY=
AWS_DEFAULT_REGION=us-east-1
EOF
    echo "  Created .env (Census/CDC work without API keys)"
fi

# ── Sync dependencies ───────────────────────────────────────────────────────
echo "Syncing Python dependencies..."
uv sync

# ── Run pipeline ─────────────────────────────────────────────────────────────
echo ""
echo "========================================="
echo "envbench pipeline - stage: $STAGE"
echo "Started: $(date)"
echo "========================================="
echo ""

if [[ "$STAGE" == "all" ]]; then
    uv run python scripts/run_pipeline.py
else
    uv run python scripts/run_pipeline.py --stage "$STAGE"
fi

EXIT_CODE=$?

echo ""
echo "========================================="
echo "Pipeline finished: $(date)"
echo "Exit code: $EXIT_CODE"
echo "========================================="

if [[ $EXIT_CODE -eq 0 ]]; then
    echo ""
    echo "Output files:"
    if [[ -d data/benchmark ]]; then
        ls -lh data/benchmark/*.parquet 2>/dev/null || echo "  (no parquet files yet)"
        ls -lh data/benchmark/metadata.json 2>/dev/null || true
    fi
fi

exit $EXIT_CODE
