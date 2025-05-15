#!/bin/bash
# Run MoleculerPy test service

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MOLECULERPY_DIR="$(dirname "$SCRIPT_DIR")"
VENV_PYTHON="$MOLECULERPY_DIR/.venv/bin/python"

# Check if venv exists
if [ ! -f "$VENV_PYTHON" ]; then
    echo "Error: Python venv not found at $MOLECULERPY_DIR/.venv"
    echo "Create it with: python3.12 -m venv $MOLECULERPY_DIR/.venv"
    echo "Then install: $MOLECULERPY_DIR/.venv/bin/pip install -e $MOLECULERPY_DIR"
    exit 1
fi

# Set environment variables (can be overridden)
export NATS_URL="${NATS_URL:-nats://localhost:4222}"
export MOLECULER_NAMESPACE="${MOLECULER_NAMESPACE:-graphrag}"
export MOLECULER_NODE_ID="${MOLECULER_NODE_ID:-moleculerpy-test}"

# Run the service
exec "$VENV_PYTHON" "$SCRIPT_DIR/gerts_test_service.py" "$@"
