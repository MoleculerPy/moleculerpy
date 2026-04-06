#!/usr/bin/env bash
#
# Full Integration Pipeline — end-to-end verification
#
# Runs the complete methodology pipeline with real services:
# 1. Docker compose up (NATS, Valkey, MQTT, RabbitMQ, Kafka)
# 2. Wait for all services to be healthy
# 3. ruff format check
# 4. ruff lint check
# 5. mypy --strict
# 6. Unit tests (pytest tests/unit/)
# 7. E2E tests (pytest tests/e2e/)
# 8. Integration tests (pytest tests/integration/)
# 9. Demo matrix (all serializers × all transporters with real brokers)
# 10. Generate report
# 11. Docker compose down (always, even on failure)
#
# Exit codes:
#   0  — all passed
#   1  — any step failed
#   2  — docker unavailable
#   3  — interrupted
#
# Usage:
#   ./scripts/run-full-pipeline.sh              # full pipeline
#   ./scripts/run-full-pipeline.sh --no-docker  # skip docker, only in-process tests
#   ./scripts/run-full-pipeline.sh --quick      # skip slow integration tests
#   ./scripts/run-full-pipeline.sh --no-cleanup # keep docker running after
#
set -euo pipefail

# --- Colors ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

# --- Paths ---
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

VENV_PY="$REPO_ROOT/.venv/bin/python"
VENV_PIP="$REPO_ROOT/.venv/bin/pip"
VENV_RUFF="$REPO_ROOT/.venv/bin/ruff"
VENV_MYPY="$REPO_ROOT/.venv/bin/mypy"
VENV_PYTEST="$REPO_ROOT/.venv/bin/pytest"

DOCKER_COMPOSE="$REPO_ROOT/tests/integration/docker-compose.yaml"
REPORT_DIR="$REPO_ROOT/pipeline-reports"
REPORT_FILE="$REPORT_DIR/report-$(date +%Y%m%d-%H%M%S).txt"

# --- Arg parsing ---
NO_DOCKER=0
QUICK=0
NO_CLEANUP=0
for arg in "$@"; do
    case "$arg" in
        --no-docker) NO_DOCKER=1 ;;
        --quick) QUICK=1 ;;
        --no-cleanup) NO_CLEANUP=1 ;;
        -h|--help)
            grep '^#' "$0" | head -30 | sed 's|^# \?||'
            exit 0
            ;;
        *)
            echo "Unknown option: $arg"
            exit 2
            ;;
    esac
done

# --- Step tracking ---
STEPS=()
STEP_STATUS=()
STEP_TIMES=()
FAILED=0

log_header() {
    echo ""
    echo -e "${BOLD}${BLUE}=== $1 ===${NC}"
    echo ""
}

run_step() {
    local name="$1"
    shift
    local cmd=("$@")
    local start end duration status

    STEPS+=("$name")
    echo -e "${BOLD}[RUN]${NC} $name"
    start=$(date +%s)

    if "${cmd[@]}"; then
        status="PASS"
        end=$(date +%s)
        duration=$((end - start))
        STEP_STATUS+=("PASS")
        STEP_TIMES+=("$duration")
        echo -e "${GREEN}[PASS]${NC} $name (${duration}s)"
    else
        status="FAIL"
        end=$(date +%s)
        duration=$((end - start))
        STEP_STATUS+=("FAIL")
        STEP_TIMES+=("$duration")
        FAILED=1
        echo -e "${RED}[FAIL]${NC} $name (${duration}s)"
    fi
}

# --- Cleanup ---
cleanup() {
    local exit_code=$?
    if [[ $NO_CLEANUP -eq 0 && $NO_DOCKER -eq 0 ]]; then
        log_header "Cleanup: docker compose down"
        docker compose -f "$DOCKER_COMPOSE" down --timeout 10 2>/dev/null || true
        docker rm -f demo-valkey 2>/dev/null || true
    fi
    exit $exit_code
}
trap cleanup EXIT INT TERM

# --- Prep ---
mkdir -p "$REPORT_DIR"
exec > >(tee "$REPORT_FILE") 2>&1

log_header "MoleculerPy Full Pipeline — $(date)"
echo "Repo: $REPO_ROOT"
echo "Python: $($VENV_PY --version 2>&1)"
echo "Report: $REPORT_FILE"
echo ""

if [[ ! -x "$VENV_PY" ]]; then
    echo -e "${RED}ERROR:${NC} .venv not found. Run: python3.12 -m venv .venv && pip install -e .[dev,msgpack,cbor,protobuf]"
    exit 2
fi

# ==========================================================================
# Phase 0: Docker services
# ==========================================================================
if [[ $NO_DOCKER -eq 0 ]]; then
    log_header "Phase 0: Start Docker services"

    if ! docker ps >/dev/null 2>&1; then
        echo -e "${RED}ERROR:${NC} Docker not available. Use --no-docker to skip."
        exit 2
    fi

    # Start compose services (idempotent — skip if already running)
    docker compose -f "$DOCKER_COMPOSE" up -d 2>&1 || true

    # Add valkey on 6381 if not running
    if ! docker ps --format "{{.Names}}" | grep -q "demo-valkey"; then
        docker run -d --name demo-valkey -p 6381:6379 valkey/valkey:7-alpine >/dev/null 2>&1 || true
    fi

    # Wait for health — poll ports
    echo "Waiting for services to be reachable..."
    for i in $(seq 1 30); do
        READY=1
        for port in 4222 6381 1883 5672 9092; do
            if ! (echo > /dev/tcp/localhost/$port) >/dev/null 2>&1; then
                READY=0
                break
            fi
        done
        if [[ $READY -eq 1 ]]; then
            echo -e "${GREEN}All services reachable${NC} (after ${i}s)"
            break
        fi
        sleep 1
    done

    # Extra grace for kafka
    sleep 3
    docker ps --format "{{.Names}}\t{{.Status}}" | grep -E "nats|mosquitto|rabbit|kafka|valkey" || true
fi

# ==========================================================================
# Phase 1: Code quality
# ==========================================================================
log_header "Phase 1: Code Quality"

run_step "ruff format --check" "$VENV_RUFF" format --check moleculerpy/ tests/
run_step "ruff check" "$VENV_RUFF" check moleculerpy/ tests/
run_step "mypy --strict" "$VENV_MYPY" moleculerpy/

# ==========================================================================
# Phase 2: Unit & E2E tests
# ==========================================================================
log_header "Phase 2: Unit & E2E Tests"

run_step "pytest unit (2260+)" "$VENV_PYTEST" tests/unit/ -q --tb=line
run_step "pytest e2e (70)" "$VENV_PYTEST" tests/e2e/ -q --tb=line

# ==========================================================================
# Phase 3: Integration tests (require Docker)
# ==========================================================================
if [[ $NO_DOCKER -eq 0 && $QUICK -eq 0 ]]; then
    log_header "Phase 3: Integration Tests"

    run_step "pytest integration" "$VENV_PYTEST" tests/integration/ -q --tb=line || true
else
    echo "Skipping integration tests (--no-docker or --quick)"
fi

# ==========================================================================
# Phase 4: Demo matrix (the real functional coverage)
# ==========================================================================
log_header "Phase 4: Demo Matrix (Real Functional Coverage)"
echo ""
echo "This runs ALL serializers × ALL transporters against real brokers."
echo "Each cell tests: broker lifecycle, local call, complex payload,"
echo "remote call, benchmark. This is the MOST IMPORTANT test stage."
echo ""

run_step "demo_matrix (all combinations)" "$VENV_PY" examples/demo_matrix.py || true

# ==========================================================================
# Final Report
# ==========================================================================
log_header "Pipeline Report"

printf "%-50s %-8s %s\n" "Step" "Status" "Time"
printf -- "------------------------------------------------------------------\n"
for i in "${!STEPS[@]}"; do
    name="${STEPS[$i]}"
    status="${STEP_STATUS[$i]}"
    time_s="${STEP_TIMES[$i]}s"
    if [[ "$status" == "PASS" ]]; then
        printf "%-50s ${GREEN}%-8s${NC} %s\n" "$name" "$status" "$time_s"
    else
        printf "%-50s ${RED}%-8s${NC} %s\n" "$name" "$status" "$time_s"
    fi
done
printf -- "------------------------------------------------------------------\n"

PASSED_COUNT=$(printf '%s\n' "${STEP_STATUS[@]}" | grep -c "PASS" || true)
FAILED_COUNT=$(printf '%s\n' "${STEP_STATUS[@]}" | grep -c "FAIL" || true)
TOTAL_COUNT="${#STEPS[@]}"

echo ""
echo -e "Total: $TOTAL_COUNT  |  ${GREEN}Passed: $PASSED_COUNT${NC}  |  ${RED}Failed: $FAILED_COUNT${NC}"
echo ""
echo "Full report saved to: $REPORT_FILE"

if [[ $FAILED -eq 0 ]]; then
    echo -e "${BOLD}${GREEN}PIPELINE PASSED${NC}"
    exit 0
else
    echo -e "${BOLD}${RED}PIPELINE FAILED${NC}"
    exit 1
fi
