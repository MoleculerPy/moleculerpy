# MoleculerPy - Production Docker Image
# Multi-stage build for minimal image size (~60-80MB)
#
# Build:  docker build -t moleculerpy .
# Run:    docker run --rm moleculerpy python -c "import moleculerpy"

# ── Stage 1: Builder ─────────────────────────────
FROM python:3.12-alpine AS builder

RUN apk add --no-cache gcc musl-dev

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY pyproject.toml README.md ./
COPY moleculerpy/ ./moleculerpy/

RUN pip install --no-cache-dir .

# ── Stage 2: Runtime ─────────────────────────────
FROM python:3.12-alpine

LABEL maintainer="MoleculerPy Team <explosivebit@gmail.com>"
LABEL org.opencontainers.image.source="https://github.com/MoleculerPy/moleculerpy"

# Non-root user
RUN addgroup -S moleculer && adduser -S moleculer -G moleculer

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

USER moleculer

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import moleculerpy; print('ok')" || exit 1

CMD ["python"]
