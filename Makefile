.PHONY: help install test test-unit test-integration test-integration-docker clean lint format typecheck proto-gen proto-check

help:
	@echo "Available commands:"
	@echo "  make install              - Install package in development mode"
	@echo "  make test                 - Run all tests"
	@echo "  make test-unit            - Run unit tests only"
	@echo "  make test-integration     - Run integration tests"
	@echo "  make test-integration-docker - Run integration tests with Docker"
	@echo "  make lint                 - Run linting checks"
	@echo "  make format               - Format code"
	@echo "  make typecheck            - Run type checking"
	@echo "  make proto-gen            - Regenerate packets_pb2.py from packets.proto"
	@echo "  make proto-check          - Verify packets_pb2.py is in sync with packets.proto"
	@echo "  make clean                - Clean up generated files and containers"

PROTO_DIR := moleculerpy/serializers/proto
PROTO_PB2 := $(PROTO_DIR)/packets_pb2.py

proto-gen:
	.venv/bin/python -m grpc_tools.protoc --python_out=$(PROTO_DIR) -I $(PROTO_DIR) packets.proto

proto-check:
	@cp $(PROTO_PB2) /tmp/packets_pb2_before.py
	@.venv/bin/python -m grpc_tools.protoc --python_out=$(PROTO_DIR) -I $(PROTO_DIR) packets.proto
	@sed -i.bak '/^# -\*- coding: utf-8 -\*-$$/d' $(PROTO_PB2) && rm -f $(PROTO_PB2).bak
	@.venv/bin/ruff format --quiet $(PROTO_PB2) || true
	@if ! diff -q $(PROTO_PB2) /tmp/packets_pb2_before.py > /dev/null; then \
		cp /tmp/packets_pb2_before.py $(PROTO_PB2); \
		echo "ERROR: packets_pb2.py is out of sync with packets.proto. Run 'make proto-gen' and commit."; \
		exit 1; \
	fi
	@echo "proto-check: OK"

install:
	pip install -e .[test]
	cd tests/integration/node_services && npm install

test: test-unit

test-unit:
	pytest tests/unit -v

# test-integration:
# 	cd tests/integration && python run_integration_tests.py

# test-integration-docker:
# 	cd tests/integration && python run_integration_tests.py

lint:
	ruff check moleculerpy tests

format:
	ruff format moleculerpy tests

typecheck:
	mypy moleculerpy

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf build dist .pytest_cache .mypy_cache
	cd tests/integration && docker compose down 2>/dev/null || true
