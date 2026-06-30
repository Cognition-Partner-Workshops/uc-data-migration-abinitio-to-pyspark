# Makefile for Ab Initio -> PySpark Migration Project
#
# In the Ab Initio world there was no build system — graphs were run via the
# Co>Operating System (`air sandbox run`) and scheduled with AutoSys/Control-M.
# This Makefile provides a standardized developer workflow for the PySpark target:
# generate the legacy "before" data, run the converted pipeline into an isolated
# namespace, and reconcile the result against the source.
#
# NS is the namespace (schema prefix) for a run. Outputs land under out/<NS>/...
# so multiple runs (NS=dev, NS=alice, NS=child1, ...) never collide and the
# durable "before" data in data/raw/ is never touched.
#   make demo-up   NS=alice    # generate source (idempotent) + run pipeline + reconcile
#   make demo-down NS=alice    # drop only that namespace's outputs

.PHONY: install seed run reconcile demo-up demo-down test lint lint-fix ci clean help

NS ?= dev
PY ?= python
export PYTHONPATH := .

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'

install: ## Install runtime + dev dependencies and pre-commit hooks
	pip install -r requirements.txt -r verify/requirements.txt
	pre-commit install || true

seed: ## (Re)generate the legacy raw extracts into data/raw/ (idempotent)
	$(PY) seed/generate_source.py

run: ## Run the converted PySpark pipeline into namespace NS
	$(PY) -m src.run_pipeline --namespace $(NS)

reconcile: ## Source -> target reconciliation report for namespace NS (fails on divergence)
	$(PY) verify/reconcile.py --namespace $(NS)

demo-up: seed run reconcile ## Full "after" state for NS: seed + run + reconcile

demo-down: ## Tear down one namespace's outputs (NS=...); raw data untouched
	rm -rf out/$(NS)
	@echo "dropped out/$(NS)"

test: ## Run the pytest suite (end-to-end pipeline + reconciliation)
	$(PY) -m pytest tests/ -q

lint: ## Lint with ruff and check formatting
	ruff check src verify seed tests
	ruff format --check src verify seed tests

lint-fix: ## Auto-fix lint + formatting
	ruff check --fix src verify seed tests
	ruff format src verify seed tests

ci: lint test ## Run the full CI pipeline locally (lint + tests)

clean: ## Remove generated outputs and caches
	rm -rf out .pytest_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
