PYTHON ?= .venv/bin/python
SYSTEM_PYTHON ?= python3
DATASET ?= sift1m
METRIC ?= l2
LISTS ?= 1000
PROBES ?= 32
MEMORY ?= 256MB
ARGS ?=

.PHONY: bootstrap setup database-ready test sql-test smoke verify download run clean-runtime

bootstrap:
	$(SYSTEM_PYTHON) -m venv .venv
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

setup:
	$(PYTHON) scripts/setup_database.py

database-ready:
	@if [ -n "$${PG_DSN:-}" ]; then \
		echo "Using external PG_DSN"; \
	else \
		$(PYTHON) scripts/setup_database.py; \
	fi

test:
	$(PYTHON) -m unittest discover -s tests -p 'test_*.py'

sql-test:
	$(MAKE) -C .. installcheck

smoke:
	$(PYTHON) scripts/generate_smoke_dataset.py --output .runtime/smoke-data
	@if [ -z "$${PG_DSN:-}" ]; then $(PYTHON) scripts/setup_database.py; fi
	$(PYTHON) ivfflat_diagnosis/run_matrix.py \
		--metric l2 --data-root .runtime/smoke-data --rows 2000 --lists 20 \
		--probes 1 5 --query-count 20 --repeat 1 --warmup 2 --k 10 \
		--sample-interval 0.05 --maintenance-work-mem 256MB \
		--allow-nonstandard-dataset-size --skip-failure-tests \
		--run-id automated-smoke $(ARGS)
	$(PYTHON) scripts/verify_smoke_result.py \
		results/sift1m-l2/diag-automated-smoke-l2

verify: test database-ready sql-test smoke

download:
	$(PYTHON) scripts/download_dataset.py --dataset $(METRIC) --data-root data

run:
	$(PYTHON) ivfflat_diagnosis/run_matrix.py --metric $(METRIC) \
		--lists $(LISTS) --probes $(PROBES) \
		--maintenance-work-mem $(MEMORY) $(ARGS)

clean-runtime:
	@echo "Runtime cleanup is intentionally explicit. Remove benchmark/data, benchmark/results,"
	@echo "benchmark/pgdata, or benchmark/.runtime only after confirming they are not in use."
