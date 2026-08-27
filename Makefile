.PHONY: test lint typecheck run-chaos report clean clean-all docker-up docker-down evidence

test:
	pytest -q

lint:
	ruff check src tests scripts

typecheck:
	mypy src

run-chaos:
	python scripts/run_chaos.py --config configs/default.yaml --out reports/metrics.json

report:
	python scripts/generate_report.py --metrics reports/metrics.json --out reports/final_report.md

evidence:
	python scripts/redis_evidence.py

docker-up:
	docker compose up -d

docker-down:
	docker compose down

clean:
	# Deliberately does NOT delete reports/ - metrics.json and final_report.md
	# are graded deliverables.  Use `make clean-all` to wipe them too.
	rm -rf .pytest_cache .ruff_cache .mypy_cache

clean-all: clean
	rm -rf reports/metrics*.json reports/metrics*.csv reports/final_report.md
