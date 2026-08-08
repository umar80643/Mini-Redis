install:
	python -m pip install -e ".[dev]"
run:
	python -m miniredis
test:
	pytest -q
lint:
	ruff check .
format:
	black .
format-check:
	black --check .
typecheck:
	mypy miniredis
check: lint format-check typecheck test
benchmark:
	python -m miniredis & \
	SERVER_PID=$$!; sleep 1; \
	cd benchmarks && python suite.py --requests 3000 --concurrency 1 10 50; \
	kill $$SERVER_PID
docker-build:
	docker build -t miniredis .
docker-up:
	docker compose up --build
docker-down:
	docker compose down
clean:
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	rm -rf .pytest_cache .mypy_cache .ruff_cache
