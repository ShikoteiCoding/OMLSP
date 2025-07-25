# Python development
check:
	uv run --active ruff check --fix src/

format:
	uv run --active ruff format src/

sync:
	uv sync --active

run:
	PYTHONPATH=src/ uv run --active src/main.py examples/basic.sql

test:
	PYTHONPATH=src/ uv run --active pytest tests/ -v