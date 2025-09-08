# Python development
check:
	uv run --active ruff check --fix src/

format:
	uv run --active ruff format src/
	uv run --active ruff format tests/

sync:
	uv sync --active

run:
	PYTHONPATH=src/ uv run --active src/main.py examples/basic.sql

test:
	PYTHONPATH=src/ uv run --active pytest tests/ -v

dev-entrypoint:
	PYTHONPATH=src/ uv run --active src/entrypoint.py

client:
	PYTHONPATH=src/ uv run --active src/client.py 1

test:
	PYTHONPATH=src/ uv run pytest tests/ -vv