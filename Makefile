# Python development
format:
	uv run --active ruff check --fix

sync:
	uv sync --active

run:
	PYTHONPATH=src/ uv run --active src/main.py