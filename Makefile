# Python development
check:
	uv run --active ruff check --fix src/

format:
	uv run --active ruff format src/
	uv run --active ruff format tests/

sync:
	uv sync --active

# Run from shell as
# make run
# make run f=websocket
run:
	sh run.sh $(f)

dev-entrypoint:
	PYTHONPATH=src/ uv run --active src/entrypoint.py

client:
	PYTHONPATH=src/ uv run --active src/client.py 1

test:
	PYTHONPATH=src/ uv run --active pytest tests/ -vv

ruff:
	make check && make format
