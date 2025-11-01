FOLDER="examples"

ARG="$1"
FILENAME=${ARG:-"basic"}

PYTHONPATH=src/ uv run --active src/main.py $FOLDER/$FILENAME.sql