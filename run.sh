FOLDER="examples"

FILENAME="$1"

if [[ -z $FILENAME ]]; then
    echo "here"
    PYTHONPATH=src/ uv run --active src/main.py
    
else
    PYTHONPATH=src/ uv run --active src/main.py --entrypoint $FOLDER/$FILENAME.sql
fi