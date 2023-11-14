set -e

# Run static type checker and verify formatting guidelines
python3 -m mypy pysqlsync
python3 -m flake8 pysqlsync
python3 -m mypy tests
python3 -m flake8 tests
