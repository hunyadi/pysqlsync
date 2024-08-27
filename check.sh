set -e

PYTHON=python3

# Run static type checker and verify formatting guidelines
$PYTHON -m mypy pysqlsync
$PYTHON -m flake8 pysqlsync
$PYTHON -m mypy tests
$PYTHON -m flake8 tests
