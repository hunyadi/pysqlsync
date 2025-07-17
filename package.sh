set -e

PYTHON_EXECUTABLE=${PYTHON:-python3}

if [ -d dist ]; then rm -rf dist; fi
for dir in *.egg-info; do
    if [ -d "$dir" ]; then rm -rf "$dir"; fi
done

./check.sh
$PYTHON_EXECUTABLE -m build --sdist --wheel

# Verify if the package builds successfully for all Python target versions
docker build --build-arg PYTHON_VERSION=3.9 .
docker build --build-arg PYTHON_VERSION=3.10 .
docker build --build-arg PYTHON_VERSION=3.11 .
docker build --build-arg PYTHON_VERSION=3.12 .
docker build --build-arg PYTHON_VERSION=3.13 .
