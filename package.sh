set -e

# Verify if the package builds successfully for all Python target versions
if [ -d dist ]; then rm -rf dist; fi
if [ -d pysqlsync.egg-info ]; then rm -rf pysqlsync.egg-info; fi
./check.sh
python3 -m build
docker build --build-arg PYTHON_VERSION=3.9 .
docker build --build-arg PYTHON_VERSION=3.10 .
docker build --build-arg PYTHON_VERSION=3.11 .
docker build --build-arg PYTHON_VERSION=3.12 .
