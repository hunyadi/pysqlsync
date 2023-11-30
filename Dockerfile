ARG PYTHON_VERSION=3.9
FROM python:${PYTHON_VERSION}-slim-bookworm
RUN apt-get update && apt-get install -y unixodbc-dev
COPY dist/*.whl dist/
RUN PIP_DISABLE_PIP_VERSION_CHECK=1 python3 -m pip install `ls -1 dist/pysqlsync-*.whl`[mssql,mysql,postgresql,tsv]
COPY tests/ tests/
RUN python3 -m compileall -q .
RUN TEST_SKIP_INTEGRATION=1 python3 -m unittest discover
