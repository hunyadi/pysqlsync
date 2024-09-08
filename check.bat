@echo off
setlocal

set python=python.exe

%python% -m mypy pysqlsync || exit /b
%python% -m flake8 pysqlsync || exit /b
%python% -m mypy tests || exit /b
%python% -m flake8 tests || exit /b

:quit
