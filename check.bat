@echo off
setlocal

set python=python.exe

rem Run static type checker and verify formatting guidelines
rem %python% -m ruff check
if errorlevel 1 goto error
rem %python% -m ruff format --check
if errorlevel 1 goto error

%python% -m mypy pysqlsync
if errorlevel 1 goto error
%python% -m flake8 pysqlsync
if errorlevel 1 goto error

%python% -m mypy tests
if errorlevel 1 goto error
%python% -m flake8 tests
if errorlevel 1 goto error

goto :EOF

:error
exit /b 1
