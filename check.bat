@echo off
setlocal

set python=python.exe

rem Run static type checker and verify formatting guidelines
%python% -m ruff check
if errorlevel 1 goto error
%python% -m ruff format --check
if errorlevel 1 goto error

%python% -m mypy pysqlsync
if errorlevel 1 goto error
%python% -m mypy tests
if errorlevel 1 goto error

goto :EOF

:error
exit /b 1
