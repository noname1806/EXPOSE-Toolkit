@echo off
REM One-command reproduction entry point for Windows.
REM Usage:
REM   reproduce_all.bat
REM   reproduce_all.bat C:\path\to\corpus.jsonl
REM   reproduce_all.bat C:\path\to\corpus.jsonl C:\path\to\output

setlocal
set "HERE=%~dp0"
cd /d "%HERE%"

if "%~1"=="" (set "INPUT=%HERE%results.jsonl") else (set "INPUT=%~1")
if "%~2"=="" (set "OUTPUT=%HERE%output") else (set "OUTPUT=%~2")

echo SIG-Toolkit reproduction
echo   input : %INPUT%
echo   output: %OUTPUT%

python run_pipeline.py --input "%INPUT%" --output "%OUTPUT%"
endlocal
