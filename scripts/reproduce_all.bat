@echo off
rem EXPOSE-Toolkit: single-command reproduction (Windows).
rem
rem Reproduces every quantitative claim in
rem   "Call Me Maybe? Exposing Patterns of Shadow Scam Ecosystems
rem    via Open-Source Victim Complaints"
rem from the packaged 800notes corpus.
rem
rem Usage:
rem   scripts\reproduce_all.bat
rem   scripts\reproduce_all.bat --refresh-carrier
rem   scripts\reproduce_all.bat --download-ftc
rem
rem Wall time on a 2023 laptop: ~8 minutes for the local stages.

setlocal
set "HERE=%~dp0.."
pushd "%HERE%"

where python >nul 2>nul
if errorlevel 1 (
    echo ERROR: python not found in PATH
    popd
    exit /b 1
)

echo [reproduce_all] working directory : %HERE%
for /f "delims=" %%v in ('python --version 2^>^&1') do echo [reproduce_all] python            : %%v
echo [reproduce_all] running EXPOSE-Toolkit pipeline...
python run_pipeline.py %*

echo.
echo [reproduce_all] Done.
echo [reproduce_all] Headline reports under output\:
dir /b output\*report*.txt 2>nul

popd
endlocal
