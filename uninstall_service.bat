@echo off
:: ============================================================================
:: Delta Exchange India Breakout Alerts System - Windows Service Uninstaller
:: ============================================================================

title Uninstalling Delta Crypto Alerts Windows Service...
echo ============================================================================
echo  Delta Exchange India Breakout Alert System - Windows Service Uninstaller
echo ============================================================================
echo.

net session >nul 2>&1
if %errorLevel% neq 0 (
    echo [ERROR] This script must be run as Administrator!
    pause
    exit /b 1
)

set SERVICE_NAME=DeltaCryptoAlerts
set APP_DIR=%~dp0

set PYTHON_EXE=
for /f "tokens=*" %%i in ('py -c "import sys; print(sys.executable)" 2^>nul') do set "PYTHON_EXE=%%i"

if "%PYTHON_EXE%"=="" (
    for /f "tokens=*" %%i in ('where python 2^>nul') do set "PYTHON_EXE=%%i"
)

echo Stopping Service '%SERVICE_NAME%'...
sc stop %SERVICE_NAME% >nul 2>&1
if exist "%APP_DIR%nssm.exe" (
    "%APP_DIR%nssm.exe" stop %SERVICE_NAME% >nul 2>&1
    "%APP_DIR%nssm.exe" remove %SERVICE_NAME% confirm >nul 2>&1
) else (
    cd /d "%APP_DIR%"
    if not "%PYTHON_EXE%"=="" (
        "%PYTHON_EXE%" service_wrapper.py remove >nul 2>&1
    )
    sc delete %SERVICE_NAME% >nul 2>&1
)

echo Service '%SERVICE_NAME%' uninstalled cleanly.
echo.
pause
