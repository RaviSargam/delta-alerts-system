@echo off
:: ============================================================================
:: Delta Exchange India Breakout Alerts System - Windows Service Installer
:: Registers application as an unattended, continuous Windows Background Service
:: ============================================================================

title Installing Delta Crypto Alerts Windows Service...
echo ============================================================================
echo  Delta Exchange India Breakout Alert System - Windows Service Setup
echo ============================================================================
echo.

:: 1. Check Administrator Privileges
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo [ERROR] This installer must be run as Administrator!
    echo Please right-click install_service.bat and select "Run as administrator".
    echo.
    pause
    exit /b 1
)

set SERVICE_NAME=DeltaCryptoAlerts
set DISPLAY_NAME=Delta Exchange Crypto Breakout Alerts System
set APP_DIR=%~dp0
if "%APP_DIR:~-1%"=="\" set "APP_DIR=%APP_DIR:~0,-1%"
set LOG_DIR=C:\ProgramData\DeltaAlerts

:: Dynamically detect Python executable path
set PYTHON_EXE=
for /f "tokens=*" %%i in ('py -c "import sys; print(sys.executable)" 2^>nul') do set "PYTHON_EXE=%%i"

if "%PYTHON_EXE%"=="" (
    for /f "tokens=*" %%i in ('where python 2^>nul') do set "PYTHON_EXE=%%i"
)

if "%PYTHON_EXE%"=="" (
    echo [ERROR] Python executable could not be detected.
    echo Please make sure Python or the 'py' launcher is installed.
    pause
    exit /b 1
)

echo Detected Python executable: %PYTHON_EXE%
echo.

:: 2. Create Log Directory
echo [1/4] Preparing log directory at %LOG_DIR%...
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

:: 3. Install Required Dependencies
echo [2/4] Installing Python requirements...
"%PYTHON_EXE%" -m pip install --upgrade pip >nul 2>&1
"%PYTHON_EXE%" -m pip install -r "%APP_DIR%\requirements.txt"
if %errorLevel% neq 0 (
    echo [WARNING] Dependency installation returned warnings. Proceeding...
)

:: 4. Register Windows Service via NSSM
echo [3/4] Registering Windows Service '%SERVICE_NAME%'...

if exist "%APP_DIR%\nssm.exe" (
    echo Using NSSM service manager...
    "%APP_DIR%\nssm.exe" stop %SERVICE_NAME% >nul 2>&1
    "%APP_DIR%\nssm.exe" remove %SERVICE_NAME% confirm >nul 2>&1

    "%APP_DIR%\nssm.exe" install %SERVICE_NAME% "%PYTHON_EXE%"
    "%APP_DIR%\nssm.exe" set %SERVICE_NAME% AppParameters "-u main.py"
    "%APP_DIR%\nssm.exe" set %SERVICE_NAME% DisplayName "%DISPLAY_NAME%"
    "%APP_DIR%\nssm.exe" set %SERVICE_NAME% Description "Continuous background monitoring of crypto breakouts on Delta Exchange India with Telegram alerts."
    "%APP_DIR%\nssm.exe" set %SERVICE_NAME% Start SERVICE_AUTO_START
    "%APP_DIR%\nssm.exe" set %SERVICE_NAME% AppDirectory "%APP_DIR%"
    "%APP_DIR%\nssm.exe" set %SERVICE_NAME% AppStdout "%LOG_DIR%\service_stdout.log"
    "%APP_DIR%\nssm.exe" set %SERVICE_NAME% AppStderr "%LOG_DIR%\service_stderr.log"
    "%APP_DIR%\nssm.exe" set %SERVICE_NAME% AppRotateFiles 1
    "%APP_DIR%\nssm.exe" set %SERVICE_NAME% AppRotateOnline 1
    "%APP_DIR%\nssm.exe" set %SERVICE_NAME% AppRotateSeconds 86400
    "%APP_DIR%\nssm.exe" set %SERVICE_NAME% AppRotateBytes 10485760
) else (
    echo Registering via PyWin32 Service Framework...
    cd /d "%APP_DIR%"
    "%PYTHON_EXE%" service_wrapper.py --startup auto install
    sc config %SERVICE_NAME% start= auto >nul 2>&1
)

:: 5. Start Service
echo [4/4] Starting Service '%SERVICE_NAME%'...
sc start %SERVICE_NAME% >nul 2>&1
if %errorLevel% neq 0 (
    if exist "%APP_DIR%\nssm.exe" (
        "%APP_DIR%\nssm.exe" start %SERVICE_NAME%
    )
)

echo.
echo ============================================================================
echo  Service Installation Completed Successfully!
echo ============================================================================
echo Service Name: %SERVICE_NAME%
echo Startup Type: Automatic (Unattended background boot)
echo Log Location: %LOG_DIR%\app.log
echo.
sc query %SERVICE_NAME%
echo.
pause
