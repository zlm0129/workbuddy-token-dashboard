@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ============================================
echo  WorkBuddy Token Monitor
echo ============================================
where python >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Python not found. Install Python 3.10+ and add it to PATH.
  pause
  exit /b 1
)
start "" http://127.0.0.1:8788
python -u wb_usage_server.py --port 8788
pause
