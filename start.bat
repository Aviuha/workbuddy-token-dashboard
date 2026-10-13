@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

set "PORT=8899"

rem ==== 依次尝试：WorkBuddy 自带 Python → PATH 中的 python / py ====
set "PY="
for /d %%i in ("%USERPROFILE%\.workbuddy\binaries\python\versions\*") do (
  if exist "%%i\python.exe" set "PY=%%i\python.exe"
)
if "%PY%"=="" (
  where python >nul 2>nul && set "PY=python"
)
if "%PY%"=="" (
  where py >nul 2>nul && set "PY=py"
)
if "%PY%"=="" (
  echo 未找到 Python，请先安装 Python 3.11+ 并勾选 "Add to PATH"：https://www.python.org/downloads/
  pause
  exit /b 1
)

rem ==== 检查服务是否已在运行 ====
netstat -ano | findstr ":%PORT% " | findstr "LISTENING" >nul 2>nul
if not errorlevel 1 (
  echo 服务已在运行: http://127.0.0.1:%PORT%
) else (
  start "WorkBuddy Token Dashboard" /min cmd /c ""%PY%" server.py"
  timeout /t 3 /nobreak >nul
  echo 已启动: http://127.0.0.1:%PORT%
  echo （服务在最小化的命令行窗口中运行，关闭该窗口即停止服务）
)

start "" "http://127.0.0.1:%PORT%"
endlocal
