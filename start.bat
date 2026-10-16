@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

rem WorkBuddy Token Dashboard — 启动脚本（Windows）
rem
rem   start.bat           启动服务（已在运行则仅提示）
rem   start.bat restart   重启服务（先停止再启动；修改后端代码后用这个重新加载）
rem   start.bat stop      停止服务
rem
rem 也可直接双击本文件启动（等同 start）。

set "PORT=8899"
set "CMD=%~1"
if "%CMD%"=="" set "CMD=start"

if /i "%CMD%"=="start"   goto cmd_start
if /i "%CMD%"=="restart" goto cmd_restart
if /i "%CMD%"=="stop"    goto cmd_stop
echo 用法: start.bat [start^|restart^|stop]
exit /b 1

:cmd_stop
call :stop_service
exit /b

:cmd_restart
netstat -ano | findstr /C:":%PORT% " | findstr /C:"LISTENING" >nul 2>nul
if errorlevel 1 (
  echo 服务未在运行，直接启动
) else (
  call :stop_service_quiet
)
goto :start_service

:cmd_start
netstat -ano | findstr /C:":%PORT% " | findstr /C:"LISTENING" >nul 2>nul
if not errorlevel 1 (
  echo 服务已在运行: http://127.0.0.1:%PORT%
  echo （修改后端代码后重新加载请执行: start.bat restart）
  start "" "http://127.0.0.1:%PORT%"
  exit /b 0
)
goto :start_service

:start_service
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
start "WorkBuddy Token Dashboard" /min cmd /c ""%PY%" -u server.py"
timeout /t 3 /nobreak >nul
echo 已启动: http://127.0.0.1:%PORT%
echo （服务在最小化的命令行窗口中运行，关闭该窗口即停止服务）
start "" "http://127.0.0.1:%PORT%"
exit /b 0

:stop_service
netstat -ano | findstr /C:":%PORT% " | findstr /C:"LISTENING" >nul 2>nul
if errorlevel 1 (
  echo 服务未在运行
  exit /b 0
)
set "SVC_PID="
for /f "tokens=5" %%p in ('netstat -ano ^| findstr /C:":%PORT% " ^| findstr /C:"LISTENING"') do set "SVC_PID=%%p"
if "%SVC_PID%"=="" (
  echo 服务未在运行
  exit /b 0
)
echo 正在停止服务 (PID: %SVC_PID%)...
taskkill /F /PID %SVC_PID% >nul 2>nul
timeout /t 1 /nobreak >nul
netstat -ano | findstr /C:":%PORT% " | findstr /C:"LISTENING" >nul 2>nul
if errorlevel 1 (
  echo 服务已停止
  exit /b 0
)
echo 停止失败，请手动检查（任务管理器中的 python 进程）
exit /b 1

:stop_service_quiet
set "SVC_PID="
for /f "tokens=5" %%p in ('netstat -ano ^| findstr /C:":%PORT% " ^| findstr /C:"LISTENING"') do set "SVC_PID=%%p"
if not "%SVC_PID%"=="" (
  echo 正在停止服务 (PID: %SVC_PID%)...
  taskkill /F /PID %SVC_PID% >nul 2>nul
  timeout /t 1 /nobreak >nul
)
exit /b 0
