#!/bin/bash
# WorkBuddy Token Dashboard — 启动脚本（macOS / Linux）
#
#   ./start.sh           启动服务（已在运行则仅提示）
#   ./start.sh restart   重启服务（先停止再启动；修改后端代码后用这个重新加载）
#   ./start.sh stop      停止服务
#
cd "$(dirname "$0")"

PORT=8899
CMD="${1:-start}"

# 依次尝试：WorkBuddy 自带 Python（3.13.12）→ 任意已装版本 → 系统 python3
PY=""
for cand in \
  "$HOME/.workbuddy/binaries/python/versions/3.13.12/bin/python3" \
  "$HOME"/.workbuddy/binaries/python/versions/*/bin/python3 \
  "$(command -v python3)" \
  "$(command -v python)"
do
  if [ -n "$cand" ] && [ -x "$cand" ]; then PY="$cand"; break; fi
done

if [ -z "$PY" ]; then
  echo "未找到可用的 python3，请先安装 Python 3.11+：https://www.python.org/downloads/"
  exit 1
fi

# 版本提示（需要 3.11+）
if ! "$PY" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' 2>/dev/null; then
  echo "警告: $PY 版本低于 3.11，可能无法正常解析数据，建议安装 Python 3.11+"
fi

HAS_LSOF=0
command -v lsof >/dev/null 2>&1 && HAS_LSOF=1

# 查找看板服务进程：只认「监听端口」的服务进程（-sTCP:LISTEN），
# 避免把浏览器里打开中的页面连接（客户端 socket）误当成服务进程
service_pids() {
  if [ "$HAS_LSOF" = "1" ]; then
    lsof -ti :"$PORT" -sTCP:LISTEN 2>/dev/null
  elif command -v pgrep >/dev/null 2>&1; then
    pgrep -f "python.*server\.py" 2>/dev/null
  fi
}

is_running() {
  [ -n "$(service_pids)" ]
}

stop_service() {
  local pids
  pids=$(service_pids)
  if [ -z "$pids" ]; then
    echo "服务未在运行"
    return 0
  fi
  echo "正在停止服务 (PID: $(echo "$pids" | tr '\n' ' '))..."
  echo "$pids" | xargs kill 2>/dev/null
  i=0
  while [ "$i" -lt 5 ]; do
    if ! is_running; then break; fi
    sleep 1
    i=$((i + 1))
  done
  if is_running; then
    echo "正常停止超时，强制结束..."
    pids=$(service_pids)
    if [ -n "$pids" ]; then echo "$pids" | xargs kill -9 2>/dev/null; fi
    sleep 1
  fi
  if is_running; then
    echo "停止失败，请手动检查进程"
    return 1
  fi
  echo "服务已停止"
  return 0
}

start_service() {
  # -u：无缓冲输出，启动信息与错误实时写入 server.log
  nohup "$PY" -u server.py > server.log 2>&1 &
  if [ "$HAS_LSOF" = "1" ]; then
    i=0
    while [ "$i" -lt 24 ]; do    # 首启建索引需要几秒，最多等 12 秒
      if is_running; then echo "已启动: http://127.0.0.1:$PORT"; return 0; fi
      sleep 0.5
      i=$((i + 1))
    done
    echo "启动失败，请查看 server.log"
    return 1
  else
    sleep 3
    echo "已启动: http://127.0.0.1:$PORT"
    return 0
  fi
}

case "$CMD" in
  start)
    if is_running; then
      echo "服务已在运行: http://127.0.0.1:$PORT"
      echo "（修改后端代码后重新加载请执行: ./start.sh restart）"
    else
      start_service || exit 1
    fi
    ;;
  restart)
    if is_running; then
      echo "正在重启服务..."
      stop_service || exit 1
    else
      echo "服务未在运行，直接启动"
    fi
    start_service || exit 1
    ;;
  stop)
    stop_service
    exit $?
    ;;
  *)
    echo "用法: ./start.sh [start|restart|stop]"
    exit 1
    ;;
esac

# 打开浏览器（macOS: open / Linux: xdg-open）
if command -v open >/dev/null 2>&1; then
  open "http://127.0.0.1:$PORT"
elif command -v xdg-open >/dev/null 2>&1; then
  xdg-open "http://127.0.0.1:$PORT"
else
  echo "请手动打开浏览器访问 http://127.0.0.1:$PORT"
fi
