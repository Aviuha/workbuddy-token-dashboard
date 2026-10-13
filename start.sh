#!/bin/bash
# WorkBuddy Token Dashboard — 启动脚本（macOS / Linux）
cd "$(dirname "$0")"

PORT=8899

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

RUNNING=0
if command -v lsof >/dev/null 2>&1; then
  lsof -i :$PORT >/dev/null 2>&1 && RUNNING=1
fi

if [ "$RUNNING" = "1" ]; then
  echo "服务已在运行: http://127.0.0.1:$PORT"
else
  nohup "$PY" server.py > server.log 2>&1 &
  sleep 2
  OK=0
  if command -v lsof >/dev/null 2>&1; then
    lsof -i :$PORT >/dev/null 2>&1 && OK=1
  else
    OK=1
  fi
  if [ "$OK" = "1" ]; then
    echo "已启动: http://127.0.0.1:$PORT"
  else
    echo "启动失败，请查看 server.log"
    exit 1
  fi
fi

# 打开浏览器（macOS: open / Linux: xdg-open）
if command -v open >/dev/null 2>&1; then
  open "http://127.0.0.1:$PORT"
elif command -v xdg-open >/dev/null 2>&1; then
  xdg-open "http://127.0.0.1:$PORT"
else
  echo "请手动打开浏览器访问 http://127.0.0.1:$PORT"
fi
