#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 服务端同样为大量连接预留文件描述符，并保留 core dump 便于诊断。
ulimit -c unlimited
ulimit -SHn 65535

exec erl -pa "$PROJECT_DIR/ebin" \
    -eval 'ok = application:start(chat).'
