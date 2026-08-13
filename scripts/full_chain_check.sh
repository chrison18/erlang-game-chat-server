#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 每次从当前源码重新编译，再以非交互 VM 执行真实 TCP 回归。
"$PROJECT_DIR/scripts/compile.sh"
exec erl -noshell -pa "$PROJECT_DIR/ebin" \
    -s chat_full_chain_check run \
    -s init stop
