#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EBIN_DIR="$PROJECT_DIR/ebin"

# 项目不依赖 rebar3，直接编译服务端、客户端和全链路检查模块。
mkdir -p "$EBIN_DIR"
rm -f "$EBIN_DIR"/*.beam

shopt -s nullglob
SOURCE_FILES=(
    "$PROJECT_DIR"/src/server/*.erl
    "$PROJECT_DIR"/src/client/*.erl
    "$PROJECT_DIR"/test/*.erl
)

erlc -Wall \
    -I "$PROJECT_DIR/include" \
    -o "$EBIN_DIR" \
    "${SOURCE_FILES[@]}"

cp "$PROJECT_DIR/src/server/chat.app.src" "$EBIN_DIR/chat.app"

echo "Compiled chat to $EBIN_DIR"
