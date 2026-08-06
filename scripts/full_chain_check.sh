#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

"$PROJECT_DIR/scripts/compile.sh"
exec erl -noshell -pa "$PROJECT_DIR/ebin" \
    -s chat_full_chain_check run \
    -s init stop
