#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

ulimit -c unlimited
ulimit -SHn 65535

exec erl -pa "$PROJECT_DIR/ebin" \
    -eval 'ok = application:start(chat).'
