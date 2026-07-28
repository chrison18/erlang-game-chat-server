#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

exec erl -pa "$PROJECT_DIR/ebin" \
    -eval 'ok = application:start(chat).'
