#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${CAPACITY_PORT:-5557}"
BATCH_SIZE="${CAPACITY_BATCH_SIZE:-100}"
BATCH_PAUSE_SEC="${CAPACITY_BATCH_PAUSE_SEC:-0.5}"
RPC_TIMEOUT_SEC="${CAPACITY_RPC_TIMEOUT_SEC:-120}"
SAMPLE_SEC=5
HOLD_SEC="${CAPACITY_HOLD_SEC:-120}"
TARGET="${CAPACITY_TARGET:-10000}"
LOG_FILE="${CAPACITY_LOG_FILE:-}"
NODE_SUFFIX="${PORT}_$$"
HOST="$(hostname -s)"
COOKIE="capacity_${NODE_SUFFIX}"
SERVER_NODE="capserver_${NODE_SUFFIX}@${HOST}"
CLIENT_A_NODE="capclienta_${NODE_SUFFIX}@${HOST}"
CLIENT_B_NODE="capclientb_${NODE_SUFFIX}@${HOST}"
SERVER_PID=""
CLIENT_A_PID=""
CLIENT_B_PID=""

if [ -n "$LOG_FILE" ]; then
    exec > >(tee "$LOG_FILE") 2>&1
fi

cleanup() {
    trap - EXIT INT TERM
    [ -n "$CLIENT_A_PID" ] && kill "$CLIENT_A_PID" 2>/dev/null || true
    [ -n "$CLIENT_B_PID" ] && kill "$CLIENT_B_PID" 2>/dev/null || true
    [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

rpc() {
    local node="$1" module="$2" function="$3" args="$4"
    erl_call -r -n "$node" -c "$COOKIE" -timeout "$RPC_TIMEOUT_SEC" \
        -a "$module $function [$args]"
}

wait_for_rpc() {
    local node="$1" attempt
    for attempt in $(seq 1 150); do
        if rpc "$node" supervisor count_children chat_client_sup >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.2
    done
    return 1
}

wait_for_online_count() {
    local target="$1" attempt count
    for attempt in $(seq 1 600); do
        count="$(rpc "$SERVER_NODE" ets info 'online_roles, size')"
        if [ "$count" -eq "$target" ]; then
            printf 'online_gate target=%s reached=%s\n' "$target" "$count"
            return 0
        fi
        sleep 0.2
    done
    echo "online count did not reach $target (last=$count)" >&2
    print_metrics gate_failed
    return 1
}

print_metrics() {
    local label="$1"
    printf 'metrics_at=%s label=%s\n' "$(date -Is)" "$label"
    printf 'server_metrics=%s\n' \
        "$(rpc "$SERVER_NODE" chat_metrics snapshot '')"
    printf 'client_a_metrics=%s\n' \
        "$(rpc "$CLIENT_A_NODE" chat_load_test metrics '')"
    printf 'client_b_metrics=%s\n' \
        "$(rpc "$CLIENT_B_NODE" chat_load_test metrics '')"
}

observe_stage() {
    local target="$1" duration="$2" elapsed=0 online server_metrics client_a_metrics client_b_metrics
    while [ "$elapsed" -le "$duration" ]; do
        online="$(rpc "$SERVER_NODE" ets info 'online_roles, size')"
        server_metrics="$(rpc "$SERVER_NODE" chat_metrics snapshot '')"
        client_a_metrics="$(rpc "$CLIENT_A_NODE" chat_load_test metrics '')"
        client_b_metrics="$(rpc "$CLIENT_B_NODE" chat_load_test metrics '')"
        printf 'sample_at=%s stage=%s elapsed=%s online=%s\n' \
            "$(date -Is)" "$target" "$elapsed" "$online"
        printf 'server_metrics=%s\nclient_a_metrics=%s\nclient_b_metrics=%s\n' \
            "$server_metrics" "$client_a_metrics" "$client_b_metrics"
        if [ "$online" -ne "$target" ]; then
            echo "online count changed during stage $target" >&2
            return 1
        fi
        [ "$elapsed" -eq "$duration" ] && break
        sleep "$SAMPLE_SEC"
        elapsed=$((elapsed + SAMPLE_SEC))
    done
}

wait_for_listener() {
    local attempt
    for attempt in $(seq 1 150); do
        if timeout 1 bash -c "exec 3<>/dev/tcp/127.0.0.1/$PORT; exec 3>&-" 2>/dev/null; then
            return 0
        fi
        sleep 0.2
    done
    return 1
}

start_range() {
    local node="$1" start="$2" end="$3" expected reply
    [ "$start" -le "$end" ] || return 0
    expected=$((end - start + 1))
    reply="$(rpc "$node" chat_load_test start "$start, $end" | tr -d '[:space:]')"
    if [ "$reply" != "{ok,$expected}" ]; then
        echo "client range $start..$end failed: $reply" >&2
        return 1
    fi
}

start_stage() {
    local start_a="$1" end_a="$2" start_b="$3" end_b="$4"
    local a b
    a="$start_a"
    b="$start_b"
    while [ "$a" -le "$end_a" ] || [ "$b" -le "$end_b" ]; do
        local a_end=$((a + BATCH_SIZE - 1))
        local b_end=$((b + BATCH_SIZE - 1))
        [ "$a_end" -gt "$end_a" ] && a_end="$end_a"
        [ "$b_end" -gt "$end_b" ] && b_end="$end_b"
        start_range "$CLIENT_A_NODE" "$a" "$a_end" &
        local a_pid=$!
        start_range "$CLIENT_B_NODE" "$b" "$b_end" &
        local b_pid=$!
        wait "$a_pid"
        wait "$b_pid"
        printf 'ramp client_a=%s client_b=%s online=%s\n' \
            "$a_end" "$b_end" \
            "$(rpc "$SERVER_NODE" ets info 'online_roles, size')"
        if [ $((a_end % 1000)) -eq 0 ]; then
            print_metrics "ramp_$((a_end + b_end - split))"
        fi
        a=$((a_end + 1))
        b=$((b_end + 1))
        sleep "$BATCH_PAUSE_SEC"
    done
}

"$PROJECT_DIR/scripts/compile.sh"
ulimit -SHn 65535

erl -noshell -sname "capserver_${NODE_SUFFIX}" -setcookie "$COOKIE" \
    -pa "$PROJECT_DIR/ebin" \
    -eval "ok = application:load(chat), application:set_env(chat, port, $PORT), ok = application:start(chat), receive after infinity -> ok end." &
SERVER_PID=$!

if ! wait_for_listener || ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "listener did not become ready on 127.0.0.1:$PORT" >&2
    exit 1
fi

erl -noshell -sname "capclienta_${NODE_SUFFIX}" -setcookie "$COOKIE" \
    -pa "$PROJECT_DIR/ebin" \
    -eval "application:set_env(chat, client_host, \"127.0.0.1\"), application:set_env(chat, port, $PORT), {ok, _} = chat_client_sup:start_link(), receive after infinity -> ok end." &
CLIENT_A_PID=$!
erl -noshell -sname "capclientb_${NODE_SUFFIX}" -setcookie "$COOKIE" \
    -pa "$PROJECT_DIR/ebin" \
    -eval "application:set_env(chat, client_host, \"127.0.0.1\"), application:set_env(chat, port, $PORT), {ok, _} = chat_client_sup:start_link(), receive after infinity -> ok end." &
CLIENT_B_PID=$!

sleep 1
if ! wait_for_rpc "$CLIENT_A_NODE" || ! wait_for_rpc "$CLIENT_B_NODE"; then
    echo "client BEAM did not become ready" >&2
    exit 1
fi

split=$((TARGET / 2))
start_stage 1 "$split" "$((split + 1))" "$TARGET"
wait_for_online_count "$TARGET"
printf 'stage=%s complete\n' "$TARGET"
observe_stage "$TARGET" "$HOLD_SEC"
printf 'stage=%s passed hold_sec=%s\n' "$TARGET" "$HOLD_SEC"
