#!/bin/bash
# End-to-end test of the alluxio:// plugin against a local moto + fake gateway.
#
# Run INSIDE the devcontainer (host Python won't load manylinux .so):
#   devcontainer exec --workspace-folder third_party/runai-model-streamer \
#       tests/local_plugin/run_test.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
GATEWAY_PORT=29998
WORKER_PORT=29999

cleanup() {
    set +e
    [[ -n "${MOTO_PID:-}" ]] && kill "$MOTO_PID" 2>/dev/null
    [[ -n "${GW_PID:-}" ]]   && kill "$GW_PID"   2>/dev/null
    wait 2>/dev/null
}
trap cleanup EXIT

echo "== start moto on :$WORKER_PORT =="
moto_server -p "$WORKER_PORT" -H 0.0.0.0 >/tmp/moto.log 2>&1 &
MOTO_PID=$!

echo "== start fake gateway on :$GATEWAY_PORT =="
python3 "$HERE/fake_gateway.py" \
    --port "$GATEWAY_PORT" \
    --worker "http://localhost:$WORKER_PORT" \
    >/tmp/gateway.log 2>&1 &
GW_PID=$!

# Wait for both to be reachable
for i in 1 2 3 4 5 6 7 8 9 10; do
    curl -sf "http://localhost:$WORKER_PORT/" >/dev/null 2>&1 && break
    sleep 0.3
done
for i in 1 2 3 4 5 6 7 8 9 10; do
    curl -sf -o /dev/null -w "%{http_code}\n" \
        --max-time 1 "http://localhost:$GATEWAY_PORT/" 2>/dev/null | grep -q 307 && break
    sleep 0.3
done

echo "== run plugin test =="
python3 "$HERE/run_test.py"

echo
echo "== gateway log (last 5 lines) =="
tail -5 /tmp/gateway.log
