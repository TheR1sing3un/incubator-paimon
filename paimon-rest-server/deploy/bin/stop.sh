#!/usr/bin/env bash
# ============================================================
# Paimon REST Catalog Server - Stop Script
# ============================================================
set -e

BIN_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVER_HOME="$(cd "${BIN_DIR}/.." && pwd)"
PID_FILE="${SERVER_HOME}/paimon-rest-server.pid"

if [ ! -f "${PID_FILE}" ]; then
    echo "[WARN] PID file not found. Server may not be running."
    exit 0
fi

PID=$(cat "${PID_FILE}")

if ! kill -0 "${PID}" 2>/dev/null; then
    echo "[WARN] Process ${PID} not running. Removing stale PID file."
    rm -f "${PID_FILE}"
    exit 0
fi

echo "[INFO] Stopping Paimon REST Server (PID: ${PID}) ..."
kill "${PID}"

TIMEOUT=30
ELAPSED=0
while kill -0 "${PID}" 2>/dev/null; do
    if [ ${ELAPSED} -ge ${TIMEOUT} ]; then
        echo "[WARN] Not stopped after ${TIMEOUT}s. Sending SIGKILL ..."
        kill -9 "${PID}" 2>/dev/null || true
        break
    fi
    sleep 1
    ELAPSED=$((ELAPSED + 1))
done

rm -f "${PID_FILE}"
echo "[OK]   Server stopped."
