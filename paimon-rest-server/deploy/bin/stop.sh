#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
