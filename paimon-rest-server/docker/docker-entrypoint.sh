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

set -e

# ============================================================
# Custom entrypoint: starts MySQL first, then Paimon REST Server
# ============================================================

SERVER_HOME="${SERVER_HOME:-/opt/paimon-rest-server}"
LOG_DIR="${SERVER_HOME}/logs"

echo "============================================================"
echo " Paimon REST Server Docker Container"
echo "============================================================"

# --- 1. Start MySQL in background using the official entrypoint ---
echo "[1/3] Starting MySQL..."
mysql-entrypoint.sh mysqld &
MYSQL_PID=$!

# --- 2. Wait for MySQL to be ready ---
echo "[2/3] Waiting for MySQL to be ready..."
for i in $(seq 1 60); do
    if mysqladmin ping -h 127.0.0.1 -u root -p"${MYSQL_ROOT_PASSWORD}" --silent 2>/dev/null; then
        echo "       MySQL is ready."
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "[ERROR] MySQL did not become ready in 60 seconds."
        exit 1
    fi
    sleep 1
done

# --- 3. Start Paimon REST Server in foreground ---
echo "[3/3] Starting Paimon REST Server..."
export DAEMON_MODE=false
exec java \
    ${JAVA_OPTS:--Xms512m -Xmx2g} \
    ${GC_OPTS:--XX:+UseParallelGC} \
    -Dlog.dir="${LOG_DIR}" \
    -cp "${SERVER_HOME}/conf:${SERVER_HOME}/lib/*" \
    org.apache.paimon.rest.server.RESTCatalogServer \
    --config "${SERVER_HOME}/conf/server.properties" \
    "$@"
