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
# Paimon REST Catalog Server - Startup Script
# ============================================================

# Re-exec with bash if invoked via sh/dash
if [ -z "${BASH_VERSION}" ]; then
    exec bash "$0" "$@"
fi

set -e

# ---- Resolve directories ----
# Use BASH_SOURCE to correctly resolve path even through symlinks or full-path invocation
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do
    DIR="$(cd -P "$(dirname "$SOURCE")" && pwd -P)"
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
done
BIN_DIR="$(cd -P "$(dirname "$SOURCE")" && pwd -P)"
SERVER_HOME="$(cd -P "${BIN_DIR}/.." && pwd -P)"
CONF_DIR="${SERVER_HOME}/conf"
LIB_DIR="${SERVER_HOME}/lib"
LOG_DIR="/home/web_server/kuaishou-runner/logs/$MY_POD_NAME"
PID_FILE="${SERVER_HOME}/paimon-rest-server.pid"

# ---- Check if already running ----
if [ -f "${PID_FILE}" ]; then
    OLD_PID=$(cat "${PID_FILE}")
    if kill -0 "${OLD_PID}" 2>/dev/null; then
        echo "[ERROR] Paimon REST Server is already running (PID: ${OLD_PID})"
        echo "        Run bin/stop.sh first."
        exit 1
    else
        rm -f "${PID_FILE}"
    fi
fi

# ---- Validate ----
if [ ! -d "${LIB_DIR}" ] || [ -z "$(ls -A "${LIB_DIR}"/*.jar 2>/dev/null)" ]; then
    echo "[ERROR] No JAR files found in ${LIB_DIR}"
    exit 1
fi

mkdir -p "${LOG_DIR}"

# ---- Config file ----
CONFIG_FILE="${CONF_DIR}/server.properties"
CONFIG_ARGS=()
if [ -f "${CONFIG_FILE}" ]; then
    CONFIG_ARGS+=(--config "${CONFIG_FILE}")
else
    echo "[WARN]  ${CONFIG_FILE} not found, using defaults."
fi

# ---- Build classpath ----
# conf/ first (log4j2.xml), then all jars in lib/
CLASSPATH="${CONF_DIR}:${LIB_DIR}/*"

# Hadoop config
if [ -n "${HADOOP_CONF_DIR}" ] && [ -d "${HADOOP_CONF_DIR}" ]; then
    CLASSPATH="${CLASSPATH}:${HADOOP_CONF_DIR}"
fi

# User-provided extra classpath
if [ -n "${EXTRA_CLASSPATH}" ]; then
    CLASSPATH="${CLASSPATH}:${EXTRA_CLASSPATH}"
fi

# ---- JVM Options ----
JAVA_OPTS="${JAVA_OPTS:--Xms512m -Xmx56g}"
GC_OPTS="${GC_OPTS:--XX:+UseParallelGC}"
JMX_OPTS=""
if [ -n "${JMX_PORT}" ]; then
    JMX_OPTS="-Dcom.sun.management.jmxremote \
              -Dcom.sun.management.jmxremote.port=${JMX_PORT} \
              -Dcom.sun.management.jmxremote.authenticate=false \
              -Dcom.sun.management.jmxremote.ssl=false"
fi

MAIN_CLASS="org.apache.paimon.rest.server.RESTCatalogServer"
DAEMON_MODE="${DAEMON_MODE:-true}"

SERVER_COMMAND="java ${JAVA_OPTS} ${GC_OPTS} ${JMX_OPTS} -Dlog.dir=\"${LOG_DIR}\" -cp \"${CLASSPATH}\" ${MAIN_CLASS}"
for arg in "${CONFIG_ARGS[@]}"; do
    SERVER_COMMAND+=" $(printf '%q' "$arg")"
done
for arg in "$@"; do
    SERVER_COMMAND+=" $(printf '%q' "$arg")"
done

export SUPERVISOR_PROGRAM0="paimon-rest-server"
export SUPERVISOR_COMMAND0="${SERVER_COMMAND}"

echo "============================================================"
echo " Paimon REST Catalog Server"
echo "============================================================"
echo " SERVER_HOME         : ${SERVER_HOME}"
echo " CONF_DIR            : ${CONF_DIR}"
echo " LOG_DIR             : ${LOG_DIR}"
echo " JAVA_OPTS           : ${JAVA_OPTS}"
echo " DAEMON              : ${DAEMON_MODE}"
echo " SUPERVISOR_PROGRAM0 : ${SUPERVISOR_PROGRAM0}"
echo " SUPERVISOR_COMMAND0 : ${SUPERVISOR_COMMAND0}"
echo "============================================================"

exec kcsize supervisor