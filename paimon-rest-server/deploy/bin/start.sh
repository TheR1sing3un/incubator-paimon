#!/usr/bin/env bash
# ============================================================
# Paimon REST Catalog Server - Startup Script
# ============================================================
set -e

# ---- Resolve directories ----
BIN_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVER_HOME="$(cd "${BIN_DIR}/.." && pwd)"
CONF_DIR="${SERVER_HOME}/conf"
LIB_DIR="${SERVER_HOME}/lib"
LOG_DIR="${SERVER_HOME}/logs"
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
if [ -f "${CONFIG_FILE}" ]; then
    CONFIG_ARGS="--config ${CONFIG_FILE}"
else
    echo "[WARN]  ${CONFIG_FILE} not found, using defaults."
    CONFIG_ARGS=""
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
JAVA_OPTS="${JAVA_OPTS:--Xms512m -Xmx2g}"
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

echo "============================================================"
echo " Paimon REST Catalog Server"
echo "============================================================"
echo " SERVER_HOME : ${SERVER_HOME}"
echo " CONF_DIR    : ${CONF_DIR}"
echo " LOG_DIR     : ${LOG_DIR}"
echo " JAVA_OPTS   : ${JAVA_OPTS}"
echo " DAEMON      : ${DAEMON_MODE}"
echo "============================================================"

if [ "${DAEMON_MODE}" = "true" ]; then
    nohup java \
        ${JAVA_OPTS} \
        ${GC_OPTS} \
        ${JMX_OPTS} \
        -Dlog.dir="${LOG_DIR}" \
        -cp "${CLASSPATH}" \
        ${MAIN_CLASS} \
        ${CONFIG_ARGS} \
        "$@" \
        >> "${LOG_DIR}/stdout.log" 2>&1 &

    PID=$!
    echo "${PID}" > "${PID_FILE}"
    echo "[OK] Started (PID: ${PID})"
    echo "     Logs: ${LOG_DIR}/paimon-rest-server.log"
    echo "     Stop: bin/stop.sh"

    sleep 2
    if ! kill -0 "${PID}" 2>/dev/null; then
        echo "[ERROR] Process exited. Check ${LOG_DIR}/stdout.log"
        rm -f "${PID_FILE}"
        exit 1
    fi
else
    exec java \
        ${JAVA_OPTS} \
        ${GC_OPTS} \
        ${JMX_OPTS} \
        -Dlog.dir="${LOG_DIR}" \
        -cp "${CLASSPATH}" \
        ${MAIN_CLASS} \
        ${CONFIG_ARGS} \
        "$@"
fi
