#!/usr/bin/env bash
# ============================================================
# Build and run Paimon REST Server Docker image
#
# Usage:
#   ./build-docker.sh          # build only
#   ./build-docker.sh run      # build and run
#   ./build-docker.sh run -d   # build and run detached
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DOCKER_DIR="${SCRIPT_DIR}"
IMAGE_NAME="paimon-rest-server"
IMAGE_TAG="latest"

echo "============================================================"
echo " Building Paimon REST Server Docker Image"
echo "============================================================"

# --- 1. Build the Maven project ---
echo "[1/3] Building Maven project..."
cd "${PROJECT_DIR}/.."
mvn -pl paimon-rest-server -am -DskipTests package -Dcheckstyle.skip -Dspotless.check.skip -Denforcer.skip

# --- 2. Prepare Docker build context ---
echo "[2/3] Preparing Docker build context..."
BUILD_CONTEXT="${DOCKER_DIR}/build-context"
rm -rf "${BUILD_CONTEXT}"
mkdir -p "${BUILD_CONTEXT}/lib" \
         "${BUILD_CONTEXT}/conf" \
         "${BUILD_CONTEXT}/bin" \
         "${BUILD_CONTEXT}/sql"

# Copy jars
cp "${PROJECT_DIR}/target/"paimon-rest-server-*.jar "${BUILD_CONTEXT}/lib/" 2>/dev/null || true
cp "${PROJECT_DIR}/target/lib/"*.jar "${BUILD_CONTEXT}/lib/" 2>/dev/null || true

# Copy config files
cp "${DOCKER_DIR}/conf/server-docker.properties" "${BUILD_CONTEXT}/conf/server-docker.properties"
cp "${PROJECT_DIR}/deploy/log4j2.xml" "${BUILD_CONTEXT}/conf/log4j2.xml"

# Copy scripts
cp "${PROJECT_DIR}/deploy/start.sh" "${BUILD_CONTEXT}/bin/"
cp "${PROJECT_DIR}/deploy/stop.sh"  "${BUILD_CONTEXT}/bin/"

# Copy SQL
cp "${PROJECT_DIR}/src/main/resources/init.sql" "${BUILD_CONTEXT}/sql/init.sql"
cp "${DOCKER_DIR}/sql/test-data.sql" "${BUILD_CONTEXT}/sql/test-data.sql"

# Copy Docker files
cp "${DOCKER_DIR}/Dockerfile" "${BUILD_CONTEXT}/"
cp "${DOCKER_DIR}/docker-entrypoint.sh" "${BUILD_CONTEXT}/"

# --- 3. Build Docker image ---
echo "[3/3] Building Docker image: ${IMAGE_NAME}:${IMAGE_TAG}"
cd "${BUILD_CONTEXT}"
docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" .

# Cleanup build context
rm -rf "${BUILD_CONTEXT}"

echo ""
echo "============================================================"
echo " Build complete: ${IMAGE_NAME}:${IMAGE_TAG}"
echo "============================================================"
echo ""
echo " Run:"
echo "   docker run -p 26754:26754 -p 3306:3306 ${IMAGE_NAME}:${IMAGE_TAG}"
echo ""

# --- Optional: run ---
if [ "$1" = "run" ]; then
    shift
    echo "Starting container..."
    docker run -p 26754:26754 -p 3306:3306 "$@" "${IMAGE_NAME}:${IMAGE_TAG}"
fi
