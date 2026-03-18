#!/bin/bash
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

# Run E2E integration tests: PyPaimon ↔ real Java REST Catalog Server
#
# Usage:
#   ./dev/run_rest_e2e_tests.sh                  # run all E2E tests
#   ./dev/run_rest_e2e_tests.sh -k test_write    # run tests matching pattern
#   ./dev/run_rest_e2e_tests.sh --no-build       # skip JAR build

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PAIMON_PYTHON_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$PAIMON_PYTHON_DIR/.." && pwd)"
REST_SERVER_DIR="$PROJECT_ROOT/paimon-rest-server"

# Find the shaded JAR (glob for any version)
find_jar() {
    local jar
    jar=$(ls "$REST_SERVER_DIR"/target/paimon-rest-server-*-server.jar 2>/dev/null | head -1)
    echo "$jar"
}

# Parse --no-build flag
NO_BUILD=false
PYTEST_ARGS=()
for arg in "$@"; do
    if [[ "$arg" == "--no-build" ]]; then
        NO_BUILD=true
    else
        PYTEST_ARGS+=("$arg")
    fi
done

# Step 1: Build JAR if needed
JAR_PATH=$(find_jar)
if [[ -z "$JAR_PATH" ]] && [[ "$NO_BUILD" == "true" ]]; then
    echo "ERROR: REST server JAR not found and --no-build was specified."
    echo "Build it first: cd $PROJECT_ROOT && mvn package -pl paimon-rest-server -am -DskipTests"
    exit 1
fi

if [[ -z "$JAR_PATH" ]]; then
    echo "==> Building REST server JAR..."
    cd "$PROJECT_ROOT"
    mvn package -pl paimon-rest-server -am -DskipTests -q
    JAR_PATH=$(find_jar)
    if [[ -z "$JAR_PATH" ]]; then
        echo "ERROR: JAR build succeeded but JAR file not found."
        exit 1
    fi
fi

echo "==> Using REST server JAR: $JAR_PATH"

# Step 2: Verify Java is available
if ! java -version 2>/dev/null; then
    echo "ERROR: Java not found. JDK 8 or 11 is required."
    exit 1
fi

# Step 3: Run E2E tests
export PAIMON_REST_SERVER_JAR="$JAR_PATH"
cd "$PAIMON_PYTHON_DIR"

echo "==> Running E2E REST integration tests..."
pytest pypaimon/tests/e2e_rest/ -v -m e2e_rest "${PYTEST_ARGS[@]}"
