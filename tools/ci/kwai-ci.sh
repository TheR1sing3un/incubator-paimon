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

# ============================================================================
# Paimon CI script for internal KDEV pipelines.
#
# Usage:
#   ./tools/ci/kwai-ci.sh compile         # Build all (run once before tests)
#   ./tools/ci/kwai-ci.sh <test-step>     # Run a specific test step
#
# Pipeline structure (same as Flink):
#
#   [compile]  (serial, run once)
#       ├── core           (parallel)
#       ├── flink-common   (parallel)
#       ├── flink-others   (parallel)
#       ├── spark-2.12     (parallel)
#       ├── spark-2.13     (parallel)
#       └── e2e-flink1     (parallel)
# ============================================================================

set -euo pipefail

MODULE=${1:?"Usage: $0 <compile|core|flink-common|flink-others|spark-2.12|spark-2.13|e2e-flink1|core-jdk11|flink2|e2e-flink2>"}

# Workspace-local Maven repo — isolates from other pipelines on the same machine
REPO_LOCAL="${KCI_ENGINE_WORKSPACE:-${JOB_WS_ROOT:-/tmp}}/paimon"
mkdir -p "${REPO_LOCAL}"

export MAVEN_OPTS="${MAVEN_OPTS:-} -Xmx4096m"
MVN_COMMON="-B -ntp -Dmaven.repo.local=${REPO_LOCAL} --fail-at-end"

# Random timezone for test discovery
jvm_timezone="GMT$(python -c 'import random; h=random.randint(-12,14); m=random.choice([0,30,45]); print("+{:02d}:{:02d}".format(h,m) if h>=0 else "-{:02d}:{:02d}".format(abs(h),m))')"

case "${MODULE}" in

  # ===========================================================================
  # COMPILE — run once before all test steps
  # ===========================================================================
  compile)
    echo "[kwai-ci] Running: mvn ${MVN_COMMON} -T 1C clean install -DskipTests -Dskip.frontend=true -Dfast"
    mvn ${MVN_COMMON} -T 1C clean install \
        -DskipTests \
        -Dskip.frontend=true \
        -Dfast
    ;;

  # ===========================================================================
  # JDK 8 test steps
  # ===========================================================================
  core)
    echo "[kwai-ci] Running: mvn ${MVN_COMMON} -T 2C verify (core, excluding faiss/lance/spark/frontend/e2e) -Duser.timezone=${jvm_timezone}"
    mvn ${MVN_COMMON} -T 2C verify \
        -pl '!paimon-frontend,!paimon-e2e-tests,!paimon-faiss/paimon-faiss-jni,!paimon-faiss/paimon-faiss-index,!paimon-faiss/paimon-faiss-e2e-test,!paimon-lance,!org.apache.paimon:paimon-spark-ut_2.12,!org.apache.paimon:paimon-spark-3.5_2.12,!org.apache.paimon:paimon-spark-3.4_2.12,!org.apache.paimon:paimon-spark-3.3_2.12,!org.apache.paimon:paimon-spark-3.2_2.12,!paimon-hive/paimon-hive-connector-common,!paimon-hive/paimon-hive-connector-2.1,!paimon-hive/paimon-hive-connector-2.1-cdh-6.3,!paimon-hive/paimon-hive-connector-2.2,!paimon-hive/paimon-hive-connector-2.3,!paimon-hive/paimon-hive-connector-3.1' \
        -Pskip-paimon-flink-tests \
        -Dskip.frontend=true \
        -Dcheckstyle.skip -Dspotless.check.skip -Drat.skip \
        -Duser.timezone=${jvm_timezone}
    ;;

  flink-common)
    export MAVEN_OPTS="${MAVEN_OPTS:-} -XX:+UseG1GC -XX:CICompilerCount=2"
    echo "[kwai-ci] MAVEN_OPTS=${MAVEN_OPTS}"
    echo "[kwai-ci] Running: mvn ${MVN_COMMON} -T 2C test verify -Pflink1,spark3 -pl org.apache.paimon:paimon-flink-common -Duser.timezone=${jvm_timezone}"
    mvn ${MVN_COMMON} -T 2C test verify \
        -Pflink1,spark3 \
        -pl "org.apache.paimon:paimon-flink-common" \
        -Duser.timezone=${jvm_timezone}
    ;;

  flink-others)
    export MAVEN_OPTS="${MAVEN_OPTS:-} -XX:+UseG1GC -XX:CICompilerCount=2"
    echo "[kwai-ci] MAVEN_OPTS=${MAVEN_OPTS}"
    echo "[kwai-ci] Running: mvn ${MVN_COMMON} -T 2C test verify -Pflink1,spark3 -pl org.apache.paimon:paimon-flink-cdc,org.apache.paimon:paimon-flink-1.16,org.apache.paimon:paimon-flink-1.17,org.apache.paimon:paimon-flink-1.18,org.apache.paimon:paimon-flink-1.19,org.apache.paimon:paimon-flink-1.20 -Duser.timezone=${jvm_timezone}"
    mvn ${MVN_COMMON} -T 2C test verify \
        -Pflink1,spark3 \
        -pl "org.apache.paimon:paimon-flink-cdc,org.apache.paimon:paimon-flink-1.16,org.apache.paimon:paimon-flink-1.17,org.apache.paimon:paimon-flink-1.18,org.apache.paimon:paimon-flink-1.19,org.apache.paimon:paimon-flink-1.20" \
        -Duser.timezone=${jvm_timezone}
    ;;

  spark-2.12)
    echo "[kwai-ci] Running: mvn ${MVN_COMMON} -T 1 install (spark-2.12, skip tests)"
    mvn ${MVN_COMMON} -T 1 install \
        -Dmaven.test.skip=true \
        -Dskip.frontend=true \
        -Pspark3,flink1 \
        -Dcheckstyle.skip -Dspotless.check.skip -Drat.skip

    echo "[kwai-ci] Running: mvn ${MVN_COMMON} -T 2C verify (spark-2.12) -Duser.timezone=${jvm_timezone}"
    mvn ${MVN_COMMON} -T 2C verify \
        -pl "org.apache.paimon:paimon-spark-ut_2.12,org.apache.paimon:paimon-spark-3.5_2.12,org.apache.paimon:paimon-spark-3.4_2.12,org.apache.paimon:paimon-spark-3.3_2.12,org.apache.paimon:paimon-spark-3.2_2.12" \
        -Pspark3,flink1 \
        -Duser.timezone=${jvm_timezone}
    ;;

  spark-2.13)
    echo "[kwai-ci] Running: mvn ${MVN_COMMON} -T 1 install (spark-2.13, skip tests)"
    mvn ${MVN_COMMON} -T 1 install \
        -Dmaven.test.skip=true \
        -Dskip.frontend=true \
        -Pspark3,flink1,scala-2.13 \
        -Dcheckstyle.skip -Dspotless.check.skip -Drat.skip

    echo "[kwai-ci] Running: mvn ${MVN_COMMON} -T 2C verify (spark-2.13) -Duser.timezone=${jvm_timezone}"
    mvn ${MVN_COMMON} -T 2C verify \
        -pl "org.apache.paimon:paimon-spark-ut_2.13,org.apache.paimon:paimon-spark-3.5_2.13,org.apache.paimon:paimon-spark-3.4_2.13,org.apache.paimon:paimon-spark-3.3_2.13,org.apache.paimon:paimon-spark-3.2_2.13" \
        -Pspark3,flink1,scala-2.13 \
        -Duser.timezone=${jvm_timezone}
    ;;

  e2e-flink1)
    echo "[kwai-ci] Running: mvn ${MVN_COMMON} -T 1C test -Pflink1,spark3 -pl paimon-e2e-tests -Duser.timezone=${jvm_timezone}"
    mvn ${MVN_COMMON} -T 1C test \
        -Pflink1,spark3 \
        -pl paimon-e2e-tests \
        -Duser.timezone=${jvm_timezone}
    ;;

  # ===========================================================================
  # JDK 11 test steps
  # ===========================================================================
  core-jdk11)
    echo "[kwai-ci] Running: mvn ${MVN_COMMON} -T 1C test verify (core-jdk11, excluding faiss/lance/spark/frontend/e2e) -Duser.timezone=${jvm_timezone}"
    mvn ${MVN_COMMON} -T 1C test verify \
        -Pflink1,spark3,paimon-lucene \
        -pl '!paimon-frontend,!paimon-e2e-tests,!paimon-faiss/paimon-faiss-jni,!paimon-faiss/paimon-faiss-index,!paimon-faiss/paimon-faiss-e2e-test,!paimon-lance,!org.apache.paimon:paimon-hive-connector-3.1,!org.apache.paimon:paimon-spark-ut_2.12,!org.apache.paimon:paimon-spark-3.5_2.12,!org.apache.paimon:paimon-spark-3.4_2.12,!org.apache.paimon:paimon-spark-3.3_2.12,!org.apache.paimon:paimon-spark-3.2_2.12,!paimon-hive/paimon-hive-connector-common,!paimon-hive/paimon-hive-connector-2.1,!paimon-hive/paimon-hive-connector-2.1-cdh-6.3,!paimon-hive/paimon-hive-connector-2.2,!paimon-hive/paimon-hive-connector-2.3' \
        -Pskip-paimon-flink-tests \
        -Dskip.frontend=true \
        -Dcheckstyle.skip -Dspotless.check.skip -Drat.skip \
        -Duser.timezone=${jvm_timezone}
    ;;

  flink2)
    echo "[kwai-ci] Running: mvn ${MVN_COMMON} -T 2C test verify -Pflink2,spark3 -pl org.apache.paimon:paimon-flink-2.0,org.apache.paimon:paimon-flink-2.1,org.apache.paimon:paimon-flink-2.2,org.apache.paimon:paimon-flink-common -Duser.timezone=${jvm_timezone}"
    mvn ${MVN_COMMON} -T 2C test verify \
        -Pflink2,spark3 \
        -pl "org.apache.paimon:paimon-flink-2.0,org.apache.paimon:paimon-flink-2.1,org.apache.paimon:paimon-flink-2.2,org.apache.paimon:paimon-flink-common" \
        -Duser.timezone=${jvm_timezone}
    ;;

  e2e-flink2)
    echo "[kwai-ci] Running: mvn ${MVN_COMMON} -T 1C test -Pflink2,spark3 -pl paimon-e2e-tests -Pjava11 -Duser.timezone=${jvm_timezone}"
    mvn ${MVN_COMMON} -T 1C test \
        -Pflink2,spark3 \
        -pl paimon-e2e-tests \
        -Pjava11 \
        -Duser.timezone=${jvm_timezone}
    ;;

  *)
    echo "Unknown module: ${MODULE}"
    echo "Usage: $0 <compile|core|flink-common|flink-others|spark-2.12|spark-2.13|e2e-flink1|core-jdk11|flink2|e2e-flink2>"
    exit 1
    ;;

esac
