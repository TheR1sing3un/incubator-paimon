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

set -euo pipefail

###############################################################################
# Paimon REST Server - Regression Test Script
#
# Usage:
#   ./regression-test.sh <base_url> [prefix]
#
# Examples:
#   ./regression-test.sh http://localhost:8080
#   ./regression-test.sh http://localhost:8080 my-catalog
#   ./regression-test.sh https://paimon.example.com my-catalog
###############################################################################

BASE_URL="${1:?Usage: $0 <base_url> [prefix]}"
PREFIX="${2:-}"

# Strip trailing slash
BASE_URL="${BASE_URL%/}"

# Build path prefix
if [[ -n "$PREFIX" ]]; then
    API="/v1/${PREFIX}"
else
    API="/v1"
fi

# Test resources (will be created and cleaned up)
TEST_DB="regression_test_db_$(date +%s)"
TEST_TABLE="regression_test_table"
TEST_VIEW="regression_test_view"
TEST_FUNCTION="regression_test_func"
TEST_BRANCH="regression_test_branch"
TEST_TAG="regression_test_tag"
TEST_CONSUMER="regression_test_consumer"

# Counters
TOTAL=0
PASSED=0
FAILED=0
SKIPPED=0
FAILURES=""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m'

###############################################################################
# Helpers
###############################################################################

log_header() {
    echo -e "\n${CYAN}══════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}══════════════════════════════════════════════════════════════${NC}"
}

log_test() {
    echo -e "\n${YELLOW}── [$1] $2 $3${NC}"
}

run_test() {
    local method="$1"
    local path="$2"
    local description="$3"
    local body="${4:-}"
    local expected_status="${5:-2}"  # prefix match: "2" matches 2xx, "200" matches exactly 200

    TOTAL=$((TOTAL + 1))
    log_test "$method" "$path" "$description"

    local url="${BASE_URL}${path}"
    local curl_args=(-s -o /tmp/paimon_test_body -w "%{http_code}" -X "$method")
    curl_args+=(-H "Content-Type: application/json")

    if [[ -n "$body" ]]; then
        curl_args+=(-d "$body")
    fi

    local http_code
    http_code=$(curl "${curl_args[@]}" "$url" 2>/dev/null) || {
        echo -e "  ${RED}FAIL${NC} - curl error (connection refused?)"
        FAILED=$((FAILED + 1))
        FAILURES="${FAILURES}\n  [${method}] ${path} - ${description} (curl error)"
        return
    }

    local response_body
    response_body=$(cat /tmp/paimon_test_body 2>/dev/null || echo "")

    if [[ "$http_code" == ${expected_status}* ]]; then
        echo -e "  ${GREEN}PASS${NC} - HTTP ${http_code}"
        PASSED=$((PASSED + 1))
    else
        echo -e "  ${RED}FAIL${NC} - HTTP ${http_code} (expected ${expected_status}xx)"
        echo "  Response: ${response_body:0:500}"
        FAILED=$((FAILED + 1))
        FAILURES="${FAILURES}\n  [${method}] ${path} - ${description} (HTTP ${http_code})"
    fi
}

skip_test() {
    local method="$1"
    local path="$2"
    local description="$3"
    local reason="$4"

    TOTAL=$((TOTAL + 1))
    SKIPPED=$((SKIPPED + 1))
    log_test "$method" "$path" "$description"
    echo -e "  ${YELLOW}SKIP${NC} - ${reason}"
}

###############################################################################
# 0. Health Check
###############################################################################
log_header "0. Health Check"

run_test GET "/health" "Server health check" "" "200"

###############################################################################
# 1. Config
###############################################################################
log_header "1. Config"

run_test GET "/v1/config" "Get catalog config" "" "200"

###############################################################################
# 2. Database - Create & List
###############################################################################
log_header "2. Database Operations"

run_test POST "${API}/databases" \
    "Create test database" \
    "{\"name\": \"${TEST_DB}\", \"options\": {\"comment\": \"regression test\"}}" \
    "200"

run_test GET "${API}/databases" \
    "List databases" "" "200"

run_test GET "${API}/databases?maxResults=1" \
    "List databases with pagination" "" "200"

run_test GET "${API}/databases?databaseNamePattern=${TEST_DB}" \
    "List databases with name pattern" "" "200"

run_test GET "${API}/databases/${TEST_DB}" \
    "Get database details" "" "200"

###############################################################################
# 3. Table - Create & CRUD
###############################################################################
log_header "3. Table Operations"

run_test POST "${API}/databases/${TEST_DB}/tables" \
    "Create test table" \
    "{
        \"identifier\": {\"database\": \"${TEST_DB}\", \"object\": \"${TEST_TABLE}\"},
        \"schema\": {
            \"fields\": [
                {\"id\": 0, \"name\": \"id\", \"type\": \"INT\"},
                {\"id\": 1, \"name\": \"name\", \"type\": \"STRING\"},
                {\"id\": 2, \"name\": \"dt\", \"type\": \"STRING\"}
            ],
            \"partitionKeys\": [\"dt\"],
            \"primaryKeys\": [\"id\", \"dt\"],
            \"options\": {\"bucket\": \"1\"},
            \"comment\": \"regression test table\"
        }
    }" \
    "200"

run_test GET "${API}/databases/${TEST_DB}/tables" \
    "List tables in database" "" "200"

run_test GET "${API}/databases/${TEST_DB}/tables?maxResults=1" \
    "List tables with pagination" "" "200"

run_test GET "${API}/databases/${TEST_DB}/tables?tableNamePattern=${TEST_TABLE}" \
    "List tables with name pattern" "" "200"

run_test GET "${API}/databases/${TEST_DB}/tables?tableType=TABLE" \
    "List tables with type filter" "" "200"

run_test GET "${API}/databases/${TEST_DB}/table-details" \
    "List table details" "" "200"

run_test GET "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}" \
    "Get table details" "" "200"

run_test GET "${API}/tables" \
    "List tables globally" "" "200"

run_test GET "${API}/tables?maxResults=1" \
    "List tables globally with pagination" "" "200"

# Alter table - add column
run_test POST "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}" \
    "Alter table - add column" \
    "{\"changes\": [{\"action\": \"addColumn\", \"fieldNames\": [\"age\"], \"dataType\": \"INT\"}]}" \
    "200"

###############################################################################
# 4. Snapshot Operations
###############################################################################
log_header "4. Snapshot Operations"

run_test GET "${API}/databases/hudi_dev/tables/regression_test_table/snapshot" \
    "Get latest snapshot" "" "2"

run_test GET "${API}/databases/hudi_dev/tables/regression_test_table/snapshots" \
    "List snapshots" "" "200"

run_test GET "${API}/databases/hudi_dev/tables/regression_test_table/snapshots?maxResults=1" \
    "List snapshots with pagination" "" "200"

# Load specific snapshot (version 1 may or may not exist)
run_test GET "${API}/databases/hudi_dev/tables/${TEST_TABLE}/snapshots/1" \
    "Load snapshot version 1 (may 404 if no data)" "" "2"

###############################################################################
# 5. Schema Operations (conditional - only if AbstractCatalog)
###############################################################################
log_header "5. Schema Operations"

run_test GET "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}/schemas" \
    "List schemas" "" "2"

run_test GET "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}/schemas/0" \
    "Get schema version 0" "" "2"

###############################################################################
# 6. Partition Operations
###############################################################################
log_header "6. Partition Operations"

run_test GET "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}/partitions" \
    "List partitions" "" "200"

run_test GET "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}/partitions?maxResults=1" \
    "List partitions with pagination" "" "200"

run_test POST "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}/partitions/list-by-names" \
    "List partitions by names" \
    "{\"partitionSpecs\": [{\"dt\": \"2024-01-01\"}]}" \
    "200"

run_test POST "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}/partitions/mark" \
    "Mark partitions done" \
    "{\"partitionSpecs\": [{\"dt\": \"2024-01-01\"}]}" \
    "2"

###############################################################################
# 7. Branch Operations
###############################################################################
log_header "7. Branch Operations"

run_test POST "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}/branches" \
    "Create branch" \
    "{\"branch\": \"${TEST_BRANCH}\"}" \
    "200"

run_test GET "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}/branches" \
    "List branches" "" "200"

run_test GET "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}/branches/${TEST_BRANCH}" \
    "Get branch details" "" "200"

run_test GET "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}/diff?left=main&right=${TEST_BRANCH}" \
    "Diff branches" "" "2"

# Branch merge (create a second branch to merge)
run_test POST "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}/branches" \
    "Create branch for merge test" \
    "{\"branch\": \"${TEST_BRANCH}_merge\"}" \
    "200"

# Cleanup merge branch
run_test DELETE "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}/branches/${TEST_BRANCH}_merge" \
    "Delete merge test branch" "" "2"

run_test DELETE "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}/branches/${TEST_BRANCH}" \
    "Delete branch" "" "200"

###############################################################################
# 8. Tag Operations
###############################################################################
log_header "8. Tag Operations"

run_test POST "${API}/databases/hudi_dev/tables/regression_test_table/tags" \
    "Create tag" \
    "{\"tagName\": \"${TEST_TAG}\", \"ignoreIfExists\": true}" \
    "200"

run_test GET "${API}/databases/hudi_dev/tables/regression_test_table/tags" \
    "List tags" "" "200"

run_test GET "${API}/databases/hudi_dev/tables/regression_test_table/tags?maxResults=1" \
    "List tags with pagination" "" "200"

run_test GET "${API}/databases/hudi_dev/tables/regression_test_table/tags?tagNamePrefix=regression" \
    "List tags with name prefix" "" "200"

run_test GET "${API}/databases/hudi_dev/tables/regression_test_table/tags/${TEST_TAG}" \
    "Get tag details" "" "200"

run_test DELETE "${API}/databases/hudi_dev/tables/regression_test_table/tags/${TEST_TAG}" \
    "Delete tag" "" "200"

###############################################################################
# 9. Consumer Operations
###############################################################################
log_header "9. Consumer Operations"

###############################################################################
# 10. Table Token & Auth
###############################################################################
log_header "10. Table Token & Auth"

run_test GET "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}/token" \
    "Get table token" "" "2"

###############################################################################
# 11. Commit Operations
###############################################################################
log_header "11. Commit Operations"

run_test GET "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}/commits" \
    "List commits" "" "2"

run_test GET "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}/commits?maxResults=1" \
    "List commits with pagination" "" "2"

###############################################################################
# 14. Table Rename
###############################################################################
log_header "14. Rename Operations"

TEST_TABLE_RENAMED="${TEST_TABLE}_renamed"
run_test POST "${API}/tables/rename" \
    "Rename table" \
    "{
        \"source\": {\"database\": \"${TEST_DB}\", \"object\": \"${TEST_TABLE}\"},
        \"destination\": {\"database\": \"${TEST_DB}\", \"object\": \"${TEST_TABLE_RENAMED}\"}
    }" \
    "200"

# Verify rename succeeded
run_test GET "${API}/databases/${TEST_DB}/tables/${TEST_TABLE_RENAMED}" \
    "Verify table renamed" "" "200"

# Rename back
run_test POST "${API}/tables/rename" \
    "Rename table back" \
    "{
        \"source\": {\"database\": \"${TEST_DB}\", \"object\": \"${TEST_TABLE_RENAMED}\"},
        \"destination\": {\"database\": \"${TEST_DB}\", \"object\": \"${TEST_TABLE}\"}
    }" \
    "200"

###############################################################################
# 16. Rollback (best-effort)
###############################################################################
log_header "16. Rollback"

skip_test POST "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}/rollback" \
    "Rollback table" "Skipped - requires valid snapshot state to rollback to"

###############################################################################
# 17. Commit (best-effort)
###############################################################################
log_header "17. Commit"

skip_test POST "${API}/databases/${TEST_DB}/tables/${TEST_TABLE}/commit" \
    "Commit snapshot" "Skipped - requires valid snapshot payload"

###############################################################################
# 18. Register Table (best-effort)
###############################################################################
log_header "18. Register Table"

skip_test POST "${API}/databases/${TEST_DB}/register" \
    "Register table" "Skipped - requires valid filesystem path to existing table"

###############################################################################
# 19. Negative Test Cases - Error Handling
###############################################################################
log_header "19. Negative Tests - Error Handling"

run_test GET "${API}/databases/nonexistent_db_12345" \
    "Get non-existent database (expect 404)" "" "404"

run_test GET "${API}/databases/${TEST_DB}/tables/nonexistent_table_12345" \
    "Get non-existent table (expect 404)" "" "404"

run_test DELETE "${API}/databases/nonexistent_db_12345" \
    "Delete non-existent database (expect 404)" "" "404"

run_test POST "${API}/databases" \
    "Create database with empty body (expect 4xx)" \
    "{}" \
    "4"

run_test POST "${API}/databases/${TEST_DB}/tables" \
    "Create table with empty body (expect 4xx)" \
    "{}" \
    "4"

run_test GET "/v1/nonexistent_endpoint_12345" \
    "Access non-existent endpoint (expect 404)" "" "404"

###############################################################################
# Cleanup
###############################################################################
log_header "CLEANUP"

echo "Cleaning up test resources..."

# Delete function
curl -s -o /dev/null -X DELETE "${BASE_URL}${API}/databases/${TEST_DB}/functions/${TEST_FUNCTION}" 2>/dev/null || true
echo "  Deleted function: ${TEST_FUNCTION}"

# Delete view
curl -s -o /dev/null -X DELETE "${BASE_URL}${API}/databases/${TEST_DB}/views/${TEST_VIEW}" 2>/dev/null || true
echo "  Deleted view: ${TEST_VIEW}"

# Delete table
curl -s -o /dev/null -X DELETE "${BASE_URL}${API}/databases/${TEST_DB}/tables/${TEST_TABLE}" 2>/dev/null || true
echo "  Deleted table: ${TEST_TABLE}"

# Delete database
curl -s -o /dev/null -X DELETE "${BASE_URL}${API}/databases/${TEST_DB}" 2>/dev/null || true
echo "  Deleted database: ${TEST_DB}"

# Clean temp file
rm -f /tmp/paimon_test_body

###############################################################################
# Summary
###############################################################################
log_header "TEST SUMMARY"

echo -e "  Base URL:  ${BASE_URL}"
echo -e "  Prefix:    ${PREFIX:-<none>}"
echo ""
echo -e "  Total:     ${TOTAL}"
echo -e "  ${GREEN}Passed:    ${PASSED}${NC}"
echo -e "  ${RED}Failed:    ${FAILED}${NC}"
echo -e "  ${YELLOW}Skipped:   ${SKIPPED}${NC}"

if [[ $FAILED -gt 0 ]]; then
    echo -e "\n${RED}Failed tests:${FAILURES}${NC}"
    echo ""
    exit 1
else
    echo -e "\n${GREEN}All tests passed!${NC}"
    echo ""
    exit 0
fi
