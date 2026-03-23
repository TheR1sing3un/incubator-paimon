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
# Generate test data for Paimon REST Server
# Creates databases, tables, snapshots, branches, and tags
# ============================================================
set -e

BASE_URL="${BASE_URL:-http://localhost:26754}"
PREFIX="v1/paimon"

# Helper function
api() {
  local method="$1" path="$2" body="$3"
  if [ -n "$body" ]; then
    curl -s -X "$method" "${BASE_URL}/${PREFIX}/${path}" \
      -H "Content-Type: application/json" \
      -d "$body"
  else
    curl -s -X "$method" "${BASE_URL}/${PREFIX}/${path}"
  fi
}

echo "============================================================"
echo " Generating test data for Paimon REST Server"
echo " Server: ${BASE_URL}"
echo "============================================================"

# ---- 1. Create Databases ----
echo ""
echo "[1/5] Creating databases..."

for db in "test_db" "production" "staging"; do
  echo "  Creating database: $db"
  api POST "databases" "{\"name\": \"$db\", \"options\": {\"comment\": \"$db database\", \"owner\": \"admin\"}}" > /dev/null
done

echo "  Databases created:"
api GET "databases" | python3 -m json.tool 2>/dev/null || api GET "databases"

# ---- 2. Create Tables ----
echo ""
echo "[2/5] Creating tables..."

# test_db.user_events - partitioned by dt, pk: user_id
echo "  Creating test_db.user_events..."
api POST "databases/test_db/tables" '{
  "identifier": {"database": "test_db", "object": "user_events"},
  "schema": {
    "fields": [
      {"id": 0, "name": "user_id", "type": "BIGINT NOT NULL"},
      {"id": 1, "name": "event_type", "type": "VARCHAR(100)"},
      {"id": 2, "name": "event_time", "type": "TIMESTAMP(3)"},
      {"id": 3, "name": "page_url", "type": "VARCHAR(500)"},
      {"id": 4, "name": "session_id", "type": "VARCHAR(64)"},
      {"id": 5, "name": "dt", "type": "VARCHAR(10) NOT NULL"}
    ],
    "partitionKeys": ["dt"],
    "primaryKeys": ["user_id", "dt"],
    "options": {"bucket": "4", "changelog-producer": "input", "snapshot.num-retained.max": "50"},
    "comment": "User behavior event tracking table"
  }
}' > /dev/null

# test_db.order_details - partitioned by dt, pk: order_id
echo "  Creating test_db.order_details..."
api POST "databases/test_db/tables" '{
  "identifier": {"database": "test_db", "object": "order_details"},
  "schema": {
    "fields": [
      {"id": 0, "name": "order_id", "type": "BIGINT NOT NULL"},
      {"id": 1, "name": "user_id", "type": "BIGINT"},
      {"id": 2, "name": "product_name", "type": "VARCHAR(200)"},
      {"id": 3, "name": "amount", "type": "DECIMAL(18, 2)"},
      {"id": 4, "name": "status", "type": "VARCHAR(20)"},
      {"id": 5, "name": "created_at", "type": "TIMESTAMP(3)"},
      {"id": 6, "name": "dt", "type": "VARCHAR(10) NOT NULL"}
    ],
    "partitionKeys": ["dt"],
    "primaryKeys": ["order_id", "dt"],
    "options": {"bucket": "8", "merge-engine": "deduplicate"},
    "comment": "Order details table with deduplication"
  }
}' > /dev/null

# production.click_stream - partitioned by dt, pk: click_id
echo "  Creating production.click_stream..."
api POST "databases/production/tables" '{
  "identifier": {"database": "production", "object": "click_stream"},
  "schema": {
    "fields": [
      {"id": 0, "name": "click_id", "type": "BIGINT NOT NULL"},
      {"id": 1, "name": "user_id", "type": "BIGINT"},
      {"id": 2, "name": "page_id", "type": "VARCHAR(100)"},
      {"id": 3, "name": "click_time", "type": "TIMESTAMP(3)"},
      {"id": 4, "name": "referrer", "type": "VARCHAR(500)"},
      {"id": 5, "name": "device_type", "type": "VARCHAR(50)"},
      {"id": 6, "name": "dt", "type": "VARCHAR(10) NOT NULL"}
    ],
    "partitionKeys": ["dt"],
    "primaryKeys": ["click_id", "dt"],
    "options": {"bucket": "16", "changelog-producer": "lookup", "snapshot.num-retained.max": "100"},
    "comment": "Click stream data with lookup changelog"
  }
}' > /dev/null

# production.user_profile - no partition, pk: user_id, partial-update
echo "  Creating production.user_profile..."
api POST "databases/production/tables" '{
  "identifier": {"database": "production", "object": "user_profile"},
  "schema": {
    "fields": [
      {"id": 0, "name": "user_id", "type": "BIGINT NOT NULL"},
      {"id": 1, "name": "username", "type": "VARCHAR(100)"},
      {"id": 2, "name": "email", "type": "VARCHAR(200)"},
      {"id": 3, "name": "age", "type": "INT"},
      {"id": 4, "name": "city", "type": "VARCHAR(100)"},
      {"id": 5, "name": "last_login", "type": "TIMESTAMP(3)"},
      {"id": 6, "name": "registration_date", "type": "DATE"}
    ],
    "partitionKeys": [],
    "primaryKeys": ["user_id"],
    "options": {"bucket": "4", "merge-engine": "partial-update"},
    "comment": "User profile table with partial update"
  }
}' > /dev/null

# staging.test_sink - simple append-only table
echo "  Creating staging.test_sink..."
api POST "databases/staging/tables" '{
  "identifier": {"database": "staging", "object": "test_sink"},
  "schema": {
    "fields": [
      {"id": 0, "name": "id", "type": "BIGINT"},
      {"id": 1, "name": "data", "type": "VARCHAR(500)"},
      {"id": 2, "name": "ts", "type": "TIMESTAMP(3)"}
    ],
    "partitionKeys": [],
    "primaryKeys": [],
    "options": {"bucket": "2", "write-mode": "append-only"},
    "comment": "Test sink table for staging"
  }
}' > /dev/null

echo "  Tables created."

# ---- 3. Commit Snapshots ----
echo ""
echo "[3/5] Committing snapshots..."

# Helper: commit a snapshot
commit_snapshot() {
  local db="$1" table="$2" snap_id="$3" schema_id="$4" commit_kind="$5" total_records="$6" delta_records="$7" time_millis="$8" properties="${9:-null}"
  local table_uuid
  # Get table UUID
  table_uuid=$(api GET "databases/${db}/tables/${table}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('uuid',''))" 2>/dev/null || echo "")

  api POST "databases/${db}/tables/${table}/commit" "{
    \"tableId\": \"${table_uuid}\",
    \"snapshot\": {
      \"version\": 3,
      \"id\": ${snap_id},
      \"schemaId\": ${schema_id},
      \"baseManifestList\": \"manifest-list-base-${snap_id}\",
      \"deltaManifestList\": \"manifest-list-delta-${snap_id}\",
      \"commitUser\": \"test-user\",
      \"commitIdentifier\": ${snap_id}00,
      \"commitKind\": \"${commit_kind}\",
      \"timeMillis\": ${time_millis},
      \"totalRecordCount\": ${total_records},
      \"deltaRecordCount\": ${delta_records},
      \"changelogRecordCount\": 0,
      \"properties\": ${properties}
    }
  }" > /dev/null
}

# Base timestamp: 2026-03-20 00:00:00 UTC = 1774060800000
BASE_TS=1774060800000
HOUR=$((3600 * 1000))

# test_db.user_events - 5 snapshots (with committer/message properties)
echo "  Committing snapshots for test_db.user_events..."
for i in 1 2 3 4 5; do
  ts=$((BASE_TS + i * HOUR))
  kind="APPEND"
  [ "$i" -eq 3 ] && kind="COMPACT"
  total=$((i * 10000))
  delta=$((i * 2000))
  props="{\"paimon.commit.committer\": \"flink-job-01\", \"paimon.commit.message\": \"Batch ingestion #${i}\"}"
  commit_snapshot "test_db" "user_events" "$i" "0" "$kind" "$total" "$delta" "$ts" "$props"
  echo "    Snapshot $i committed (${kind})"
done

# test_db.order_details - 4 snapshots
echo "  Committing snapshots for test_db.order_details..."
for i in 1 2 3 4; do
  ts=$((BASE_TS + i * HOUR * 2))
  kind="APPEND"
  [ "$i" -eq 4 ] && kind="COMPACT"
  total=$((i * 5000))
  delta=$((i * 1000))
  commit_snapshot "test_db" "order_details" "$i" "0" "$kind" "$total" "$delta" "$ts"
  echo "    Snapshot $i committed (${kind})"
done

# production.click_stream - 6 snapshots (with metadata properties)
echo "  Committing snapshots for production.click_stream..."
for i in 1 2 3 4 5 6; do
  ts=$((BASE_TS + i * HOUR))
  kind="APPEND"
  [ "$i" -eq 4 ] && kind="COMPACT"
  [ "$i" -eq 6 ] && kind="COMPACT"
  total=$((i * 20000))
  delta=$((i * 5000))
  props="{\"paimon.commit.committer\": \"streaming-etl\", \"paimon.commit.message\": \"Streaming sync batch ${i}\"}"
  commit_snapshot "production" "click_stream" "$i" "0" "$kind" "$total" "$delta" "$ts" "$props"
  echo "    Snapshot $i committed (${kind})"
done

# production.user_profile - 3 snapshots
echo "  Committing snapshots for production.user_profile..."
for i in 1 2 3; do
  ts=$((BASE_TS + i * HOUR * 3))
  total=$((i * 3000))
  delta=$((i * 500))
  commit_snapshot "production" "user_profile" "$i" "0" "APPEND" "$total" "$delta" "$ts"
  echo "    Snapshot $i committed"
done

# staging.test_sink - 2 snapshots
echo "  Committing snapshots for staging.test_sink..."
for i in 1 2; do
  ts=$((BASE_TS + i * HOUR))
  commit_snapshot "staging" "test_sink" "$i" "0" "APPEND" "$((i * 1000))" "$((i * 500))" "$ts"
  echo "    Snapshot $i committed"
done

# ---- 4. Create Tags ----
echo ""
echo "[4/5] Creating tags..."

# Tags for test_db.user_events
echo "  Creating tags for test_db.user_events..."
api POST "databases/test_db/tables/user_events/tags" '{"tagName": "v1.0", "snapshotId": 1, "ignoreIfExists": true}' > /dev/null
echo "    Tag v1.0 -> snapshot 1"
api POST "databases/test_db/tables/user_events/tags" '{"tagName": "v1.1", "snapshotId": 3, "ignoreIfExists": true}' > /dev/null
echo "    Tag v1.1 -> snapshot 3"
api POST "databases/test_db/tables/user_events/tags" '{"tagName": "v2.0", "snapshotId": 5, "ignoreIfExists": true}' > /dev/null
echo "    Tag v2.0 -> snapshot 5"

# Tags for production.click_stream
echo "  Creating tags for production.click_stream..."
api POST "databases/production/tables/click_stream/tags" '{"tagName": "release-2026-03-20", "snapshotId": 3, "ignoreIfExists": true}' > /dev/null
echo "    Tag release-2026-03-20 -> snapshot 3"
api POST "databases/production/tables/click_stream/tags" '{"tagName": "release-2026-03-21", "snapshotId": 6, "ignoreIfExists": true}' > /dev/null
echo "    Tag release-2026-03-21 -> snapshot 6"

# Tags for test_db.order_details
echo "  Creating tags for test_db.order_details..."
api POST "databases/test_db/tables/order_details/tags" '{"tagName": "daily-2026-03-20", "snapshotId": 2, "ignoreIfExists": true}' > /dev/null
echo "    Tag daily-2026-03-20 -> snapshot 2"

# ---- 5. Create Branches ----
echo ""
echo "[5/5] Creating branches..."

# Branches for test_db.user_events
echo "  Creating branches for test_db.user_events..."
api POST "databases/test_db/tables/user_events/branches" '{"branch": "dev", "fromSnapshotId": 3}' > /dev/null
echo "    Branch dev -> from snapshot 3"
api POST "databases/test_db/tables/user_events/branches" '{"branch": "feature-new-schema", "fromSnapshotId": 5}' > /dev/null
echo "    Branch feature-new-schema -> from snapshot 5"
api POST "databases/test_db/tables/user_events/branches" '{"branch": "hotfix-v1", "fromTag": "v1.0"}' > /dev/null
echo "    Branch hotfix-v1 -> from tag v1.0"

# Branches for production.click_stream
echo "  Creating branches for production.click_stream..."
api POST "databases/production/tables/click_stream/branches" '{"branch": "experiment-a", "fromSnapshotId": 4}' > /dev/null
echo "    Branch experiment-a -> from snapshot 4"

echo ""
echo "============================================================"
echo " Test data generation complete!"
echo "============================================================"
echo ""
echo " Summary:"
echo "   Databases: test_db, production, staging"
echo "   Tables: 5 (user_events, order_details, click_stream, user_profile, test_sink)"
echo "   Snapshots: 20 total"
echo "   Tags: 6 (v1.0, v1.1, v2.0, release-*, daily-*)"
echo "   Branches: 4 (dev, feature-new-schema, hotfix-v1, experiment-a)"
echo ""
echo " Frontend: ${BASE_URL}"
echo " API: ${BASE_URL}/v1/config"
echo "============================================================"
