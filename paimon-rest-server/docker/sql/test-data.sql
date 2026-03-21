-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- ============================================================
-- Test Data for Paimon REST Catalog Server
-- ============================================================

USE paimon_catalog;

-- Test databases
INSERT INTO paimon_database (database_name, properties, created_by, created_at, updated_at)
VALUES
    ('test_db', '{"comment": "Default test database"}', 'admin', UNIX_TIMESTAMP() * 1000, UNIX_TIMESTAMP() * 1000),
    ('production', '{"comment": "Production database", "owner": "data-team"}', 'admin', UNIX_TIMESTAMP() * 1000, UNIX_TIMESTAMP() * 1000),
    ('staging', '{"comment": "Staging environment database"}', 'admin', UNIX_TIMESTAMP() * 1000, UNIX_TIMESTAMP() * 1000);

-- Test tables
INSERT INTO paimon_table (database_name, table_name, default_branch, state, properties, created_by, created_at, updated_at)
VALUES
    ('test_db', 'user_events', 'main', 'ACTIVE', '{"bucket": "4", "changelog-producer": "input"}', 'admin', UNIX_TIMESTAMP() * 1000, UNIX_TIMESTAMP() * 1000),
    ('test_db', 'order_details', 'main', 'ACTIVE', '{"bucket": "8", "merge-engine": "deduplicate"}', 'admin', UNIX_TIMESTAMP() * 1000, UNIX_TIMESTAMP() * 1000),
    ('test_db', 'deleted_table', 'main', 'DELETED', '{}', 'admin', UNIX_TIMESTAMP() * 1000, UNIX_TIMESTAMP() * 1000),
    ('production', 'click_stream', 'main', 'ACTIVE', '{"bucket": "16", "changelog-producer": "lookup", "snapshot.num-retained.max": "100"}', 'admin', UNIX_TIMESTAMP() * 1000, UNIX_TIMESTAMP() * 1000),
    ('production', 'user_profile', 'main', 'ACTIVE', '{"bucket": "4", "merge-engine": "partial-update"}', 'admin', UNIX_TIMESTAMP() * 1000, UNIX_TIMESTAMP() * 1000),
    ('staging', 'test_sink', 'main', 'ACTIVE', '{"bucket": "2"}', 'admin', UNIX_TIMESTAMP() * 1000, UNIX_TIMESTAMP() * 1000);

-- Test operation logs
INSERT INTO paimon_op_log (database_name, table_name, user_id, user_name, operation_type, target_type, target_id, request_json, result_json, status, created_at)
VALUES
    ('test_db', 'user_events', 'admin', 'Administrator', 'CREATE_TABLE', 'TABLE', 'user_events', '{"schema": "..."}', '{"table_id": 1}', 'SUCCESS', UNIX_TIMESTAMP() * 1000),
    ('test_db', 'order_details', 'admin', 'Administrator', 'CREATE_TABLE', 'TABLE', 'order_details', '{"schema": "..."}', '{"table_id": 2}', 'SUCCESS', UNIX_TIMESTAMP() * 1000),
    ('production', 'click_stream', 'admin', 'Administrator', 'CREATE_TABLE', 'TABLE', 'click_stream', '{"schema": "..."}', '{"table_id": 4}', 'SUCCESS', UNIX_TIMESTAMP() * 1000),
    ('test_db', 'user_events', 'user1', 'Zhang San', 'COMMIT', 'COMMIT', 'snapshot-1', '{"identifier": "test_db.user_events"}', '{"snapshot_id": 1}', 'SUCCESS', UNIX_TIMESTAMP() * 1000),
    ('test_db', 'user_events', 'user1', 'Zhang San', 'COMMIT', 'COMMIT', 'snapshot-2', '{"identifier": "test_db.user_events"}', '{"snapshot_id": 2}', 'SUCCESS', UNIX_TIMESTAMP() * 1000);
