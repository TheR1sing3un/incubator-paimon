-- ============================================================
-- Test Data for Paimon REST Catalog Server
-- ============================================================

USE paimon_catalog;

-- Test operation logs
INSERT INTO paimon_op_log (database_name, table_name, user_id, user_name, operation_type, target_type, target_id, request_json, result_json, status, created_at)
VALUES
    ('test_db', 'user_events', 'admin', 'Administrator', 'CREATE_TABLE', 'TABLE', 'user_events', '{"schema": "..."}', '{"table_id": 1}', 'SUCCESS', UNIX_TIMESTAMP() * 1000),
    ('test_db', 'order_details', 'admin', 'Administrator', 'CREATE_TABLE', 'TABLE', 'order_details', '{"schema": "..."}', '{"table_id": 2}', 'SUCCESS', UNIX_TIMESTAMP() * 1000),
    ('production', 'click_stream', 'admin', 'Administrator', 'CREATE_TABLE', 'TABLE', 'click_stream', '{"schema": "..."}', '{"table_id": 4}', 'SUCCESS', UNIX_TIMESTAMP() * 1000),
    ('test_db', 'user_events', 'user1', 'Zhang San', 'COMMIT', 'COMMIT', 'snapshot-1', '{"identifier": "test_db.user_events"}', '{"snapshot_id": 1}', 'SUCCESS', UNIX_TIMESTAMP() * 1000),
    ('test_db', 'user_events', 'user1', 'Zhang San', 'COMMIT', 'COMMIT', 'snapshot-2', '{"identifier": "test_db.user_events"}', '{"snapshot_id": 2}', 'SUCCESS', UNIX_TIMESTAMP() * 1000);
