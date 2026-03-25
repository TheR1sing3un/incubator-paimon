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

create database if not exists paimon_catalog;
-- ============================================================
-- paimon_op_log: Paimon Catalog Server 操作审计日志
-- 记录 Paimon Catalog Server 执行的所有写操作，用于审计与故障排查
-- ============================================================
CREATE TABLE `paimon_catalog`.`paimon_op_log` (
                                                  id              BIGINT        PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
                                                  database_name   VARCHAR(256)  NOT NULL COMMENT 'Paimon database 名',
                                                  table_name      VARCHAR(256)  NOT NULL COMMENT 'Paimon 表名',
                                                  user_id         VARCHAR(64)   NOT NULL COMMENT '操作用户ID',
                                                  user_name       VARCHAR(256)  NOT NULL COMMENT '操作用户名',
                                                  operation_type  VARCHAR(64)   NOT NULL COMMENT '操作类型（CREATE_TABLE/DROP_TABLE/ALTER_TABLE/CREATE_BRANCH/DELETE_BRANCH/COMMIT/MERGE/RESET/CREATE_TAG/DELETE_TAG/...）',
                                                  target_type     VARCHAR(64)   NOT NULL COMMENT '目标类型（TABLE/BRANCH/COMMIT/TAG/...）',
                                                  target_id       VARCHAR(256)  NOT NULL COMMENT '目标ID（如分支名、commit_id、tag 名）',
                                                  request_json    JSON          NULL     COMMENT '请求内容摘要',
                                                  result_json     JSON          NULL     COMMENT '执行结果（含 commit_id、snapshot_id 等）',
                                                  status          VARCHAR(16)   NOT NULL COMMENT '执行状态：SUCCESS/FAILED/UNKNOWN',
                                                  error_message   VARCHAR(2048) NULL     COMMENT '错误信息（status=FAILED 时填写）',
                                                  created_at      BIGINT        NOT NULL COMMENT '创建时间',
                                                  KEY idx_db_tbl_time (database_name, table_name, created_at),
                                                  KEY idx_db_user_time (database_name, table_name, user_id, created_at),
                                                  KEY idx_tbl_type_target (table_name, target_type, target_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='Paimon Catalog Server 操作审计日志';