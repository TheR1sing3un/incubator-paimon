create database paimon_catalog;

-- ============================================================
-- paimon_database: Paimon Database 元信息
-- 存储 Paimon Catalog 管理的 Database 元信息，用于快速查询及扩展属性存储
-- ============================================================
CREATE TABLE `paimon_catalog`.`paimon_database` (
                                                    id              BIGINT        PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
                                                    database_name   VARCHAR(256)  NOT NULL COMMENT 'Paimon database 名',
                                                    properties      JSON          NULL     COMMENT 'Database 扩展属性（JSON）',
                                                    created_by      VARCHAR(64)   NOT NULL COMMENT '创建者',
                                                    created_at      BIGINT        NOT NULL COMMENT '创建时间（epoch millis）',
                                                    updated_at      BIGINT        NOT NULL COMMENT '更新时间（epoch millis）',
                                                    UNIQUE KEY uniq_database (database_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='Paimon Database 元信息';

-- ============================================================
-- paimon_table: Paimon 表注册信息
-- 存储 Paimon Catalog 管理的表元信息，用于快速查询表是否存在及其基本配置
-- ============================================================
CREATE TABLE `paimon_catalog`.`paimon_table` (
                                                 id              BIGINT        PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
                                                 database_name   VARCHAR(256)  NOT NULL COMMENT 'Paimon database 名',
                                                 table_name      VARCHAR(256)  NOT NULL COMMENT 'Paimon 表名',
                                                 default_branch  VARCHAR(128)  NOT NULL DEFAULT 'main' COMMENT '默认分支',
                                                 state           VARCHAR(16)   NOT NULL DEFAULT 'ACTIVE' COMMENT '表状态：ACTIVE/DELETED',
                                                 properties      JSON          NULL     COMMENT '表扩展属性（JSON）',
                                                 created_by      VARCHAR(64)   NOT NULL COMMENT '创建者',
                                                 created_at      BIGINT        NOT NULL COMMENT '创建时间',
                                                 updated_at      BIGINT        NOT NULL COMMENT '更新时间',
                                                 UNIQUE KEY uniq_table (database_name, table_name),
                                                 KEY idx_db_state (database_name, state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='Paimon 表注册信息';

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