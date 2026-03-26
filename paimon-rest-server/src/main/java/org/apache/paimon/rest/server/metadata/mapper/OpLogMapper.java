/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.rest.server.metadata.mapper;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/** MyBatis mapper for paimon_op_log table. */
@Mapper
public interface OpLogMapper {

    @Insert(
            "INSERT INTO paimon_catalog.paimon_op_log "
                    + "(database_name, table_name, user_id, user_name, "
                    + "operation_type, target_type, target_id, "
                    + "request_json, result_json, status, error_message, created_at) "
                    + "VALUES (#{databaseName}, #{tableName}, #{userId}, #{userName}, "
                    + "#{operationType}, #{targetType}, #{targetId}, "
                    + "#{requestJson}, #{resultJson}, #{status}, #{errorMessage}, #{createdAt})")
    void insert(
            @Param("databaseName") String databaseName,
            @Param("tableName") String tableName,
            @Param("userId") String userId,
            @Param("userName") String userName,
            @Param("operationType") String operationType,
            @Param("targetType") String targetType,
            @Param("targetId") String targetId,
            @Param("requestJson") String requestJson,
            @Param("resultJson") String resultJson,
            @Param("status") String status,
            @Param("errorMessage") String errorMessage,
            @Param("createdAt") long createdAt);

    @Select("SELECT * FROM paimon_catalog.paimon_op_log WHERE operation_type = #{operationType}")
    @Results({
        @Result(column = "database_name", property = "database_name"),
        @Result(column = "table_name", property = "table_name"),
        @Result(column = "user_id", property = "user_id"),
        @Result(column = "user_name", property = "user_name"),
        @Result(column = "operation_type", property = "operation_type"),
        @Result(column = "target_type", property = "target_type"),
        @Result(column = "target_id", property = "target_id"),
        @Result(column = "request_json", property = "request_json"),
        @Result(column = "result_json", property = "result_json"),
        @Result(column = "error_message", property = "error_message"),
        @Result(column = "created_at", property = "created_at")
    })
    List<Map<String, Object>> selectByOperationType(@Param("operationType") String operationType);

    @Select("SELECT * FROM paimon_catalog.paimon_op_log WHERE status = #{status}")
    @Results({
        @Result(column = "database_name", property = "database_name"),
        @Result(column = "table_name", property = "table_name"),
        @Result(column = "user_id", property = "user_id"),
        @Result(column = "user_name", property = "user_name"),
        @Result(column = "operation_type", property = "operation_type"),
        @Result(column = "target_type", property = "target_type"),
        @Result(column = "target_id", property = "target_id"),
        @Result(column = "request_json", property = "request_json"),
        @Result(column = "result_json", property = "result_json"),
        @Result(column = "error_message", property = "error_message"),
        @Result(column = "created_at", property = "created_at")
    })
    List<Map<String, Object>> selectByStatus(@Param("status") String status);

    @Select("SELECT COUNT(*) FROM paimon_catalog.paimon_op_log")
    int count();

    @Delete("DELETE FROM paimon_catalog.paimon_op_log " + "WHERE created_at < #{cutoffMillis}")
    int deleteOlderThan(@Param("cutoffMillis") long cutoffMillis);
}
