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

package org.apache.paimon.rest.server;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;

/** Configuration options for the REST Catalog Server. */
public class RESTCatalogServerOptions {

    public static final ConfigOption<String> HOST =
            ConfigOptions.key("rest-server.host")
                    .stringType()
                    .defaultValue("0.0.0.0")
                    .withDescription("The host address to bind the REST server.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("rest-server.port")
                    .intType()
                    .defaultValue(8080)
                    .withDescription("The port to bind the REST server.");

    public static final ConfigOption<String> PREFIX =
            ConfigOptions.key("rest-server.prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The URL path prefix for all REST API endpoints.");

    public static final ConfigOption<Integer> IO_THREADS =
            ConfigOptions.key("rest-server.io-threads")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Number of Netty event loop threads.");

    public static final ConfigOption<Integer> WORKER_THREADS =
            ConfigOptions.key("rest-server.worker-threads")
                    .intType()
                    .defaultValue(16)
                    .withDescription("Number of worker threads for processing requests.");

    public static final ConfigOption<Integer> MAX_CONTENT_LENGTH =
            ConfigOptions.key("rest-server.max-content-length")
                    .intType()
                    .defaultValue(10 * 1024 * 1024)
                    .withDescription("Maximum allowed content length for HTTP requests in bytes.");

    public static final ConfigOption<Integer> SO_BACKLOG =
            ConfigOptions.key("rest-server.so-backlog")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "The maximum length of the TCP accept queue. "
                                    + "Controls how many pending connections can wait "
                                    + "when all worker threads are busy.");

    public static final ConfigOption<String> METADATA_RESOURCE_ID =
            ConfigOptions.key("rest-server.metadata.resource-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "KsDataSource resource ID for the metadata store. "
                                    + "When set, uses KsDataSourceFactory.getDataSource(resourceId) "
                                    + "instead of HikariCP with JDBC URL. "
                                    + "Takes priority over rest-server.metadata.jdbc-url.");

    public static final ConfigOption<String> METADATA_JDBC_URL =
            ConfigOptions.key("rest-server.metadata.jdbc-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "JDBC URL for the metadata store (e.g. MySQL). "
                                    + "When set, Git-style commit endpoints are enabled.");

    public static final ConfigOption<String> METADATA_JDBC_USER =
            ConfigOptions.key("rest-server.metadata.jdbc-user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("JDBC username for the metadata store.");

    public static final ConfigOption<String> METADATA_JDBC_PASSWORD =
            ConfigOptions.key("rest-server.metadata.jdbc-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("JDBC password for the metadata store.");

    public static final ConfigOption<Integer> METADATA_POOL_MAX_SIZE =
            ConfigOptions.key("rest-server.metadata.pool.max-size")
                    .intType()
                    .defaultValue(10)
                    .withDescription("Maximum number of connections in the metadata store pool.");

    public static final ConfigOption<Integer> METADATA_POOL_MIN_IDLE =
            ConfigOptions.key("rest-server.metadata.pool.min-idle")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "Minimum number of idle connections in the metadata store pool.");

    public static final ConfigOption<Long> METADATA_POOL_CONNECTION_TIMEOUT =
            ConfigOptions.key("rest-server.metadata.pool.connection-timeout-ms")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription(
                            "Maximum time in milliseconds to wait for a connection from the pool.");

    public static final ConfigOption<Boolean> FRONTEND_ENABLED =
            ConfigOptions.key("rest-server.frontend.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to serve the frontend Web UI from classpath static resources.");

    public static final ConfigOption<String> AUTH_URL =
            ConfigOptions.key("rest-server.auth.url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "URL of the external authentication service. "
                                    + "When set, each request's token (from the Authorization header) "
                                    + "is forwarded to this URL for validation. "
                                    + "When not set, authentication is disabled (all requests allowed).");

    public static final ConfigOption<Integer> OP_LOG_RETENTION_DAYS =
            ConfigOptions.key("rest-server.metadata.op-log.retention-days")
                    .intType()
                    .defaultValue(30)
                    .withDescription(
                            "Number of days to retain audit log entries in paimon_op_log. "
                                    + "Entries older than this are periodically deleted. "
                                    + "Set to -1 to disable automatic cleanup.");

    public static final ConfigOption<String> HADOOP_USER_NAME =
            ConfigOptions.key("rest-server.hadoop-user-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Hadoop user name used to access HDFS. "
                                    + "When set, the HADOOP_USER_NAME environment variable "
                                    + "will be set at server startup.");

    private RESTCatalogServerOptions() {}
}
