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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.server.auth.AuthChannelHandler;
import org.apache.paimon.rest.server.auth.AuthContext;
import org.apache.paimon.rest.server.auth.TokenAuthenticator;
import org.apache.paimon.rest.server.metadata.JdbcMetadataStore;
import org.apache.paimon.rest.server.metadata.MetadataStore;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST Catalog Server entry point. Starts a Netty HTTP server backed by a FileSystemCatalog. */
public class RESTCatalogServer {

    private static final Logger LOG = LoggerFactory.getLogger(RESTCatalogServer.class);

    private final Options options;
    private Catalog catalog;
    private MetadataStore metadataStore;
    private HttpServer httpServer;

    public RESTCatalogServer(Options options) {
        this.options = options;
        this.catalog = null;
    }

    /** Constructor with a pre-built catalog, useful for testing. */
    public RESTCatalogServer(Options options, Catalog catalog) {
        this.options = options;
        this.catalog = catalog;
    }

    public void start() throws Exception {
        String warehouse = options.get(CatalogOptions.WAREHOUSE);
        if (warehouse == null) {
            throw new IllegalArgumentException(
                    "warehouse is required. Set it via --warehouse or "
                            + CatalogOptions.WAREHOUSE.key());
        }

        if (this.catalog == null) {
            if (!options.containsKey(CatalogOptions.METASTORE.key())) {
                throw new IllegalArgumentException(
                        "metastore is required. Set it via --"
                                + CatalogOptions.METASTORE.key()
                                + " (e.g. filesystem, hive, jdbc).");
            }
            CatalogContext catalogContext = CatalogContext.create(options);
            this.catalog = CatalogFactory.createCatalog(catalogContext);
        }

        String host = options.get(RESTCatalogServerOptions.HOST);
        int port = options.get(RESTCatalogServerOptions.PORT);
        String prefix = options.get(RESTCatalogServerOptions.PREFIX);
        int ioThreads = options.get(RESTCatalogServerOptions.IO_THREADS);
        int workerThreads = options.get(RESTCatalogServerOptions.WORKER_THREADS);
        int maxContentLength = options.get(RESTCatalogServerOptions.MAX_CONTENT_LENGTH);

        String authUrl = options.get(RESTCatalogServerOptions.AUTH_URL);
        TokenAuthenticator authenticator = createAuthenticator(authUrl);

        AuthChannelHandler authHandler = new AuthChannelHandler(authenticator);
        this.metadataStore = createMetadataStore();
        RouteDispatcher dispatcher = new RouteDispatcher(catalog, prefix, warehouse, metadataStore);
        HttpRequestHandler handler = new HttpRequestHandler(dispatcher);
        this.httpServer =
                new HttpServer(
                        host,
                        port,
                        ioThreads,
                        workerThreads,
                        maxContentLength,
                        authHandler,
                        handler);
        this.httpServer.start();

        LOG.info("REST Catalog Server started. Warehouse: {}", warehouse);
    }

    public void shutdown() {
        if (httpServer != null) {
            httpServer.shutdown();
        }
        if (metadataStore != null) {
            try {
                metadataStore.close();
            } catch (Exception e) {
                LOG.warn("Error closing metadata store", e);
            }
        }
        if (catalog != null) {
            try {
                catalog.close();
            } catch (Exception e) {
                LOG.warn("Error closing catalog", e);
            }
        }
    }

    public int getPort() {
        return httpServer != null ? httpServer.getPort() : -1;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    private TokenAuthenticator createAuthenticator(String authUrl) {
        if (authUrl == null || authUrl.isEmpty()) {
            LOG.info("Authentication disabled (rest-server.auth.url not set)");
            return token -> AuthContext.ANONYMOUS;
        }
        // TODO: implement HTTP-based token forwarding to authUrl
        // The flow: POST token to authUrl, parse response into AuthContext
        LOG.info("Authentication enabled, auth service URL: {}", authUrl);
        throw new UnsupportedOperationException(
                "External auth service not yet implemented. URL: " + authUrl);
    }

    private MetadataStore createMetadataStore() {
        String jdbcUrl = options.getString(RESTCatalogServerOptions.METADATA_JDBC_URL);
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            return null;
        }
        String username = options.getString(RESTCatalogServerOptions.METADATA_JDBC_USER);
        String password = options.getString(RESTCatalogServerOptions.METADATA_JDBC_PASSWORD);
        int maxPoolSize = options.get(RESTCatalogServerOptions.METADATA_POOL_MAX_SIZE);
        int minIdle = options.get(RESTCatalogServerOptions.METADATA_POOL_MIN_IDLE);
        long connectionTimeout =
                options.get(RESTCatalogServerOptions.METADATA_POOL_CONNECTION_TIMEOUT);

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username != null ? username : "");
        config.setPassword(password != null ? password : "");
        config.setMaximumPoolSize(maxPoolSize);
        config.setMinimumIdle(minIdle);
        config.setConnectionTimeout(connectionTimeout);
        config.setPoolName("paimon-metadata");

        LOG.info("Metadata store enabled with JDBC URL: {}", jdbcUrl);
        return new JdbcMetadataStore(new HikariDataSource(config));
    }

    public static void main(String[] args) throws Exception {
        Options options = parseArgs(args);
        RESTCatalogServer server = new RESTCatalogServer(options);

        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    LOG.info("Shutdown hook triggered");
                                    server.shutdown();
                                }));

        server.start();

        // Block main thread
        Thread.currentThread().join();
    }

    private static Options parseArgs(String[] args) {
        Options options = new Options();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--") && i + 1 < args.length) {
                String key = arg.substring(2);
                String value = args[i + 1];
                options.setString(key, value);
                i++;
            } else if (arg.contains("=")) {
                String[] parts = arg.split("=", 2);
                String key = parts[0];
                if (key.startsWith("--")) {
                    key = key.substring(2);
                }
                options.setString(key, parts[1]);
            }
        }
        return options;
    }
}
