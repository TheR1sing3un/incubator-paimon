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
import org.apache.paimon.catalog.RESTFileSystemCatalog;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.server.auth.AuthChannelHandler;
import org.apache.paimon.rest.server.auth.AuthContext;
import org.apache.paimon.rest.server.auth.TokenAuthenticator;
import org.apache.paimon.rest.server.metadata.JdbcMetadataStore;
import org.apache.paimon.rest.server.metadata.MetadataStore;
import org.apache.paimon.rest.server.utils.MetricsFileIO;
import org.apache.paimon.rest.server.utils.PerfUtil;

import com.kuaishou.infra.framework.datasource.KsDataSourceFactory;
import com.kuaishou.kling.lakehouse.metrics.MetricsConfig;
import com.kuaishou.kling.lakehouse.metrics.MetricsReporter;
import com.kuaishou.kling.lakehouse.metrics.filter.CallerRegistry;
import com.kuaishou.kling.lakehouse.metrics.jvm.JvmMetricsCollector;
import com.kuaishou.kling.lakehouse.metrics.pool.ConnectionPoolMetricsCollector;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** REST Catalog Server entry point. Starts a Netty HTTP server backed by a FileSystemCatalog. */
public class RESTCatalogServer {

    private static final Logger LOG = LoggerFactory.getLogger(RESTCatalogServer.class);

    private final Options options;
    private Catalog catalog;
    private MetadataStore metadataStore;
    private HttpServer httpServer;
    private CallerRegistry callerRegistry;
    private JvmMetricsCollector jvmCollector;
    private ConnectionPoolMetricsCollector poolCollector;
    @Nullable private HikariDataSource hikariDataSource;
    @Nullable private DataSource metadataDataSource;
    @Nullable private ScheduledExecutorService metricsHealthChecker;

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
        // Set HADOOP_USER_NAME early, before any Hadoop FileSystem is created
        String hadoopUserName = options.getString(RESTCatalogServerOptions.HADOOP_USER_NAME);
        if (hadoopUserName != null && !hadoopUserName.isEmpty()) {
            System.setProperty("HADOOP_USER_NAME", hadoopUserName);
            LOG.info("HADOOP_USER_NAME set to: {}", hadoopUserName);
        }

        String warehouse = options.get(CatalogOptions.WAREHOUSE);
        if (warehouse == null) {
            throw new IllegalArgumentException(
                    "warehouse is required. Set it via --warehouse or "
                            + CatalogOptions.WAREHOUSE.key());
        }

        String host = options.get(RESTCatalogServerOptions.HOST);
        String autoPort = System.getenv("AUTO_PORT0");
        int port =
                autoPort != null && !autoPort.isEmpty()
                        ? Integer.parseInt(autoPort)
                        : options.get(RESTCatalogServerOptions.PORT);

        // Initialize lakehouse-metrics
        String localHostname;
        try {
            localHostname = java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to resolve local IP for metrics", e);
        }
        String projectVersion = RESTCatalogServer.class.getPackage().getImplementationVersion();
        if (projectVersion == null || projectVersion.isEmpty()) {
            projectVersion = "dev";
        }

        // Initialize metrics infrastructure (must precede all metric-emitting code)
        initMetrics(localHostname, port, projectVersion);
        initCallerRegistry();

        if (this.catalog == null) {
            CatalogContext catalogContext = CatalogContext.create(options);
            FileIO fileIO = new MetricsFileIO(FileIO.get(new Path(warehouse), catalogContext));
            this.catalog = new RESTFileSystemCatalog(fileIO, new Path(warehouse), catalogContext);
        }

        String prefix = options.get(RESTCatalogServerOptions.PREFIX);
        int bossThreads = options.get(RESTCatalogServerOptions.BOSS_THREADS);
        int ioThreads = options.get(RESTCatalogServerOptions.IO_THREADS);
        int workerThreads = options.get(RESTCatalogServerOptions.WORKER_THREADS);
        int maxContentLength = options.get(RESTCatalogServerOptions.MAX_CONTENT_LENGTH);
        int soBacklog = options.get(RESTCatalogServerOptions.SO_BACKLOG);
        Integer soRcvBuf = options.getOptional(RESTCatalogServerOptions.SO_RCVBUF).orElse(null);

        String authUrl = options.get(RESTCatalogServerOptions.AUTH_URL);
        TokenAuthenticator authenticator = createAuthenticator(authUrl);
        AuthChannelHandler authHandler = new AuthChannelHandler(authenticator);

        this.metadataStore = createMetadataStore();

        // Start collectors (after metadata store, so hikariDataSource is available)
        startCollectors();

        RouteDispatcher dispatcher =
                new RouteDispatcher(catalog, prefix, warehouse, metadataStore, callerRegistry);
        boolean frontendEnabled = options.get(RESTCatalogServerOptions.FRONTEND_ENABLED);
        HttpRequestHandler handler = new HttpRequestHandler(dispatcher, frontendEnabled);
        this.httpServer =
                new HttpServer(
                        host,
                        port,
                        bossThreads,
                        ioThreads,
                        workerThreads,
                        maxContentLength,
                        soBacklog,
                        soRcvBuf,
                        authHandler,
                        handler);
        this.httpServer.start();

        LOG.info("REST Catalog Server started. Warehouse: {}", warehouse);
    }

    public void shutdown() {
        if (httpServer != null) {
            httpServer.shutdown();
        }
        if (metricsHealthChecker != null) {
            metricsHealthChecker.shutdownNow();
        }
        if (poolCollector != null) {
            try {
                poolCollector.stop();
            } catch (Exception e) {
                LOG.warn("Error stopping ConnectionPoolMetricsCollector", e);
            }
        }
        if (jvmCollector != null) {
            try {
                jvmCollector.stop();
            } catch (Exception e) {
                LOG.warn("Error stopping JvmMetricsCollector", e);
            }
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

    private void initMetrics(String localHostname, int port, String projectVersion) {
        try {
            MetricsConfig metricsConfig =
                    buildMetricsConfig(
                            options,
                            localHostname,
                            port,
                            projectVersion,
                            System.getenv("KWS_SERVICE_NAME"),
                            System.getenv("GRAY_GROUP_NAME"),
                            System.getenv("KCS_IMAGE_VERSION"),
                            System.getenv("MY_POD_NAME"));
            MetricsReporter.init(metricsConfig);
            PerfUtil.setNamespace(metricsConfig.getNamespace());
            LOG.info(
                    "MetricsReporter initialized: service={}, cluster={}, namespace={}, "
                            + "deployGroup={}, version={}, confVersion={}",
                    metricsConfig.getService(),
                    metricsConfig.getCluster(),
                    metricsConfig.getNamespace(),
                    metricsConfig.getDeployGroup(),
                    metricsConfig.getVersion(),
                    metricsConfig.getConfVersion());
        } catch (Exception e) {
            LOG.warn("Failed to initialize MetricsReporter, metrics will be unavailable", e);
        }
    }

    /**
     * Build a {@link MetricsConfig} from config options, with optional environment variable
     * overrides. Aligned with {@code DatasetCatalogServer}: namespace is {@code
     * "{cluster}.{METRICS_NAMESPACE}"}, deployGroup prefers {@code GRAY_GROUP_NAME} then {@code
     * KCS_IMAGE_VERSION} then config, and version prefers {@code KCS_IMAGE_VERSION} then {@code
     * projectVersion}. This method is pure (no side effects) and package-visible for testing.
     */
    static MetricsConfig buildMetricsConfig(
            Options options,
            String localHostname,
            int port,
            String projectVersion,
            @Nullable String envCluster,
            @Nullable String envGrayGroup,
            @Nullable String envDeployGroup,
            @Nullable String envPodName) {
        String cluster =
                envCluster != null
                        ? envCluster.replace(".", "_")
                        : options.get(RESTCatalogServerOptions.METRICS_CLUSTER);

        String service = options.get(RESTCatalogServerOptions.METRICS_SERVICE);
        String namespace = cluster + ".kling.paimon.catalog";

        String deployGroup;
        if (envGrayGroup != null) {
            deployGroup = envGrayGroup;
        } else if (envDeployGroup != null) {
            deployGroup = envDeployGroup;
        } else {
            deployGroup = options.get(RESTCatalogServerOptions.METRICS_DEPLOY_GROUP);
        }

        String version = envDeployGroup != null ? envDeployGroup : projectVersion;

        String confVersion = options.get(RESTCatalogServerOptions.METRICS_CONF_VERSION);

        return MetricsConfig.builder()
                .service(service)
                .cluster(cluster)
                .host(localHostname)
                .port(String.valueOf(port))
                .deployGroup(deployGroup)
                .version(version)
                .podName(envPodName != null ? envPodName : "")
                .namespace(namespace)
                .confVersion(confVersion)
                .build();
    }

    private void initCallerRegistry() {
        String callerList = options.get(RESTCatalogServerOptions.METRICS_CALLER_REGISTRY);
        if (callerList != null && !callerList.isEmpty()) {
            Set<String> knownCallers =
                    Arrays.stream(callerList.split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .collect(Collectors.toSet());
            this.callerRegistry = new CallerRegistry(knownCallers);
            LOG.info(
                    "CallerRegistry initialized with {} known callers: {}",
                    knownCallers.size(),
                    knownCallers);
        } else {
            this.callerRegistry = new CallerRegistry(Collections.emptySet());
            LOG.info("CallerRegistry initialized with empty caller list");
        }
    }

    private void startCollectors() {
        try {
            this.jvmCollector = new JvmMetricsCollector();
            this.jvmCollector.start();
            LOG.info("JvmMetricsCollector started");
        } catch (Exception e) {
            LOG.warn("Failed to start JvmMetricsCollector", e);
        }

        if (this.metadataDataSource != null) {
            try {
                this.poolCollector = new ConnectionPoolMetricsCollector();
                if (this.metadataDataSource instanceof HikariDataSource) {
                    this.poolCollector.register((HikariDataSource) this.metadataDataSource);
                } else {
                    this.poolCollector.registerLazy(this.metadataDataSource);
                }
                this.poolCollector.start();
                LOG.info(
                        "ConnectionPoolMetricsCollector started for DataSource: {}",
                        this.metadataDataSource.getClass().getSimpleName());
            } catch (Exception e) {
                LOG.warn("Failed to start ConnectionPoolMetricsCollector", e);
            }
        } else {
            LOG.info("No DataSource available, skipping ConnectionPoolMetricsCollector");
        }

        // Periodic metrics health check
        this.metricsHealthChecker =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "metrics-health-check");
                            t.setDaemon(true);
                            return t;
                        });
        this.metricsHealthChecker.scheduleAtFixedRate(
                this::logMetricsHealth, 1, 5, TimeUnit.MINUTES);
        LOG.info("Metrics health checker started (every 5 minutes)");
    }

    private void logMetricsHealth() {
        try {
            long errorCount = MetricsReporter.getReportErrorCount();
            MetricsConfig config = MetricsReporter.getConfig();
            if (config != null) {
                LOG.info(
                        "Metrics health: namespace={}, service={}, cluster={}, "
                                + "reportErrorCount={}, jvmCollector={}, poolCollector={}",
                        config.getNamespace(),
                        config.getService(),
                        config.getCluster(),
                        errorCount,
                        jvmCollector != null ? "running" : "off",
                        poolCollector != null ? "running" : "off");
            } else {
                LOG.warn("Metrics health: MetricsReporter not initialized (config is null)");
            }
        } catch (Exception e) {
            LOG.warn("Failed to log metrics health", e);
        }
    }

    private MetadataStore createMetadataStore() {
        int retentionDays = options.get(RESTCatalogServerOptions.OP_LOG_RETENTION_DAYS);

        // Priority 1: KsDataSource resource ID
        String resourceId = options.getString(RESTCatalogServerOptions.METADATA_RESOURCE_ID);
        if (resourceId != null && !resourceId.isEmpty()) {
            LOG.info("Metadata store enabled with KsDataSource resource ID: {}", resourceId);
            DataSource dataSource = KsDataSourceFactory.getDataSource(resourceId);
            this.metadataDataSource = dataSource;
            return new JdbcMetadataStore(dataSource, retentionDays);
        }

        // Priority 2: JDBC URL with HikariCP (for local dev / testing)
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
        HikariDataSource ds = new HikariDataSource(config);
        this.hikariDataSource = ds;
        this.metadataDataSource = ds;
        return new JdbcMetadataStore(ds, retentionDays);
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
        Map<String, String> cliMap = new LinkedHashMap<>();
        String configFile = null;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--") && i + 1 < args.length) {
                String key = arg.substring(2);
                String value = args[i + 1];
                if ("config".equals(key)) {
                    configFile = value;
                } else {
                    cliMap.put(key, value);
                }
                i++;
            } else if (arg.contains("=")) {
                String[] parts = arg.split("=", 2);
                String key = parts[0];
                if (key.startsWith("--")) {
                    key = key.substring(2);
                }
                if ("config".equals(key)) {
                    configFile = parts[1];
                } else {
                    cliMap.put(key, parts[1]);
                }
            }
        }

        Map<String, String> fileMap = Collections.emptyMap();
        if (configFile != null) {
            fileMap = loadConfigFile(configFile);
            LOG.info("Loaded configuration from file: {}", configFile);
        }

        // File first, CLI overrides (Options(map1, map2) applies map2 after map1)
        return new Options(fileMap, cliMap);
    }

    static Map<String, String> loadConfigFile(String path) {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(path)) {
            props.load(fis);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to load config file: " + path, e);
        }
        Map<String, String> map = new LinkedHashMap<>();
        for (String name : props.stringPropertyNames()) {
            map.put(name, props.getProperty(name));
        }
        return map;
    }
}
