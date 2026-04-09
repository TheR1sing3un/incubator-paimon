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

package org.apache.paimon.spark.dataset;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.spark.SparkTable;
import org.apache.paimon.spark.catalog.SparkBaseCatalog;
import org.apache.paimon.spark.dataset.model.ConfigResponse;
import org.apache.paimon.spark.dataset.model.DatasetInfo;
import org.apache.paimon.spark.dataset.model.NamespaceInfo;
import org.apache.paimon.table.Table;

import org.apache.spark.sql.PaimonSparkSession$;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.spark.util.OptionUtils.copyWithSQLConf;

/**
 * Spark Catalog that maps logical dataset names to physical Paimon tables.
 *
 * <p>Configuration (aligned with Paimon conventions):
 *
 * <pre>{@code
 * # Paimon catalog (standard, already configured)
 * spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog
 * spark.sql.catalog.paimon.metastore=rest
 * spark.sql.catalog.paimon.uri=http://kling-paimon-rest-catalog:26754
 * spark.sql.catalog.paimon.warehouse=viewfs://hadoop-lt-cluster/hudi/paimon/
 * spark.sql.catalog.paimon.token.provider=noop
 *
 * # Dataset catalog — only dataset-specific options needed.
 * # All other Paimon options (token.provider, etc.) are inherited from the paimon catalog.
 * spark.sql.catalog.dataset=org.apache.paimon.spark.dataset.DatasetCatalog
 * spark.sql.catalog.dataset.uri=http://dataset-catalog:8080
 * # Optional: auth-token, user-id, context-path, paimon-catalog-name
 * }</pre>
 *
 * <p>Usage: {@code SELECT * FROM dataset.namespace.dataset_name}
 *
 * <p>Workflow: logical name → dataset-catalog REST API → physical Paimon table location → load via
 * internal Paimon Catalog.
 *
 * <p>Paimon option resolution order (highest to lowest priority):
 *
 * <ol>
 *   <li>Explicit user overrides via {@code paimon.*} options (e.g., {@code
 *       spark.sql.catalog.dataset.paimon.token.provider=bear})
 *   <li>Connection info from dataset-catalog {@code /config} endpoint (uri, prefix, warehouse)
 *   <li>Inherited defaults from reference Paimon catalog (configurable via {@code
 *       paimon-catalog-name}, default: "paimon")
 *   <li>Fallback: {@code token.provider=noop}
 * </ol>
 */
public class DatasetCatalog extends SparkBaseCatalog implements SupportsNamespaces {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetCatalog.class);

    private Catalog paimonCatalog;
    private DatasetRestClient client;
    private String defaultDatabase;

    /** Default constructor used by Spark via reflection. */
    public DatasetCatalog() {}

    /** Package-private constructor for testing. */
    DatasetCatalog(
            String name, Catalog paimonCatalog, DatasetRestClient client, String defaultDatabase) {
        this.catalogName = name;
        this.paimonCatalog = paimonCatalog;
        this.client = client;
        this.defaultDatabase = defaultDatabase;
    }

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        this.catalogName = name;

        // 1. Create dataset-catalog REST client
        String uri = options.get("uri");
        if (uri == null || uri.isEmpty()) {
            throw new IllegalArgumentException(
                    "DatasetCatalog requires 'uri' option pointing to the dataset-catalog REST API");
        }
        String authToken = options.getOrDefault("auth-token", "");
        String userId = options.getOrDefault("user-id", "");
        String contextPath =
                options.getOrDefault("context-path", DatasetRestClient.DEFAULT_CONTEXT_PATH);
        this.client = new DatasetRestClient(uri, contextPath, authToken, userId);

        // 2. Resolve Paimon catalog options.
        //    Priority: user paimon.* overrides > /config endpoint > inherited from ref catalog
        //    Connection options (metastore, uri, prefix, warehouse) are NOT inherited —
        //    they must come from /config to avoid accidentally connecting to a wrong Paimon
        // service.
        SparkSession sparkSession = PaimonSparkSession$.MODULE$.active();

        //    2a. Collect user-provided "paimon.*" overrides (highest priority)
        Map<String, String> userPaimonOpts = new HashMap<>();
        for (Map.Entry<String, String> entry : options.asCaseSensitiveMap().entrySet()) {
            if (entry.getKey().startsWith("paimon.")) {
                userPaimonOpts.put(entry.getKey().substring("paimon.".length()), entry.getValue());
            }
        }

        Map<String, String> paimonOpts = new HashMap<>();

        //    2b. Inherit non-connection options from reference Paimon catalog (e.g.,
        // token.provider)
        String refCatalogName = options.getOrDefault("paimon-catalog-name", "paimon");
        String refPrefix = "spark.sql.catalog." + refCatalogName + ".";
        Set<String> connectionKeys =
                new HashSet<>(Arrays.asList("metastore", "uri", "prefix", "warehouse"));
        for (scala.Tuple2<String, String> kv : sparkSession.sparkContext().conf().getAll()) {
            if (kv._1().startsWith(refPrefix)) {
                String key = kv._1().substring(refPrefix.length());
                if (!connectionKeys.contains(key)) {
                    paimonOpts.put(key, kv._2());
                }
            }
        }
        LOG.info(
                "DatasetCatalog '{}' inherited {} options from catalog '{}'",
                name,
                paimonOpts.size(),
                refCatalogName);

        //    2c. Fetch connection config from /config endpoint (skip if user provides direct
        // config)
        boolean hasDirectPaimonConfig =
                userPaimonOpts.containsKey("metastore")
                        || userPaimonOpts.containsKey("uri")
                        || userPaimonOpts.containsKey("warehouse");
        if (!hasDirectPaimonConfig) {
            ConfigResponse config = client.getConfig();
            LOG.info(
                    "DatasetCatalog '{}' /config response: uri={}, prefix={}, warehouse={}",
                    name,
                    config.getPaimonRestUri(),
                    config.getPaimonRestPrefix(),
                    config.getPaimonRestWarehouse());
            paimonOpts.put("metastore", "rest");
            paimonOpts.put("uri", config.getPaimonRestUri());
            if (config.getPaimonRestPrefix() != null) {
                paimonOpts.put("prefix", config.getPaimonRestPrefix());
            }
            if (config.getPaimonRestWarehouse() != null) {
                paimonOpts.put("warehouse", config.getPaimonRestWarehouse());
            }
        }

        //    2d. Fallback: default token.provider to "noop" if not set anywhere
        paimonOpts.putIfAbsent("token.provider", "noop");

        //    2e. User paimon.* overrides always win
        paimonOpts.putAll(userPaimonOpts);
        LOG.info("DatasetCatalog '{}' final paimon options: {}", name, paimonOpts);

        // 3. Create internal Paimon Catalog for loading physical tables
        CatalogContext ctx =
                CatalogContext.create(
                        Options.fromMap(paimonOpts), sparkSession.sessionState().newHadoopConf());
        this.paimonCatalog = CatalogFactory.createCatalog(ctx);

        this.defaultDatabase = options.getOrDefault("defaultDatabase", "default");
    }

    @Override
    public Catalog paimonCatalog() {
        return paimonCatalog;
    }

    @Override
    public String paimonCatalogName() {
        return catalogName;
    }

    // ======================== Table Operations ========================

    @Override
    public org.apache.spark.sql.connector.catalog.Table loadTable(Identifier ident)
            throws NoSuchTableException {
        return loadDatasetTable(ident, Collections.emptyMap());
    }

    /**
     * Time travel by version string.
     *
     * <p>SQL: {@code SELECT * FROM dataset.ns.name VERSION AS OF 1}
     */
    public SparkTable loadTable(Identifier ident, String version) throws NoSuchTableException {
        LOG.info("Time travel to version '{}'.", version);
        return loadDatasetTable(
                ident, Collections.singletonMap(CoreOptions.SCAN_VERSION.key(), version));
    }

    /**
     * Time travel by timestamp (microseconds from Spark, converted to milliseconds for Paimon).
     *
     * <p>SQL: {@code SELECT * FROM dataset.ns.name TIMESTAMP AS OF '2024-01-01'}
     */
    public SparkTable loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
        // Spark passes microseconds, Paimon uses milliseconds
        timestamp = timestamp / 1000;
        LOG.info("Time travel target timestamp is {} milliseconds.", timestamp);
        return loadDatasetTable(
                ident,
                Collections.singletonMap(
                        CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), String.valueOf(timestamp)));
    }

    /**
     * Loads a dataset table by resolving the logical name to a physical Paimon table.
     *
     * <p>Steps: (1) parse logical identifier → (2) REST API lookup → (3) construct Paimon
     * identifier with branch → (4) load from Paimon catalog → (5) wrap as SparkTable.
     */
    private SparkTable loadDatasetTable(Identifier ident, Map<String, String> extraOptions)
            throws NoSuchTableException {
        // 1. Parse logical identifier
        DatasetIdentifier dsIdent = DatasetIdentifier.of(ident);

        // 2. REST API: logical name → physical location
        DatasetInfo info =
                client.getDatasetByName(dsIdent.getNamespace(), dsIdent.getDatasetName());
        LOG.debug(
                "Resolved dataset '{}' to physical table: {}.{}",
                dsIdent,
                info.getDatabaseName(),
                info.getTableName());

        // 3. Construct Paimon identifier (pass through $suffix as-is, e.g., $branch_xxx,
        // $snapshots)
        String objectName = info.getTableName();
        if (dsIdent.hasSuffix()) {
            objectName = objectName + "$" + dsIdent.getSuffix();
        }
        org.apache.paimon.catalog.Identifier paimonId =
                org.apache.paimon.catalog.Identifier.create(info.getDatabaseName(), objectName);

        // 4. Load from Paimon catalog
        try {
            Table table = paimonCatalog.getTable(paimonId);

            // 5. Merge SQL conf and extra options
            table = copyWithSQLConf(table, catalogName, paimonId, extraOptions);

            // 6. Return SparkTable
            return new SparkTable(table);
        } catch (Catalog.TableNotExistException e) {
            throw new NoSuchTableException(ident);
        }
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        if (namespace == null || namespace.length == 0) {
            throw new NoSuchNamespaceException(new String[] {"<empty>"});
        }
        try {
            List<DatasetInfo> datasets = client.listDatasets(namespace[0]);
            return datasets.stream()
                    .map(ds -> Identifier.of(namespace, ds.getDatasetName()))
                    .toArray(Identifier[]::new);
        } catch (Exception e) {
            throw new NoSuchNamespaceException(namespace);
        }
    }

    @Override
    public boolean tableExists(Identifier ident) {
        try {
            DatasetIdentifier dsIdent = DatasetIdentifier.of(ident);
            return client.datasetExists(dsIdent.getNamespace(), dsIdent.getDatasetName());
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void invalidateTable(Identifier ident) {
        // No caching to invalidate in the dataset catalog itself.
        // The underlying Paimon catalog handles its own caching.
    }

    // ======================== Namespace Operations ========================

    @Override
    public String[] defaultNamespace() {
        return new String[] {defaultDatabase};
    }

    @Override
    public String[][] listNamespaces() {
        List<NamespaceInfo> namespaces = client.listNamespaces();
        String[][] result = new String[namespaces.size()][];
        for (int i = 0; i < namespaces.size(); i++) {
            result[i] = new String[] {namespaces.get(i).getName()};
        }
        return result;
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        if (namespace.length == 0) {
            return listNamespaces();
        }
        if (!client.namespaceExists(namespace[0])) {
            throw new NoSuchNamespaceException(namespace);
        }
        return new String[0][];
    }

    @Override
    public boolean namespaceExists(String[] namespace) {
        if (namespace == null || namespace.length == 0) {
            return false;
        }
        return client.namespaceExists(namespace[0]);
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace)
            throws NoSuchNamespaceException {
        if (!namespaceExists(namespace)) {
            throw new NoSuchNamespaceException(namespace);
        }
        return Collections.emptyMap();
    }

    // ======================== Read-Only: Unsupported Write Operations ========================

    @Override
    public org.apache.spark.sql.connector.catalog.Table createTable(
            Identifier ident,
            StructType schema,
            Transform[] partitions,
            Map<String, String> properties) {
        throw new UnsupportedOperationException(
                "DatasetCatalog is read-only. Cannot create table: " + ident);
    }

    @Override
    public org.apache.spark.sql.connector.catalog.Table alterTable(
            Identifier ident, TableChange... changes) {
        throw new UnsupportedOperationException(
                "DatasetCatalog is read-only. Cannot alter table: " + ident);
    }

    @Override
    public boolean dropTable(Identifier ident) {
        throw new UnsupportedOperationException(
                "DatasetCatalog is read-only. Cannot drop table: " + ident);
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent) {
        throw new UnsupportedOperationException(
                "DatasetCatalog is read-only. Cannot rename table: " + oldIdent);
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata) {
        throw new UnsupportedOperationException(
                "DatasetCatalog is read-only. Cannot create namespace.");
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes) {
        throw new UnsupportedOperationException(
                "DatasetCatalog is read-only. Cannot alter namespace.");
    }

    public boolean dropNamespace(String[] namespace) {
        throw new UnsupportedOperationException(
                "DatasetCatalog is read-only. Cannot drop namespace.");
    }

    public boolean dropNamespace(String[] namespace, boolean cascade) {
        throw new UnsupportedOperationException(
                "DatasetCatalog is read-only. Cannot drop namespace.");
    }
}
