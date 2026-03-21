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

package org.apache.paimon.query.sdk;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.query.RemoteTableQuery;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Client for remote Paimon table lookups. */
public final class PaimonRemoteLookupClient implements AutoCloseable {

    private static final String HADOOP_CONF_DIR_PROP = "paimon.hadoop.conf.dir";
    private static final String HADOOP_CONF_DIR_ENV = "PAIMON_HADOOP_CONF_DIR";
    private static final String HIVE_CONF_DIR_PROP = "paimon.hive.conf.dir";
    private static final String HIVE_CONF_DIR_ENV = "PAIMON_HIVE_CONF_DIR";
    private static final String WAREHOUSE_PROP = "paimon.warehouse";
    private static final String WAREHOUSE_ENV = "PAIMON_WAREHOUSE";
    private static final String METASTORE_URI_PROP = "paimon.metastore.uri";
    private static final String METASTORE_URI_ENV = "PAIMON_METASTORE_URI";

    private static final String HADOOP_CONF_RESOURCE_DIR = "hadoop-conf";
    private static final String DEFAULT_METASTORE_URI = "thrift://lt.ks.metastore.internal:9083";
    private static final String DEFAULT_WAREHOUSE =
            "viewfs://hadoop-lt-cluster/home/hdp/tmp/tmp/paimon";

    private static volatile String embeddedHadoopConfDir;

    private final Catalog catalog;
    private final Table table;

    private PaimonRemoteLookupClient(Catalog catalog, Table table) {
        this.catalog = catalog;
        this.table = table;
    }

    public static PaimonRemoteLookupClient openHive(String db, String tableName) throws Exception {
        Objects.requireNonNull(db, "db must not be null");
        Objects.requireNonNull(tableName, "tableName must not be null");

        String hadoopConfDir = resolveHadoopConfDir();
        String hiveConfDir = resolveHiveConfDir();
        String metastoreUri =
                resolveOptionalWithDefault(
                        METASTORE_URI_PROP, METASTORE_URI_ENV, DEFAULT_METASTORE_URI);
        String warehouse =
                resolveOptionalWithDefault(WAREHOUSE_PROP, WAREHOUSE_ENV, DEFAULT_WAREHOUSE);

        Options options = new Options();
        options.set("metastore", "hive");
        options.set("cache.manifest.small-file-threshold", "16 MB");
        options.set("hadoop-conf-dir", hadoopConfDir);
        options.set("warehouse", warehouse);
        if (hiveConfDir != null) {
            options.set("hive-conf-dir", hiveConfDir);
        }
        options.set("uri", metastoreUri);

        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        Table table;
        try {
            table = catalog.getTable(Identifier.create(db, tableName));
        } catch (Catalog.TableNotExistException e) {
            catalog.close();
            throw new IllegalArgumentException("Table not exist: " + db + "." + tableName, e);
        }

        if (!(table instanceof FileStoreTable)) {
            catalog.close();
            throw new IllegalStateException("Only FileStoreTable is supported.");
        }

        if (!RemoteTableQuery.isRemoteServiceAvailable((FileStoreTable) table)) {
            catalog.close();
            throw new IllegalStateException(
                    "Query service not available. Start it first: CALL sys.query_service('"
                            + db
                            + "."
                            + tableName
                            + "', parallelism)");
        }

        return new PaimonRemoteLookupClient(catalog, table);
    }

    private static String resolveHiveConfDir() {
        // Keep hive conf optional by default. If needed, callers can still provide
        // -Dpaimon.hive.conf.dir or PAIMON_HIVE_CONF_DIR explicitly.
        return resolveOptional(HIVE_CONF_DIR_PROP, HIVE_CONF_DIR_ENV);
    }

    private static String resolveOptional(String systemProperty, String envVar) {
        String configured = trimToNull(System.getProperty(systemProperty));
        if (configured != null) {
            return configured;
        }
        return trimToNull(System.getenv(envVar));
    }

    private static String resolveOptionalWithDefault(
            String systemProperty, String envVar, String defaultValue) {
        String configured = resolveOptional(systemProperty, envVar);
        if (configured != null) {
            return configured;
        }
        return defaultValue;
    }

    private static String resolveHadoopConfDir() throws IOException {
        String configured = trimToNull(System.getProperty(HADOOP_CONF_DIR_PROP));
        if (configured != null) {
            return configured;
        }

        configured = trimToNull(System.getenv(HADOOP_CONF_DIR_ENV));
        if (configured != null) {
            return configured;
        }

        String cached = embeddedHadoopConfDir;
        if (cached != null) {
            return cached;
        }

        synchronized (PaimonRemoteLookupClient.class) {
            if (embeddedHadoopConfDir == null) {
                embeddedHadoopConfDir =
                        extractEmbeddedConfDir(
                                HADOOP_CONF_RESOURCE_DIR,
                                HADOOP_CONF_DIR_PROP,
                                HADOOP_CONF_DIR_ENV);
            }
            return embeddedHadoopConfDir;
        }
    }

    private static String extractEmbeddedConfDir(
            String resourceDir, String systemProperty, String envVar) throws IOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl = PaimonRemoteLookupClient.class.getClassLoader();
        }

        List<String> fileList = readResourceFileList(cl, resourceDir);
        if (fileList.isEmpty()) {
            throw new IllegalStateException(
                    "No conf files found for "
                            + resourceDir
                            + ". Set -D"
                            + systemProperty
                            + "=/path/to/conf or export "
                            + envVar
                            + "=/path/to/conf");
        }

        Path tempDir = Files.createTempDirectory("paimon-" + resourceDir + "-");
        tempDir.toFile().deleteOnExit();

        for (String fileName : fileList) {
            String resourcePath = resourceDir + "/" + fileName;
            try (InputStream in = cl.getResourceAsStream(resourcePath)) {
                if (in == null) {
                    throw new IllegalStateException(
                            "Missing embedded resource: "
                                    + resourcePath
                                    + ". Set -D"
                                    + systemProperty
                                    + "=/path/to/conf or export "
                                    + envVar
                                    + "=/path/to/conf");
                }

                Path target = tempDir.resolve(fileName);
                if (target.getParent() != null) {
                    Files.createDirectories(target.getParent());
                }
                Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
                target.toFile().deleteOnExit();
            }
        }

        return tempDir.toAbsolutePath().toString();
    }

    private static List<String> readResourceFileList(ClassLoader cl, String resourceDir)
            throws IOException {
        String listPath = resourceDir + "/files.list";
        try (InputStream in = cl.getResourceAsStream(listPath)) {
            if (in == null) {
                return new ArrayList<>();
            }

            List<String> files = new ArrayList<>();
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String value = trimToNull(line);
                    if (value == null || value.startsWith("#")) {
                        continue;
                    }
                    files.add(value);
                }
            }
            return files;
        }
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    /** Create a new session per thread. Do not share across threads. */
    public Session newSession() {
        return new Session(new RemoteTableQuery(table));
    }

    @Override
    public void close() throws Exception {
        catalog.close();
    }

    /** A thread-local session for performing remote lookups. */
    public static final class Session implements AutoCloseable {

        private final RemoteTableQuery query;

        private Session(RemoteTableQuery query) {
            this.query = query;
        }

        public Session project(String... fields) {
            Objects.requireNonNull(fields, "fields must not be null");
            query.withValueProjection(fields);
            return this;
        }

        public boolean exists(String... primaryKeyValues) throws IOException {
            return query.lookup(primaryKeyValues) != null;
        }

        public Map<String, String> lookupAsMap(String... primaryKeyValues) throws IOException {
            return query.lookupAsMap(primaryKeyValues);
        }

        public Map<String, String> lookupAsMapProjected(String[] fields, String... primaryKeyValues)
                throws IOException {
            query.withValueProjection(fields);
            try {
                return query.lookupAsMap(primaryKeyValues);
            } finally {
                query.withValueProjection((int[]) null); // always reset
            }
        }

        @Override
        public void close() throws IOException {
            query.close();
        }
    }
}
