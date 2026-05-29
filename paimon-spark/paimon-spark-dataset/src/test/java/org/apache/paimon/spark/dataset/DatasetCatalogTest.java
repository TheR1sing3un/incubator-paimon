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
import org.apache.paimon.spark.SparkTable;
import org.apache.paimon.spark.dataset.model.DatasetInfo;
import org.apache.paimon.spark.dataset.model.NamespaceInfo;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DatasetCatalog}.
 *
 * <p>Uses Mockito to mock the Paimon Catalog and DatasetRestClient, with a minimal SparkSession for
 * SQLConf support.
 */
class DatasetCatalogTest {

    private static SparkSession spark;

    private Catalog mockPaimonCatalog;
    private DatasetRestClient mockRestClient;
    private DatasetCatalog catalog;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder().master("local[2]").getOrCreate();
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    @BeforeEach
    void setUp() {
        mockPaimonCatalog = mock(Catalog.class);
        mockRestClient = mock(DatasetRestClient.class);
        catalog = new DatasetCatalog("", mockPaimonCatalog, mockRestClient, "default");
        // Schema overlay is session-level via SQLConf; default is no conf set, so no overlay.
        // Tests that exercise overlay paths call setPublicView(...) explicitly.
        spark.conf().unset(DatasetCatalog.PUBLIC_VIEW_KEY);
    }

    /** Convenience: declare a session-level public-view for tests that exercise overlay paths. */
    private void setPublicView(String viewValue) {
        spark.conf().set(DatasetCatalog.PUBLIC_VIEW_KEY, viewValue);
    }

    /** Convenience: clear the public-view conf. */
    private void clearPublicView() {
        spark.conf().unset(DatasetCatalog.PUBLIC_VIEW_KEY);
    }

    // ======================== loadTable — basic read ========================

    @Test
    void loadTableResolvesLogicalNameToPhysicalTable() throws Exception {
        // dataset-catalog returns: my_dataset → paimon_db.physical_table
        when(mockRestClient.getDatasetByName("ns", "my_dataset"))
                .thenReturn(new DatasetInfo("my_dataset", "paimon_db", "physical_table"));

        Table mockTable = createMockTable();
        org.apache.paimon.catalog.Identifier expectedId =
                org.apache.paimon.catalog.Identifier.create("paimon_db", "physical_table");
        when(mockPaimonCatalog.getTable(expectedId)).thenReturn(mockTable);

        org.apache.spark.sql.connector.catalog.Table result =
                catalog.loadTable(Identifier.of(new String[] {"ns"}, "my_dataset"));

        assertThat(result).isInstanceOf(SparkTable.class);
        verify(mockPaimonCatalog).getTable(expectedId);
    }

    @Test
    void loadTableNotFoundThrowsNoSuchTableException() throws Exception {
        when(mockRestClient.getDatasetByName("ns", "missing"))
                .thenReturn(new DatasetInfo("missing", "db", "not_exist"));

        when(mockPaimonCatalog.getTable(any()))
                .thenThrow(
                        new Catalog.TableNotExistException(
                                org.apache.paimon.catalog.Identifier.create("db", "not_exist")));

        assertThatThrownBy(() -> catalog.loadTable(Identifier.of(new String[] {"ns"}, "missing")))
                .isInstanceOf(NoSuchTableException.class);
    }

    @Test
    void loadTableRestErrorPropagatesAsRuntimeException() throws Exception {
        when(mockRestClient.getDatasetByName("ns", "ds"))
                .thenThrow(new RuntimeException("Connection refused"));

        assertThatThrownBy(() -> catalog.loadTable(Identifier.of(new String[] {"ns"}, "ds")))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Connection refused");
    }

    // ======================== loadTable — suffix ($branch_xxx, $snapshots)
    // ========================

    @Test
    void loadTableWithBranchConstructsCorrectPaimonIdentifier() throws Exception {
        when(mockRestClient.getDatasetByName("ns", "my_dataset"))
                .thenReturn(new DatasetInfo("my_dataset", "db", "tbl"));

        Table mockTable = createMockTable();
        // User writes: my_dataset$branch_feature → suffix "branch_feature" passed through as-is
        org.apache.paimon.catalog.Identifier expectedId =
                org.apache.paimon.catalog.Identifier.create("db", "tbl$branch_feature");
        when(mockPaimonCatalog.getTable(expectedId)).thenReturn(mockTable);

        org.apache.spark.sql.connector.catalog.Table result =
                catalog.loadTable(Identifier.of(new String[] {"ns"}, "my_dataset$branch_feature"));

        assertThat(result).isInstanceOf(SparkTable.class);
        verify(mockPaimonCatalog).getTable(expectedId);
    }

    @Test
    void loadTableWithBranchNotFoundThrows() throws Exception {
        when(mockRestClient.getDatasetByName("ns", "ds"))
                .thenReturn(new DatasetInfo("ds", "db", "tbl"));

        when(mockPaimonCatalog.getTable(
                        org.apache.paimon.catalog.Identifier.create("db", "tbl$branch_nonexist")))
                .thenThrow(
                        new Catalog.TableNotExistException(
                                org.apache.paimon.catalog.Identifier.create(
                                        "db", "tbl$branch_nonexist")));

        assertThatThrownBy(
                        () ->
                                catalog.loadTable(
                                        Identifier.of(new String[] {"ns"}, "ds$branch_nonexist")))
                .isInstanceOf(NoSuchTableException.class);
    }

    // ======================== loadTable — time travel (version) ========================

    @Test
    void loadTableWithVersionPassesScanOption() throws Exception {
        when(mockRestClient.getDatasetByName("ns", "ds"))
                .thenReturn(new DatasetInfo("ds", "db", "tbl"));

        Table mockTable = createMockTable();
        when(mockPaimonCatalog.getTable(any())).thenReturn(mockTable);

        org.apache.spark.sql.connector.catalog.Table result =
                catalog.loadTable(Identifier.of(new String[] {"ns"}, "ds"), "42");

        assertThat(result).isNotNull();
        // Verify that copy was called with version option
        verify(mockTable).copy(argMapContaining(CoreOptions.SCAN_VERSION.key(), "42"));
    }

    // ======================== loadTable — time travel (timestamp) ========================

    @Test
    void loadTableWithTimestampConvertsMicroToMilli() throws Exception {
        when(mockRestClient.getDatasetByName("ns", "ds"))
                .thenReturn(new DatasetInfo("ds", "db", "tbl"));

        Table mockTable = createMockTable();
        when(mockPaimonCatalog.getTable(any())).thenReturn(mockTable);

        // Spark passes microseconds: 1_700_000_000_000_000 μs = 1_700_000_000_000 ms
        long timestampMicros = 1_700_000_000_000_000L;
        org.apache.spark.sql.connector.catalog.Table result =
                catalog.loadTable(Identifier.of(new String[] {"ns"}, "ds"), timestampMicros);

        assertThat(result).isNotNull();
        // Should be converted to milliseconds
        verify(mockTable)
                .copy(
                        argMapContaining(
                                CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
                                String.valueOf(timestampMicros / 1000)));
    }

    @Test
    void loadTableNoPublicViewConfNoOverlay() throws Exception {
        // Without the per-dataset public-view conf, overlay never engages — this is the default
        // path for all datasets that haven't been opted in.
        when(mockRestClient.getDatasetByName("ns", "A1"))
                .thenReturn(new DatasetInfo("A1", "db", "t_a1"));

        Table tableA =
                createMockTable(
                        RowType.builder()
                                .field("id", DataTypes.INT())
                                .field("name", DataTypes.STRING())
                                .build());
        when(mockPaimonCatalog.getTable(any())).thenReturn(tableA);

        org.apache.spark.sql.connector.catalog.Table result =
                catalog.loadTable(Identifier.of(new String[] {"ns"}, "A1"));

        assertThat(result).isInstanceOf(SparkTable.class);
        // Resolver isn't invoked, so view dataset (e.g. "B") is never looked up.
        verify(mockRestClient, org.mockito.Mockito.never()).getDatasetByName("ns", "B");
    }

    // ======================== loadTable — schema overlay ========================
    // Note: the "successful wrap" path (overlay actually applied to a real Paimon table) is
    // covered by DatasetCatalogSQLTest in paimon-spark-ut, where we have a real filesystem
    // Paimon catalog. Here we cover only the gating + validation logic that can be exercised
    // without bootstrapping a real FileStoreTable.

    @Test
    void loadTableSelfReferenceOverlayIsNoOp() throws Exception {
        // Public-view conf points to the very dataset being queried → resolver short-circuits;
        // a typical case is "global view = B, query B".
        setPublicView("ns.A1");
        try {
            when(mockRestClient.getDatasetByName("ns", "A1"))
                    .thenReturn(new DatasetInfo("A1", "db", "t_a1"));

            Table tableA = createMockTable(RowType.builder().field("id", DataTypes.INT()).build());
            when(mockPaimonCatalog.getTable(any())).thenReturn(tableA);

            org.apache.spark.sql.connector.catalog.Table result =
                    catalog.loadTable(Identifier.of(new String[] {"ns"}, "A1"));

            assertThat(result).isInstanceOf(SparkTable.class);
            // Only A is looked up; resolver returns before any second REST call.
            verify(mockRestClient, org.mockito.Mockito.times(1)).getDatasetByName("ns", "A1");
        } finally {
            clearPublicView();
        }
    }

    @Test
    void loadTableOverlayValidationFailsOnExtraColumn() throws Exception {
        // A has 'extra' which B does not → validation must fail.
        setPublicView("ns.B");
        try {
            when(mockRestClient.getDatasetByName("ns", "A1"))
                    .thenReturn(new DatasetInfo("A1", "db", "t_a1"));
            when(mockRestClient.getDatasetByName("ns", "B"))
                    .thenReturn(new DatasetInfo("B", "db", "t_b"));

            FileStoreTable tableA =
                    createMockFileStoreTable(
                            RowType.builder()
                                    .field("id", DataTypes.INT())
                                    .field("extra", DataTypes.STRING())
                                    .build());
            Table tableB = createMockTable(RowType.builder().field("id", DataTypes.INT()).build());
            when(mockPaimonCatalog.getTable(
                            org.apache.paimon.catalog.Identifier.create("db", "t_a1")))
                    .thenReturn(tableA);
            when(mockPaimonCatalog.getTable(
                            org.apache.paimon.catalog.Identifier.create("db", "t_b")))
                    .thenReturn(tableB);

            assertThatThrownBy(() -> catalog.loadTable(Identifier.of(new String[] {"ns"}, "A1")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("extra")
                    .hasMessageContaining("not present");
        } finally {
            clearPublicView();
        }
    }

    @Test
    void loadTableOverlayValidationFailsOnTypeMismatch() throws Exception {
        // A.id is INT, B.id is BIGINT → type mismatch, must reject.
        setPublicView("ns.B");
        try {
            when(mockRestClient.getDatasetByName("ns", "A1"))
                    .thenReturn(new DatasetInfo("A1", "db", "t_a1"));
            when(mockRestClient.getDatasetByName("ns", "B"))
                    .thenReturn(new DatasetInfo("B", "db", "t_b"));

            FileStoreTable tableA =
                    createMockFileStoreTable(
                            RowType.builder().field("id", DataTypes.INT()).build());
            Table tableB =
                    createMockTable(RowType.builder().field("id", DataTypes.BIGINT()).build());
            when(mockPaimonCatalog.getTable(
                            org.apache.paimon.catalog.Identifier.create("db", "t_a1")))
                    .thenReturn(tableA);
            when(mockPaimonCatalog.getTable(
                            org.apache.paimon.catalog.Identifier.create("db", "t_b")))
                    .thenReturn(tableB);

            assertThatThrownBy(() -> catalog.loadTable(Identifier.of(new String[] {"ns"}, "A1")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Types must match");
        } finally {
            clearPublicView();
        }
    }

    @Test
    void loadTableOverlaySkipsForSuffixPath() throws Exception {
        // Suffix path (e.g. $snapshots) bypasses overlay even when public-view is set.
        setPublicView("ns.B");
        try {
            when(mockRestClient.getDatasetByName("ns", "A1"))
                    .thenReturn(new DatasetInfo("A1", "db", "t_a1"));

            Table tableA = createMockTable(RowType.builder().field("id", DataTypes.INT()).build());
            when(mockPaimonCatalog.getTable(
                            org.apache.paimon.catalog.Identifier.create("db", "t_a1$snapshots")))
                    .thenReturn(tableA);

            org.apache.spark.sql.connector.catalog.Table result =
                    catalog.loadTable(Identifier.of(new String[] {"ns"}, "A1$snapshots"));

            assertThat(result).isInstanceOf(SparkTable.class);
            // B should never be looked up for suffix paths
            verify(mockRestClient, org.mockito.Mockito.never()).getDatasetByName("ns", "B");
        } finally {
            clearPublicView();
        }
    }

    @Test
    void loadTableInvalidPublicViewValueThrows() throws Exception {
        // Conf value missing the '<ns>.<name>' separator → parser throws IllegalArgumentException.
        setPublicView("no_dot_in_value");
        try {
            when(mockRestClient.getDatasetByName("ns", "A1"))
                    .thenReturn(new DatasetInfo("A1", "db", "t_a1"));
            Table tableA = createMockTable(RowType.builder().field("id", DataTypes.INT()).build());
            when(mockPaimonCatalog.getTable(any())).thenReturn(tableA);

            assertThatThrownBy(() -> catalog.loadTable(Identifier.of(new String[] {"ns"}, "A1")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Invalid view ref");
        } finally {
            clearPublicView();
        }
    }

    // ======================== listTables ========================

    @Test
    void listTablesDelegatesToRestClient() throws Exception {
        when(mockRestClient.listDatasets("ns"))
                .thenReturn(
                        Arrays.asList(
                                new DatasetInfo("ds1", "db", "t1"),
                                new DatasetInfo("ds2", "db", "t2")));

        Identifier[] result = catalog.listTables(new String[] {"ns"});

        assertThat(result).hasSize(2);
        assertThat(result[0].name()).isEqualTo("ds1");
        assertThat(result[1].name()).isEqualTo("ds2");
        assertThat(result[0].namespace()).isEqualTo(new String[] {"ns"});
    }

    @Test
    void listTablesEmptyNamespaceThrows() {
        assertThatThrownBy(() -> catalog.listTables(new String[] {}))
                .isInstanceOf(NoSuchNamespaceException.class);
    }

    // ======================== namespace operations ========================

    @Test
    void listNamespaces() {
        when(mockRestClient.listNamespaces())
                .thenReturn(Arrays.asList(new NamespaceInfo("ns1"), new NamespaceInfo("ns2")));

        String[][] result = catalog.listNamespaces();

        assertThat(result.length).isEqualTo(2);
        assertThat(result[0]).isEqualTo(new String[] {"ns1"});
        assertThat(result[1]).isEqualTo(new String[] {"ns2"});
    }

    @Test
    void namespaceExistsDelegatesToClient() {
        when(mockRestClient.namespaceExists("ns")).thenReturn(true);
        when(mockRestClient.namespaceExists("missing")).thenReturn(false);

        assertThat(catalog.namespaceExists(new String[] {"ns"})).isTrue();
        assertThat(catalog.namespaceExists(new String[] {"missing"})).isFalse();
        assertThat(catalog.namespaceExists(new String[] {})).isFalse();
        assertThat(catalog.namespaceExists(null)).isFalse();
    }

    @Test
    void tableExistsDelegatesToClient() {
        when(mockRestClient.datasetExists("ns", "ds")).thenReturn(true);
        when(mockRestClient.datasetExists("ns", "missing")).thenReturn(false);

        assertThat(catalog.tableExists(Identifier.of(new String[] {"ns"}, "ds"))).isTrue();
        assertThat(catalog.tableExists(Identifier.of(new String[] {"ns"}, "missing"))).isFalse();
    }

    @Test
    void defaultNamespace() {
        assertThat(catalog.defaultNamespace()).isEqualTo(new String[] {"default"});
    }

    // ======================== read-only operations ========================

    @Test
    void createTableThrowsUnsupported() {
        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        Identifier.of(new String[] {"ns"}, "t"),
                                        new StructType(),
                                        new Transform[] {},
                                        Collections.emptyMap()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void dropTableThrowsUnsupported() {
        assertThatThrownBy(() -> catalog.dropTable(Identifier.of(new String[] {"ns"}, "t")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void alterTableThrowsUnsupported() {
        assertThatThrownBy(() -> catalog.alterTable(Identifier.of(new String[] {"ns"}, "t")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void renameTableThrowsUnsupported() {
        assertThatThrownBy(
                        () ->
                                catalog.renameTable(
                                        Identifier.of(new String[] {"ns"}, "a"),
                                        Identifier.of(new String[] {"ns"}, "b")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void createNamespaceThrowsUnsupported() {
        assertThatThrownBy(
                        () -> catalog.createNamespace(new String[] {"ns"}, Collections.emptyMap()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void alterNamespaceThrowsUnsupported() {
        assertThatThrownBy(() -> catalog.alterNamespace(new String[] {"ns"}))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void dropNamespaceThrowsUnsupported() {
        assertThatThrownBy(() -> catalog.dropNamespace(new String[] {"ns"}))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // ======================== catalog metadata ========================

    @Test
    void nameReturnsCatalogName() {
        assertThat(catalog.name()).isEqualTo("");
    }

    @Test
    void paimonCatalogReturnsDelegatedCatalog() {
        assertThat(catalog.paimonCatalog()).isSameAs(mockPaimonCatalog);
    }

    // ======================== Helpers ========================

    private Table createMockTable() {
        return createMockTable(
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("name", DataTypes.STRING())
                        .build());
    }

    private Table createMockTable(RowType rowType) {
        Table table = mock(Table.class);
        when(table.name()).thenReturn("test_table");
        when(table.fullName()).thenReturn("db.test_table");
        when(table.rowType()).thenReturn(rowType);
        when(table.partitionKeys()).thenReturn(Collections.emptyList());
        when(table.primaryKeys()).thenReturn(Collections.emptyList());
        when(table.options()).thenReturn(Collections.emptyMap());
        when(table.comment()).thenReturn(Optional.empty());
        when(table.copy(any())).thenReturn(table);
        return table;
    }

    /**
     * Creates a FileStoreTable mock used in tests that exercise overlay validation paths (resolver
     * casts to FileStoreTable then calls schema().fields()). Only the schema-related stubs that
     * validation reaches before throwing are set up; the test asserts that validation throws before
     * any further state matters.
     */
    private FileStoreTable createMockFileStoreTable(RowType rowType) {
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.name()).thenReturn("test_table");
        when(table.fullName()).thenReturn("db.test_table");
        when(table.rowType()).thenReturn(rowType);
        when(table.partitionKeys()).thenReturn(Collections.emptyList());
        when(table.primaryKeys()).thenReturn(Collections.emptyList());
        when(table.options()).thenReturn(Collections.emptyMap());
        when(table.comment()).thenReturn(Optional.empty());
        when(table.copy(any(Map.class))).thenReturn(table);

        org.apache.paimon.schema.TableSchema schema =
                new org.apache.paimon.schema.TableSchema(
                        0L,
                        rowType.getFields(),
                        Math.max(0, rowType.getFieldCount() - 1),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        null);
        when(table.schema()).thenReturn(schema);
        return table;
    }

    /**
     * Custom Mockito argument matcher: verifies the Map contains the expected key-value pair. Uses
     * Mockito.argThat indirectly via this helper to keep test code readable.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, String> argMapContaining(String key, String value) {
        return org.mockito.ArgumentMatchers.argThat(
                map -> map != null && value.equals(((Map<String, String>) map).get(key)));
    }
}
