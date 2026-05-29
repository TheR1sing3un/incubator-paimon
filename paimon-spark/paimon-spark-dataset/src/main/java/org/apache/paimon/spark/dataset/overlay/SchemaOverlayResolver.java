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

package org.apache.paimon.spark.dataset.overlay;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.spark.SparkTable;
import org.apache.paimon.spark.dataset.DatasetRestClient;
import org.apache.paimon.spark.dataset.model.DatasetInfo;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import org.apache.spark.sql.connector.catalog.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Resolves and applies schema overlay by leveraging Paimon's existing schema-evolution machinery.
 *
 * <p>Given dataset A and an {@link Identifier} {@code view} (sourced from SQLConf {@code
 * spark.paimon.dataset.schema-overlay.public-view.<ns>.<name>} at submission time), this resolver:
 *
 * <ol>
 *   <li>Loads {@code view}'s physical Paimon table to get its schema (call it B).
 *   <li>Validates that A's columns are a subset of B's (by name and exact type match).
 *   <li>Synthesizes a new {@link TableSchema} that has B's column ordering, retaining A's existing
 *       field IDs for shared columns and assigning fresh IDs (above A's {@code highestFieldId}) for
 *       B-only columns.
 *   <li>The synthetic {@code TableSchema} carries a fresh {@code schemaId} that is intentionally
 *       distinct from A's actual schema IDs. Paimon's read pipeline checks {@code
 *       currentTableSchema.id() != fileSchema.id()} to engage its schema-evolution path
 *       (devolveFilters, stats evolution, NULL-fill for missing field IDs). A synthetic schema with
 *       a distinct ID makes Paimon think it's reading "old" files through a "newer" schema —
 *       exactly the ALTER ADD COLUMN scenario it already supports.
 *   <li>Calls {@link FileStoreTable#copy(TableSchema)} to obtain a Paimon table that uses the
 *       synthetic schema as its current schema while still reading A's physical files.
 * </ol>
 *
 * <p>Result: Paimon natively delivers columnar NULL-fill for B-only columns, drops predicates
 * referencing those columns at {@code devolveFilters}, and short-circuits stats-based file pruning
 * where possible. No Spark V2 layer wrapping is needed.
 *
 * <p>Behavior contract:
 *
 * <ul>
 *   <li>{@code view == null} → no overlay (caller should not call this).
 *   <li>Self-reference (view points back to current dataset) → returns inner unchanged.
 *   <li>Validation failure (extra column in A, or type mismatch) → throws {@link
 *       IllegalStateException}.
 *   <li>Referenced dataset / Paimon table not found → throws {@link RuntimeException} wrapping the
 *       cause.
 *   <li>Paimon table is not a {@link FileStoreTable} → overlay is skipped silently (defensive).
 * </ul>
 */
public final class SchemaOverlayResolver {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaOverlayResolver.class);

    /**
     * Offset added to A's actual {@code schemaId} to derive the synthetic {@code schemaId}. Large
     * enough that A's natural schema-evolution lineage will not realistically reach the synthetic
     * ID (the synthetic schema lives in memory only and is not registered with the on-disk schema
     * directory).
     */
    private static final long SYNTHETIC_SCHEMA_ID_OFFSET = 1_000_000L;

    private SchemaOverlayResolver() {}

    /**
     * Wraps {@code paimonTableA} with a schema overlay pointing at {@code view}; returns the
     * resulting {@link SparkTable}. The {@code view} is supplied by the caller (typically resolved
     * from SQLConf).
     *
     * @param paimonTableA Paimon table backing dataset A
     * @param aNamespace dataset A's namespace (for self-reference detection and logging)
     * @param aDatasetName dataset A's name (for self-reference detection and logging)
     * @param view the dataset whose schema A should overlay onto; must be non-null
     * @param client used to look up the view dataset's physical mapping
     * @param paimonCatalog used to load the view dataset's physical Paimon table
     */
    public static org.apache.spark.sql.connector.catalog.Table maybeWrap(
            Table paimonTableA,
            String aNamespace,
            String aDatasetName,
            Identifier view,
            DatasetRestClient client,
            Catalog paimonCatalog) {
        if (view == null) {
            // Caller is expected to gate on null already; defensive fallback.
            return SparkTable.of(paimonTableA);
        }
        String viewNamespace = view.namespace().length > 0 ? view.namespace()[0] : null;
        String viewName = view.name();
        if (aNamespace != null
                && aNamespace.equals(viewNamespace)
                && aDatasetName != null
                && aDatasetName.equals(viewName)) {
            LOG.warn(
                    "schema overlay skipped: self-reference dataset='{}.{}' view='{}'",
                    aNamespace,
                    aDatasetName,
                    view);
            return SparkTable.of(paimonTableA);
        }

        if (!(paimonTableA instanceof FileStoreTable)) {
            LOG.warn(
                    "schema overlay skipped: non-FileStoreTable type={} dataset='{}.{}'",
                    paimonTableA.getClass().getSimpleName(),
                    aNamespace,
                    aDatasetName);
            return SparkTable.of(paimonTableA);
        }

        // Resolve B (the view dataset's physical mapping)
        DatasetInfo bInfo;
        try {
            bInfo = client.getDatasetByName(viewNamespace, viewName);
        } catch (Exception e) {
            LOG.error(
                    "schema overlay FAILED: cannot resolve view='{}' for dataset='{}.{}': {}",
                    view,
                    aNamespace,
                    aDatasetName,
                    e.getMessage());
            throw new RuntimeException(
                    "Failed to resolve view dataset '"
                            + view
                            + "' (referenced by dataset '"
                            + aDatasetName
                            + "' via SQLConf): "
                            + e.getMessage(),
                    e);
        }

        if (bInfo.getDatabaseName() == null || bInfo.getTableName() == null) {
            LOG.error(
                    "schema overlay FAILED: view='{}' has no physical table mapping (database_name/table_name null)",
                    view);
            throw new IllegalStateException(
                    "view dataset '"
                            + view
                            + "' has no physical table mapping (database_name/table_name)");
        }

        org.apache.paimon.catalog.Identifier bId =
                org.apache.paimon.catalog.Identifier.create(
                        bInfo.getDatabaseName(), bInfo.getTableName());
        Table tableB;
        try {
            tableB = paimonCatalog.getTable(bId);
        } catch (Catalog.TableNotExistException e) {
            LOG.error(
                    "schema overlay FAILED: view='{}' Paimon table not found: paimonId={}",
                    view,
                    bId);
            throw new RuntimeException(
                    "view dataset '" + view + "' references missing Paimon table " + bId, e);
        }

        FileStoreTable fileStoreTableA = (FileStoreTable) paimonTableA;
        TableSchema actualSchema = fileStoreTableA.schema();
        RowType bRowType = tableB.rowType();

        validate(actualSchema.fields(), bRowType.getFields(), aDatasetName, view);

        TableSchema syntheticSchema = synthesizeSchema(actualSchema, bRowType);
        FileStoreTable syntheticTable = fileStoreTableA.copy(syntheticSchema);

        // Compute the added columns (in synthetic but not in actual). Cheap O(synthetic.size).
        List<DataField> addedFields = computeAddedFields(actualSchema, syntheticSchema);

        // Single 4-line summary on successful application: drop into ops grep
        // 'schema overlay applied' to verify "is overlay actually working for this dataset?".
        LOG.info(
                "schema overlay applied: dataset='{}.{}' → view='{}'",
                aNamespace,
                aDatasetName,
                view);
        LOG.info(
                "  actual    schema id={} fields={}",
                actualSchema.id(),
                describeFields(actualSchema.fields()));
        LOG.info(
                "  synthetic schema id={} fields={}",
                syntheticSchema.id(),
                describeFields(syntheticSchema.fields()));
        LOG.info("  added columns (NULL-filled at read time): {}", describeFields(addedFields));

        return SparkTable.of(syntheticTable);
    }

    /**
     * Parses the value of the {@code spark.paimon.dataset.schema-overlay.public-view} SQLConf into
     * a Spark V2 {@link Identifier}. The value format is {@code <viewNamespace>.<viewName>}; the
     * first {@code '.'} splits namespace from name.
     *
     * @param value raw conf value, e.g. {@code "test_ns.B"}; may be {@code null} or empty
     * @return parsed identifier, or {@code null} when {@code value} is null/blank
     * @throws IllegalArgumentException when the value has no {@code '.'} separator
     */
    @Nullable
    public static Identifier parseViewConfValue(@Nullable String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        int dot = trimmed.indexOf('.');
        if (dot <= 0 || dot == trimmed.length() - 1) {
            throw new IllegalArgumentException(
                    "Invalid view ref '"
                            + value
                            + "': expected '<namespace>.<name>' (e.g. 'test_ns.public_table_b')");
        }
        return Identifier.of(new String[] {trimmed.substring(0, dot)}, trimmed.substring(dot + 1));
    }

    /** Compact one-line description of a list of {@link DataField} for log lines. */
    private static String describeFields(List<DataField> fields) {
        if (fields.isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            DataField f = fields.get(i);
            sb.append(f.name()).append(":fid=").append(f.id()).append(":").append(f.type());
        }
        sb.append("]");
        return sb.toString();
    }

    /** Returns the fields present in {@code synthetic} but absent from {@code actual} by name. */
    private static List<DataField> computeAddedFields(TableSchema actual, TableSchema synthetic) {
        Set<String> actualNames = new HashSet<>();
        for (DataField f : actual.fields()) {
            actualNames.add(f.name());
        }
        List<DataField> added = new ArrayList<>();
        for (DataField f : synthetic.fields()) {
            if (!actualNames.contains(f.name())) {
                added.add(f);
            }
        }
        return added;
    }

    /**
     * Constructs the synthetic {@link TableSchema}.
     *
     * <p>Field assembly:
     *
     * <ul>
     *   <li>Iterate B's fields in B's natural order (so the overlay schema column order matches
     *       B's).
     *   <li>For each B field present in A by name, reuse A's existing field ID (preserving Paimon's
     *       fid → physical-column mapping for A's data files).
     *   <li>For each B field absent from A, allocate a fresh field ID starting at {@code
     *       actual.highestFieldId() + 1}, ensuring no collision with A's lineage.
     * </ul>
     *
     * <p>The synthetic {@code schemaId} is offset from A's actual ID by {@link
     * #SYNTHETIC_SCHEMA_ID_OFFSET} — distinct enough that read-time {@code tableSchema.id() !=
     * fileSchema.id()} comparisons reliably engage Paimon's schema-evolution path.
     */
    private static TableSchema synthesizeSchema(TableSchema actual, RowType bRowType) {
        Map<String, DataField> aByName = new HashMap<>();
        for (DataField f : actual.fields()) {
            aByName.put(f.name(), f);
        }

        int nextFid = actual.highestFieldId() + 1;
        List<DataField> syntheticFields = new ArrayList<>(bRowType.getFieldCount());
        for (DataField bField : bRowType.getFields()) {
            DataField aField = aByName.get(bField.name());
            if (aField != null) {
                // Shared column: reuse A's fid + type (validate already ensured types match).
                syntheticFields.add(aField);
            } else {
                // B-only column: assign a fresh fid above A's highestFieldId.
                syntheticFields.add(new DataField(nextFid++, bField.name(), bField.type()));
            }
        }

        int newHighestFieldId = Math.max(actual.highestFieldId(), nextFid - 1);
        long syntheticSchemaId = actual.id() + SYNTHETIC_SCHEMA_ID_OFFSET;

        return new TableSchema(
                syntheticSchemaId,
                syntheticFields,
                newHighestFieldId,
                actual.partitionKeys(),
                actual.primaryKeys(),
                actual.options(),
                actual.comment());
    }

    /**
     * Validates that A's columns are a subset of B's, with exact name and type match.
     *
     * <p>Cheap insurance against schema drift after a CTAS-style derivation: if dataset A is later
     * altered (column added/dropped/retyped), the contract with B may break, and we want to fail
     * loudly at read time instead of silently mis-shaping rows.
     */
    private static void validate(
            List<DataField> aFields, List<DataField> bFields, String aName, Identifier view) {
        Map<String, DataType> bByName = new HashMap<>();
        for (DataField bf : bFields) {
            bByName.put(bf.name(), bf.type());
        }
        for (DataField af : aFields) {
            DataType bType = bByName.get(af.name());
            if (bType == null) {
                throw new IllegalStateException(
                        "Schema overlay validation failed: dataset '"
                                + aName
                                + "' has column '"
                                + af.name()
                                + "' which is not present in view dataset '"
                                + view
                                + "'. The dataset's physical schema must be a subset of the view schema.");
            }
            if (!bType.equals(af.type())) {
                throw new IllegalStateException(
                        "Schema overlay validation failed: dataset '"
                                + aName
                                + "' column '"
                                + af.name()
                                + "' has type "
                                + af.type().asSQLString()
                                + " but view dataset '"
                                + view
                                + "' has type "
                                + bType.asSQLString()
                                + ". Types must match exactly.");
            }
        }
    }
}
