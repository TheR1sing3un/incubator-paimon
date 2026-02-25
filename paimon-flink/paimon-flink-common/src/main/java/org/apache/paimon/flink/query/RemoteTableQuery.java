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

package org.apache.paimon.flink.query;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.query.QueryLocationImpl;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.service.client.KvQueryClient;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.query.TableQuery;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.TypeUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.paimon.service.ServiceManager.PRIMARY_KEY_LOOKUP;

/** Implementation for {@link TableQuery} to lookup data from remote service. */
public class RemoteTableQuery implements TableQuery {

    private final FileStoreTable table;
    private final KvQueryClient client;
    private final InternalRowSerializer keySerializer;

    @Nullable private int[] projection;
    @Nullable private List<String> projectionNames;

    public RemoteTableQuery(Table table) {
        this.table = (FileStoreTable) table;
        ServiceManager manager = this.table.store().newServiceManager();
        this.client = new KvQueryClient(new QueryLocationImpl(manager), 1, this.table.schema());
        this.keySerializer =
                InternalSerializers.create(TypeUtils.project(table.rowType(), table.primaryKeys()));
    }

    public static boolean isRemoteServiceAvailable(FileStoreTable table) {
        return table.store().newServiceManager().service(PRIMARY_KEY_LOOKUP).isPresent();
    }

    @Nullable
    @Override
    public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key) throws IOException {
        BinaryRow row;
        try {
            row =
                    client.getValues(
                                    partition,
                                    bucket,
                                    new BinaryRow[] {keySerializer.toBinaryRow(key)})
                            .get()[0];
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }

        return applyProjection(row);
    }

    /**
     * Lookup value by primary key for unpartitioned primary key table.
     *
     * <p>The returned value is null if it does not exist.
     */
    @Nullable
    public InternalRow lookup(InternalRow key) throws IOException {
        BinaryRow row;
        try {
            row = client.getValueByPrimaryKey(keySerializer.toBinaryRow(key)).get().value();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }

        return applyProjection(row);
    }

    /**
     * Lookup value by primary key strings for unpartitioned primary key table.
     *
     * <p>Primary key values must follow the order of table primary keys.
     */
    @Nullable
    public InternalRow lookup(String... primaryKeyValues) throws IOException {
        Preconditions.checkArgument(
                table.partitionKeys().isEmpty(), "Only support unpartitioned primary key table.");
        RowType pkType = table.schema().logicalTrimmedPrimaryKeysType();
        Preconditions.checkArgument(
                primaryKeyValues.length == pkType.getFieldCount(),
                "Primary key value count %s does not match primary key field count %s.",
                primaryKeyValues.length,
                pkType.getFieldCount());

        GenericRow key = new GenericRow(primaryKeyValues.length);
        for (int i = 0; i < primaryKeyValues.length; i++) {
            String value = primaryKeyValues[i];
            if (value == null) {
                key.setField(i, null);
            } else {
                key.setField(i, TypeUtils.castFromString(value, pkType.getTypeAt(i)));
            }
        }
        return lookup(key);
    }

    /**
     * Lookup value by primary key strings and return a map of column name to string value.
     *
     * <p>Primary key values must follow the order of table primary keys.
     */
    @Nullable
    public Map<String, String> lookupAsMap(String... primaryKeyValues) throws IOException {
        InternalRow row = lookup(primaryKeyValues);
        return row == null ? null : toStringMap(row);
    }

    /** Lookup value by primary key and return a map of column name to string value. */
    @Nullable
    public Map<String, String> lookupAsMap(InternalRow key) throws IOException {
        InternalRow row = lookup(key);
        return row == null ? null : toStringMap(row);
    }

    @Nullable
    private InternalRow applyProjection(@Nullable InternalRow row) {
        if (projection == null) {
            return row;
        }

        if (row == null) {
            return null;
        }

        return ProjectedRow.from(projection).replaceRow(row);
    }

    @Override
    public RemoteTableQuery withValueProjection(int[] projection) {
        this.projection = projection;
        if (projection == null) {
            this.projectionNames = null;
        } else {
            List<String> fieldNames = table.rowType().getFieldNames();
            List<String> names = new ArrayList<>(projection.length);
            for (int index : projection) {
                names.add(fieldNames.get(index));
            }
            this.projectionNames = names;
        }
        return this;
    }

    /** Set value projection by column names. */
    public RemoteTableQuery withValueProjection(String... fieldNames) {
        Preconditions.checkNotNull(fieldNames, "Projection field names should not be null.");
        List<String> names = Arrays.asList(fieldNames);
        this.projection = table.schema().projection(names);
        this.projectionNames = new ArrayList<>(names);
        return this;
    }

    @Override
    public InternalRowSerializer createValueSerializer() {
        return InternalSerializers.create(TypeUtils.project(table.rowType(), projection));
    }

    @Override
    public void close() throws IOException {
        client.shutdown();
    }

    @VisibleForTesting
    public CompletableFuture<Void> cancel() {
        return client.shutdownFuture();
    }

    private Map<String, String> toStringMap(InternalRow row) {
        RowType rowType =
                projection == null
                        ? table.rowType()
                        : TypeUtils.project(table.rowType(), projection);
        List<String> names = projectionNames == null ? rowType.getFieldNames() : projectionNames;
        Preconditions.checkArgument(
                names.size() == rowType.getFieldCount(),
                "Projection names size %s does not match row field count %s.",
                names.size(),
                rowType.getFieldCount());

        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            DataType fieldType = rowType.getTypeAt(i);
            Object fieldValue = InternalRowUtils.get(row, i, fieldType);
            if (fieldValue == null) {
                result.put(names.get(i), null);
            } else {
                @SuppressWarnings("unchecked")
                CastExecutor<Object, ?> cast =
                        (CastExecutor<Object, ?>) CastExecutors.resolveToString(fieldType);
                Object casted = cast.cast(fieldValue);
                result.put(names.get(i), casted == null ? null : casted.toString());
            }
        }
        return result;
    }
}
