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

package org.apache.paimon.flink.kafka;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.SerializableSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * A {@link TableRead} proxy that detects Paimon table schema changes and transparently rebuilds the
 * underlying delegate with the latest schema.
 *
 * <p>On each {@link #createReader(Split)} call, this class checks if the table's schema has changed
 * (via {@link SchemaManager#latest()}). If so, it reloads the table and creates a new {@link
 * TableRead} with the updated schema, ensuring that newly added columns are included in the read
 * output.
 */
public class SchemaEvolvingTableRead implements TableRead, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaEvolvingTableRead.class);

    private final SerializableSupplier<FileStoreTable> tableSupplier;
    private final SchemaManager schemaManager;

    private TableRead delegate;
    private long currentSchemaId;

    private IOManager ioManager;
    private MetricRegistry metricRegistry;
    @Nullable private Consumer<List<DataField>> schemaChangeListener;

    public SchemaEvolvingTableRead(
            SerializableSupplier<FileStoreTable> tableSupplier,
            FileStoreTable initialTable,
            TableRead initialRead) {
        this.tableSupplier = tableSupplier;
        this.schemaManager = new SchemaManager(initialTable.fileIO(), initialTable.location());
        this.delegate = initialRead;

        Optional<TableSchema> latest = schemaManager.latest();
        this.currentSchemaId = latest.map(TableSchema::id).orElse(-1L);
    }

    /** Set a listener that is called when schema changes are detected. */
    public void setSchemaChangeListener(Consumer<List<DataField>> listener) {
        this.schemaChangeListener = listener;
    }

    @Override
    public TableRead withMetricRegistry(MetricRegistry registry) {
        this.metricRegistry = registry;
        delegate.withMetricRegistry(registry);
        return this;
    }

    @Override
    public TableRead executeFilter() {
        delegate.executeFilter();
        return this;
    }

    @Override
    public TableRead withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        delegate.withIOManager(ioManager);
        return this;
    }

    @Override
    public RecordReader<InternalRow> createReader(Split split) throws IOException {
        checkSchemaEvolution();
        return delegate.createReader(split);
    }

    private void checkSchemaEvolution() {
        Optional<TableSchema> latestOpt = schemaManager.latest();
        if (!latestOpt.isPresent()) {
            return;
        }

        long latestId = latestOpt.get().id();
        if (latestId == currentSchemaId) {
            return;
        }

        LOG.info("Schema changed from {} to {}, rebuilding TableRead", currentSchemaId, latestId);

        try {
            FileStoreTable freshTable = tableSupplier.get();
            TableRead newRead = freshTable.newReadBuilder().newRead();
            if (ioManager != null) {
                newRead.withIOManager(ioManager);
            }
            if (metricRegistry != null) {
                newRead.withMetricRegistry(metricRegistry);
            }
            this.delegate = newRead;
            this.currentSchemaId = latestId;

            if (schemaChangeListener != null) {
                schemaChangeListener.accept(latestOpt.get().fields());
            }
        } catch (Exception e) {
            LOG.warn("Failed to rebuild TableRead for schema evolution, keeping old read", e);
        }
    }

    @Override
    public void close() throws IOException {
        // No resources to close — tableSupplier is stateless
    }
}
