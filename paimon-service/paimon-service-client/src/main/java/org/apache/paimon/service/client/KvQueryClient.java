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

package org.apache.paimon.service.client;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.bucket.BucketFunction;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.query.QueryLocation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.service.exceptions.UnknownPartitionBucketException;
import org.apache.paimon.service.messages.KvRequest;
import org.apache.paimon.service.messages.KvResponse;
import org.apache.paimon.service.network.NetworkClient;
import org.apache.paimon.service.network.messages.MessageSerializer;
import org.apache.paimon.service.network.stats.DisabledServiceRequestStats;
import org.apache.paimon.utils.FutureUtils;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** A class for the Client to get values from Servers. */
public class KvQueryClient {

    private static final Logger LOG = LoggerFactory.getLogger(KvQueryClient.class);

    private final NetworkClient<KvRequest, KvResponse> networkClient;
    private final QueryLocation queryLocation;
    @Nullable private final TableSchema tableSchema;
    private volatile UnpartitionedPrimaryKeyLookup primaryKeyLookup;

    public KvQueryClient(QueryLocation queryLocation, int numEventLoopThreads) {
        this(queryLocation, numEventLoopThreads, null, false);
    }

    public KvQueryClient(
            QueryLocation queryLocation, int numEventLoopThreads, TableSchema tableSchema) {
        this(queryLocation, numEventLoopThreads, tableSchema, true);
    }

    private KvQueryClient(
            QueryLocation queryLocation,
            int numEventLoopThreads,
            @Nullable TableSchema tableSchema,
            boolean requireSchema) {
        this.queryLocation = queryLocation;
        this.tableSchema = requireSchema ? Preconditions.checkNotNull(tableSchema) : tableSchema;
        final MessageSerializer<KvRequest, KvResponse> messageSerializer =
                new MessageSerializer<>(
                        new KvRequest.KvRequestDeserializer(),
                        new KvResponse.KvResponseDeserializer());

        this.networkClient =
                new NetworkClient<>(
                        "Kv Query Client",
                        numEventLoopThreads,
                        messageSerializer,
                        new DisabledServiceRequestStats());
    }

    public CompletableFuture<BinaryRow[]> getValues(
            BinaryRow partition, int bucket, BinaryRow[] keys) {
        CompletableFuture<BinaryRow[]> response = new CompletableFuture<>();
        executeActionAsync(response, new KvRequest(partition, bucket, keys), false);
        return response;
    }

    /**
     * Get value by primary key for unpartitioned primary key table.
     *
     * <p>Requires {@link #KvQueryClient(QueryLocation, int, TableSchema)} to initialize.
     */
    public CompletableFuture<KvLookupResult> getValueByPrimaryKey(BinaryRow primaryKey) {
        Preconditions.checkState(
                tableSchema != null,
                "Primary key lookup is not initialized. "
                        + "Use KvQueryClient(QueryLocation, int, TableSchema).");
        UnpartitionedPrimaryKeyLookup lookup = primaryKeyLookup;
        if (lookup == null) {
            synchronized (this) {
                if (primaryKeyLookup == null) {
                    primaryKeyLookup = new UnpartitionedPrimaryKeyLookup(tableSchema);
                }
                lookup = primaryKeyLookup;
            }
        }
        int bucket = lookup.bucket(primaryKey);
        return getValues(lookup.partition(), bucket, new BinaryRow[] {primaryKey})
                .thenApply(values -> new KvLookupResult(values[0]));
    }

    private void executeActionAsync(
            final CompletableFuture<BinaryRow[]> result,
            final KvRequest request,
            final boolean update) {
        if (!result.isDone()) {
            final CompletableFuture<KvResponse> operationFuture = getResponse(request, update);
            operationFuture.whenCompleteAsync(
                    (t, throwable) -> {
                        if (throwable != null) {
                            if (throwable instanceof UnknownPartitionBucketException
                                    || throwable.getCause() instanceof ConnectException) {

                                // These failures are likely to be caused by out-of-sync location.
                                // Therefore, we retry this query and force lookup the location.

                                LOG.debug(
                                        "Retrying after failing to retrieve state due to: {}.",
                                        throwable.getMessage());
                                executeActionAsync(result, request, true);
                            } else {
                                result.completeExceptionally(throwable);
                            }
                        } else {
                            result.complete(t.values());
                        }
                    });

            result.whenComplete((t, throwable) -> operationFuture.cancel(false));
        }
    }

    private CompletableFuture<KvResponse> getResponse(
            final KvRequest request, final boolean forceUpdate) {
        InetSocketAddress serverAddress =
                queryLocation.getLocation(request.partition(), request.bucket(), forceUpdate);
        if (serverAddress == null) {
            return FutureUtils.completedExceptionally(
                    new RuntimeException("Cannot find address for bucket: " + request.bucket()));
        }
        return networkClient.sendRequest(serverAddress, request);
    }

    public void shutdown() {
        try {
            shutdownFuture().get(60L, TimeUnit.SECONDS);
            LOG.info("{} was shutdown successfully.", networkClient.getClientName());
        } catch (Exception e) {
            LOG.warn(String.format("%s shutdown failed.", networkClient.getClientName()), e);
        }
    }

    public CompletableFuture<Void> shutdownFuture() {
        return networkClient.shutdown();
    }

    /** Result for primary key lookup. */
    public static final class KvLookupResult {

        private final BinaryRow value;

        private KvLookupResult(BinaryRow value) {
            this.value = value;
        }

        public BinaryRow value() {
            return value;
        }

        public boolean exists() {
            return value != null;
        }
    }

    private static final class UnpartitionedPrimaryKeyLookup {

        private final Projection bucketKeyProjection;
        private final BucketFunction bucketFunction;
        private final int numBuckets;

        private UnpartitionedPrimaryKeyLookup(TableSchema schema) {
            Preconditions.checkArgument(
                    schema.partitionKeys().isEmpty(),
                    "Only support unpartitioned primary key table.");
            Preconditions.checkArgument(
                    !schema.primaryKeys().isEmpty(), "Primary key is required.");
            Preconditions.checkArgument(
                    schema.numBuckets() > 0, "Only support fixed bucket for lookup.");

            List<String> primaryKeys = schema.trimmedPrimaryKeys();
            List<String> bucketKeys = schema.bucketKeys();
            int[] mapping = new int[bucketKeys.size()];
            for (int i = 0; i < bucketKeys.size(); i++) {
                int index = primaryKeys.indexOf(bucketKeys.get(i));
                Preconditions.checkArgument(
                        index >= 0,
                        "Bucket key %s is not in primary keys %s.",
                        bucketKeys.get(i),
                        primaryKeys);
                mapping[i] = index;
            }

            this.bucketKeyProjection =
                    CodeGenUtils.newProjection(schema.logicalTrimmedPrimaryKeysType(), mapping);
            this.bucketFunction =
                    BucketFunction.create(
                            new CoreOptions(schema.options()), schema.logicalBucketKeyType());
            this.numBuckets = schema.numBuckets();
        }

        public BinaryRow partition() {
            return BinaryRow.EMPTY_ROW;
        }

        public int bucket(BinaryRow primaryKey) {
            BinaryRow bucketKey = bucketKeyProjection.apply(primaryKey);
            return bucketFunction.bucket(bucketKey, numBuckets);
        }
    }
}
