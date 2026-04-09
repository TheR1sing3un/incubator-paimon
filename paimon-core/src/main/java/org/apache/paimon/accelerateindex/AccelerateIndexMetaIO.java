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

package org.apache.paimon.accelerateindex;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Read/write utility for {@link AccelerateIndexMeta} stored as {@code
 * __accelerate_index_meta.json}.
 *
 * <p>Supports CAS (compare-and-swap) updates via version number for optimistic concurrency control.
 */
public class AccelerateIndexMetaIO {

    private AccelerateIndexMetaIO() {}

    private static final int MAX_CAS_RETRIES = 10;

    /** Read meta from the given path. Returns {@code null} if the file does not exist. */
    @Nullable
    public static AccelerateIndexMeta read(FileIO fileIO, Path metaPath) throws IOException {
        try {
            String json = fileIO.readFileUtf8(metaPath);
            if (json == null || json.trim().isEmpty()) {
                return null;
            }
            return JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readValue(json, AccelerateIndexMeta.class);
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    /** Read meta, returning empty meta if the file does not exist. */
    public static AccelerateIndexMeta readOrEmpty(FileIO fileIO, Path metaPath) throws IOException {
        AccelerateIndexMeta meta = read(fileIO, metaPath);
        return meta != null ? meta : AccelerateIndexMeta.empty();
    }

    /**
     * Write meta to the given path atomically using {@link FileIO#overwriteFileUtf8}.
     *
     * <p>This avoids the crash-unsafe pattern of delete-then-rename, where a crash between delete
     * and rename would lose the meta file entirely.
     */
    public static void write(FileIO fileIO, Path metaPath, AccelerateIndexMeta meta)
            throws IOException {
        String json =
                JsonSerdeUtil.OBJECT_MAPPER_INSTANCE
                        .writerWithDefaultPrettyPrinter()
                        .writeValueAsString(meta);
        fileIO.overwriteFileUtf8(metaPath, json);
    }

    /**
     * Perform a CAS (compare-and-swap) update on the meta file.
     *
     * <p>Reads the current meta, applies the update function, verifies the version has not changed,
     * and writes back. If a version conflict is detected, the update function is re-applied to the
     * latest state and retried.
     *
     * @param fileIO the file I/O handle
     * @param metaPath the path to the meta file
     * @param updateFn function that takes the current meta and returns updated entries
     * @throws IOException if I/O errors occur or max retries exceeded
     */
    public static void casUpdate(
            FileIO fileIO,
            Path metaPath,
            Function<AccelerateIndexMeta, List<AccelerateIndexEntry>> updateFn)
            throws IOException {
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            AccelerateIndexMeta current = readOrEmpty(fileIO, metaPath);
            int expectedVersion = current.version();

            List<AccelerateIndexEntry> updatedEntries = updateFn.apply(current);

            AccelerateIndexMeta latest = readOrEmpty(fileIO, metaPath);
            if (latest.version() == expectedVersion) {
                AccelerateIndexMeta newMeta = current.withNewVersion(updatedEntries);
                write(fileIO, metaPath, newMeta);
                return;
            }

            // Version conflict: exponential backoff before retry
            try {
                long backoffMs = Math.min(50L * (1L << attempt), 5000L);
                Thread.sleep(backoffMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("CAS update interrupted", ie);
            }
        }
        throw new IOException(
                "Failed to CAS update accelerate index meta after " + MAX_CAS_RETRIES + " retries");
    }

    /**
     * Merge two entry lists by index_id. Entries from {@code updated} take precedence over entries
     * from {@code existing} with the same index_id.
     */
    static List<AccelerateIndexEntry> mergeEntries(
            List<AccelerateIndexEntry> existing, List<AccelerateIndexEntry> updated) {
        Map<String, AccelerateIndexEntry> merged = new HashMap<>();
        for (AccelerateIndexEntry entry : existing) {
            merged.put(entry.indexId(), entry);
        }
        for (AccelerateIndexEntry entry : updated) {
            merged.put(entry.indexId(), entry);
        }
        return new ArrayList<>(merged.values());
    }
}
