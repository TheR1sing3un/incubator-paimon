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
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link AccelerateIndexMetaIO} with real file I/O. */
class AccelerateIndexMetaIOTest {

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private Path metaPath;

    @BeforeEach
    void setUp() {
        fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.toUri());
        metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
    }

    private AccelerateIndexEntry createEntry(String indexId, AccelerateIndexState state) {
        return new AccelerateIndexEntry(
                indexId,
                5,
                "lumina",
                "l2",
                128,
                state,
                indexId + ".aindex",
                Collections.singletonList(
                        new AccelerateIndexDataFileInfo(indexId + "-f1.parquet", 1000, 0)),
                1000,
                0,
                "digest",
                1,
                100,
                4096,
                null,
                null,
                null,
                0);
    }

    // ---- read ----

    @Test
    void testReadNonExistentReturnsNull() throws IOException {
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(meta).isNull();
    }

    @Test
    void testReadOrEmptyNonExistent() throws IOException {
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.readOrEmpty(fileIO, metaPath);
        assertThat(meta).isNotNull();
        assertThat(meta.version()).isEqualTo(0);
        assertThat(meta.entries()).isEmpty();
    }

    // ---- write + read round-trip ----

    @Test
    void testWriteAndRead() throws IOException {
        AccelerateIndexMeta original =
                new AccelerateIndexMeta(
                        1,
                        System.currentTimeMillis(),
                        Collections.singletonList(
                                createEntry("idx-1", AccelerateIndexState.READY)));

        AccelerateIndexMetaIO.write(fileIO, metaPath, original);
        assertThat(fileIO.exists(metaPath)).isTrue();

        AccelerateIndexMeta loaded = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(loaded).isNotNull();
        assertThat(loaded.version()).isEqualTo(1);
        assertThat(loaded.entries()).hasSize(1);
        assertThat(loaded.entries().get(0).indexId()).isEqualTo("idx-1");
        assertThat(loaded.entries().get(0).state()).isEqualTo(AccelerateIndexState.READY);
    }

    @Test
    void testWriteOverwritesExisting() throws IOException {
        AccelerateIndexMeta v1 =
                new AccelerateIndexMeta(
                        1,
                        1000L,
                        Collections.singletonList(
                                createEntry("idx-1", AccelerateIndexState.READY)));
        AccelerateIndexMetaIO.write(fileIO, metaPath, v1);

        AccelerateIndexMeta v2 =
                new AccelerateIndexMeta(
                        2,
                        2000L,
                        Collections.singletonList(
                                createEntry("idx-2", AccelerateIndexState.PENDING)));
        AccelerateIndexMetaIO.write(fileIO, metaPath, v2);

        AccelerateIndexMeta loaded = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(loaded).isNotNull();
        assertThat(loaded.version()).isEqualTo(2);
        assertThat(loaded.entries()).hasSize(1);
        assertThat(loaded.entries().get(0).indexId()).isEqualTo("idx-2");
    }

    @Test
    void testWriteEmptyMeta() throws IOException {
        AccelerateIndexMeta empty = AccelerateIndexMeta.empty();
        AccelerateIndexMetaIO.write(fileIO, metaPath, empty);

        AccelerateIndexMeta loaded = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(loaded).isNotNull();
        assertThat(loaded.version()).isEqualTo(0);
        assertThat(loaded.entries()).isEmpty();
    }

    @Test
    void testWriteWithMultipleEntries() throws IOException {
        List<AccelerateIndexEntry> entries =
                Arrays.asList(
                        createEntry("idx-1", AccelerateIndexState.READY),
                        createEntry("idx-2", AccelerateIndexState.BUILDING),
                        createEntry("idx-3", AccelerateIndexState.FAILED));
        AccelerateIndexMeta meta = new AccelerateIndexMeta(5, 1000L, entries);
        AccelerateIndexMetaIO.write(fileIO, metaPath, meta);

        AccelerateIndexMeta loaded = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(loaded).isNotNull();
        assertThat(loaded.entries()).hasSize(3);
    }

    @Test
    void testNoTempFileLeftAfterWrite() throws IOException {
        AccelerateIndexMeta meta = AccelerateIndexMeta.empty();
        AccelerateIndexMetaIO.write(fileIO, metaPath, meta);

        // Check no temp files remain in the directory
        Path bucketPath = metaPath.getParent();
        assertThat(
                        fileIO.listStatus(bucketPath).length == 1
                                && fileIO.listStatus(bucketPath)[0]
                                        .getPath()
                                        .getName()
                                        .equals(AccelerateIndexConstants.META_FILE_NAME))
                .isTrue();
    }

    // ---- casUpdate ----

    @Test
    void testCasUpdateOnNewFile() throws IOException {
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                current -> {
                    assertThat(current.version()).isEqualTo(0);
                    assertThat(current.entries()).isEmpty();
                    return Collections.singletonList(
                            createEntry("idx-1", AccelerateIndexState.PENDING));
                });

        AccelerateIndexMeta loaded = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(loaded).isNotNull();
        assertThat(loaded.version()).isEqualTo(1);
        assertThat(loaded.entries()).hasSize(1);
        assertThat(loaded.entries().get(0).indexId()).isEqualTo("idx-1");
    }

    @Test
    void testCasUpdateIncrementsVersion() throws IOException {
        // Initial write
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                current ->
                        Collections.singletonList(
                                createEntry("idx-1", AccelerateIndexState.PENDING)));

        // Second update
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                current -> {
                    assertThat(current.version()).isEqualTo(1);
                    List<AccelerateIndexEntry> updated = new ArrayList<>(current.entries());
                    updated.get(0).setState(AccelerateIndexState.READY);
                    return updated;
                });

        AccelerateIndexMeta loaded = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(loaded).isNotNull();
        assertThat(loaded.version()).isEqualTo(2);
        assertThat(loaded.entries().get(0).state()).isEqualTo(AccelerateIndexState.READY);
    }

    @Test
    void testCasUpdateAddNewEntries() throws IOException {
        // First: create one entry
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                current ->
                        Collections.singletonList(
                                createEntry("idx-1", AccelerateIndexState.READY)));

        // Second: add another entry
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                current -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(current.entries());
                    entries.add(createEntry("idx-2", AccelerateIndexState.PENDING));
                    return entries;
                });

        AccelerateIndexMeta loaded = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(loaded).isNotNull();
        assertThat(loaded.entries()).hasSize(2);
    }

    // ---- mergeEntries ----

    @Test
    void testMergeEntriesUnion() {
        AccelerateIndexEntry e1 = createEntry("idx-1", AccelerateIndexState.READY);
        AccelerateIndexEntry e2 = createEntry("idx-2", AccelerateIndexState.PENDING);
        AccelerateIndexEntry e3 = createEntry("idx-3", AccelerateIndexState.BUILDING);

        List<AccelerateIndexEntry> existing = Arrays.asList(e1, e2);
        List<AccelerateIndexEntry> updated = Collections.singletonList(e3);

        List<AccelerateIndexEntry> merged = AccelerateIndexMetaIO.mergeEntries(existing, updated);
        assertThat(merged).hasSize(3);
    }

    @Test
    void testMergeEntriesUpdatedTakesPrecedence() {
        AccelerateIndexEntry existing = createEntry("idx-1", AccelerateIndexState.PENDING);
        AccelerateIndexEntry updated = createEntry("idx-1", AccelerateIndexState.READY);

        List<AccelerateIndexEntry> merged =
                AccelerateIndexMetaIO.mergeEntries(
                        Collections.singletonList(existing), Collections.singletonList(updated));
        assertThat(merged).hasSize(1);
        assertThat(merged.get(0).state()).isEqualTo(AccelerateIndexState.READY);
    }

    @Test
    void testMergeEntriesEmpty() {
        List<AccelerateIndexEntry> merged =
                AccelerateIndexMetaIO.mergeEntries(
                        Collections.emptyList(), Collections.emptyList());
        assertThat(merged).isEmpty();
    }

    @Test
    void testMergeEntriesExistingEmpty() {
        AccelerateIndexEntry e1 = createEntry("idx-1", AccelerateIndexState.READY);
        List<AccelerateIndexEntry> merged =
                AccelerateIndexMetaIO.mergeEntries(
                        Collections.emptyList(), Collections.singletonList(e1));
        assertThat(merged).hasSize(1);
    }

    // ---- write preserves full JSON content ----

    @Test
    void testWriteProducesValidJson() throws IOException {
        AccelerateIndexEntry entry = createEntry("idx-full", AccelerateIndexState.READY);
        entry.setIndexChecksum("sha256:abc123");
        AccelerateIndexMeta meta =
                new AccelerateIndexMeta(10, 9876543210L, Collections.singletonList(entry));

        AccelerateIndexMetaIO.write(fileIO, metaPath, meta);

        String rawJson = fileIO.readFileUtf8(metaPath);
        assertThat(rawJson).contains("\"version\" : 10");
        assertThat(rawJson).contains("\"updated_at_ms\" : 9876543210");
        assertThat(rawJson).contains("\"index_id\" : \"idx-full\"");
        assertThat(rawJson).contains("\"index_checksum\" : \"sha256:abc123\"");
    }

    // ---- CAS sequential conflict & version integrity ----

    /**
     * Tests sequential CAS updates: each update sees the correct previous version and entries. This
     * verifies version monotonicity and data integrity across multiple sequential updates.
     */
    @Test
    void testCasSequentialConflict() throws IOException {
        // CAS update 1: add entry A
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                current -> {
                    assertThat(current.version()).isEqualTo(0);
                    return Collections.singletonList(
                            createEntry("idx-a", AccelerateIndexState.READY));
                });

        AccelerateIndexMeta after1 = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(after1.version()).isEqualTo(1);
        assertThat(after1.entries()).hasSize(1);

        // CAS update 2: add entry B alongside A
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                current -> {
                    assertThat(current.version()).isEqualTo(1);
                    assertThat(current.entries()).hasSize(1);
                    assertThat(current.entries().get(0).indexId()).isEqualTo("idx-a");
                    List<AccelerateIndexEntry> entries = new ArrayList<>(current.entries());
                    entries.add(createEntry("idx-b", AccelerateIndexState.PENDING));
                    return entries;
                });

        AccelerateIndexMeta after2 = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(after2.version()).isEqualTo(2);
        assertThat(after2.entries()).hasSize(2);

        // CAS update 3: transition entry B from PENDING to READY
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                current -> {
                    assertThat(current.version()).isEqualTo(2);
                    List<AccelerateIndexEntry> entries = new ArrayList<>(current.entries());
                    for (AccelerateIndexEntry e : entries) {
                        if (e.indexId().equals("idx-b")) {
                            e.setState(AccelerateIndexState.READY);
                        }
                    }
                    return entries;
                });

        AccelerateIndexMeta after3 = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(after3.version()).isEqualTo(3);
        assertThat(after3.entries()).hasSize(2);
        // Both entries should now be READY
        for (AccelerateIndexEntry e : after3.entries()) {
            assertThat(e.state()).isEqualTo(AccelerateIndexState.READY);
        }
    }

    /**
     * Tests that version is strictly monotonically increasing across many CAS updates, and that
     * updatedAtMs is always non-decreasing.
     */
    @Test
    void testCasVersionIntegrity() throws IOException {
        int numUpdates = 10;
        long prevUpdatedAt = 0;

        for (int i = 0; i < numUpdates; i++) {
            final int idx = i;
            AccelerateIndexMetaIO.casUpdate(
                    fileIO,
                    metaPath,
                    current -> {
                        assertThat(current.version()).isEqualTo(idx);
                        return Collections.singletonList(
                                createEntry("idx-" + idx, AccelerateIndexState.READY));
                    });
        }

        AccelerateIndexMeta lastMeta = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(lastMeta).isNotNull();
        assertThat(lastMeta.version()).isEqualTo(numUpdates);
        // Only the last entry should remain (each update replaces all entries)
        assertThat(lastMeta.entries()).hasSize(1);
        assertThat(lastMeta.entries().get(0).indexId()).isEqualTo("idx-" + (numUpdates - 1));
        // updatedAtMs should be positive
        assertThat(lastMeta.updatedAtMs()).isGreaterThan(0);
    }

    // ---- write safety: overwrite without crash window ----

    /**
     * Tests that write() overwrites existing meta file directly without leaving the file in a
     * missing state. Previously, write() used deleteQuietly+rename which could lose the file if a
     * crash occurred between delete and rename.
     */
    @Test
    void testWriteOverwriteDoesNotDeleteFirst() throws IOException {
        // Write initial meta
        AccelerateIndexMeta v1 =
                new AccelerateIndexMeta(
                        1,
                        1000L,
                        Collections.singletonList(
                                createEntry("idx-1", AccelerateIndexState.READY)));
        AccelerateIndexMetaIO.write(fileIO, metaPath, v1);
        assertThat(fileIO.exists(metaPath)).isTrue();

        // Overwrite with v2 — file should always exist (no delete-then-rename gap)
        AccelerateIndexMeta v2 =
                new AccelerateIndexMeta(
                        2,
                        2000L,
                        Collections.singletonList(
                                createEntry("idx-2", AccelerateIndexState.PENDING)));
        AccelerateIndexMetaIO.write(fileIO, metaPath, v2);
        assertThat(fileIO.exists(metaPath)).isTrue();

        AccelerateIndexMeta loaded = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(loaded.version()).isEqualTo(2);
        assertThat(loaded.entries().get(0).indexId()).isEqualTo("idx-2");
    }

    // ---- CAS retry on conflict ----

    /**
     * Tests that casUpdate retries with fresh state when a concurrent modification is detected.
     * Simulates conflict by having multiple threads modify the meta concurrently.
     */
    @Test
    void testCasConcurrentUpdatesAllSucceed() throws Exception {
        int numThreads = 4;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);

        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            futures.add(
                    executor.submit(
                            () -> {
                                try {
                                    barrier.await();
                                    AccelerateIndexMetaIO.casUpdate(
                                            fileIO,
                                            metaPath,
                                            current -> {
                                                List<AccelerateIndexEntry> entries =
                                                        new ArrayList<>(current.entries());
                                                entries.add(
                                                        createEntry(
                                                                "idx-t" + threadId,
                                                                AccelerateIndexState.READY));
                                                return entries;
                                            });
                                    successCount.incrementAndGet();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }));
        }

        for (Future<?> f : futures) {
            f.get();
        }
        executor.shutdown();

        // All threads should succeed (CAS retries handle conflicts)
        assertThat(successCount.get()).isEqualTo(numThreads);

        // Meta file should exist and have a valid version
        AccelerateIndexMeta loaded = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(loaded).isNotNull();
        assertThat(loaded.version()).isGreaterThan(0);
    }

    /**
     * Tests that casUpdate re-applies updateFn to the latest state on conflict, not stale state.
     * Verifies version monotonicity after interleaved CAS updates.
     */
    @Test
    void testCasRetriesWithFreshState() throws IOException {
        // Write initial state with version 1
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                current ->
                        Collections.singletonList(
                                createEntry("idx-init", AccelerateIndexState.READY)));

        // Do 5 sequential CAS updates, each adding its own entry
        for (int i = 0; i < 5; i++) {
            final int idx = i;
            AccelerateIndexMetaIO.casUpdate(
                    fileIO,
                    metaPath,
                    current -> {
                        // updateFn should always see the latest version
                        assertThat(current.version()).isGreaterThan(0);
                        List<AccelerateIndexEntry> entries = new ArrayList<>(current.entries());
                        entries.add(createEntry("idx-seq-" + idx, AccelerateIndexState.BUILDING));
                        return entries;
                    });
        }

        AccelerateIndexMeta loaded = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(loaded).isNotNull();
        assertThat(loaded.version()).isEqualTo(6); // 1 initial + 5 updates
    }
}
