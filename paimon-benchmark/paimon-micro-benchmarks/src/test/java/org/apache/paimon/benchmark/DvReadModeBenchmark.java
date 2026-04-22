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

package org.apache.paimon.benchmark;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.Split;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;

/**
 * Benchmark for DV (Deletion Vector) read optimization under 2-level LSM.
 *
 * <p>Three modes are compared across four data layouts:
 *
 * <ul>
 *   <li><b>no-dv</b>: non-DV primary-key table. Under {@link
 *       org.apache.paimon.table.source.MergeTreeSplitGenerator#splitForBatch}, still reaches the
 *       rawConvertible top-level fast path when all files are at one level ({@code oneLevel}), and
 *       still reaches raw read on singleton IntervalPartition sections when L0 is mixed in. What
 *       non-DV PK tables do <em>not</em> have is file-level value-stats pruning (see design doc
 *       §7.1) — that only fires on DV tables and only with a selective predicate.
 *   <li><b>dv-performance</b>: DV enabled, PERFORMANCE mode. Filters out level-0 at scan time and
 *       serves every remaining file through raw read + DV pre-filter. Fastest, but does not see
 *       uncompacted level-0 overwrites.
 *   <li><b>dv-freshness</b>: DV enabled, FRESHNESS mode. Includes level-0, merges it with
 *       overlapping level-1 inside one split; non-overlapping level-1 files still keep the
 *       rawConvertible + DV fast path. Strictly better than non-DV only when a predicate fires
 *       value-stats pruning; without a predicate the two are essentially equivalent.
 * </ul>
 *
 * <h3>Data layout construction</h3>
 *
 * <p>A deletion vector is only produced by the lookup compaction path (i.e. when an L0 batch is
 * compacted against existing L1+ files). A single "write base then compact" does not produce any
 * DV, because the first compaction has nothing to look up against. To reach a realistic "DV already
 * recorded on L1" state, every case runs the same warm-up:
 *
 * <pre>
 *   Stage 1 (warm-up): write {@code [0, BASE_ROW_COUNT)} to L0, commit, full compact
 *                       → L1 holds base data, no DV yet.
 *   Stage 2 (warm-up): write {@code WARMUP_OVERWRITE_COUNT} overwrites on
 *                       {@code [0, WARMUP_OVERWRITE_COUNT)}, commit, full compact
 *                       → DV is written on overlapping base rows in L1;
 *                         overwritten rows land as a new L1 file. No L0 remains.
 * </pre>
 *
 * <p>After the warm-up, each case optionally adds a stage-3 L0 whose keys are <em>disjoint</em>
 * from the warm-up overwrite key range, so the DV state on L1 stays "clean" and the L0-vs-L1
 * overlap is fully controlled by the stage-3 key plan:
 *
 * <ul>
 *   <li>{@link L0Plan#NONE}: skip stage 3 → steady state with no L0.
 *   <li>{@link L0Plan#NARROW}: stage 3 writes a small concentrated batch on {@code
 *       [WARMUP_OVERWRITE_COUNT, WARMUP_OVERWRITE_COUNT + NARROW_L0_COUNT)}, without compacting →
 *       L0 overlaps only a small sub-range of L1.
 *   <li>{@link L0Plan#WIDE}: stage 3 writes a larger scattered batch sampled from {@code
 *       [WARMUP_OVERWRITE_COUNT, BASE_ROW_COUNT)}, without compacting → L0 overlaps most L1 files.
 * </ul>
 *
 * <h3>Semantic caveat for the L0 cases</h3>
 *
 * <p>PERFORMANCE mode filters out L0 at scan time, so in {@code testReadWithL0Narrow} and {@code
 * testReadWithL0Wide} it returns a snapshot that does <em>not</em> reflect stage-3 overwrites.
 * FRESHNESS and no-dv return the fully up-to-date logical state. This is the design-intended
 * trade-off; row counts printed before each benchmark run verify the physical outputs are
 * comparable (identical keyspace cardinality) — only the per-key value differs for overwritten keys
 * under PERFORMANCE.
 */
public class DvReadModeBenchmark extends TableBenchmark {

    private static final int BASE_ROW_COUNT = 500_000;

    /** Stage-2 (warm-up) overwrites: keys {@code [0, WARMUP_OVERWRITE_COUNT)}. */
    private static final int WARMUP_OVERWRITE_COUNT = 100_000;

    /** Stage-3 narrow L0: 25k keys concentrated in a small range disjoint from stage 2. */
    private static final int NARROW_L0_COUNT = 25_000;

    /** Stage-3 wide L0: 120k keys scattered across {@code [100k, 500k)}. */
    private static final int WIDE_L0_COUNT = 120_000;

    /** Range predicate: {@code k BETWEEN PREDICATE_LO AND PREDICATE_HI - 1}. */
    private static final int PREDICATE_LO = 200_000;

    private static final int PREDICATE_HI = 250_000;

    private static final int READ_ITERATIONS = 3;
    private static final int BENCHMARK_ITERS = 10;
    private static final int VALUE_COUNT = 20;

    /** Deterministic seed so the "wide" sampling is reproducible across runs. */
    private static final long WIDE_SAMPLE_SEED = 0xDEADBEEFL;

    private final RandomDataGenerator random = new RandomDataGenerator();

    /**
     * Case 1: no L0. All three modes go through the {@code splitForBatch()} top-level fast path —
     * DV modes satisfy the {@code deletionVectorsEnabled} branch, non-DV satisfies {@code
     * oneLevel}. Without a selective predicate, there is no reader-level difference, so all three
     * should land at roughly the same speed.
     *
     * <p>Observed (macOS, Apple M2 Pro, JDK 8), Best/Avg ms per 1.5M rows:
     *
     * <pre>
     *   no-dv           293 / 295    1.00X
     *   dv-performance  288 / 295    1.00X
     *   dv-freshness    293 / 333    1.00X
     * </pre>
     */
    @Test
    public void testReadNoL0() throws Exception {
        runCase("dv-read-no-l0", L0Plan.NONE, null);
    }

    /**
     * Case 2: L0 with a narrow key-range (25k keys concentrated in a small slice).
     *
     * <p>{@code dv-performance} wins because it skips L0 entirely. {@code dv-freshness} and {@code
     * no-dv} both fall into IntervalPartition; non-overlapping L1 files go raw, overlapping ones go
     * merge — and since non-DV PK tables also reach the raw path for singleton sections (see design
     * doc §7.1), without a predicate the two end up essentially equal.
     *
     * <p>Observed (macOS, Apple M2 Pro, JDK 8), Best/Avg ms per 1.5M rows:
     *
     * <pre>
     *   no-dv           346 / 471    1.00X
     *   dv-performance  262 / 285    1.32X
     *   dv-freshness    312 / 325    1.11X
     * </pre>
     */
    @Test
    public void testReadWithL0Narrow() throws Exception {
        runCase("dv-read-l0-narrow", L0Plan.NARROW, null);
    }

    /**
     * Case 3: L0 with a wide key-range (120k keys scattered across 400k key slice).
     *
     * <p>Same mechanics as Case 2 but the L0 now overlaps most L1 files, so the rawConvertible
     * fraction under FRESHNESS shrinks. {@code dv-performance} still wins by skipping L0; FRESHNESS
     * and no-dv stay paired. See {@link #testReadWithL0WideFiltered} for the variant that actually
     * exercises FRESHNESS's only "strictly better than non-DV" lever.
     *
     * <p>Observed (macOS, Apple M2 Pro, JDK 8), Best/Avg ms per 1.5M rows:
     *
     * <pre>
     *   no-dv           340 / 368    1.00X
     *   dv-performance  263 / 268    1.29X
     *   dv-freshness    346 / 356    0.98X
     * </pre>
     */
    @Test
    public void testReadWithL0Wide() throws Exception {
        runCase("dv-read-l0-wide", L0Plan.WIDE, null);
    }

    /**
     * Case 4: same data layout as {@link #testReadWithL0Wide}, plus a selective range predicate
     * {@code k BETWEEN PREDICATE_LO AND PREDICATE_HI - 1} (~10% of the keyspace, hitting ~2 of the
     * ~12 base L1 files).
     *
     * <p>This is the case that actually validates the design's "FRESHNESS &ge; non-DV" claim:
     * file-level value-stats pruning is disabled on non-DV primary-key tables (see §7.1 of the
     * design doc), so non-DV must open every L1 file regardless of the predicate. Under FRESHNESS,
     * L1+ files whose min/max stats fall outside the predicate range are skipped at plan time.
     *
     * <p>Observed (macOS, Apple M2 Pro, JDK 8), Best/Avg ms per 150k matching rows:
     *
     * <pre>
     *   no-dv           151 / 180    1.00X   (no file-level pruning, scans all L1)
     *   dv-performance   51 /  52    2.96X   (pruning + skips L0)
     *   dv-freshness    134 / 148    1.13X   (pruning, still reads L0)
     * </pre>
     *
     * <p>The 1.13X gap between {@code no-dv} and {@code dv-freshness} is the concrete measurement
     * of FRESHNESS's "lower-bound advantage" over non-DV PK tables.
     */
    @Test
    public void testReadWithL0WideFiltered() throws Exception {
        runCase("dv-read-l0-wide-filtered", L0Plan.WIDE, PredicatePlan.RANGE);
    }

    private void runCase(String label, L0Plan plan, PredicatePlan predicatePlan) throws Exception {
        Table noDv = prepareTable(noDvOptions(), label + "_no_dv", plan);
        Table dvPerf =
                prepareTable(
                        dvOptions(CoreOptions.DvReadMode.PERFORMANCE), label + "_dv_perf", plan);
        Table dvFresh =
                prepareTable(
                        dvOptions(CoreOptions.DvReadMode.FRESHNESS), label + "_dv_fresh", plan);

        // Print the logical row count each mode will return, to make any stale-read
        // difference under PERFORMANCE explicit.
        System.out.printf(
                "[%s] row counts: no-dv=%d, dv-perf=%d, dv-fresh=%d%n",
                label,
                countRows(noDv, predicatePlan),
                countRows(dvPerf, predicatePlan),
                countRows(dvFresh, predicatePlan));

        long rowScaleHint =
                predicatePlan == null ? BASE_ROW_COUNT : (long) (PREDICATE_HI - PREDICATE_LO);
        Benchmark benchmark =
                new Benchmark(label, (long) READ_ITERATIONS * rowScaleHint)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);

        addReadCase(benchmark, "no-dv", noDv, predicatePlan);
        addReadCase(benchmark, "dv-performance", dvPerf, predicatePlan);
        addReadCase(benchmark, "dv-freshness", dvFresh, predicatePlan);
        benchmark.run();
    }

    // ========================= Options =========================

    /**
     * Common options: 2-level LSM, explicit-compaction-only (auto trigger suppressed so stage-3 L0
     * stays put, but explicit {@code write.compact(...)} in warm-up stages still runs).
     *
     * <p>{@code target-file-size} is intentionally shrunk well below the 128MB primary-key default
     * so that the ~100MB base is split into roughly a dozen L1 files. Otherwise the whole base fits
     * in one L1 file, any non-empty stage-3 L0 overlaps all of it, and FRESHNESS loses its
     * rawConvertible-on-non-overlapping-L1 advantage entirely (because there is no "non-overlapping
     * L1" to speak of).
     */
    private Options baseOptions() {
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_ORC);
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.NUM_LEVELS, 2);
        options.set(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER, 999);
        options.set(CoreOptions.NUM_SORTED_RUNS_STOP_TRIGGER, 999);
        options.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.ofMebiBytes(8));
        return options;
    }

    private Options noDvOptions() {
        Options options = baseOptions();
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, false);
        return options;
    }

    private Options dvOptions(CoreOptions.DvReadMode readMode) {
        Options options = baseOptions();
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        options.set(CoreOptions.DV_READ_MODE, readMode);
        return options;
    }

    // ========================= Data Preparation =========================

    /**
     * Key-distribution shape of the stage-3 L0 batch. The baseline is stage-2 L1: {@code
     * BASE_ROW_COUNT=500k} keys spread across {@code [0, 500k)}, split by {@code
     * TARGET_FILE_SIZE=8MB} into roughly <b>12 L1 files</b>, each covering a contiguous ~42k-wide
     * key slice. The three plans pick the L0 overlap fraction against this L1 layout:
     *
     * <ul>
     *   <li>{@link #NONE}: no stage-3 L0. All 12 L1 files run non-overlapping → every mode stays on
     *       the top-level fast path.
     *   <li>{@link #NARROW}: {@code NARROW_L0_COUNT=25k} keys packed into the contiguous range
     *       {@code [100k, 125k)}. Overlaps only the 1 L1 file covering {@code [~84k, ~126k)} (and,
     *       at the boundary, brushes one neighbor). <b>Overlap ratio ≈ 1/12 ≈ 8%</b> — the
     *       remaining 10–11 L1 files still go raw under FRESHNESS, so the merge cost is paid on
     *       only ~1 file per bucket.
     *   <li>{@link #WIDE}: {@code WIDE_L0_COUNT=120k} keys uniformly sampled from {@code [100k,
     *       500k)} (a 400k-wide pool). Sampling density is 120k / 400k = 30%, meaning every L1 file
     *       in {@code [100k, 500k)} contains on average one L0 key every 3–4 rows — they all
     *       overlap in practice. Only the ~2 L1 files covering {@code [0, 100k)} stay clean.
     *       <b>Overlap ratio ≈ 10/12 ≈ 83%</b> — FRESHNESS loses its fast-path advantage almost
     *       entirely and degenerates to whole-bucket merge-on-read.
     * </ul>
     *
     * <p>The NARROW vs WIDE split is what makes the benchmark discriminate between "FRESHNESS
     * retains most of PERFORMANCE's raw-read speedup" (NARROW) and "FRESHNESS collapses back to
     * non-DV parity on raw speed; only value-stats pruning can save it" (WIDE).
     */
    private enum L0Plan {
        NONE,
        NARROW,
        WIDE
    }

    private InternalRow newRowWithKey(int key) {
        GenericRow row = new GenericRow(1 + VALUE_COUNT);
        row.setField(0, key);
        for (int i = 1; i <= VALUE_COUNT; i++) {
            row.setField(i, BinaryString.fromString(random.nextHexString(10)));
        }
        return row;
    }

    /**
     * Warm-up (stages 1 and 2) guarantees L1 has DV-marked rows, mirroring a realistic DV steady
     * state before any stage-3 L0 is layered on top.
     */
    private Table prepareTable(Options options, String tableName, L0Plan plan) throws Exception {
        Table table = createTable(options, tableName, Collections.singletonList("k"));
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = writeBuilder.newWrite();
        write.withIOManager(new IOManagerImpl(tempFile.toString()));
        StreamTableCommit commit = writeBuilder.newCommit();

        // Stage 1: base data → L0, then full compact → L1 (no DV yet).
        for (int i = 0; i < BASE_ROW_COUNT; i++) {
            write.write(newRowWithKey(i));
        }
        List<CommitMessage> messages = write.prepareCommit(false, 1);
        commit.commit(1, messages);
        write.compact(EMPTY_ROW, 0, true);
        messages = write.prepareCommit(true, 2);
        commit.commit(2, messages);

        // Stage 2 (warm-up): overwrite keys [0, WARMUP_OVERWRITE_COUNT), full compact.
        // This is the only compaction that runs against a non-empty L1, so this is
        // where DV actually gets written.
        for (int i = 0; i < WARMUP_OVERWRITE_COUNT; i++) {
            write.write(newRowWithKey(i));
        }
        messages = write.prepareCommit(false, 3);
        commit.commit(3, messages);
        write.compact(EMPTY_ROW, 0, true);
        messages = write.prepareCommit(true, 4);
        commit.commit(4, messages);

        // Stage 3 (case-specific): optional L0 that stays uncompacted.
        // Keys are disjoint from stage 2's [0, WARMUP_OVERWRITE_COUNT) so stage-3 L0
        // only interacts with the original base L1 file, keeping case semantics clean.
        int[] stage3Keys = stage3Keys(plan);
        if (stage3Keys.length > 0) {
            for (int key : stage3Keys) {
                write.write(newRowWithKey(key));
            }
            messages = write.prepareCommit(false, 5);
            commit.commit(5, messages);
            // No compact: L0 remains, no DV produced for it.
        }

        write.close();
        commit.close();
        return table;
    }

    private int[] stage3Keys(L0Plan plan) {
        switch (plan) {
            case NONE:
                return new int[0];
            case NARROW:
                {
                    int[] keys = new int[NARROW_L0_COUNT];
                    for (int i = 0; i < NARROW_L0_COUNT; i++) {
                        keys[i] = WARMUP_OVERWRITE_COUNT + i;
                    }
                    return keys;
                }
            case WIDE:
                {
                    // Sample WIDE_L0_COUNT distinct keys from [WARMUP_OVERWRITE_COUNT,
                    // BASE_ROW_COUNT)
                    // via a partial Fisher-Yates shuffle. Deterministic seed for reproducibility.
                    int pool = BASE_ROW_COUNT - WARMUP_OVERWRITE_COUNT;
                    int[] universe = new int[pool];
                    for (int i = 0; i < pool; i++) {
                        universe[i] = WARMUP_OVERWRITE_COUNT + i;
                    }
                    Random r = new Random(WIDE_SAMPLE_SEED);
                    for (int i = 0; i < WIDE_L0_COUNT; i++) {
                        int j = i + r.nextInt(pool - i);
                        int tmp = universe[i];
                        universe[i] = universe[j];
                        universe[j] = tmp;
                    }
                    int[] keys = new int[WIDE_L0_COUNT];
                    System.arraycopy(universe, 0, keys, 0, WIDE_L0_COUNT);
                    return keys;
                }
            default:
                throw new IllegalStateException("Unknown plan: " + plan);
        }
    }

    // ========================= Read Helpers =========================

    private enum PredicatePlan {
        /** {@code k BETWEEN PREDICATE_LO AND PREDICATE_HI - 1}. */
        RANGE
    }

    private org.apache.paimon.predicate.Predicate buildPredicate(
            Table table, PredicatePlan predicatePlan) {
        if (predicatePlan == null) {
            return null;
        }
        switch (predicatePlan) {
            case RANGE:
                return new org.apache.paimon.predicate.PredicateBuilder(table.rowType())
                        .between(0, PREDICATE_LO, PREDICATE_HI - 1);
            default:
                throw new IllegalStateException("Unknown predicate plan: " + predicatePlan);
        }
    }

    private long countRows(Table table, PredicatePlan predicatePlan) throws Exception {
        org.apache.paimon.table.source.ReadBuilder readBuilder = table.newReadBuilder();
        org.apache.paimon.predicate.Predicate predicate = buildPredicate(table, predicatePlan);
        if (predicate != null) {
            readBuilder = readBuilder.withFilter(predicate);
        }
        long count = 0;
        List<Split> splits = readBuilder.newScan().plan().splits();
        for (Split split : splits) {
            try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(split)) {
                RecordReader.RecordIterator<InternalRow> it;
                while ((it = reader.readBatch()) != null) {
                    while (it.next() != null) {
                        count++;
                    }
                    it.releaseBatch();
                }
            }
        }
        return count;
    }

    private void addReadCase(
            Benchmark benchmark, String name, Table table, PredicatePlan predicatePlan) {
        benchmark.addCase(
                name,
                BENCHMARK_ITERS,
                () -> {
                    try {
                        for (int i = 0; i < READ_ITERATIONS; i++) {
                            org.apache.paimon.table.source.ReadBuilder readBuilder =
                                    table.newReadBuilder();
                            org.apache.paimon.predicate.Predicate predicate =
                                    buildPredicate(table, predicatePlan);
                            if (predicate != null) {
                                readBuilder = readBuilder.withFilter(predicate);
                            }
                            List<Split> splits = readBuilder.newScan().plan().splits();
                            AtomicLong count = new AtomicLong(0);
                            for (Split split : splits) {
                                RecordReader<InternalRow> reader =
                                        readBuilder.newRead().createReader(split);
                                reader.forEachRemaining(row -> count.incrementAndGet());
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
