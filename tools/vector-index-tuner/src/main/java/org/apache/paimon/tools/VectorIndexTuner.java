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

package org.apache.paimon.tools;

import org.aliyun.lumina.Lumina;
import org.aliyun.lumina.LuminaBuilder;
import org.aliyun.lumina.LuminaDataset;
import org.aliyun.lumina.LuminaFileInput;
import org.aliyun.lumina.LuminaFileOutput;
import org.aliyun.lumina.LuminaSearcher;
import org.aliyun.lumina.MetricType;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

/**
 * Vector Index Parameter Tuner.
 *
 * <p>Reads a .vector.bin file, computes brute-force ground truth for sample queries, then
 * iterates over parameter combinations building DiskANN indexes and measuring recall@K.
 * Stops when recall >= target (default 95%).
 *
 * <p>Usage: java -jar vector-index-tuner.jar <vector.bin> [options]
 *
 * <p>Options:
 *   --dim=2048          Vector dimension (default: 2048)
 *   --metric=cosine     Distance metric: l2, cosine, inner_product (default: cosine)
 *   --topk=100          Top-K for recall measurement (default: 100)
 *   --queries=200       Number of sample queries (default: 200)
 *   --target=0.95       Target recall (default: 0.95)
 *   --max-rows=50000    Max rows to use from the file (default: all)
 *   --threads=8         Build thread count (default: 8)
 */
public class VectorIndexTuner {

    private static final int DEFAULT_DIM = 2048;
    private static final String DEFAULT_METRIC = "cosine";
    private static final int DEFAULT_TOPK = 100;
    private static final int DEFAULT_QUERIES = 200;
    private static final double DEFAULT_TARGET = 0.95;
    private static final int DEFAULT_THREADS = 8;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java -jar vector-index-tuner.jar <vector.bin> [options]");
            System.err.println("Options: --dim=2048 --metric=cosine --topk=100 --queries=200 --target=0.95 --max-rows=50000 --threads=8");
            System.exit(1);
        }

        String vectorFile = args[0];
        int dim = getIntArg(args, "dim", DEFAULT_DIM);
        String metric = getStringArg(args, "metric", DEFAULT_METRIC);
        int topK = getIntArg(args, "topk", DEFAULT_TOPK);
        int numQueries = getIntArg(args, "queries", DEFAULT_QUERIES);
        double targetRecall = getDoubleArg(args, "target", DEFAULT_TARGET);
        int maxRows = getIntArg(args, "max-rows", -1);
        int threads = getIntArg(args, "threads", DEFAULT_THREADS);

        Lumina.loadLibrary();

        int bytesPerVector = ((dim * 4 + 7) / 8) * 8;
        File file = new File(vectorFile);
        long fileSize = file.length();
        int totalRows = (int) (fileSize / bytesPerVector);

        if (maxRows > 0 && maxRows < totalRows) {
            totalRows = maxRows;
        }

        System.out.printf("=== Vector Index Tuner ===%n");
        System.out.printf("File: %s (%d rows, %d dim, %s)%n", vectorFile, totalRows, dim, metric);
        System.out.printf("TopK: %d, Queries: %d, Target recall: %.1f%%%n", topK, numQueries, targetRecall * 100);
        System.out.println();

        // Load vectors
        System.out.println("Loading vectors...");
        float[] vectors = loadVectors(file, totalRows, dim, bytesPerVector);
        System.out.printf("Loaded %d vectors (%d MB)%n", totalRows, (long) totalRows * dim * 4 / 1024 / 1024);

        // Generate random queries from the dataset
        Random rng = new Random(42);
        int[] queryIndices = new int[numQueries];
        for (int i = 0; i < numQueries; i++) {
            queryIndices[i] = rng.nextInt(totalRows);
        }
        float[] queryVectors = new float[numQueries * dim];
        for (int i = 0; i < numQueries; i++) {
            System.arraycopy(vectors, queryIndices[i] * dim, queryVectors, i * dim, dim);
        }

        // Compute ground truth (brute-force)
        System.out.println("Computing brute-force ground truth...");
        int[][] groundTruth = computeGroundTruth(vectors, totalRows, queryVectors, numQueries, dim, topK, metric);
        System.out.println("Ground truth computed.");
        System.out.println();

        // Parameter sweep: encoding=sq8 fixed, search_list_size=1500 fixed, beam_width=4 fixed
        // Enumerate ef_construction and neighbor_count from low to high, stop when target met
        String encoding = "sq8";
        int searchListSize = 1500;
        int beamWidth = 4;
        int[] efConstructions = {128, 256, 512, 1024, 2048};
        int[] neighborCounts = {32, 64, 96, 128, 160};

        List<Result> results = new ArrayList<>();
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "vector-tuner-" + System.currentTimeMillis());
        tempDir.mkdirs();

        MetricType metricType = metricToLuminaType(metric);
        boolean targetMet = false;

        System.out.println("Starting parameter sweep (sq8, list_size=1500, beam_width=4)...");
        System.out.printf("%-6s %-6s %-8s %-10s %-10s%n", "ef_c", "nbr", "recall", "build_ms", "search_ms");
        System.out.println("--------------------------------------------------");

        for (int efC : efConstructions) {
            if (targetMet) break;
            for (int nbr : neighborCounts) {
                if (targetMet) break;

                Map<String, String> buildOpts = new LinkedHashMap<>();
                buildOpts.put("index.type", "diskann");
                buildOpts.put("index.dimension", String.valueOf(dim));
                buildOpts.put("encoding.type", encoding);
                buildOpts.put("diskann.build.ef_construction", String.valueOf(efC));
                buildOpts.put("diskann.build.neighbor_count", String.valueOf(nbr));
                buildOpts.put("diskann.build.thread_count", String.valueOf(threads));
                buildOpts.put("pretrain.sample_ratio", "0.2");

                File indexFile = new File(tempDir, "index_" + efC + "_" + nbr + ".idx");

                long buildStart = System.currentTimeMillis();
                try {
                    buildIndex(vectors, totalRows, dim, metricType, buildOpts, indexFile);
                } catch (Exception e) {
                    System.err.printf("Build failed for ef=%d nbr=%d: %s%n", efC, nbr, e.getMessage());
                    continue;
                }
                long buildTime = System.currentTimeMillis() - buildStart;

                Map<String, String> searchOpts = new LinkedHashMap<>();
                searchOpts.put("diskann.search.list_size", String.valueOf(searchListSize));
                searchOpts.put("diskann.search.beam_width", String.valueOf(beamWidth));

                long searchStart = System.currentTimeMillis();
                double recall;
                try {
                    recall = searchAndMeasureRecall(
                            indexFile, dim, metricType, buildOpts, searchOpts,
                            queryVectors, numQueries, topK, groundTruth);
                } catch (Exception e) {
                    System.err.printf("Search failed for ef=%d nbr=%d: %s%n", efC, nbr, e.getMessage());
                    indexFile.delete();
                    continue;
                }
                long searchTime = System.currentTimeMillis() - searchStart;

                System.out.printf("%-6d %-6d %-8.4f %-10d %-10d%n", efC, nbr, recall, buildTime, searchTime);

                results.add(new Result(encoding, efC, nbr, 0, searchListSize, beamWidth, recall, buildTime, searchTime));
                indexFile.delete();

                if (recall >= targetRecall) {
                    targetMet = true;
                    System.out.printf("%n=== TARGET RECALL %.1f%% ACHIEVED ===%n", targetRecall * 100);
                    System.out.println("Optimal parameters:");
                    System.out.printf("  encoding.type = %s%n", encoding);
                    System.out.printf("  diskann.build.ef_construction = %d%n", efC);
                    System.out.printf("  diskann.build.neighbor_count = %d%n", nbr);
                    System.out.printf("  diskann.build.thread_count = %d%n", threads);
                    System.out.printf("  diskann.search.list_size = %d%n", searchListSize);
                    System.out.printf("  diskann.search.beam_width = %d%n", beamWidth);
                    System.out.printf("  recall@%d = %.4f%n", topK, recall);
                    System.out.printf("  build_time = %d ms%n", buildTime);
                    System.out.printf("  search_time = %d ms (for %d queries)%n", searchTime, numQueries);
                }
            }
        }

        if (!targetMet) {
            System.out.printf("%nWARNING: Target recall %.1f%% NOT achieved with any parameter combination.%n", targetRecall * 100);
            System.out.println("Best result:");
            results.sort(Comparator.comparingDouble((Result r) -> -r.recall));
            if (!results.isEmpty()) {
                Result best = results.get(0);
                System.out.printf("  ef_construction=%d, neighbor_count=%d, recall=%.4f%n",
                        best.efConstruction, best.neighborCount, best.recall);
            }
        }

        // Print all results summary
        System.out.println("\n=== ALL RESULTS ===");
        System.out.printf("%-6s %-6s %-8s %-10s%n", "ef_c", "nbr", "recall", "build_ms");
        for (Result r : results) {
            System.out.printf("%-6d %-6d %-8.4f %-10d%n", r.efConstruction, r.neighborCount, r.recall, r.buildTimeMs);
        }

        // Cleanup
        tempDir.delete();
    }

    private static float[] loadVectors(File file, int rows, int dim, int bytesPerVector) throws Exception {
        float[] result = new float[rows * dim];
        try (RandomAccessFile raf = new RandomAccessFile(file, "r");
             FileChannel channel = raf.getChannel()) {
            ByteBuffer buf = ByteBuffer.allocate(bytesPerVector).order(ByteOrder.nativeOrder());
            for (int i = 0; i < rows; i++) {
                buf.clear();
                channel.read(buf);
                buf.flip();
                for (int d = 0; d < dim; d++) {
                    result[i * dim + d] = buf.getFloat();
                }
            }
        }
        return result;
    }

    private static int[][] computeGroundTruth(float[] vectors, int n, float[] queries, int nq, int dim, int topK, String metric) {
        int[][] truth = new int[nq][topK];
        for (int q = 0; q < nq; q++) {
            // Max-heap: peek() returns the LARGEST distance among the K nearest
            PriorityQueue<long[]> heap = new PriorityQueue<>(
                    (a, b) -> Float.compare(
                            Float.intBitsToFloat((int) b[1]),
                            Float.intBitsToFloat((int) a[1])));
            for (int i = 0; i < n; i++) {
                float dist = computeDistance(vectors, i * dim, queries, q * dim, dim, metric);
                int distBits = Float.floatToIntBits(dist);
                if (heap.size() < topK) {
                    heap.add(new long[]{i, distBits});
                } else if (dist < Float.intBitsToFloat((int) heap.peek()[1])) {
                    heap.poll();
                    heap.add(new long[]{i, distBits});
                }
            }
            List<long[]> sorted = new ArrayList<>(heap);
            sorted.sort((a, b) -> Float.compare(
                    Float.intBitsToFloat((int) a[1]),
                    Float.intBitsToFloat((int) b[1])));
            for (int i = 0; i < topK && i < sorted.size(); i++) {
                truth[q][i] = (int) sorted.get(i)[0];
            }
        }
        return truth;
    }

    private static float computeDistance(float[] a, int aOff, float[] b, int bOff, int dim, String metric) {
        switch (metric) {
            case "l2": {
                float sum = 0;
                for (int i = 0; i < dim; i++) {
                    float d = a[aOff + i] - b[bOff + i];
                    sum += d * d;
                }
                return sum;
            }
            case "cosine": {
                float dot = 0, normA = 0, normB = 0;
                for (int i = 0; i < dim; i++) {
                    dot += a[aOff + i] * b[bOff + i];
                    normA += a[aOff + i] * a[aOff + i];
                    normB += b[bOff + i] * b[bOff + i];
                }
                float sim = dot / (float) (Math.sqrt(normA) * Math.sqrt(normB) + 1e-10);
                return 1.0f - sim; // distance = 1 - similarity
            }
            case "inner_product": {
                float dot = 0;
                for (int i = 0; i < dim; i++) {
                    dot += a[aOff + i] * b[bOff + i];
                }
                return -dot; // negate so smaller = better
            }
            default:
                throw new IllegalArgumentException("Unknown metric: " + metric);
        }
    }

    private static void buildIndex(float[] vectors, int n, int dim, MetricType metricType,
                                   Map<String, String> opts, File outputFile) throws Exception {
        LuminaBuilder builder = LuminaBuilder.create("diskann", dim, metricType, opts);

        // Pretrain with 20% sample
        int sampleSize = Math.max(1000, (int) (n * 0.2));
        if (sampleSize > n) sampleSize = n;
        float[] sample = Arrays.copyOf(vectors, sampleSize * dim);
        builder.pretrainFrom(new InMemoryDataset(sample, sampleSize, dim));

        // Insert all vectors
        builder.insertFrom(new InMemoryDataset(vectors, n, dim));

        // Dump to file
        try (FileOutputStream fos = new FileOutputStream(outputFile)) {
            builder.dump(new OutputStreamAdapter(fos));
        }
        builder.close();
    }

    private static double searchAndMeasureRecall(File indexFile, int dim, MetricType metricType,
                                                  Map<String, String> buildOpts,
                                                  Map<String, String> searchOpts,
                                                  float[] queries, int nq, int topK,
                                                  int[][] groundTruth) throws Exception {
        Map<String, String> allOpts = new LinkedHashMap<>(buildOpts);
        allOpts.putAll(searchOpts);

        LuminaSearcher searcher = LuminaSearcher.create("diskann", dim, metricType, searchOpts);
        RandomAccessFile raf = new RandomAccessFile(indexFile, "r");
        RandomAccessFileInput fileInput = new RandomAccessFileInput(raf);
        searcher.open(fileInput, indexFile.length());

        float[] distances = new float[nq * topK];
        long[] labels = new long[nq * topK];
        searcher.search(nq, queries, topK, distances, labels, searchOpts);
        searcher.close();
        raf.close();

        // Compute recall
        double totalRecall = 0;
        for (int q = 0; q < nq; q++) {
            Set<Integer> truthSet = new HashSet<>();
            for (int i = 0; i < topK; i++) {
                truthSet.add(groundTruth[q][i]);
            }
            int hits = 0;
            for (int i = 0; i < topK; i++) {
                int label = (int) labels[q * topK + i];
                if (label >= 0 && truthSet.contains(label)) {
                    hits++;
                }
            }
            totalRecall += (double) hits / topK;
        }
        return totalRecall / nq;
    }

    /** Adapts a RandomAccessFile to LuminaFileInput. */
    static class RandomAccessFileInput implements LuminaFileInput {
        private final RandomAccessFile raf;

        RandomAccessFileInput(RandomAccessFile raf) {
            this.raf = raf;
        }

        @Override
        public int read(byte[] b, int off, int len) throws java.io.IOException {
            return raf.read(b, off, len);
        }

        @Override
        public void seek(long pos) throws java.io.IOException {
            raf.seek(pos);
        }

        @Override
        public long getPos() throws java.io.IOException {
            return raf.getFilePointer();
        }

        @Override
        public void close() throws java.io.IOException {
            raf.close();
        }
    }

    /** Adapts a FileOutputStream to LuminaFileOutput. */
    static class OutputStreamAdapter implements LuminaFileOutput {
        private final FileOutputStream fos;

        OutputStreamAdapter(FileOutputStream fos) {
            this.fos = fos;
        }

        @Override
        public void write(byte[] b, int off, int len) throws java.io.IOException {
            fos.write(b, off, len);
            pos += len;
        }

        @Override
        public long getPos() {
            return pos;
        }

        private long pos = 0;

        @Override
        public void close() throws java.io.IOException {
            fos.close();
        }

        @Override
        public void flush() throws java.io.IOException {
            fos.flush();
        }
    }

    private static MetricType metricToLuminaType(String metric) {
        switch (metric) {
            case "l2": return MetricType.L2;
            case "inner_product": return MetricType.INNER_PRODUCT;
            case "cosine": return MetricType.COSINE;
            default: throw new IllegalArgumentException("Unknown metric: " + metric);
        }
    }

    private static void printResult(Result r) {
        System.out.printf("  encoding.type = %s%n", r.encoding);
        System.out.printf("  diskann.build.ef_construction = %d%n", r.efConstruction);
        System.out.printf("  diskann.build.neighbor_count = %d%n", r.neighborCount);
        if (r.encoding.equals("pq") && r.pqM > 0) {
            System.out.printf("  encoding.pq.m = %d%n", r.pqM);
        }
        System.out.printf("  diskann.search.list_size = %d%n", r.searchListSize);
        System.out.printf("  diskann.search.beam_width = %d%n", r.beamWidth);
        System.out.printf("  recall@%d = %.4f%n", 100, r.recall);
        System.out.printf("  build_time = %d ms, search_time = %d ms%n", r.buildTimeMs, r.searchTimeMs);
    }

    private static int getIntArg(String[] args, String name, int defaultVal) {
        for (String arg : args) {
            if (arg.startsWith("--" + name + "=")) {
                return Integer.parseInt(arg.substring(name.length() + 3));
            }
        }
        return defaultVal;
    }

    private static double getDoubleArg(String[] args, String name, double defaultVal) {
        for (String arg : args) {
            if (arg.startsWith("--" + name + "=")) {
                return Double.parseDouble(arg.substring(name.length() + 3));
            }
        }
        return defaultVal;
    }

    private static String getStringArg(String[] args, String name, String defaultVal) {
        for (String arg : args) {
            if (arg.startsWith("--" + name + "=")) {
                return arg.substring(name.length() + 3);
            }
        }
        return defaultVal;
    }

    /** In-memory LuminaDataset backed by a float array. */
    static class InMemoryDataset implements LuminaDataset {
        private final float[] data;
        private final int totalRows;
        private final int dim;
        private int cursor;
        private static final int BATCH_SIZE = 10000;

        InMemoryDataset(float[] data, int totalRows, int dim) {
            this.data = data;
            this.totalRows = totalRows;
            this.dim = dim;
            this.cursor = 0;
        }

        @Override
        public int dim() {
            return dim;
        }

        @Override
        public long totalSize() {
            return totalRows;
        }

        @Override
        public long getNextBatch(float[] buffer, long[] ids) {
            if (cursor >= totalRows) {
                return 0;
            }
            int batchRows = Math.min(BATCH_SIZE, totalRows - cursor);
            int floats = batchRows * dim;
            System.arraycopy(data, cursor * dim, buffer, 0, floats);
            for (int i = 0; i < batchRows; i++) {
                ids[i] = cursor + i;
            }
            cursor += batchRows;
            return batchRows;
        }
    }

    static class Result {
        final String encoding;
        final int efConstruction;
        final int neighborCount;
        final int pqM;
        final int searchListSize;
        final int beamWidth;
        final double recall;
        final long buildTimeMs;
        final long searchTimeMs;

        Result(String encoding, int efConstruction, int neighborCount, int pqM,
               int searchListSize, int beamWidth, double recall, long buildTimeMs, long searchTimeMs) {
            this.encoding = encoding;
            this.efConstruction = efConstruction;
            this.neighborCount = neighborCount;
            this.pqM = pqM;
            this.searchListSize = searchListSize;
            this.beamWidth = beamWidth;
            this.recall = recall;
            this.buildTimeMs = buildTimeMs;
            this.searchTimeMs = searchTimeMs;
        }
    }
}
