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

package org.apache.paimon.rest.server.utils;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.PositionOutputStreamWrapper;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;
import org.apache.paimon.options.Options;

import com.kuaishou.kling.lakehouse.metrics.MetricsReporter;
import com.kuaishou.kling.lakehouse.metrics.dependency.DependencyTracker;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link FileIO} wrapper that reports per-operation HDFS/FileIO metrics via {@link
 * MetricsReporter}.
 *
 * <p>Instruments the 8 core abstract methods with latency, error count, slow-op tracking, and
 * operation-level metrics using the same naming pattern as catalog metrics: operation name as
 * metric name and `metric_type` tag for semantic distinction. Stream operations additionally track
 * cumulative bytes read/written.
 *
 * <p>Metrics reported:
 *
 * <ul>
 *   <li>{@code <opName>{metric_type=hdfs_op_total}} — per-op-type call count
 *   <li>{@code <opName>{metric_type=hdfs_op_latency}} — per-op-type latency in ms
 *   <li>{@code <opName>{metric_type=hdfs_op_error}} — per-op-type error count
 *   <li>{@code <opName>{metric_type=hdfs_op_slow_1s}} — ops exceeding 1 second
 *   <li>{@code <opName>{metric_type=hdfs_op_slow_5s}} — ops exceeding 5 seconds
 *   <li>{@code <opName>{metric_type=hdfs_op_exception, exception_class=...}} — exception count
 *   <li>{@code hdfs_read_bytes} — bytes read per stream lifetime
 *   <li>{@code hdfs_write_bytes} — bytes written per stream lifetime
 * </ul>
 */
public class MetricsFileIO implements FileIO {

    interface MetricsEmitListener {
        void onCount(String name, Map<String, String> tags);

        void onValue(String name, long value, @Nullable Map<String, String> tags);
    }

    @Nullable private static volatile MetricsEmitListener testMetricsListener;

    static void setTestMetricsListener(@Nullable MetricsEmitListener listener) {
        testMetricsListener = listener;
    }

    private static final long serialVersionUID = 1L;

    private static final long SLOW_THRESHOLD_1S = 1000;
    private static final long SLOW_THRESHOLD_5S = 5000;

    private final FileIO delegate;

    public MetricsFileIO(FileIO delegate) {
        this.delegate = delegate;
    }

    public FileIO getDelegate() {
        return delegate;
    }

    @Override
    public MetricsFileIO copyWithOptions(Options newOptions) {
        FileIO newDelegate = delegate.copyWithOptions(newOptions);
        if (newDelegate == delegate) {
            return this;
        }
        return new MetricsFileIO(newDelegate);
    }

    // --- pass-through without metrics ---

    @Override
    public boolean isObjectStore() {
        return delegate.isObjectStore();
    }

    @Override
    public void configure(CatalogContext context) {
        delegate.configure(context);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    // --- instrumented methods ---

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        try {
            return DependencyTracker.trackCall(
                    "hdfs",
                    "open_input",
                    DependencyTracker.StageType.HDFS,
                    () -> {
                        long start = System.currentTimeMillis();
                        try {
                            SeekableInputStream stream = delegate.newInputStream(path);
                            long duration = System.currentTimeMillis() - start;
                            reportSuccess("open_input", duration);
                            return new MetricsInputStream(stream);
                        } catch (IOException e) {
                            long duration = System.currentTimeMillis() - start;
                            reportError("open_input", duration, e);
                            throw e;
                        }
                    });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to track HDFS input open", e);
        }
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        try {
            return DependencyTracker.trackCall(
                    "hdfs",
                    "open_output",
                    DependencyTracker.StageType.HDFS,
                    () -> {
                        long start = System.currentTimeMillis();
                        try {
                            PositionOutputStream stream = delegate.newOutputStream(path, overwrite);
                            long duration = System.currentTimeMillis() - start;
                            reportSuccess("open_output", duration);
                            return new MetricsOutputStream(stream);
                        } catch (IOException e) {
                            long duration = System.currentTimeMillis() - start;
                            reportError("open_output", duration, e);
                            throw e;
                        }
                    });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to track HDFS output open", e);
        }
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return wrapFileOp("get_status", () -> delegate.getFileStatus(path));
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return wrapFileOp("list", () -> delegate.listStatus(path));
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return wrapFileOp("exists", () -> delegate.exists(path));
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return wrapFileOp("delete", () -> delegate.delete(path, recursive));
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return wrapFileOp("mkdirs", () -> delegate.mkdirs(path));
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return wrapFileOp("rename", () -> delegate.rename(src, dst));
    }

    // --- metrics reporting helpers ---

    @FunctionalInterface
    private interface IOCallable<T> {
        T call() throws IOException;
    }

    private <T> T wrapFileOp(String opName, IOCallable<T> callable) throws IOException {
        try {
            return DependencyTracker.trackCall(
                    "hdfs",
                    opName,
                    DependencyTracker.StageType.HDFS,
                    () -> {
                        long start = System.currentTimeMillis();
                        try {
                            T result = callable.call();
                            long duration = System.currentTimeMillis() - start;
                            reportSuccess(opName, duration);
                            return result;
                        } catch (IOException e) {
                            long duration = System.currentTimeMillis() - start;
                            reportError(opName, duration, e);
                            throw e;
                        }
                    });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to track HDFS operation: " + opName, e);
        }
    }

    private void reportSuccess(String opName, long duration) {
        emitCount(opName, hdfsMetricTags(opName, "hdfs_op_total"));
        emitValue(opName, duration, hdfsMetricTags(opName, "hdfs_op_latency"));
        if (duration >= SLOW_THRESHOLD_1S) {
            emitCount(opName, hdfsMetricTags(opName, "hdfs_op_slow_1s"));
        }
        if (duration >= SLOW_THRESHOLD_5S) {
            emitCount(opName, hdfsMetricTags(opName, "hdfs_op_slow_5s"));
        }
    }

    private void reportError(String opName, long duration, IOException e) {
        reportSuccess(opName, duration);
        emitCount(opName, hdfsMetricTags(opName, "hdfs_op_error"));
        Map<String, String> exceptionTags = hdfsMetricTags(opName, "hdfs_op_exception");
        exceptionTags.put("exception_class", e.getClass().getSimpleName());
        emitCount(opName, exceptionTags);
    }

    private static void emitCount(String name, Map<String, String> tags) {
        MetricsReporter.count(name, tags);
        MetricsEmitListener listener = testMetricsListener;
        if (listener != null) {
            listener.onCount(name, tags);
        }
    }

    private static void emitValue(String name, long value, @Nullable Map<String, String> tags) {
        if (tags == null) {
            MetricsReporter.value(name, value);
        } else {
            MetricsReporter.value(name, value, tags);
        }
        MetricsEmitListener listener = testMetricsListener;
        if (listener != null) {
            listener.onValue(name, value, tags);
        }
    }

    private static Map<String, String> hdfsMetricTags(String opName, String metricType) {
        Map<String, String> tags = new HashMap<>();
        tags.put("op", opName);
        tags.put("metric_type", metricType);
        return tags;
    }

    // --- stream wrappers ---

    /** Wraps {@link SeekableInputStream} to count bytes read, reporting on close. */
    private static class MetricsInputStream extends SeekableInputStreamWrapper {
        private final AtomicLong bytesRead = new AtomicLong(0);

        MetricsInputStream(SeekableInputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            int b = in.read();
            if (b >= 0) {
                bytesRead.incrementAndGet();
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = in.read(b, off, len);
            if (n > 0) {
                bytesRead.addAndGet(n);
            }
            return n;
        }

        @Override
        public void close() throws IOException {
            try {
                in.close();
            } finally {
                long total = bytesRead.get();
                if (total > 0) {
                    emitValue("hdfs_read_bytes", total, null);
                }
            }
        }
    }

    /** Wraps {@link PositionOutputStream} to count bytes written, reporting on close. */
    private static class MetricsOutputStream extends PositionOutputStreamWrapper {
        private final AtomicLong bytesWritten = new AtomicLong(0);

        MetricsOutputStream(PositionOutputStream out) {
            super(out);
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            bytesWritten.incrementAndGet();
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);
            bytesWritten.addAndGet(b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            bytesWritten.addAndGet(len);
        }

        @Override
        public void close() throws IOException {
            try {
                out.close();
            } finally {
                long total = bytesWritten.get();
                if (total > 0) {
                    emitValue("hdfs_write_bytes", total, null);
                }
            }
        }
    }
}
