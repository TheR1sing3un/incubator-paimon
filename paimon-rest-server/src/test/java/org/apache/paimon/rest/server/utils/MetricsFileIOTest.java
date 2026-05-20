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
import org.apache.paimon.fs.SeekableInputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link MetricsFileIO}. */
class MetricsFileIOTest {

    @AfterEach
    void tearDown() {
        MetricsFileIO.setTestMetricsListener(null);
    }

    @Test
    void testSuccessMetricsUseOpNameAndMetricTypeTags() throws Exception {
        CapturingMetricsListener listener = new CapturingMetricsListener();
        MetricsFileIO.setTestMetricsListener(listener);
        MetricsFileIO fileIO = new MetricsFileIO(new FakeFileIO());

        try (SeekableInputStream input = fileIO.newInputStream(new Path("file:/tmp/input"))) {
            while (input.read() >= 0) {
                // drain
            }
        }

        assertThat(listener.countNames).contains("open_input");
        assertThat(listener.valueNames).contains("open_input", "hdfs_read_bytes");
        assertThat(listener.findCountTags("open_input", "hdfs_op_total"))
                .containsEntry("op", "open_input")
                .containsEntry("metric_type", "hdfs_op_total");
        assertThat(listener.findValueTags("open_input", "hdfs_op_latency"))
                .containsEntry("op", "open_input")
                .containsEntry("metric_type", "hdfs_op_latency");
    }

    @Test
    void testErrorMetricsIncludeExceptionClass() {
        CapturingMetricsListener listener = new CapturingMetricsListener();
        MetricsFileIO.setTestMetricsListener(listener);
        MetricsFileIO fileIO = new MetricsFileIO(new ThrowingFileIO());

        assertThatThrownBy(() -> fileIO.exists(new Path("file:/tmp/missing")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("boom");

        assertThat(listener.countNames).contains("exists");
        assertThat(listener.findCountTags("exists", "hdfs_op_error"))
                .containsEntry("op", "exists")
                .containsEntry("metric_type", "hdfs_op_error");
        assertThat(listener.findCountTags("exists", "hdfs_op_exception"))
                .containsEntry("op", "exists")
                .containsEntry("metric_type", "hdfs_op_exception")
                .containsEntry("exception_class", "IOException");
    }

    @Test
    void testByteMetricsStillReported() throws Exception {
        CapturingMetricsListener listener = new CapturingMetricsListener();
        MetricsFileIO.setTestMetricsListener(listener);
        MetricsFileIO fileIO = new MetricsFileIO(new FakeFileIO());

        try (SeekableInputStream input = fileIO.newInputStream(new Path("file:/tmp/input"))) {
            while (input.read() >= 0) {
                // drain
            }
        }

        try (PositionOutputStream output =
                fileIO.newOutputStream(new Path("file:/tmp/output"), true)) {
            output.write(new byte[] {1, 2, 3, 4});
        }

        assertThat(listener.valueNames).contains("hdfs_read_bytes", "hdfs_write_bytes");
    }

    private static final class CapturingMetricsListener
            implements MetricsFileIO.MetricsEmitListener {
        private final List<String> countNames = new ArrayList<>();
        private final List<Map<String, String>> countTags = new ArrayList<>();
        private final List<String> valueNames = new ArrayList<>();
        private final List<Map<String, String>> valueTags = new ArrayList<>();

        @Override
        public void onCount(String name, Map<String, String> tags) {
            countNames.add(name);
            countTags.add(copyTags(tags));
        }

        @Override
        public void onValue(String name, long value, Map<String, String> tags) {
            valueNames.add(name);
            valueTags.add(copyTags(tags));
        }

        private Map<String, String> findCountTags(String name, String metricType) {
            for (int i = 0; i < countNames.size(); i++) {
                if (name.equals(countNames.get(i))
                        && metricType.equals(countTags.get(i).get("metric_type"))) {
                    return countTags.get(i);
                }
            }
            return new HashMap<>();
        }

        private Map<String, String> findValueTags(String name, String metricType) {
            for (int i = 0; i < valueNames.size(); i++) {
                if (name.equals(valueNames.get(i))
                        && metricType.equals(valueTags.get(i).get("metric_type"))) {
                    return valueTags.get(i);
                }
            }
            return new HashMap<>();
        }

        private Map<String, String> copyTags(Map<String, String> tags) {
            return tags == null ? new HashMap<String, String>() : new HashMap<>(tags);
        }
    }

    private static class FakeFileIO implements FileIO {

        @Override
        public boolean isObjectStore() {
            return false;
        }

        @Override
        public void configure(CatalogContext context) {
            // no-op
        }

        @Override
        public SeekableInputStream newInputStream(Path path) {
            return new InMemorySeekableInputStream(new byte[] {1, 2, 3});
        }

        @Override
        public PositionOutputStream newOutputStream(Path path, boolean overwrite) {
            return new InMemoryPositionOutputStream();
        }

        @Override
        public FileStatus getFileStatus(Path path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus[] listStatus(Path path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(Path path) throws IOException {
            return true;
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            return true;
        }

        @Override
        public boolean mkdirs(Path path) throws IOException {
            return true;
        }

        @Override
        public boolean rename(Path src, Path dst) throws IOException {
            return true;
        }

        @Override
        public void close() throws IOException {
            // no-op
        }
    }

    private static class ThrowingFileIO extends FakeFileIO {
        @Override
        public boolean exists(Path path) throws IOException {
            throw new IOException("boom");
        }
    }

    private static final class InMemorySeekableInputStream extends SeekableInputStream {
        private final ByteArrayInputStream delegate;
        private int position;

        private InMemorySeekableInputStream(byte[] data) {
            this.delegate = new ByteArrayInputStream(data);
            this.position = 0;
        }

        @Override
        public int read() {
            int result = delegate.read();
            if (result >= 0) {
                position++;
            }
            return result;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            int read = delegate.read(b, off, len);
            if (read > 0) {
                position += read;
            }
            return read;
        }

        @Override
        public void seek(long desired) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class InMemoryPositionOutputStream extends PositionOutputStream {
        private final ByteArrayOutputStream delegate = new ByteArrayOutputStream();

        @Override
        public long getPos() {
            return delegate.size();
        }

        @Override
        public void write(int b) {
            delegate.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            delegate.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            delegate.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        public void sync() {
            // no-op
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
