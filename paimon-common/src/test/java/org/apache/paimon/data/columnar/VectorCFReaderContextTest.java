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

package org.apache.paimon.data.columnar;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VectorCFReaderContext}. */
public class VectorCFReaderContextTest {

    @Test
    public void testResolveFilePath() {
        Map<Integer, String> fileIdToPath = new HashMap<>();
        fileIdToPath.put(123, "/bucket-0/data-abc.vector.bin");
        fileIdToPath.put(456, "/bucket-0/data-def.vector.bin");

        VectorCFReaderContext ctx = new VectorCFReaderContext(fileIdToPath, new int[0], new int[0]);

        assertThat(ctx.resolveFilePath(123)).isEqualTo("/bucket-0/data-abc.vector.bin");
        assertThat(ctx.resolveFilePath(456)).isEqualTo("/bucket-0/data-def.vector.bin");
        assertThat(ctx.resolveFilePath(999)).isNull();
    }

    @Test
    public void testBytesPerVectorAndDimension() {
        int[] bpv = {0, 16, 0, 32};
        int[] dim = {0, 4, 0, 8};

        VectorCFReaderContext ctx =
                new VectorCFReaderContext(Collections.singletonMap(1, "/f"), bpv, dim);

        assertThat(ctx.bytesPerVector(0)).isEqualTo(0);
        assertThat(ctx.bytesPerVector(1)).isEqualTo(16);
        assertThat(ctx.bytesPerVector(3)).isEqualTo(32);
        assertThat(ctx.dimension(1)).isEqualTo(4);
        assertThat(ctx.dimension(3)).isEqualTo(8);
    }

    @Test
    public void testOutOfBoundsReturnsZero() {
        VectorCFReaderContext ctx =
                new VectorCFReaderContext(
                        Collections.singletonMap(1, "/f"), new int[] {16}, new int[] {4});

        assertThat(ctx.bytesPerVector(-1)).isEqualTo(0);
        assertThat(ctx.bytesPerVector(5)).isEqualTo(0);
        assertThat(ctx.dimension(-1)).isEqualTo(0);
        assertThat(ctx.dimension(5)).isEqualTo(0);
    }

    @Test
    public void testHasVectorFiles() {
        VectorCFReaderContext withFiles =
                new VectorCFReaderContext(
                        Collections.singletonMap(1, "/f"), new int[0], new int[0]);
        assertThat(withFiles.hasVectorFiles()).isTrue();

        VectorCFReaderContext empty =
                new VectorCFReaderContext(Collections.emptyMap(), new int[0], new int[0]);
        assertThat(empty.hasVectorFiles()).isFalse();
    }
}
