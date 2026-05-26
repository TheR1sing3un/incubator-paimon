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

package org.apache.paimon.index;

import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.ObjectSerializerTestBase;

import java.util.LinkedHashMap;
import java.util.Random;

/** Test for {@link org.apache.paimon.index.IndexFileMetaSerializer}. */
public class IndexFileMetaSerializerTest extends ObjectSerializerTestBase<IndexFileMeta> {

    @Override
    protected ObjectSerializer<IndexFileMeta> serializer() {
        return new IndexFileMetaSerializer();
    }

    @Override
    protected IndexFileMeta object() {
        return randomIndexFile();
    }

    public static IndexFileMeta randomIndexFile() {
        Random rnd = new Random();
        if (rnd.nextBoolean()) {
            return randomHashIndexFile();
        } else {
            return randomDeletionVectorIndexFile();
        }
    }

    public static IndexFileMeta randomHashIndexFile() {
        Random rnd = new Random();
        return new IndexFileMeta(
                HashIndexFile.HASH_INDEX,
                "my_file_name" + rnd.nextLong(),
                rnd.nextInt(),
                rnd.nextInt(),
                null,
                null,
                null);
    }

    public static IndexFileMeta randomDeletionVectorIndexFile() {
        Random rnd = new Random();
        LinkedHashMap<String, DeletionVectorMeta> dvRanges = new LinkedHashMap<>();
        dvRanges.put(
                "my_file_name1",
                new DeletionVectorMeta(
                        "my_file_name1", rnd.nextInt(), rnd.nextInt(), rnd.nextLong()));
        dvRanges.put(
                "my_file_name2",
                new DeletionVectorMeta(
                        "my_file_name2", rnd.nextInt(), rnd.nextInt(), rnd.nextLong()));
        return new IndexFileMeta(
                DeletionVectorsIndexFile.DELETION_VECTORS_INDEX,
                "deletion_vectors_index_file_name" + rnd.nextLong(),
                rnd.nextInt(),
                rnd.nextInt(),
                dvRanges,
                null);
    }

    @org.junit.jupiter.api.Test
    public void testInlineMappingJsonRoundTrip() throws Exception {
        String mappingJson =
                "{\"mappings\":[{\"file_id\":12345,\"target_file_path\":\"/bucket-0/merged.vector.bin\",\"base_offset\":100}]}";
        IndexFileMeta original =
                new IndexFileMeta(
                        "VECTOR_FILE_MAPPING",
                        "inline-test.vector-mapping",
                        mappingJson.length(),
                        1,
                        null,
                        null,
                        null,
                        mappingJson);

        IndexFileMetaSerializer ser = new IndexFileMetaSerializer();
        org.apache.paimon.data.InternalRow row = ser.toRow(original);
        IndexFileMeta restored = ser.fromRow(row);

        // equals() intentionally excludes inlineMappingJson, so check explicitly
        org.assertj.core.api.Assertions.assertThat(restored.indexType())
                .isEqualTo(original.indexType());
        org.assertj.core.api.Assertions.assertThat(restored.fileName())
                .isEqualTo(original.fileName());
        org.assertj.core.api.Assertions.assertThat(restored.inlineMappingJson())
                .isEqualTo(mappingJson);
    }
}
