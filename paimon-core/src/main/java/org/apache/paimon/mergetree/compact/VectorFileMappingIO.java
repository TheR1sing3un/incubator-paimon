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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** IO utilities for reading and writing {@link VectorFileMapping} as JSON files. */
public class VectorFileMappingIO {

    public static final String MAPPING_FILE_SUFFIX = ".vector-mapping.json";

    public static VectorFileMapping read(FileIO fileIO, Path path) throws IOException {
        String json = fileIO.readFileUtf8(path);
        return JsonSerdeUtil.fromJson(json, VectorFileMapping.class);
    }

    public static Path write(FileIO fileIO, Path dir, VectorFileMapping mapping)
            throws IOException {
        String json = JsonSerdeUtil.toFlatJson(mapping);
        String fileName = UUID.randomUUID().toString() + MAPPING_FILE_SUFFIX;
        Path path = new Path(dir, fileName);
        try (OutputStream out = fileIO.newOutputStream(path, false)) {
            out.write(json.getBytes(StandardCharsets.UTF_8));
        }
        return path;
    }
}
