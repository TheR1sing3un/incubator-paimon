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

package org.apache.paimon.spark.dataset;

import org.apache.spark.sql.connector.catalog.Identifier;

import javax.annotation.Nullable;

/**
 * Parses a Spark {@link Identifier} into dataset namespace, name, and optional suffix.
 *
 * <p>Supports the Paimon {@code $} syntax transparently: {@code dataset_name$branch_xxx}, {@code
 * dataset_name$snapshots}, etc. The suffix after {@code $} is passed through to the physical Paimon
 * table name as-is, matching Paimon's native syntax.
 */
public class DatasetIdentifier {

    private final String namespace;
    private final String datasetName;
    @Nullable private final String suffix;

    private DatasetIdentifier(String namespace, String datasetName, @Nullable String suffix) {
        this.namespace = namespace;
        this.datasetName = datasetName;
        this.suffix = suffix;
    }

    /**
     * Creates a {@link DatasetIdentifier} from a Spark {@link Identifier}.
     *
     * <p>The Spark identifier's namespace[0] becomes the dataset namespace, and the identifier's
     * name is parsed for optional {@code $suffix}.
     */
    public static DatasetIdentifier of(Identifier ident) {
        String[] ns = ident.namespace();
        if (ns == null || ns.length == 0) {
            throw new IllegalArgumentException(
                    "Dataset identifier must have a namespace, got: " + ident);
        }
        String namespace = ns[0];
        String fullName = ident.name();

        // Parse suffix: dataset_name$suffix (e.g., $branch_xxx, $snapshots)
        int dollarIndex = fullName.indexOf('$');
        if (dollarIndex > 0 && dollarIndex < fullName.length() - 1) {
            String datasetName = fullName.substring(0, dollarIndex);
            String suffix = fullName.substring(dollarIndex + 1);
            return new DatasetIdentifier(namespace, datasetName, suffix);
        }

        return new DatasetIdentifier(namespace, fullName, null);
    }

    public String getNamespace() {
        return namespace;
    }

    public String getDatasetName() {
        return datasetName;
    }

    /** Returns the raw suffix after {@code $}, or null if none. */
    @Nullable
    public String getSuffix() {
        return suffix;
    }

    public boolean hasSuffix() {
        return suffix != null;
    }

    @Override
    public String toString() {
        if (suffix != null) {
            return namespace + "." + datasetName + "$" + suffix;
        }
        return namespace + "." + datasetName;
    }
}
