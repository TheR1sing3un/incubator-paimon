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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DatasetIdentifier}. */
class DatasetIdentifierTest {

    @Test
    void parseBasicName() {
        Identifier ident = Identifier.of(new String[] {"my_ns"}, "my_dataset");
        DatasetIdentifier dsId = DatasetIdentifier.of(ident);

        assertThat(dsId.getNamespace()).isEqualTo("my_ns");
        assertThat(dsId.getDatasetName()).isEqualTo("my_dataset");
        assertThat(dsId.getSuffix()).isNull();
        assertThat(dsId.hasSuffix()).isFalse();
    }

    @Test
    void parseBranchSuffix() {
        Identifier ident = Identifier.of(new String[] {"ns"}, "my_dataset$branch_feature");
        DatasetIdentifier dsId = DatasetIdentifier.of(ident);

        assertThat(dsId.getNamespace()).isEqualTo("ns");
        assertThat(dsId.getDatasetName()).isEqualTo("my_dataset");
        assertThat(dsId.getSuffix()).isEqualTo("branch_feature");
        assertThat(dsId.hasSuffix()).isTrue();
    }

    @Test
    void parseSnapshotsSuffix() {
        Identifier ident = Identifier.of(new String[] {"ns"}, "ds$snapshots");
        DatasetIdentifier dsId = DatasetIdentifier.of(ident);

        assertThat(dsId.getDatasetName()).isEqualTo("ds");
        assertThat(dsId.getSuffix()).isEqualTo("snapshots");
    }

    @Test
    void parseSuffixWithUnderscore() {
        Identifier ident = Identifier.of(new String[] {"ns"}, "ds$branch_feature_v2");
        DatasetIdentifier dsId = DatasetIdentifier.of(ident);

        assertThat(dsId.getDatasetName()).isEqualTo("ds");
        assertThat(dsId.getSuffix()).isEqualTo("branch_feature_v2");
    }

    @Test
    void dollarAtEndMeansNoSuffix() {
        // "name$" → dollarIndex = 4, but nothing after it
        Identifier ident = Identifier.of(new String[] {"ns"}, "name$");
        DatasetIdentifier dsId = DatasetIdentifier.of(ident);

        assertThat(dsId.getDatasetName()).isEqualTo("name$");
        assertThat(dsId.getSuffix()).isNull();
        assertThat(dsId.hasSuffix()).isFalse();
    }

    @Test
    void dollarAtStartMeansNoSuffix() {
        // "$branch" → dollarIndex = 0, fails > 0 check
        Identifier ident = Identifier.of(new String[] {"ns"}, "$branch");
        DatasetIdentifier dsId = DatasetIdentifier.of(ident);

        assertThat(dsId.getDatasetName()).isEqualTo("$branch");
        assertThat(dsId.getSuffix()).isNull();
    }

    @Test
    void emptyNamespaceThrows() {
        Identifier ident = Identifier.of(new String[] {}, "name");
        assertThatThrownBy(() -> DatasetIdentifier.of(ident))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toStringIncludesSuffix() {
        Identifier ident = Identifier.of(new String[] {"ns"}, "ds$branch_main");
        DatasetIdentifier dsId = DatasetIdentifier.of(ident);
        assertThat(dsId.toString()).isEqualTo("ns.ds$branch_main");
    }

    @Test
    void toStringWithoutSuffix() {
        Identifier ident = Identifier.of(new String[] {"ns"}, "ds");
        DatasetIdentifier dsId = DatasetIdentifier.of(ident);
        assertThat(dsId.toString()).isEqualTo("ns.ds");
    }

    @Test
    void multipleNamespaceUsesFirst() {
        Identifier ident = Identifier.of(new String[] {"a", "b"}, "ds");
        DatasetIdentifier dsId = DatasetIdentifier.of(ident);
        assertThat(dsId.getNamespace()).isEqualTo("a");
    }
}
