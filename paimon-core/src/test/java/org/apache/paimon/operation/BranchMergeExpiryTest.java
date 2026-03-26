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

package org.apache.paimon.operation;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that branch merge correctly rejects when source snapshots have been expired. */
public class BranchMergeExpiryTest extends BranchMergeTestBase {

    private void writeSource(int pk, String val) throws Exception {
        write(getTableDefault().switchToBranch("source"), ioManager, row(pk, val));
    }

    @Test
    public void testRejectsMergeWithExpiredSourceSnapshots() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        writeSource(2, "s1");
        writeSource(3, "s2");

        // Simulate expiry: delete source snapshot 2
        SnapshotManager sourceMgr = getTableDefault().switchToBranch("source").snapshotManager();
        sourceMgr.deleteSnapshot(2);
        sourceMgr.commitEarliestHint(3);

        assertThatThrownBy(() -> merge("source", "main"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .rootCause()
                .hasMessageContaining("has expired snapshot");
    }

    @Test
    public void testMergeSucceedsWhenAllSnapshotsAvailable() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        writeSource(2, "s1");
        writeSource(3, "s2");

        merge("source", "main");

        table = getTableDefault();
        Map<Integer, String> result = toMap(readCompact(table));
        assertThat(result).hasSize(3);
        assertThat(result).containsEntry(1, "seed");
        assertThat(result).containsEntry(2, "s1");
        assertThat(result).containsEntry(3, "s2");
    }
}
