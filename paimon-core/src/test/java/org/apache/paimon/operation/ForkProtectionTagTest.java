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

import org.apache.paimon.PagedList;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.BranchManager;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Lifecycle tests for the system fork-point protection tag {@code __sys.fork.<childBranch>}.
 *
 * <p>Covers: creation on {@code createBranch}, cleanup on {@code dropBranch}, visibility rules in
 * {@code listTagsPaged}, and the failure-loud behavior when a stale tag with the same name already
 * exists.
 */
public class ForkProtectionTagTest extends BranchMergeTestBase {

    @Test
    public void testCreateBranchCreatesForkProtectionTag() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "v1"));

        catalog.createBranch(identifier(), "feature", null);

        String forkTag = BranchManager.forkTagName("feature");
        assertThat(catalog.getTag(identifier(), forkTag)).isNotNull();
    }

    @Test
    public void testCreateBranchFromTagAlsoCreatesForkProtectionTag() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "v1"));
        table = getTableDefault();
        table.createTag("ancestor");

        catalog.createBranch(identifier(), "feature", "ancestor");

        String forkTag = BranchManager.forkTagName("feature");
        assertThat(catalog.getTag(identifier(), forkTag)).isNotNull();
    }

    @Test
    public void testDropBranchRemovesForkProtectionTag() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "v1"));

        catalog.createBranch(identifier(), "feature", null);
        catalog.dropBranch(identifier(), "feature");

        String forkTag = BranchManager.forkTagName("feature");
        assertThatThrownBy(() -> catalog.getTag(identifier(), forkTag))
                .isInstanceOf(Catalog.TagNotExistException.class);
    }

    @Test
    public void testCreateBranchFailsWhenForkTagAlreadyExists() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "v1"));

        // Pre-create a user tag that collides with the system fork tag name — simulates a
        // stale leftover from a previous branch with the same name that was improperly
        // cleaned up.
        String forkTag = BranchManager.forkTagName("feature");
        catalog.createTag(identifier(), forkTag, null, null, false);

        assertThatThrownBy(() -> catalog.createBranch(identifier(), "feature", null))
                .hasMessageContaining("already exists");
    }

    @Test
    public void testListTagsHidesSystemTagsByDefault() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "v1"));
        table = getTableDefault();
        table.createTag("user-tag");

        catalog.createBranch(identifier(), "feature", null);

        PagedList<String> tags = catalog.listTagsPaged(identifier(), null, null, null);
        assertThat(tags.getElements()).contains("user-tag");
        assertThat(tags.getElements())
                .noneMatch(name -> name.startsWith(BranchManager.SYSTEM_TAG_PREFIX));
    }

    @Test
    public void testListTagsReturnsSystemTagsWhenExplicitlyRequested() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "v1"));

        catalog.createBranch(identifier(), "feature", null);

        PagedList<String> tags =
                catalog.listTagsPaged(identifier(), null, null, BranchManager.SYSTEM_TAG_PREFIX);
        assertThat(tags.getElements()).contains(BranchManager.forkTagName("feature"));
    }
}
