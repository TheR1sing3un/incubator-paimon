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

package org.apache.paimon.catalog;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import java.io.IOException;
import java.time.Duration;

/**
 * A distributed branch lock based on atomic file creation on the underlying file system (e.g.
 * HDFS). Delegates to {@link FileBasedLock} for the core locking mechanism.
 *
 * <p>Lock files are placed under {@code {tablePath}/lock/branch/}, with each branch as a separate
 * lock name (e.g. {@code main_00000001}, {@code feature1_00000001}).
 */
public class FileBasedBranchLock {

    static final String LOCK_TYPE = "branch";

    private final FileBasedLock delegate;

    public FileBasedBranchLock(
            FileIO fileIO, Duration acquireTimeout, Duration checkMaxSleep, Duration lockTtl) {
        this.delegate =
                new FileBasedLock(fileIO, LOCK_TYPE, acquireTimeout, checkMaxSleep, lockTtl);
    }

    public Duration getLockTtl() {
        return delegate.getLockTtl();
    }

    /**
     * Acquire the distributed branch lock. Blocks until the lock is obtained or the acquire timeout
     * is exceeded.
     *
     * @param tablePath the table root path
     * @param branch the branch name used as the lock name
     * @return the lock file {@link Path} — caller must pass this to {@link #release(Path)} in a
     *     finally block
     * @throws IOException if the lock cannot be acquired within the timeout
     */
    public Path acquire(Path tablePath, String branch) throws IOException {
        return delegate.acquire(tablePath, branch);
    }

    /** Release the distributed branch lock by deleting the lock file. */
    public void release(Path lockFilePath) {
        delegate.release(lockFilePath);
    }

    /** Parse the timestamp from lock file content. Kept for backward compatibility. */
    static long parseTimestamp(String content) {
        return FileBasedLock.parseTimestamp(content);
    }
}
