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
import org.apache.paimon.utils.BranchManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

/**
 * A distributed branch lock based on atomic file creation on the underlying file system (e.g.
 * HDFS).
 *
 * <p>Lock acquisition uses {@link FileIO#tryToWriteAtomic(Path, String)} which atomically creates a
 * lock file — if the file already exists the call returns {@code false}, meaning the lock is held
 * by another process.
 *
 * <p>Stale locks (e.g. from a crashed server) are detected via a TTL: if the lock file's recorded
 * timestamp is older than {@code lockTtl}, it is considered expired and force-deleted before
 * retrying.
 */
public class FileBasedBranchLock {

    private static final Logger LOG = LoggerFactory.getLogger(FileBasedBranchLock.class);

    private static final String LOCK_FILE_NAME = ".branch_lock";
    private static final long INITIAL_RETRY_SLEEP_MS = 100;

    private final FileIO fileIO;
    private final Duration acquireTimeout;
    private final Duration checkMaxSleep;
    private final Duration lockTtl;
    private final String ownerId;

    public FileBasedBranchLock(
            FileIO fileIO, Duration acquireTimeout, Duration checkMaxSleep, Duration lockTtl) {
        this.fileIO = fileIO;
        this.acquireTimeout = acquireTimeout;
        this.checkMaxSleep = checkMaxSleep;
        this.lockTtl = lockTtl;
        this.ownerId = UUID.randomUUID().toString();
    }

    public Duration getLockTtl() {
        return lockTtl;
    }

    /**
     * Compute the lock file path for the given table and branch.
     *
     * <ul>
     *   <li>main branch: {@code {tablePath}/.branch_lock}
     *   <li>other branches: {@code {tablePath}/branch/branch-{name}/.branch_lock}
     * </ul>
     */
    static Path lockPath(Path tablePath, String branch) {
        return new Path(BranchManager.branchPath(tablePath, branch), LOCK_FILE_NAME);
    }

    /**
     * Acquire the distributed branch lock. Blocks until the lock is obtained or the acquire timeout
     * is exceeded.
     *
     * @return the lock file {@link Path} — caller must pass this to {@link #release(Path)} in a
     *     finally block
     * @throws IOException if the lock cannot be acquired within the timeout
     */
    public Path acquire(Path tablePath, String branch) throws IOException {
        Path path = lockPath(tablePath, branch);
        long deadline = System.currentTimeMillis() + acquireTimeout.toMillis();
        long sleepMs = INITIAL_RETRY_SLEEP_MS;
        long maxSleepMs = checkMaxSleep.toMillis();

        while (true) {
            String content = lockContent();
            if (fileIO.tryToWriteAtomic(path, content)) {
                LOG.debug("Acquired branch lock: {}", path);
                return path;
            }

            // Lock file exists — check if it is stale
            if (tryCleanStaleLock(path)) {
                // Stale lock cleaned, retry immediately
                continue;
            }

            // Lock is held by another live process — wait and retry
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                throw new IOException(
                        String.format(
                                "Failed to acquire branch lock [%s] within %s. "
                                        + "Another process may be holding the lock.",
                                path, acquireTimeout));
            }

            long actualSleep = Math.min(sleepMs, Math.min(remaining, maxSleepMs));
            try {
                Thread.sleep(actualSleep);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for branch lock: " + path, e);
            }
            sleepMs = Math.min(sleepMs * 2, maxSleepMs);
        }
    }

    /** Release the distributed branch lock by deleting the lock file. */
    public void release(Path lockFilePath) {
        fileIO.deleteQuietly(lockFilePath);
        LOG.debug("Released branch lock: {}", lockFilePath);
    }

    /**
     * Try to clean a stale lock file whose timestamp has exceeded the TTL.
     *
     * @return true if a stale lock was detected and deleted, false otherwise
     */
    private boolean tryCleanStaleLock(Path path) {
        try {
            if (!fileIO.exists(path)) {
                return true;
            }
            String content = fileIO.readFileUtf8(path);
            long timestamp = parseTimestamp(content);
            long age = System.currentTimeMillis() - timestamp;
            if (age > lockTtl.toMillis()) {
                LOG.warn(
                        "Detected stale branch lock [{}], age={}ms > ttl={}ms. Force deleting.",
                        path,
                        age,
                        lockTtl.toMillis());
                fileIO.deleteQuietly(path);
                return true;
            }
        } catch (Exception e) {
            // If we can't read the lock file (e.g. deleted between exists check and read),
            // treat it as cleaned so the caller retries acquisition.
            LOG.debug("Error checking lock file {}, treating as cleaned: {}", path, e.getMessage());
            return true;
        }
        return false;
    }

    private String lockContent() {
        // Simple format: owner|timestamp
        return ownerId + "|" + System.currentTimeMillis();
    }

    /**
     * Parse the timestamp from lock file content. Returns current time if parsing fails, so that an
     * unparseable lock is treated as fresh rather than stale — this avoids accidentally deleting a
     * valid lock.
     */
    static long parseTimestamp(String content) {
        try {
            int sep = content.lastIndexOf('|');
            if (sep < 0) {
                return System.currentTimeMillis();
            }
            return Long.parseLong(content.substring(sep + 1));
        } catch (NumberFormatException e) {
            return System.currentTimeMillis();
        }
    }
}
