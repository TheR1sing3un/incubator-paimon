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
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A distributed lock based on epoch-incrementing atomic file creation on the underlying file system
 * (e.g. HDFS).
 *
 * <p>All lock files are placed under {@code {tablePath}/lock/{lockType}/}. Different lock types
 * (e.g. branch locks, index build locks) use separate subdirectories to avoid interference.
 *
 * <p>Instead of delete + create (which has a non-atomic gap), this lock uses monotonically
 * increasing epoch files. The lock holder is defined as the owner of the highest-epoch file. Stale
 * locks are superseded by creating a higher epoch file, not by deleting the stale one.
 *
 * <p>Lock files follow the naming pattern: {@code {lockName}_{epoch:08d}}, e.g. {@code
 * main_00000001}, {@code main_00000002}.
 *
 * <p>Acquisition uses {@link FileIO#tryToWriteAtomic(Path, String)} which atomically creates the
 * epoch file — if the file already exists the call returns {@code false}, meaning another process
 * won the race for the same epoch. The competing processes are funneled to the same {@code
 * tryToWriteAtomic} call, so only one can win.
 *
 * <p>For long-running operations, callers should periodically call {@link #renew(Path)} to update
 * the lock timestamp, preventing stale detection while the operation is still in progress.
 */
public class FileBasedLock {

    private static final Logger LOG = LoggerFactory.getLogger(FileBasedLock.class);

    static final String LOCK_DIRECTORY = "lock";

    private static final long INITIAL_RETRY_SLEEP_MS = 100;
    private static final String EPOCH_FORMAT = "%08d";

    private final FileIO fileIO;
    private final String lockType;
    private final Duration acquireTimeout;
    private final Duration checkMaxSleep;
    private final Duration lockTtl;
    private final String ownerId;
    private final AtomicBoolean lockLost;

    /**
     * Create a file-based distributed lock.
     *
     * @param fileIO the file system
     * @param lockType the lock type, used as subdirectory name under {@code lock/} (e.g. "branch",
     *     "accelerate_index_build")
     * @param acquireTimeout maximum time to wait for lock acquisition
     * @param checkMaxSleep maximum sleep between retries
     * @param lockTtl time-to-live for stale lock detection
     */
    public FileBasedLock(
            FileIO fileIO,
            String lockType,
            Duration acquireTimeout,
            Duration checkMaxSleep,
            Duration lockTtl) {
        this.fileIO = fileIO;
        this.lockType = lockType;
        this.acquireTimeout = acquireTimeout;
        this.checkMaxSleep = checkMaxSleep;
        this.lockTtl = lockTtl;
        this.ownerId = UUID.randomUUID().toString();
        this.lockLost = new AtomicBoolean(false);
    }

    public Duration getLockTtl() {
        return lockTtl;
    }

    public String getOwnerId() {
        return ownerId;
    }

    /**
     * Returns true if the lock has been detected as lost (e.g. superseded by a higher epoch during
     * renewal). Callers should check this periodically to abort work if the lock is no longer held.
     */
    public boolean isLockLost() {
        return lockLost.get();
    }

    /**
     * Compute the lock directory for the given table path: {@code {tablePath}/lock/{lockType}/}.
     */
    Path lockDir(Path tablePath) {
        return new Path(new Path(tablePath, LOCK_DIRECTORY), lockType);
    }

    /**
     * Acquire the distributed lock for the given table and lock name. Lock files are created under
     * {@code {tablePath}/lock/{lockType}/{lockName}_{epoch}}.
     *
     * @param tablePath the table root path
     * @param lockName the logical lock name (e.g. branch name, or column_algorithm)
     * @return the actual epoch lock file path — caller must pass this to {@link #release(Path)} in
     *     a finally block
     * @throws IOException if the lock cannot be acquired within the timeout
     */
    public Path acquire(Path tablePath, String lockName) throws IOException {
        long deadline = System.currentTimeMillis() + acquireTimeout.toMillis();
        long sleepMs = INITIAL_RETRY_SLEEP_MS;
        long maxSleepMs = checkMaxSleep.toMillis();
        Path lockDir = lockDir(tablePath);

        while (true) {
            EpochFile current;
            try {
                current = findMaxEpoch(lockDir, lockName);
            } catch (IOException e) {
                LOG.debug("Failed to list lock files in {}: {}", lockDir, e.getMessage());
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    throw new IOException(
                            String.format(
                                    "Failed to acquire lock [%s/%s] within %s. "
                                            + "Cannot list lock directory: %s",
                                    lockDir, lockName, acquireTimeout, e.getMessage()),
                            e);
                }
                long actualSleep = Math.min(sleepMs, Math.min(remaining, maxSleepMs));
                try {
                    Thread.sleep(actualSleep);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "Interrupted while waiting for lock: " + lockDir + "/" + lockName, ie);
                }
                sleepMs = Math.min(sleepMs * 2, maxSleepMs);
                continue;
            }

            if (current == null) {
                Path target = epochPath(lockDir, lockName, 1);
                if (fileIO.tryToWriteAtomic(target, lockContent())) {
                    LOG.debug("Acquired lock (epoch 1): {}", target);
                    return target;
                }
                continue;
            }

            String content = readContentSafe(current.path);
            if (content == null) {
                continue;
            }

            if (isStale(content)) {
                long nextEpoch = current.epoch + 1;
                Path target = epochPath(lockDir, lockName, nextEpoch);
                if (fileIO.tryToWriteAtomic(target, lockContent())) {
                    LOG.debug("Acquired lock (epoch {}, stale predecessor): {}", nextEpoch, target);
                    cleanupOldEpochs(lockDir, lockName, nextEpoch);
                    return target;
                }
                continue;
            }

            if (ownerId.equals(parseOwnerId(content))) {
                return current.path;
            }

            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                throw new IOException(
                        String.format(
                                "Failed to acquire lock [%s/%s] within %s. "
                                        + "Another process may be holding the lock.",
                                lockDir, lockName, acquireTimeout));
            }

            long actualSleep = Math.min(sleepMs, Math.min(remaining, maxSleepMs));
            try {
                Thread.sleep(actualSleep);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(
                        "Interrupted while waiting for lock: " + lockDir + "/" + lockName, e);
            }
            sleepMs = Math.min(sleepMs * 2, maxSleepMs);
        }
    }

    /** Release the distributed lock by cleaning up older epochs then deleting the epoch file. */
    public void release(Path lockFilePath) {
        // Clean up old epochs BEFORE deleting our own epoch file.
        // If we delete ours first, another process could acquire epoch 1 in the gap,
        // and our cleanup (which deletes epoch < N) would delete that fresh lock.
        String fullName = lockFilePath.getName();
        int lastUnderscore = fullName.lastIndexOf('_');
        if (lastUnderscore > 0) {
            try {
                long epoch = Long.parseLong(fullName.substring(lastUnderscore + 1));
                String lockName = fullName.substring(0, lastUnderscore);
                cleanupOldEpochs(lockFilePath.getParent(), lockName, epoch);
            } catch (NumberFormatException ignored) {
            }
        }

        fileIO.deleteQuietly(lockFilePath);
        LOG.debug("Released lock: {}", lockFilePath);
    }

    /**
     * Renew the lock by overwriting the lock file content with a fresh timestamp. Only renews if
     * the lock is still owned by this instance. After renewal, verifies that this epoch is still
     * the highest — if a higher epoch exists, the lock has been superseded and {@link
     * #isLockLost()} will return true.
     *
     * @throws IOException if the lock file cannot be read/written or the owner has changed
     */
    public void renew(Path lockFilePath) throws IOException {
        String content;
        try {
            content = fileIO.readFileUtf8(lockFilePath);
        } catch (IOException e) {
            lockLost.set(true);
            throw new IOException("Lock file disappeared during renew: " + lockFilePath, e);
        }
        String fileOwner = parseOwnerId(content);
        if (!ownerId.equals(fileOwner)) {
            lockLost.set(true);
            throw new IOException(
                    "Lock owner mismatch on renew: expected "
                            + ownerId
                            + " but found "
                            + fileOwner);
        }
        try {
            fileIO.overwriteFileUtf8(lockFilePath, lockContent());
        } catch (IOException e) {
            lockLost.set(true);
            throw new IOException("Failed to write lock file during renew: " + lockFilePath, e);
        }

        // Verify our epoch is still the highest. If another process detected our lock as
        // stale and created a higher epoch between our read and write, we have lost the lock.
        String fullName = lockFilePath.getName();
        int lastUnderscore = fullName.lastIndexOf('_');
        if (lastUnderscore > 0) {
            try {
                long ourEpoch = Long.parseLong(fullName.substring(lastUnderscore + 1));
                String lockName = fullName.substring(0, lastUnderscore);
                EpochFile maxEpoch = findMaxEpoch(lockFilePath.getParent(), lockName);
                if (maxEpoch != null && maxEpoch.epoch > ourEpoch) {
                    lockLost.set(true);
                    throw new IOException(
                            "Lock superseded during renew: our epoch "
                                    + ourEpoch
                                    + " but found higher epoch "
                                    + maxEpoch.epoch);
                }
            } catch (NumberFormatException ignored) {
            }
        }
        LOG.debug("Renewed lock: {}", lockFilePath);
    }

    /** Build the epoch-suffixed lock file path under the lock directory. */
    static Path epochPath(Path lockDir, String lockName, long epoch) {
        return new Path(lockDir, lockName + "_" + String.format(EPOCH_FORMAT, epoch));
    }

    /** Find the lock file with the highest epoch in the lock directory. */
    private EpochFile findMaxEpoch(Path lockDir, String lockName) throws IOException {
        FileStatus[] statuses;
        try {
            statuses = fileIO.listStatus(lockDir);
        } catch (FileNotFoundException e) {
            return null;
        }
        if (statuses == null) {
            return null;
        }
        String prefix = lockName + "_";
        EpochFile max = null;
        for (FileStatus status : statuses) {
            String name = status.getPath().getName();
            if (name.startsWith(prefix)) {
                try {
                    long epoch = Long.parseLong(name.substring(prefix.length()));
                    if (max == null || epoch > max.epoch) {
                        max = new EpochFile(status.getPath(), epoch);
                    }
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return max;
    }

    /** Best-effort cleanup of epoch files older than the given epoch. */
    private void cleanupOldEpochs(Path lockDir, String lockName, long currentEpoch) {
        try {
            FileStatus[] statuses = fileIO.listStatus(lockDir);
            if (statuses == null) {
                return;
            }
            String prefix = lockName + "_";
            for (FileStatus status : statuses) {
                String name = status.getPath().getName();
                if (name.startsWith(prefix)) {
                    try {
                        long epoch = Long.parseLong(name.substring(prefix.length()));
                        if (epoch < currentEpoch) {
                            fileIO.deleteQuietly(status.getPath());
                        }
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        } catch (IOException e) {
            LOG.debug("Best-effort cleanup failed for {}: {}", lockDir, e.getMessage());
        }
    }

    private String readContentSafe(Path path) {
        try {
            return fileIO.readFileUtf8(path);
        } catch (IOException e) {
            return null;
        }
    }

    private boolean isStale(String content) {
        long timestamp = parseTimestamp(content);
        long age = System.currentTimeMillis() - timestamp;
        return age > lockTtl.toMillis();
    }

    String lockContent() {
        return ownerId + "|" + System.currentTimeMillis();
    }

    /**
     * Parse the timestamp from lock file content. Returns current time if parsing fails, so that an
     * unparseable lock is treated as fresh rather than stale.
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

    /** Parse the owner ID from lock file content. Returns empty string if parsing fails. */
    static String parseOwnerId(String content) {
        if (content == null) {
            return "";
        }
        int sep = content.lastIndexOf('|');
        if (sep < 0) {
            return content;
        }
        return content.substring(0, sep);
    }

    private static class EpochFile {
        final Path path;
        final long epoch;

        EpochFile(Path path, long epoch) {
            this.path = path;
            this.epoch = epoch;
        }
    }
}
