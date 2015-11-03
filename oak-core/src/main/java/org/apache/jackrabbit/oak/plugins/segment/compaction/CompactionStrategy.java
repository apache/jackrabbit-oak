/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.segment.compaction;

import org.apache.jackrabbit.oak.plugins.segment.SegmentId;

import javax.annotation.Nonnull;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.currentTimeMillis;

public abstract class CompactionStrategy {

    public enum CleanupType {

        /**
         * {@code CLEAN_ALL} <em>must</em> be used in conjunction with {@code cloneBinaries}
         * otherwise segments can go away ({@code SegmentNotFoundException})
         * <p>
         * Pros: best compaction results
         * <p>
         * Cons: larger repo size <em>during</em> compaction (2x). High chances that a currently
         * running diff (e.g. observation) fails with {@code SegmentNotFoundException}.
         */
        CLEAN_ALL,

        CLEAN_NONE,

        /**
         * {@code CLEAN_OLD} with {@code cloneBinaries}
         * <p>
         * Pros: better compaction results
         * <p>
         * Cons: larger repo size {@code during} compaction (2x). {@code SegmentNotFoundException}
         * with insufficiently large values for {@code olderThan}.
         * <p>
         * {@code CLEAN_OLD} without {@code cloneBinaries}
         * <p>
         * Pros: weakest compaction results, smaller size during compaction (1x + size of
         * data-segments).
         * <p>
         * Cons: {@code SegmentNotFoundException} with insufficiently large values for
         * {@code olderThan}.
         */
        CLEAN_OLD
    }

    public static final boolean PAUSE_DEFAULT = false;

    public static final boolean CLONE_BINARIES_DEFAULT = false;

    public static final CleanupType CLEANUP_DEFAULT = CleanupType.CLEAN_OLD;

    public static final long TIMESTAMP_DEFAULT = 1000 * 60 * 60 * 10;  // 10h

    public static final byte MEMORY_THRESHOLD_DEFAULT = 5;

    public static final boolean PERSIST_COMPACTION_MAP_DEFAULT = true;

    public static final byte GAIN_THRESHOLD_DEFAULT = 10;

    /**
     * Default value for {@link #getRetryCount()}
     */
    public static final int RETRY_COUNT_DEFAULT = 5;

    /**
     * Default value for {@link #getForceAfterFail()}
     */
    public static final boolean FORCE_AFTER_FAIL_DEFAULT = true;

    /**
     * No compaction at all
     */
    public static final CompactionStrategy NO_COMPACTION = new CompactionStrategy(
            true, false, CleanupType.CLEAN_NONE, 0, MEMORY_THRESHOLD_DEFAULT) {
        @Override
        public boolean compacted(@Nonnull Callable<Boolean> setHead) throws Exception {
            return false;
        }
    };

    private boolean paused;

    private boolean cloneBinaries;

    @Nonnull
    private CleanupType cleanupType;

    /**
     * anything that has a lifetime bigger than this will be removed. a value of
     * 0 (or very small) acts like a CLEANUP.NONE, a value of -1 (or negative)
     * acts like a CLEANUP.ALL
     * 
     */
    private long olderThan;

    private byte memoryThreshold = MEMORY_THRESHOLD_DEFAULT;

    private boolean persistedCompactionMap = PERSIST_COMPACTION_MAP_DEFAULT;

    private int retryCount = RETRY_COUNT_DEFAULT;

    private boolean forceAfterFail = FORCE_AFTER_FAIL_DEFAULT;

    private long compactionStart = currentTimeMillis();

    /**
     * Compaction gain estimate threshold beyond which compaction should run
     */
    private byte gainThreshold = GAIN_THRESHOLD_DEFAULT;

    /**
     * Flag that allows turning on an optimized version of the compaction
     * process in the case of offline compaction
     */
    private boolean offlineCompaction = false;

    protected CompactionStrategy(boolean paused,
            boolean cloneBinaries, @Nonnull CleanupType cleanupType, long olderThan, byte memoryThreshold) {
        checkArgument(olderThan >= 0);
        this.paused = paused;
        this.cloneBinaries = cloneBinaries;
        this.cleanupType = checkNotNull(cleanupType);
        this.olderThan = olderThan;
        this.memoryThreshold = memoryThreshold;
    }

    public boolean canRemove(SegmentId id) {
        switch (cleanupType) {
            case CLEAN_ALL:
                return true;
            case CLEAN_NONE:
                return false;
            case CLEAN_OLD:
                return compactionStart - id.getCreationTime() > olderThan;
        }
        return false;
    }

    public boolean cloneBinaries() {
        return cloneBinaries;
    }

    public boolean isPaused() {
        return paused;
    }

    public void setPaused(boolean paused) {
        this.paused = paused;
    }

    public void setCloneBinaries(boolean cloneBinaries) {
        this.cloneBinaries = cloneBinaries;
    }

    public void setCleanupType(@Nonnull CleanupType cleanupType) {
        this.cleanupType = checkNotNull(cleanupType);
    }

    public void setOlderThan(long olderThan) {
        checkArgument(olderThan >= 0);
        this.olderThan = olderThan;
    }

    String getCleanupType() {
        return cleanupType.toString();
    }

    long getOlderThan() {
        return olderThan;
    }


    @Override
    public String toString() {
        return "CompactionStrategy{" +
                "paused=" + paused +
                ", cloneBinaries=" + cloneBinaries +
                ", cleanupType=" + cleanupType +
                ", olderThan=" + olderThan +
                ", memoryThreshold=" + memoryThreshold +
                ", persistedCompactionMap=" + persistedCompactionMap +
                ", retryCount=" + retryCount +
                ", forceAfterFail=" + forceAfterFail +
                ", compactionStart=" + compactionStart +
                ", offlineCompaction=" + offlineCompaction +
                '}';
    }

    public void setCompactionStart(long ms) {
        this.compactionStart = ms;
    }

    public byte getMemoryThreshold() {
        return memoryThreshold;
    }

    public void setMemoryThreshold(byte memoryThreshold) {
        this.memoryThreshold = memoryThreshold;
    }

    public boolean getPersistCompactionMap() {
        return persistedCompactionMap;
    }

    public void setPersistCompactionMap(boolean persist) {
        persistedCompactionMap = persist;
    }

    /**
     * Get whether or not to force compact concurrent commits on top of already
     * compacted commits after the maximum number of retries has been reached.
     * Force committing tries to exclusively write lock the node store.
     * @return  {@code true} if force commit is on, {@code false} otherwise
     */
    public boolean getForceAfterFail() {
        return forceAfterFail;
    }

    /**
     * Set whether or not to force compact concurrent commits on top of already
     * compacted commits after the maximum number of retries has been reached.
     * Force committing tries to exclusively write lock the node store.
     * @param forceAfterFail
     */
    public void setForceAfterFail(boolean forceAfterFail) {
        this.forceAfterFail = forceAfterFail;
    }

    /**
     * Get the number of tries to compact concurrent commits on top of already
     * compacted commits
     * @return  retry count
     */
    public int getRetryCount() {
        return retryCount;
    }

    /**
     * Set the number of tries to compact concurrent commits on top of already
     * compacted commits
     * @param retryCount
     */
    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    /**
     * Get the compaction gain estimate threshold beyond which compaction should
     * run
     * @return gainThreshold
     */
    public byte getGainThreshold() {
        return gainThreshold;
    }

    /**
     * Set the compaction gain estimate threshold beyond which compaction should
     * run
     * @param gainThreshold
     */
    public void setGainThreshold(byte gainThreshold) {
        this.gainThreshold = gainThreshold;
    }

    public abstract boolean compacted(@Nonnull Callable<Boolean> setHead) throws Exception;

    public boolean isOfflineCompaction() {
        return offlineCompaction;
    }

    public void setOfflineCompaction(boolean offlineCompaction) {
        this.offlineCompaction = offlineCompaction;
    }

    /**
     * Check if the approximate repository size is getting too big compared with
     * the available space on disk.
     *
     * @param repositoryDiskSpace Approximate size of the disk space occupied by
     *                            the repository.
     * @param availableDiskSpace  Currently available disk space.
     * @return {@code true} if the available disk space is considered enough for
     * normal repository operations.
     */
    public boolean isDiskSpaceSufficient(long repositoryDiskSpace, long availableDiskSpace) {
        return availableDiskSpace > 0.25 * repositoryDiskSpace;
    }

}
