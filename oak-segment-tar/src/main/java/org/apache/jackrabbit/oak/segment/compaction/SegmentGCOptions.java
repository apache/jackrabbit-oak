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

package org.apache.jackrabbit.oak.segment.compaction;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This class holds configuration options for segment store revision gc.
 */
public class SegmentGCOptions {

    /**
     * Default value for {@link #isPaused()}
     */
    public static final boolean PAUSE_DEFAULT = false;

    /**
     * Default value for {@link #isEstimationDisabled()}
     */
    public static final boolean DISABLE_ESTIMATION_DEFAULT = false;

    /**
     * Default value for {@link #getRetryCount()}
     */
    public static final int RETRY_COUNT_DEFAULT = 5;

    /**
     * Default value for {@link #getForceTimeout()} in seconds.
     */
    public static final int FORCE_TIMEOUT_DEFAULT = 60;

    /**
     * Default value for {@link #getRetainedGenerations()}
     */
    public static final int RETAINED_GENERATIONS_DEFAULT = 2;

    /**
     * Default value for {@link #getGcSizeDeltaEstimation()} set to 10GB
     */
    public static final long SIZE_DELTA_ESTIMATION_DEFAULT = 10737418240L;

    /**
     * Default value for {@link #getMemoryThreshold()}
     */
    public static final int MEMORY_THRESHOLD_DEFAULT = 15;

    private boolean paused = PAUSE_DEFAULT;

    /**
     * Flag controlling whether the estimation phase will run before a GC cycle
     */
    private boolean estimationDisabled = DISABLE_ESTIMATION_DEFAULT;

    private int retryCount = RETRY_COUNT_DEFAULT;

    private int forceTimeout = FORCE_TIMEOUT_DEFAULT;

    private int retainedGenerations = RETAINED_GENERATIONS_DEFAULT;

    private boolean offline = false;

    private int memoryThreshold = MEMORY_THRESHOLD_DEFAULT;

    private boolean ocBinDeduplication = Boolean
            .getBoolean("oak.segment.compaction.binaryDeduplication");

    private long ocBinMaxSize = Long.getLong(
            "oak.segment.compaction.binaryDeduplicationMaxSize",
            100 * 1024 * 1024);

    private long gcSizeDeltaEstimation = Long.getLong(
            "oak.segment.compaction.gcSizeDeltaEstimation",
            SIZE_DELTA_ESTIMATION_DEFAULT);

    public SegmentGCOptions(boolean paused, int retryCount, int forceTimeout) {
        this.paused = paused;
        this.retryCount = retryCount;
        this.forceTimeout = forceTimeout;
    }

    public SegmentGCOptions() {
        this(PAUSE_DEFAULT, RETRY_COUNT_DEFAULT, FORCE_TIMEOUT_DEFAULT);
    }

    /**
     * Default options: {@link #PAUSE_DEFAULT}, {@link #GAIN_THRESHOLD_DEFAULT},
     * {@link #RETRY_COUNT_DEFAULT}, {@link #FORCE_TIMEOUT_DEFAULT}.
     */
    public static SegmentGCOptions defaultGCOptions() {
        return new SegmentGCOptions();
    }

    /**
     * @return  {@code true} iff revision gc is paused.
     */
    public boolean isPaused() {
        return paused;
    }

    /**
     * Set revision gc to paused.
     * @param paused
     * @return this instance
     */
    public SegmentGCOptions setPaused(boolean paused) {
        this.paused = paused;
        return this;
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
     * @return this instance
     */
    public SegmentGCOptions setRetryCount(int retryCount) {
        this.retryCount = retryCount;
        return this;
    }

    /**
     * Get the number of seconds to attempt to force compact concurrent commits on top of
     * already compacted commits after the maximum number of retries has been reached.
     * Forced compaction acquires an exclusive write lock on the node store.
     * @return  the number of seconds until forced compaction gives up and the exclusive
     *          write lock on the node store is released.
     */
    public int getForceTimeout() {
        return forceTimeout;
    }

    /**
     * Set the number of seconds to attempt to force compact concurrent commits on top of
     * already compacted commits after the maximum number of retries has been reached.
     * Forced compaction acquires an exclusively write lock on the node store.
     * @param timeout  the number of seconds until forced compaction gives up and the exclusive
     *                 lock on the node store is released.
     * @return this instance
     */
    public SegmentGCOptions setForceTimeout(int timeout) {
        this.forceTimeout = timeout;
        return this;
    }

    /**
     * Number of segment generations to retain.
     * @see #setRetainedGenerations(int)
     * @return  number of gc generations.
     */
    public int getRetainedGenerations() {
        return retainedGenerations;
    }

    /**
     * Set the number of segment generations to retain: each compaction run creates
     * a new segment generation. {@code retainGenerations} determines how many of
     * those generations are retained during cleanup.
     *
     * @param retainedGenerations  number of generations to retain. Must be {@code >= 2}.
     * @return this instance
     * @throws IllegalArgumentException if {@code retainGenerations < 2}
     */
    public SegmentGCOptions setRetainedGenerations(int retainedGenerations) {
        checkArgument(retainedGenerations > 1,
                "RetainedGenerations must not be below 2. Got %s", retainedGenerations);
        this.retainedGenerations = retainedGenerations;
        return this;
    }

    @Override
    public String toString() {
        if (offline) {
            return getClass().getSimpleName() + "{" +
                    "offline=" + offline +
                    ", retainedGenerations=" + retainedGenerations +
                    ", ocBinDeduplication=" + ocBinDeduplication +
                    ", ocBinMaxSize=" + ocBinMaxSize + "}";
        } else {
            return getClass().getSimpleName() + "{" +
                    "paused=" + paused +
                    ", estimationDisabled=" + estimationDisabled +
                    ", gcSizeDeltaEstimation=" + gcSizeDeltaEstimation +
                    ", retryCount=" + retryCount +
                    ", forceTimeout=" + forceTimeout +
                    ", retainedGenerations=" + retainedGenerations +
                    ", gcSizeDeltaEstimation=" + gcSizeDeltaEstimation + "}";
        }
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

    public boolean isOffline() {
        return offline;
    }

    /**
     * Enables the offline compaction mode, allowing for certain optimizations,
     * like reducing the retained generation to 1.
     * @return this instance
     */
    public SegmentGCOptions setOffline() {
        this.offline = true;
        this.retainedGenerations = 1;
        return this;
    }

    /**
     * Offline compaction only. Enables content based de-duplication of
     * binaries. Involves a fair amount of I/O when reading/comparing
     * potentially equal blobs. set via the
     * 'oak.segment.compaction.binaryDeduplication' system property
     * @return this instance.
     */
    public SegmentGCOptions withBinaryDeduplication() {
        this.ocBinDeduplication = true;
        return this;
    }

    public boolean isBinaryDeduplication() {
        return this.ocBinDeduplication;
    }

    /**
     * Offline compaction only. Set the upper bound for the content based
     * de-duplication checks.
     * @param binMaxSize
     * @return this instance
     */
    public SegmentGCOptions setBinaryDeduplicationMaxSize(long binMaxSize) {
        this.ocBinMaxSize = binMaxSize;
        return this;
    }

    public long getBinaryDeduplicationMaxSize() {
        return this.ocBinMaxSize;
    }

    public long getGcSizeDeltaEstimation() {
        return gcSizeDeltaEstimation;
    }

    public SegmentGCOptions setGcSizeDeltaEstimation(long gcSizeDeltaEstimation) {
        this.gcSizeDeltaEstimation = gcSizeDeltaEstimation;
        return this;
    }

    /**
     * Get the available memory threshold beyond which revision gc will be
     * canceled. Value represents a percentage so an value between {@code 0} and
     * {@code 100} will be returned.
     * @return memoryThreshold
     */
    public int getMemoryThreshold() {
        return memoryThreshold;
    }

    /**
     * Set the available memory threshold beyond which revision gc will be
     * canceled. Value represents a percentage so an input between {@code 0} and
     * {@code 100} is expected. Setting this to {@code 0} will disable the
     * check.
     * @param memoryThreshold
     * @return this instance
     */
    public SegmentGCOptions setMemoryThreshold(int memoryThreshold) {
        this.memoryThreshold = memoryThreshold;
        return this;
    }

    public boolean isEstimationDisabled() {
        return estimationDisabled;
    }

    /**
     * Disables the estimation phase, thus allowing GC to run every time.
     * @return this instance
     */
    public SegmentGCOptions setEstimationDisabled(boolean disabled) {
        this.estimationDisabled = disabled;
        return this;
    }
}
