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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.gc.GCMonitor;

/**
 * This MBean exposes the settings from {@link SegmentGCOptions} and
 * reflects the GC status as reported by the {@link GCMonitor}.
 */
public interface SegmentRevisionGC {
    String TYPE = "SegmentRevisionGarbageCollection";

    /**
     * @return  {@code true} iff revision gc is paused.
     */
    boolean isPausedCompaction();

    /**
     * Set revision gc to paused.
     * @param paused
     */
    void setPausedCompaction(boolean paused);

    /**
     * Get the number of tries to compact concurrent commits on top of already
     * compacted commits
     * @return  retry count
     */
    int getRetryCount();

    /**
     * Set the number of tries to compact concurrent commits on top of already
     * compacted commits
     * @param retryCount
     */
    void setRetryCount(int retryCount);

    /**
     * Get the number of seconds to attempt to force compact concurrent commits on top of
     * already compacted commits after the maximum number of retries has been reached.
     * Forced compaction acquires an exclusive write lock on the node store.
     * @return  the number of seconds until forced compaction gives up and the exclusive
     *          write lock on the node store is released.
     */
    int getForceTimeout();

    /**
     * Set the number of seconds to attempt to force compact concurrent commits on top of
     * already compacted commits after the maximum number of retries has been reached.
     * Forced compaction acquires an exclusively write lock on the node store.
     * @param timeout  the number of seconds until forced compaction gives up and the exclusive
     *                 lock on the node store is released.
     */
    void setForceTimeout(int timeout);

    /**
     * Number of segment generations to retain.
     * @see #setRetainedGenerations(int)
     * @return  number of gc generations.
     */
    int getRetainedGenerations();

    /**
     * Set the number of segment generations to retain: each compaction run creates
     * a new segment generation. {@code retainGenerations} determines how many of
     * those generations are retained during cleanup.
     *
     * @param retainedGenerations  number of generations to retain. Must be {@code >= 2}.
     * @throws IllegalArgumentException if {@code retainGenerations < 2}
     */
    void setRetainedGenerations(int retainedGenerations);

    long getGcSizeDeltaEstimation();

    void setGcSizeDeltaEstimation(long gcSizeDeltaEstimation);

    boolean isEstimationDisabled();

    /**
     * Disables the estimation phase, thus allowing GC to run every time.
     * @param disabled
     */
    void setEstimationDisabled(boolean disabled);

    String getGCType();

    void setGCType(String gcType);

    /**
     * Initiate a revision garbage collection operation
     */
    void startRevisionGC();

    /**
     * Cancel a running revision garbage collection operation. Does nothing
     * if revision garbage collection is not running.
     */
    void cancelRevisionGC();

    /**
     * @return  time of the last compaction in milliseconds.
     */
    long getLastCompaction();

    /**
     * @return  time of the last cleanup in milliseconds.
     */
    long getLastCleanup();

    /**
     * @return  repository size after the last cleanup.
     */
    long getLastRepositorySize();

    /**
     * @return  reclaimed size during the last cleanup.
     */
    long getLastReclaimedSize();

    /**
     * @return  last error or {@code null} if none.
     */
    @CheckForNull
    String getLastError();
    
    /**
     * @return  last log message or {@code null} if none.
     */
    @Nonnull
    String getLastLogMessage();

    /**
     * @return  current status.
     */
    @Nonnull
    String getStatus();

    /**
     * Get the available memory threshold beyond which revision gc will be
     * canceled. Value represents a percentage so an value between 0 and 100
     * will be returned.
     * @return memory threshold
     */
    int getMemoryThreshold();

    /**
     * Set the available memory threshold beyond which revision gc will be
     * canceled. Value represents a percentage so an input between {@code 0} and
     * {@code 100} is expected. Setting this to {@code 0} will disable the
     * check.
     * @param memoryThreshold
     */
    void setMemoryThreshold(int memoryThreshold);

    /**
     * @return {@code true} if there is an online compaction cycle running
     */
    boolean isRevisionGCRunning();

    /**
     * @return number of compacted nodes in the current cycle
     */
    long getCompactedNodes();

    /**
     * @return number of estimated nodes to be compacted in the current cycle.
     *         Can be {@code -1} if the estimation can't be performed
     */
    long getEstimatedCompactableNodes();

    /**
     * @return percentage of progress for the current compaction cycle. Can be
     *         {@code -1} if the estimation can't be performed.
     */
    int getEstimatedRevisionGCCompletion();

    /**
     * @return Number of nodes the monitor will log a message, {@code -1} means disabled
     */
    long getRevisionGCProgressLog();

    /**
     * Set the size of the logging interval, {@code -1} means disabled
     * @param gcProgressLog
     *            number of nodes
     */
    void setRevisionGCProgressLog(long gcProgressLog);
}
