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

/**
 * This MBean exposes the settings from {@link SegmentGCOptions}.
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
     * Get the gain estimate threshold beyond which revision gc should run
     * @return gainThreshold
     */
    int getGainThreshold();

    /**
     * Set the revision gain estimate threshold beyond which revision gc should run
     * @param gainThreshold
     */
    void setGainThreshold(int gainThreshold);

    /**
     * @return  the memory threshold below which revision gc will not run.
     */
    int getMemoryThreshold();

    /**
     * Set the memory threshold below which revision gc will not run.
     * @param memoryThreshold
     */
    void setMemoryThreshold(int memoryThreshold);

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
     * Get whether or not to force compact concurrent commits on top of already
     * compacted commits after the maximum number of retries has been reached.
     * Force committing tries to exclusively write lock the node store.
     * @return  {@code true} if force commit is on, {@code false} otherwise
     */
    boolean getForceAfterFail();

    /**
     * Set whether or not to force compact concurrent commits on top of already
     * compacted commits after the maximum number of retries has been reached.
     * Force committing tries to exclusively write lock the node store.
     * @param forceAfterFail
     */
    void setForceAfterFail(boolean forceAfterFail);

    /**
     * Get the time to wait for the lock when force compacting.
     * See {@link #setForceAfterFail(boolean)}
     * @return lock wait time in seconds.
     */
    int getLockWaitTime();

    /**
     * Set the time to wait for the lock when force compacting.
     * @param lockWaitTime  lock wait time in seconds
     */
    void setLockWaitTime(int lockWaitTime);

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

}
