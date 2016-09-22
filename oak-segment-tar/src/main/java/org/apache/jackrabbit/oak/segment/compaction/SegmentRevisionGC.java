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

    /**
     * Raise the flag to signal compaction to stop as soon as possible.
     */
    void stopCompaction();
}
