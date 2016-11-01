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

@Deprecated
public interface CompactionStrategyMBean {

    @Deprecated
    String TYPE = "CompactionStrategy";

    @Deprecated
    boolean isCloneBinaries();

    @Deprecated
    void setCloneBinaries(boolean cloneBinaries);

    @Deprecated
    boolean isPausedCompaction();

    @Deprecated
    void setPausedCompaction(boolean pausedCompaction);

    @Deprecated
    String getCleanupStrategy();

    @Deprecated
    void setCleanupStrategy(String cleanup);

    @Deprecated
    long getOlderThan();

    @Deprecated
    void setOlderThan(long olderThan);

    @Deprecated
    byte getMemoryThreshold();

    @Deprecated
    void setMemoryThreshold(byte memory);

    /**
     * Get whether or not to force compact concurrent commits on top of already
     * compacted commits after the maximum number of retries has been reached.
     * Force committing tries to exclusively write lock the node store.
     * @return  {@code true} if force commit is on, {@code false} otherwise
     */
    @Deprecated
    boolean getForceAfterFail();

    /**
     * Set whether or not to force compact concurrent commits on top of already
     * compacted commits after the maximum number of retries has been reached.
     * Force committing tries to exclusively write lock the node store.
     * @param forceAfterFail
     */
    @Deprecated
    void setForceAfterFail(boolean forceAfterFail);

    /**
     * Get the number of tries to compact concurrent commits on top of already
     * compacted commits
     * @return  retry count
     */
    @Deprecated
    int getRetryCount();

    /**
     * Set the number of tries to compact concurrent commits on top of already
     * compacted commits
     * @param retryCount
     */
    @Deprecated
    void setRetryCount(int retryCount);

    /**
     * Get the compaction gain estimate threshold beyond which compaction should
     * run
     * @return gainThreshold
     */
    @Deprecated
    byte getGainThreshold();

    /**
     * Set the compaction gain estimate threshold beyond which compaction should
     * run
     * @param gainThreshold
     */
    @Deprecated
    void setGainThreshold(byte gainThreshold);

}
