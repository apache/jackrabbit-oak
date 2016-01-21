/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.GAIN_THRESHOLD_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.NO_COMPACTION;

import java.util.concurrent.Callable;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CleanupType;

public class SegmentNodeStoreBuilder {

    private final SegmentStore store;

    private boolean isCreated;

    private boolean hasCompactionStrategy;
    private boolean pauseCompaction;
    private boolean cloneBinaries;
    private String cleanup;
    private long cleanupTs;
    private byte memoryThreshold;
    private int lockWaitTime;
    private int retryCount;
    private boolean forceAfterFail;
    private boolean persistCompactionMap;
    private byte gainThreshold;
    private CompactionStrategy compactionStrategy;

    static SegmentNodeStoreBuilder newSegmentNodeStore(SegmentStore store) {
        return new SegmentNodeStoreBuilder(store);
    }

    private SegmentNodeStoreBuilder(@Nonnull SegmentStore store) {
        this.store = store;
    }

    @Deprecated
    public SegmentNodeStoreBuilder withCompactionStrategy(
            boolean pauseCompaction, boolean cloneBinaries, String cleanup,
            long cleanupTs, byte memoryThreshold, final int lockWaitTime,
            int retryCount, boolean forceAfterFail, boolean persistCompactionMap) {
        return withCompactionStrategy(pauseCompaction, cloneBinaries, cleanup,
                cleanupTs, memoryThreshold, lockWaitTime, retryCount,
                forceAfterFail, persistCompactionMap, GAIN_THRESHOLD_DEFAULT);
    }

    public SegmentNodeStoreBuilder withCompactionStrategy(
            boolean pauseCompaction, boolean cloneBinaries, String cleanup,
            long cleanupTs, byte memoryThreshold, final int lockWaitTime,
            int retryCount, boolean forceAfterFail, boolean persistCompactionMap, byte gainThreshold) {
        this.hasCompactionStrategy = true;
        this.pauseCompaction = pauseCompaction;
        this.cloneBinaries = cloneBinaries;
        this.cleanup = cleanup;
        this.cleanupTs = cleanupTs;
        this.memoryThreshold = memoryThreshold;
        this.lockWaitTime = lockWaitTime;
        this.retryCount = retryCount;
        this.forceAfterFail = forceAfterFail;
        this.persistCompactionMap = persistCompactionMap;
        this.gainThreshold = gainThreshold;
        return this;
    }

    public CompactionStrategy getCompactionStrategy() {
        checkState(isCreated);
        return compactionStrategy;
    }

    @Nonnull
    public SegmentNodeStore create() {
        checkState(!isCreated);
        isCreated = true;
        final SegmentNodeStore segmentStore = new SegmentNodeStore(store, true);
        if (hasCompactionStrategy) {
            compactionStrategy = new CompactionStrategy(pauseCompaction,
                    cloneBinaries, CleanupType.valueOf(cleanup), cleanupTs,
                    memoryThreshold) {

                @Override
                public boolean compacted(Callable<Boolean> setHead)
                        throws Exception {
                    // Need to guard against concurrent commits to avoid
                    // mixed segments. See OAK-2192.
                    return segmentStore.locked(setHead, lockWaitTime, SECONDS);
                }
            };
            compactionStrategy.setRetryCount(retryCount);
            compactionStrategy.setForceAfterFail(forceAfterFail);
            compactionStrategy.setPersistCompactionMap(persistCompactionMap);
            compactionStrategy.setGainThreshold(gainThreshold);
        } else {
            compactionStrategy = NO_COMPACTION;
        }
        return segmentStore;
    }

}
