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

package org.apache.jackrabbit.oak.plugins.document.secondary;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.NodeStateDiffer;
import org.apache.jackrabbit.oak.plugins.index.PathFilter;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SecondaryStoreObserver implements Observer {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final NodeStore nodeStore;
    private final PathFilter pathFilter;
    private final SecondaryStoreRootObserver secondaryObserver;
    private final NodeStateDiffer differ;
    private final TimerStats local;
    private final TimerStats external;
    private boolean firstEventProcessed;

    public SecondaryStoreObserver(NodeStore nodeStore, PathFilter pathFilter, NodeStateDiffer differ) {
        this(nodeStore, pathFilter, SecondaryStoreRootObserver.NOOP, differ, StatisticsProvider.NOOP);
    }

    public SecondaryStoreObserver(NodeStore nodeStore, PathFilter pathFilter,
                                  SecondaryStoreRootObserver secondaryObserver,
                                  NodeStateDiffer differ, StatisticsProvider statisticsProvider) {
        this.nodeStore = nodeStore;
        this.pathFilter = pathFilter;
        this.secondaryObserver = secondaryObserver;
        this.differ = differ;
        this.local = statisticsProvider.getTimer("DOCUMENT_CACHE_SEC_LOCAL", StatsOptions.DEFAULT);
        this.external = statisticsProvider.getTimer("DOCUMENT_CACHE_SEC_EXTERNAL", StatsOptions.DEFAULT);
    }

    @Override
    public void contentChanged(@Nonnull NodeState root, @Nullable CommitInfo info) {
        //Diff here would also be traversing non visible areas and there
        //diffManyChildren might pose problem for e.g. data under uuid index
        if (!firstEventProcessed){
            log.info("Starting initial sync");
        }

        Stopwatch w = Stopwatch.createStarted();
        NodeState target = root;
        NodeState secondaryRoot = nodeStore.getRoot();
        NodeState base = DelegatingDocumentNodeState.wrapIfPossible(secondaryRoot, differ);
        NodeBuilder builder = secondaryRoot.builder();
        ApplyDiff diff = new PathFilteringDiff(builder, pathFilter);

        //Copy the root node meta properties
        PathFilteringDiff.copyMetaProperties((AbstractDocumentNodeState) target, builder);

        //Apply the rest of properties
        target.compareAgainstBaseState(base, diff);
        try {
            NodeState updatedSecondaryRoot = nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            secondaryObserver.contentChanged(DelegatingDocumentNodeState.wrap(updatedSecondaryRoot, differ));

            TimerStats timer = info == null ? external : local;
            timer.update(w.elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);

            if (!firstEventProcessed){
                log.info("Time taken for initial sync {}", w);
                firstEventProcessed = true;
            }
        } catch (CommitFailedException e) {
            //TODO
            log.warn("Commit to secondary store failed", e);
        }
    }

}
