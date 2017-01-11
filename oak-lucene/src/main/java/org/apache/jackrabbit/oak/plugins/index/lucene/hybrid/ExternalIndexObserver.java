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

package org.apache.jackrabbit.oak.plugins.index.lucene.hybrid;

import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneDocumentMaker;
import org.apache.jackrabbit.oak.plugins.observation.Filter;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.apache.lucene.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

class ExternalIndexObserver implements Observer, Filter {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final IndexingQueue indexingQueue;
    private final IndexTracker indexTracker;
    private final MeterStats added;
    private final TimerStats timer;

    public ExternalIndexObserver(IndexingQueue indexingQueue, IndexTracker indexTracker, StatisticsProvider statisticsProvider) {
        this.indexingQueue = checkNotNull(indexingQueue);
        this.indexTracker = checkNotNull(indexTracker);
        this.added = statisticsProvider.getMeter("HYBRID_EXTERNAL_ADDED", StatsOptions.METRICS_ONLY);
        this.timer = statisticsProvider.getTimer("HYBRID_EXTERNAL_TIME", StatsOptions.METRICS_ONLY);
    }

    @Override
    public boolean excludes(@Nonnull NodeState root, @Nonnull CommitInfo info) {
        //Only interested in external changes
        if (!info.isExternal()) {
            return true;
        }

        CommitContext commitContext = (CommitContext) info.getInfo().get(CommitContext.NAME);
        //Commit done internally i.e. one not using Root/Tree API
        if (commitContext == null) {
            return true;
        }

        IndexedPaths indexedPaths = (IndexedPaths) commitContext.get(LuceneDocumentHolder.NAME);
        //Nothing to be indexed
        if (indexedPaths == null) {
            log.debug("IndexPaths not found. Journal support missing");
            return true;
        }

        if (indexedPaths.isEmpty()){
            return true;
        }

        return false;
    }

    @Override
    public void contentChanged(@Nonnull NodeState after, @Nonnull CommitInfo info) {
        //Only interested in external changes
        if (excludes(after, info)) {
            return;
        }

        CommitContext commitContext = (CommitContext) info.getInfo().get(CommitContext.NAME);
        IndexedPaths indexedPaths = (IndexedPaths) commitContext.get(LuceneDocumentHolder.NAME);

        commitContext.remove(LuceneDocumentHolder.NAME);

        log.trace("Received indexed paths {}", indexedPaths);

        int droppedCount = 0;
        int indexedCount = 0;
        TimerStats.Context ctx = timer.time();
        Set<String> indexPaths = Sets.newHashSet();
        for (IndexedPathInfo indexData : indexedPaths) {
            String path = indexData.getPath();
            NodeState indexedNode = null;
            for (String indexPath : indexData.getIndexPaths()) {
                IndexDefinition defn = indexTracker.getIndexDefinition(indexPath);

                //Only update those indexes which are in use in "this" cluster node
                //i.e. for which IndexDefinition is being tracked by IndexTracker
                //This would avoid wasted effort for those cases where index is updated
                //but not used locally
                if (defn == null) {
                    continue;
                }

                //Lazily initialize indexedNode
                if (indexedNode == null) {
                    indexedNode = NodeStateUtils.getNode(after, path);
                }

                if (!indexedNode.exists()) {
                    continue;
                }

                IndexDefinition.IndexingRule indexingRule = defn.getApplicableIndexingRule(indexedNode);

                if (indexingRule == null) {
                    log.debug("No indexingRule found for path {} for index {}", path, indexPath);
                    continue;
                }
                indexPaths.add(indexPath);
                try {
                    Document doc = new LuceneDocumentMaker(defn, indexingRule, path).makeDocument(indexedNode);

                    if (doc != null) {
                        if (indexingQueue.add(LuceneDoc.forUpdate(indexPath, path, doc))) {
                            indexedCount++;
                        } else {
                            droppedCount++;
                        }
                    }
                } catch (Exception e) {
                    log.warn("Ignoring making LuceneDocument for path {} for index {} due to exception", path, indexPath, e);
                }
            }
        }
        if (droppedCount > 0) {
            log.warn("Dropped [{}] docs from indexing as queue is full", droppedCount);
        }
        added.mark(indexedCount);
        ctx.stop();
        log.debug("Added {} documents for {} indexes from external changes", indexedCount, indexPaths.size());
    }
}
