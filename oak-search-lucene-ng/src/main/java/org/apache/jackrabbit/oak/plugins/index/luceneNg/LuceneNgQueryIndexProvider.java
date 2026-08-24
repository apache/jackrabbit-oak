/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.plugins.index.search.util.IndexHelper;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

/**
 * QueryIndexProvider for Lucene 9 indexes.
 * Returns LuceneNgIndex instances for all Lucene 9 indexes in the repository.
 */
public class LuceneNgQueryIndexProvider implements QueryIndexProvider {

    private final LuceneNgIndexTracker tracker;

    public LuceneNgQueryIndexProvider(LuceneNgIndexTracker tracker) {
        this.tracker = tracker;
    }

    @Override
    @NotNull
    public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
        // Update tracker with current state
        tracker.update(nodeState);

        // Enumerate every currently defined lucene9 index directly off nodeState, rather than
        // off tracker.getIndexNodePaths() -- the shared FulltextIndexTracker only *caches*
        // paths that have already been opened (via a prior acquireIndexNode() call or an
        // in-place update to an already-cached path, see FulltextIndexTracker#diffAndUpdate);
        // it does not itself perform full-repository discovery of newly defined indexes on
        // update(). A brand new (never yet queried) lucene9 index must still be offered here
        // as a query index -- its LuceneNgIndex resolves/acquires the actual node lazily,
        // on demand, once the query engine calls getCost()/getPlans() on it.
        List<LuceneNgIndex> indexes = new ArrayList<>();
        NodeState oakIndex = nodeState.getChildNode("oak:index");
        for (String indexName : oakIndex.getChildNodeNames()) {
            NodeState indexState = oakIndex.getChildNode(indexName);
            if (IndexHelper.isIndexNodeOfType(indexState, LuceneNgIndexConstants.TYPE_LUCENE9)) {
                indexes.add(new LuceneNgIndex(tracker, "/oak:index/" + indexName));
            }
        }
        return indexes;
    }
}
