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

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

/**
 * QueryIndexProvider for Lucene 9 indexes.
 *
 * <p>Also implements {@link Observer} so that it can be registered as an OSGi
 * {@code Observer} service. Oak calls {@link #contentChanged} after every
 * commit, which in turn refreshes the {@link LuceneNgIndexTracker}. Wrap this
 * instance in a {@link org.apache.jackrabbit.oak.spi.commit.BackgroundObserver}
 * before registering to avoid blocking commit threads.</p>
 */
public class LuceneNgQueryIndexProvider implements QueryIndexProvider, Observer {

    private final LuceneNgIndexTracker tracker;

    public LuceneNgQueryIndexProvider(@NotNull LuceneNgIndexTracker tracker) {
        this.tracker = tracker;
    }

    // -------------------------------------------------------------------------
    // Observer — feeds committed roots into the tracker (called off commit thread
    // when wrapped in BackgroundObserver)
    // -------------------------------------------------------------------------

    @Override
    public void contentChanged(@NotNull NodeState root, @NotNull CommitInfo info) {
        tracker.update(root);
    }

    // -------------------------------------------------------------------------
    // QueryIndexProvider — reads from the already-refreshed tracker
    // -------------------------------------------------------------------------

    @Override
    @NotNull
    public List<? extends QueryIndex> getQueryIndexes(@NotNull NodeState nodeState) {
        List<LuceneNgIndex> indexes = new ArrayList<>();
        for (String indexPath : tracker.getIndexPaths()) {
            LuceneNgIndexNode indexNode = tracker.acquireIndexNode(indexPath);
            if (indexNode != null) {
                indexes.add(new LuceneNgIndex(tracker, indexPath));
            }
        }
        return indexes;
    }
}
