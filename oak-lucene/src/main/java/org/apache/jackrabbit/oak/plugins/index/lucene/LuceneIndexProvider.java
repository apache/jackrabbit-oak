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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.Closeable;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.index.aggregate.AggregateIndex;
import org.apache.jackrabbit.oak.plugins.index.lucene.score.ScorerProviderFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.ImmutableList;

/**
 * A provider for Lucene indexes.
 * 
 * @see LuceneIndex
 */
public class LuceneIndexProvider implements QueryIndexProvider, Observer, Closeable {

    protected final IndexTracker tracker;

    protected volatile QueryIndex.NodeAggregator aggregator = null;

    ScorerProviderFactory scorerFactory;

    IndexAugmentorFactory augmentorFactory;

    public LuceneIndexProvider() {
        this(new IndexTracker());
    }

    public LuceneIndexProvider(IndexCopier indexCopier) {
        this(new IndexTracker(indexCopier));
    }

    public LuceneIndexProvider(IndexTracker tracker) {
        this(tracker, ScorerProviderFactory.DEFAULT, null);
    }

    public LuceneIndexProvider(IndexTracker tracker, ScorerProviderFactory scorerFactory, IndexAugmentorFactory augmentorFactory) {
        this.tracker = tracker;
        this.scorerFactory = scorerFactory;
        this.augmentorFactory = augmentorFactory;
    }

    public void close() {
        tracker.close();
    }

    //----------------------------------------------------------< Observer >--

    @Override
    public void contentChanged(@Nonnull NodeState root,@Nonnull CommitInfo info) {
        tracker.update(root);
    }

    //------------------------------------------------< QueryIndexProvider >--

    @Override @Nonnull
    public List<QueryIndex> getQueryIndexes(NodeState nodeState) {
        return ImmutableList.<QueryIndex> of(new AggregateIndex(newLuceneIndex()), newLucenePropertyIndex());
    }

    protected LuceneIndex newLuceneIndex() {
        return new LuceneIndex(tracker, aggregator);
    }

    protected LucenePropertyIndex newLucenePropertyIndex() {
        return new LucenePropertyIndex(tracker, scorerFactory, augmentorFactory);
    }

    /**
     * sets the default node aggregator that will be used at query time
     */
    public void setAggregator(QueryIndex.NodeAggregator aggregator) {
        this.aggregator = aggregator;
    }

    // ----- helper builder method

    public LuceneIndexProvider with(QueryIndex.NodeAggregator analyzer) {
        this.setAggregator(analyzer);
        return this;
    }

    IndexTracker getTracker() {
        return tracker;
    }
}
