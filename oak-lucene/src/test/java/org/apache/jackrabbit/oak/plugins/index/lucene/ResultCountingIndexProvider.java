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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvanceFulltextQueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import javax.annotation.Nonnull;
import java.util.List;

class ResultCountingIndexProvider implements QueryIndexProvider {
    private final QueryIndexProvider delegate;
    private final CountingCursorFactory cursorFactory;

    private boolean shouldCount = false;

    private int count = 0;

    ResultCountingIndexProvider(QueryIndexProvider delegate) {
        this.delegate = delegate;
        cursorFactory = new CountingCursorFactory(this);
    }

    void setShouldCount(boolean shouldCount) {
        this.shouldCount = shouldCount;
    }

    int getCount() {
        return count;
    }

    void reset() {
        count = 0;
    }

    void incrementCount() {
        count++;
    }

    @Nonnull
    @Override
    public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
        if (shouldCount) {
            return Lists.transform(delegate.getQueryIndexes(nodeState), new Function<QueryIndex, QueryIndex>() {
                @Nonnull
                @Override
                public QueryIndex apply(@Nonnull  QueryIndex input) {
                    if (input instanceof AdvanceFulltextQueryIndex) {
                        return new CountingIndex((AdvanceFulltextQueryIndex)input, cursorFactory);
                    } else {
                        return input;
                    }
                }
            });
        } else {
            return delegate.getQueryIndexes(nodeState);
        }
    }

    private static class CountingIndex implements QueryIndex, QueryIndex.AdvancedQueryIndex {
        final AdvanceFulltextQueryIndex delegate;
        final CountingCursorFactory cursorFactory;

        CountingIndex(AdvanceFulltextQueryIndex delegate, CountingCursorFactory cursorFactory) {
            this.delegate = delegate;
            this.cursorFactory = cursorFactory;
        }

        @Override
        public double getMinimumCost() {
            return delegate.getMinimumCost();
        }

        @Override
        public double getCost(Filter filter, NodeState rootState) {
            return delegate.getCost(filter, rootState);
        }

        @Override
        public Cursor query(Filter filter, NodeState rootState) {
            return delegate.query(filter, rootState);
        }

        @Override
        public String getPlan(Filter filter, NodeState rootState) {
            return delegate.getPlan(filter, rootState);
        }

        @Override
        public String getIndexName() {
            return delegate.getIndexName();
        }

        @Override
        public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
            return delegate.getPlans(filter, sortOrder, rootState);
        }

        @Override
        public String getPlanDescription(IndexPlan plan, NodeState root) {
            return delegate.getPlanDescription(plan, root);
        }

        @Override
        public Cursor query(IndexPlan plan, NodeState rootState) {
            return cursorFactory.wrap(delegate.query(plan, rootState));
        }
    }

    private static class CountingCursorFactory {
        final ResultCountingIndexProvider provider;

        CountingCursorFactory(ResultCountingIndexProvider provider) {
            this.provider = provider;
        }

        Cursor wrap(final Cursor c) {
            return new Cursor() {
                @Override
                public IndexRow next() {
                    provider.incrementCount();
                    return c.next();
                }

                @Override
                public long getSize(Result.SizePrecision precision, long max) {
                    return c.getSize(precision, max);
                }

                @Override
                public boolean hasNext() {
                    return c.hasNext();
                }
            };
        }
    }
}
