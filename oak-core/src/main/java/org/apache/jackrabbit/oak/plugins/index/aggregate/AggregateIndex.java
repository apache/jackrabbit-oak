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
package org.apache.jackrabbit.oak.plugins.index.aggregate;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.query.index.IndexRowImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors.AbstractCursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.FulltextQueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

/**
 * A virtual full-text that can aggregate nodes based on aggregate definitions.
 * Internally, it uses another full-text index.
 */
public class AggregateIndex implements FulltextQueryIndex {

    private final FulltextQueryIndex baseIndex;

    public AggregateIndex(FulltextQueryIndex baseIndex) {
        this.baseIndex = baseIndex;
    }

    @Override
    public double getCost(Filter filter, NodeState rootState) {
        if (baseIndex == null) {
            return Double.POSITIVE_INFINITY;
        }
        return baseIndex.getCost(filter, rootState);
    }

    @Override
    public Cursor query(Filter filter, NodeState rootState) {
        // pass-through impl
        if (baseIndex.getNodeAggregator() == null) {
            return baseIndex.query(filter, rootState);
        }
        return new AggregationCursor(baseIndex.query(
                newAggregationFilter(filter), rootState),
                baseIndex.getNodeAggregator(), rootState);
    }

    private static Filter newAggregationFilter(Filter filter) {
        FilterImpl f = new FilterImpl(filter);
        // disables node type checks for now
        f.setMatchesAllTypes(true);
        return f;
    }

    @Override
    public String getPlan(Filter filter, NodeState rootState) {
        if (baseIndex == null) {
            return "aggregate no-index";
        }
        return "aggregate " + baseIndex.getPlan(filter, rootState);
    }

    @Override
    public String getIndexName() {
        if (baseIndex == null) {
            return "aggregate no-index";
        }
        return "aggregate " + baseIndex.getIndexName();
    }

    @Override
    public NodeAggregator getNodeAggregator() {
        return baseIndex.getNodeAggregator();
    }

    /**
     * An aggregation aware cursor.
     */
    private static class AggregationCursor extends AbstractCursor {

        private final Cursor cursor;
        private final NodeAggregator aggregator;
        private final NodeState rootState;

        private boolean init;
        private boolean closed;
        private Iterator<String> aggregates;

        private String item;

        public AggregationCursor(Cursor cursor, NodeAggregator aggregator,
                NodeState rootState) {
            this.cursor = cursor;
            this.aggregator = aggregator;
            this.rootState = rootState;
        }

        @Override
        public boolean hasNext() {
            if (!closed && !init) {
                fetchNext();
                init = true;
            }
            return !closed;
        }

        private void fetchNext() {
            if (aggregates != null && aggregates.hasNext()) {
                item = aggregates.next();
                init = true;
                return;
            }
            aggregates = null;
            if (cursor.hasNext()) {
                IndexRow row = cursor.next();
                String path = row.getPath();
                aggregates = Iterators.concat(ImmutableSet.of(path).iterator(),
                        aggregator.getParents(rootState, path));
                fetchNext();
                return;
            }
            closed = true;
        }

        @Override
        public IndexRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            init = false;
            return new IndexRowImpl(item);
        }
    }

}
