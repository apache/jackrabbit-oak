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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.query.fulltext.FullTextVisitor;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors.AbstractCursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.FulltextQueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Predicates;
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
        return baseIndex.getCost(filter, rootState) - 0.05;
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

        // TODO OAK-828
        // FullTextExpression constraint = filter.getFullTextConstraint();
        // constraint = getFlatConstraint(constraint);
        // f.setFullTextConstraint(constraint);

        return f;
    }

    static FullTextExpression getFlatConstraint(
            FullTextExpression constraint) {
        if (constraint == null) {
            return null;
        }
        final AtomicReference<FullTextExpression> result = new AtomicReference<FullTextExpression>();
        constraint.accept(new FullTextVisitor() {
            
            @Override
            public boolean visit(FullTextTerm term) {
                String p = term.getPropertyName();
                if (p != null) {
                    if (PathUtils.getDepth(p) > 1) {
                        // remove indirection
                        String name = PathUtils.getName(p);
                        term = new FullTextTerm(name, term);
                    }
                }
                result.set(term);
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                ArrayList<FullTextExpression> list = new ArrayList<FullTextExpression>();
                for (FullTextExpression e : and.list) {
                    list.add(getFlatConstraint(e));
                }
                result.set(new FullTextAnd(list));
                return true;
            }

            @Override
            public boolean visit(FullTextOr or) {
                ArrayList<FullTextExpression> list = new ArrayList<FullTextExpression>();
                for (FullTextExpression e : or.list) {
                    list.add(getFlatConstraint(e));
                }
                result.set(new FullTextOr(list));
                return true;
            }
            
        });
        return result.get();
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

        /**
         * the current row
         */
        private IndexRow currentRow;

        /**
         * the path of the current item of the cursor
         */
        private String currentPath;

        /**
         * all of the item's known aggregates
         */
        private Iterator<String> aggregates;

        /**
         * should enforce uniqueness of the aggregated paths
         */
        private Set<String> seenPaths = new HashSet<String>();

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
                currentPath = aggregates.next();
                init = true;
                return;
            }
            aggregates = null;
            if (cursor.hasNext()) {
                currentRow = cursor.next();
                String path = currentRow.getPath();
                aggregates = Iterators.filter(Iterators.concat(
                        Iterators.singletonIterator(path),
                        aggregator.getParents(rootState, path)), Predicates
                        .not(Predicates.in(seenPaths)));
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
            seenPaths.add(currentPath);
            init = false;
            if (currentRow.getPath().equals(currentPath)) {
                return currentRow;
            }
            // create a new overlayed index row,
            // where the path is different but all other
            // properties are kept
            return new IndexRow() {

                @Override
                public String getPath() {
                    return currentPath;
                }

                @Override
                public PropertyValue getValue(String columnName) {
                    return currentRow.getValue(columnName);
                }
                
            };
        }
    }

}
