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


import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.query.fulltext.FullTextVisitor;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors;
import org.apache.jackrabbit.oak.spi.query.Cursors.AbstractCursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.FulltextQueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

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
        double localCost = Double.POSITIVE_INFINITY;
        FullTextExpression e = filter.getFullTextConstraint();
        if (e != null && hasCompositeExpression(e)) {
            localCost = flattenCost(e, filter, baseIndex, rootState);
        }
        double baseCost = baseIndex.getCost(filter, rootState);
        return Math.min(localCost, baseCost) - 0.05;
    }

    @Override
    public Cursor query(Filter filter, NodeState rootState) {
        // pass-through impl
        if (baseIndex.getNodeAggregator() == null) {
            return baseIndex.query(filter, rootState);
        }
        return newCursor(filter, baseIndex, rootState);
    }

    private static Cursor newCursor(Filter f, FulltextQueryIndex index,
            NodeState state) {
        FullTextExpression e = f.getFullTextConstraint();
        if (hasCompositeExpression(e)) {
            Cursor c = flatten(e, f, index, state);
            if (c != null) {
                return c;
            }
        }
        return new AggregationCursor(index.query(newAggregationFilter(f, null),
                state), index.getNodeAggregator(), state);
    }

    private static boolean hasCompositeExpression(FullTextExpression ft) {
        if (ft == null) {
            return false;
        }
        final AtomicReference<Boolean> composite = new AtomicReference<Boolean>();
        composite.set(false);

        ft.accept(new FullTextVisitor() {

            @Override
            public boolean visit(FullTextTerm term) {
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                composite.set(true);
                return true;
            }

            @Override
            public boolean visit(FullTextOr or) {
                composite.set(true);
                return true;
            }
        });
        return composite.get() && !hasNegativeContains(ft);
    }

    private static boolean hasNegativeContains(FullTextExpression ft) {
        if (ft == null) {
            return false;
        }
        final AtomicReference<Boolean> hasNegative = new AtomicReference<Boolean>();
        hasNegative.set(false);

        ft.accept(new FullTextVisitor.FullTextVisitorBase() {

            @Override
            public boolean visit(FullTextTerm term) {
                if (term.isNot()) {
                    hasNegative.set(true);
                }
                return true;
            }

        });
        return hasNegative.get();
    }

    private static Cursor flatten(FullTextExpression constraint,
            final Filter filter, final FulltextQueryIndex index,
            final NodeState state) {
        if (constraint == null) {
            return null;
        }
        final AtomicReference<Cursor> result = new AtomicReference<Cursor>();
        constraint.accept(new FullTextVisitor() {

            @Override
            public boolean visit(FullTextTerm term) {
                result.set(filterToCursor(newAggregationFilter(filter, term),
                        index, state));
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                Iterator<FullTextExpression> iterator = and.list.iterator();
                Cursor c = flatten(iterator.next(), filter, index, state);
                while (iterator.hasNext()) {
                    FullTextExpression input = iterator.next();
                    Cursor newC = flatten(input, filter, index, state);
                    c = Cursors.newIntersectionCursor(c, newC,
                            filter.getQueryEngineSettings());
                }
                result.set(c);
                return true;
            }

            @Override
            public boolean visit(FullTextOr or) {
                List<Cursor> cursors = Lists.transform(or.list,
                        new Function<FullTextExpression, Cursor>() {
                            @Override
                            public Cursor apply(FullTextExpression input) {
                                return flatten(input, filter, index, state);
                            }
                        });
                result.set(Cursors.newConcatCursor(cursors,
                        filter.getQueryEngineSettings()));
                return true;
            }
        });
        return result.get();
    }

    private static double flattenCost(FullTextExpression constraint,
            final Filter filter, final FulltextQueryIndex index,
            final NodeState state) {
        if (constraint == null) {
            return Double.POSITIVE_INFINITY;
        }
        final AtomicReference<Double> result = new AtomicReference<Double>();
        result.set(0d);
        constraint.accept(new FullTextVisitor() {

            @Override
            public boolean visit(FullTextTerm term) {
                result.set(result.get() + index.getCost(newAggregationFilter(filter, term), state));
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                for (FullTextExpression input : and.list) {
                    double d = flattenCost(input, filter, index, state);
                    result.set(result.get() + d);
                }
                return true;
            }

            @Override
            public boolean visit(FullTextOr or) {
                for (FullTextExpression input : or.list) {
                    double d = flattenCost(input, filter, index, state);
                    result.set(result.get() + d);
                }
                return true;
            }
        });
        return result.get();
    }

    private static Cursor filterToCursor(Filter f, FulltextQueryIndex index,
            NodeState state) {
        return new AggregationCursor(index.query(f, state),
                index.getNodeAggregator(), state);
    }

    private static Filter newAggregationFilter(Filter filter, FullTextExpression exp) {
        FilterImpl f = new FilterImpl(filter);
        // disables node type checks for now
        f.setMatchesAllTypes(true);
        if (exp != null) {
            f.setFullTextConstraint(exp);
        }
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
