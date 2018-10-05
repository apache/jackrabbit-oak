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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextContains;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextVisitor;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.plugins.index.Cursors;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import static org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvanceFulltextQueryIndex;

/**
 * A virtual full-text that can aggregate nodes based on aggregate definitions.
 * Internally, it uses another full-text index.
 */
public class AggregateIndex implements AdvanceFulltextQueryIndex {

    private static final Logger LOG = LoggerFactory.getLogger(AggregateIndex.class);    

    private final AdvanceFulltextQueryIndex baseIndex;

    public AggregateIndex(AdvanceFulltextQueryIndex baseIndex) {
        this.baseIndex = baseIndex;
    }

    @Override
    public double getMinimumCost() {
        return baseIndex.getMinimumCost();
    }

    @Override
    public double getCost(Filter filter, NodeState rootState) {
        throw new UnsupportedOperationException("Not supported as implementing AdvancedQueryIndex");
    }

    @Override
    public Cursor query(Filter filter, NodeState rootState) {
        throw new UnsupportedOperationException("Not supported as implementing AdvancedQueryIndex");
    }

    @Override
    public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
        if (baseIndex == null) {
            return Collections.emptyList();
        }
        FullTextExpression e = filter.getFullTextConstraint();
        if (getNodeAggregator() == null || e == null) {
            // no aggregation: path-though
            return baseIndex.getPlans(filter, sortOrder, rootState);
        }
        if (!hasCompositeExpression(e)) {
            // path-though, but without node type restriction
            return baseIndex.getPlans(newAggregationFilter(filter, null), sortOrder, rootState);
        }
        AggregateIndexPlan plan = new AggregateIndexPlan(filter);
        collectCombinedPlan(e, filter, sortOrder, rootState, plan, "");
        if (plan.containsPathWithoutPlan()) {
            // the full-text index didn't return a plan
            LOG.debug("Full-text index without plan: " + e);
            return Collections.emptyList();
        }
        return Collections.singletonList((IndexPlan) plan);
    }

    @Override
    public Cursor query(IndexPlan plan, NodeState rootState) {
        if (getNodeAggregator() == null) {
            // path-though
            return baseIndex.query(plan, rootState);
        } else if (!(plan instanceof AggregateIndexPlan)) {
            // remove the node type restriction
            plan.setFilter(newAggregationFilter(plan.getFilter(), null));
            return newAggregationCursor(plan, rootState);
        }
        Filter filter = plan.getFilter();
        AggregateIndexPlan aggPlan = (AggregateIndexPlan) plan;
        FullTextExpression constraint = filter.getFullTextConstraint();
        return flatten(constraint, aggPlan, filter, rootState, "");
    }

    private static boolean hasCompositeExpression(FullTextExpression ft) {
        if (ft == null) {
            return false;
        }
        final AtomicReference<Boolean> composite = new AtomicReference<Boolean>();
        composite.set(false);

        ft.accept(new FullTextVisitor() {
            
            @Override
            public boolean visit(FullTextContains contains) {
                return contains.getBase().accept(this);
            }

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

    private Cursor flatten(FullTextExpression constraint,
            final AggregateIndexPlan plan,
            final Filter filter, final NodeState state,
            final String path) {
        if (constraint == null) {
            return null;
        }
        final AtomicReference<Cursor> result = new AtomicReference<Cursor>();
        constraint.accept(new FullTextVisitor() {
            
            @Override
            public boolean visit(FullTextContains contains) {
                return contains.getBase().accept(this);
            }

            @Override
            public boolean visit(FullTextTerm term) {
                IndexPlan p = plan.getPlan(path);
                result.set(newAggregationCursor(p, state));
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                Iterator<FullTextExpression> iterator = and.list.iterator();
                int index = 0;
                Cursor c = flatten(iterator.next(), plan, filter, state,
                        path + " and(" + index + ")");
                while (iterator.hasNext()) {
                    index++;
                    FullTextExpression input = iterator.next();
                    Cursor newC = flatten(input, plan, filter, state,
                            path + " and(" + index + ")");
                    c = Cursors.newIntersectionCursor(c, newC,
                            filter.getQueryLimits());
                }
                result.set(c);
                return true;
            }

            @Override
            public boolean visit(FullTextOr or) {
                final int[] index = new int[1];
                List<Cursor> cursors = Lists.transform(or.list,
                        new Function<FullTextExpression, Cursor>() {
                            @Override
                            public Cursor apply(FullTextExpression input) {
                                return flatten(input, plan, filter, state,
                                        path + " or(" + index[0]++ + ")");
                            }
                        });
                result.set(Cursors.newConcatCursor(cursors,
                        filter.getQueryLimits()));
                return true;
            }
        });
        return result.get();
    }

    private void collectCombinedPlan(FullTextExpression constraint,
            final Filter filter,
            final List<OrderEntry> sortOrder, 
            final NodeState state,
            final AggregateIndexPlan target,
            final String path) {
        
        constraint.accept(new FullTextVisitor() {
            
            @Override
            public boolean visit(FullTextContains contains) {
                return contains.getBase().accept(this);
            }

            @Override
            public boolean visit(FullTextTerm term) {
                List<IndexPlan> list = baseIndex.getPlans(
                        newAggregationFilter(filter, term), sortOrder, state);
                target.setPlan(path, list);
                return true;
            }

            @Override
            public boolean visit(FullTextAnd and) {
                int index = 0;
                for (FullTextExpression input : and.list) {
                    collectCombinedPlan(input, filter, sortOrder, state, target, 
                            path + " and(" + index + ")");
                    index++;
                }
                return true;
            }

            @Override
            public boolean visit(FullTextOr or) {
                int index = 0;
                for (FullTextExpression input : or.list) {
                    collectCombinedPlan(input, filter, sortOrder, state, target, 
                            path + " or(" + index + ")");
                    index++;
                }
                return true;
            }
        });
    }

    private Cursor newAggregationCursor(IndexPlan plan, NodeState rootState) {
        Cursor c = baseIndex.query(plan, rootState);
        // we possibly get results from a child,
        // so we need to wrap the cursor to do aggregation
        return new AggregationCursor(c,
                getNodeAggregator(), rootState);         
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
        throw new UnsupportedOperationException("Not supported as implementing AdvancedQueryIndex");
    }

    @Override
    public String getPlanDescription(IndexPlan plan, NodeState root) {
        if (baseIndex == null) {
            return "aggregate no-index";
        }
        StringBuilder buff = new StringBuilder("aggregate ");
        if (getNodeAggregator() == null) {
            // path-though
            buff.append(baseIndex.getPlanDescription(plan, root));
            return buff.toString();
        } else if (!(plan instanceof AggregateIndexPlan)) {
            buff.append(baseIndex.getPlanDescription(plan, root));
            return buff.toString();
        }
        AggregateIndexPlan aggPlan = (AggregateIndexPlan) plan;
        for (IndexPlan p : aggPlan.getPlans()) {
            if (p != null) {
                buff.append(baseIndex.getPlanDescription(p, root));
                buff.append(" ");
            }
        }
        return buff.toString();
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

}
