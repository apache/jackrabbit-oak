/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Result.SizePrecision;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.QueryImpl.MeasuringIterator;
import org.apache.jackrabbit.oak.query.ast.ColumnImpl;
import org.apache.jackrabbit.oak.query.ast.OrderingImpl;
import org.apache.jackrabbit.oak.query.stats.QueryStatsData.QueryExecutionStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

/**
 * Represents a union query.
 */
public class UnionQueryImpl implements Query {
    
    private static final Logger LOG = LoggerFactory.getLogger(UnionQueryImpl.class);
    
    private final boolean unionAll;
    private final Query left, right;
    private ColumnImpl[] columns;
    private OrderingImpl[] orderings;
    private boolean explain;
    private boolean measure;
    private long limit = Long.MAX_VALUE;
    private long offset;
    private long size = -1;
    private final QueryEngineSettings settings;
    private boolean isInternal;
    
    UnionQueryImpl(final boolean unionAll, final Query left, final Query right,
                   final QueryEngineSettings settings) {
        this.unionAll = unionAll;
        this.left = left;
        this.right = right;
        this.settings = settings;
    }

    @Override
    public void setExecutionContext(ExecutionContext context) {
        left.setExecutionContext(context);
        right.setExecutionContext(context);
    }

    @Override
    public void setOrderings(OrderingImpl[] orderings) {
        if (orderings == null) {
            left.setOrderings(null);
            right.setOrderings(null);
            return;
        }
        OrderingImpl[] l = new OrderingImpl[orderings.length];
        OrderingImpl[] r = new OrderingImpl[orderings.length];
        for (int i = 0; i < orderings.length; i++) {
            OrderingImpl o = orderings[i];
            l[i] = o.createCopy();
            r[i] = o.createCopy();
        }
        left.setOrderings(l);
        right.setOrderings(r);
        this.orderings = orderings;
    }

    @Override
    public void setLimit(long limit) {
        this.limit = limit;
        applyLimitOffset();
    }

    @Override
    public void setOffset(long offset) {
        this.offset = offset;
        applyLimitOffset();
    }

    private void applyLimitOffset() {
        long subqueryLimit = QueryImpl.saturatedAdd(limit, offset);
        left.setLimit(subqueryLimit);
        right.setLimit(subqueryLimit);
    }

    @Override
    public void bindValue(String key, PropertyValue value) {
        left.bindValue(key, value);
        right.bindValue(key, value);
    }

    @Override
    public void setTraversalEnabled(boolean traversal) {
        left.setTraversalEnabled(traversal);
        right.setTraversalEnabled(traversal);
    }
    
    @Override
    public  void setQueryOptions(QueryOptions options) {
        left.setQueryOptions(options);
        right.setQueryOptions(options);
    }

    @Override
    public void prepare() {
        left.prepare();
        right.prepare();
    }
    
    @Override
    public double getEstimatedCost() {
        // the cost is higher than the cost of both parts, so that
        // non-union queries are preferred over union ones
        return 10 + left.getEstimatedCost() + right.getEstimatedCost();
    }

    @Override
    public List<String> getBindVariableNames() {
        HashSet<String> set = new HashSet<String>();
        set.addAll(left.getBindVariableNames());
        set.addAll(right.getBindVariableNames());
        return new ArrayList<String>(set);
    }

    @Override
    public ColumnImpl[] getColumns() {
        if (columns != null) {
            return columns;
        }
        return left.getColumns();
    }

    @Override
    public String[] getSelectorNames() {
        return left.getSelectorNames();
    }
    
    @Override
    public int getSelectorIndex(String selectorName) {
        return left.getSelectorIndex(selectorName);
    }

    @Override
    public long getSize() {
        return size;
    }
    
    @Override
    public long getSize(SizePrecision precision, long max) {
        // Note: for "unionAll == false", overlapping entries are counted twice
        // (this can result in a larger reported size, but it is not a security problem)
        
        // ensure the queries are both executed, otherwise the cursor is not set,
        // and so the size would be -1
        left.executeQuery().getRows().iterator().hasNext();
        right.executeQuery().getRows().iterator().hasNext();
        long a = left.getSize(precision, max);
        long b = right.getSize(precision, max);
        if (a < 0 || b < 0) {
            return -1;
        }
        long total = QueryImpl.saturatedAdd(a, b);
        return Math.min(limit, total);
    }
    
    @Override
    public void setExplain(boolean explain) {
        this.explain = explain;
    }

    @Override
    public void setMeasure(boolean measure) {
        left.setMeasure(measure);
        right.setMeasure(measure);
        this.measure = measure;
    }

    @Override
    public void init() {
        left.init();
        right.init();
    }
    
    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append(left.toString());
        buff.append(" union ");
        if (unionAll) {
            buff.append("all ");
        }
        buff.append(right.toString());
        if (orderings != null) {
            buff.append(" order by ");
            int i = 0;
            for (OrderingImpl o : orderings) {
                if (i++ > 0) {
                    buff.append(", ");
                }
                buff.append(o);
            }
        }
        return buff.toString();
    }
    
    @Override
    public Result executeQuery() {
        return new ResultImpl(this);
    }
    
    @Override
    public String getPlan() {
        StringBuilder buff = new StringBuilder();
        buff.append(left.getPlan());
        buff.append(" union ");
        if (unionAll) {
            buff.append("all ");
        }
        buff.append(right.getPlan());
        return buff.toString();
    }
    
    @Override
    public String getIndexCostInfo() {
        StringBuilder buff = new StringBuilder();
        buff.append("{ ");
        buff.append(left.getIndexCostInfo());
        buff.append(", ");
        buff.append(right.getIndexCostInfo());
        buff.append(" }");
        return buff.toString();
    }

    @Override
    public Tree getTree(String path) {
        return left.getTree(path);
    }

    @Override
    public boolean isMeasureOrExplainEnabled() {
        return explain || measure;
    }

    @Override
    public int getColumnIndex(String columnName) {
        if (columns == null) {
            columns = left.getColumns();
        }
        return QueryImpl.getColumnIndex(columns, columnName);
    }
    
    @Override
    public Iterator<ResultRowImpl> getRows() {
        prepare();
        if (explain) {
            String plan = getPlan();
            columns = new ColumnImpl[] { new ColumnImpl("explain", "plan", "plan")};
            ResultRowImpl r = new ResultRowImpl(this,
                    Tree.EMPTY_ARRAY,
                    new PropertyValue[] { PropertyValues.newString(plan)},
                    null, null);
            return Arrays.asList(r).iterator();
        }
        if (LOG.isDebugEnabled()) {
            if (isInternal) {
                LOG.trace("query union plan {}", getPlan());
            } else {
                LOG.debug("query union plan {}", getPlan());
            }
        }
        boolean distinct = !unionAll;
        Comparator<ResultRowImpl> orderBy = ResultRowImpl.getComparator(orderings);

        Iterator<ResultRowImpl> it;
        final Iterator<ResultRowImpl> leftRows = left.getRows();
        final Iterator<ResultRowImpl> rightRows = right.getRows();
        Iterator<ResultRowImpl> leftIter = leftRows;
        Iterator<ResultRowImpl> rightIter = rightRows;

        // if measure retrieve the backing delegate iterator instead
        if (measure) {
            leftIter = ((MeasuringIterator) leftRows).getDelegate();
            rightIter = ((MeasuringIterator) rightRows).getDelegate();
        }
        // Since sorted by index use a merge iterator
        if (isSortedByIndex()) {
            it = FilterIterators
                .newCombinedFilter(Iterators.mergeSorted(ImmutableList.of(leftIter, rightIter), orderBy), distinct,
                    limit, offset, null, settings);
        } else {
            it = FilterIterators
            .newCombinedFilter(Iterators.concat(leftIter, rightIter), distinct, limit, offset, orderBy, settings);
        }

        if (measure) {
            // return the measuring iterator for the union
            it = new MeasuringIterator(this, it) {
                MeasuringIterator left = (MeasuringIterator) leftRows;
                MeasuringIterator right = (MeasuringIterator) rightRows;

                @Override
                protected void setColumns(ColumnImpl[] cols) {
                    columns = cols;
                    left.setColumns(cols);
                    right.setColumns(cols);
                }

                @Override
                protected Map<String, Long> getSelectorScanCount() {
                    // Merge the 2 maps from the left and right queries to get the selector counts
                    Map<String, Long> leftSelectorScan = left.getSelectorScanCount();
                    Map<String, Long> rightSelectorScan = right.getSelectorScanCount();
                    Map<String, Long> unionScan = Maps.newHashMap(leftSelectorScan);
                    for (String key : rightSelectorScan.keySet()) {
                        if (unionScan.containsKey(key)) {
                            unionScan.put(key, rightSelectorScan.get(key) + unionScan.get(key));
                        } else {
                            unionScan.put(key, rightSelectorScan.get(key));
                        }
                    }
                    return unionScan;
                }

                @Override
                protected long getReadCount() {
                    return left.getReadCount() + right.getReadCount();
                }
            };
        }

        return it;     
    }

    @Override
    public void setInternal(boolean isInternal) {
        this.isInternal = isInternal;
    }

    @Override
    public boolean isSortedByIndex() {
        return left.isSortedByIndex() && right.isSortedByIndex();
    }

    @Override
    public Query buildAlternativeQuery() {
        return this;
    }

    @Override
    public Query copyOf() throws IllegalStateException {
        return null;
    }

    @Override
    public boolean isInit() {
        return left.isInit() || right.isInit();
    }

    @Override
    public String getStatement() {
        return toString();
    }

    @Override
    public boolean isInternal() {
        return left.isInternal() || right.isInternal();
    }

    @Override
    public boolean containsUnfilteredFullTextCondition() {
        return left.containsUnfilteredFullTextCondition() || 
                right.containsUnfilteredFullTextCondition();
    }

    @Override
    public boolean isPotentiallySlow() {
        return left.isPotentiallySlow() || 
                right.isPotentiallySlow();
    }

    @Override
    public void verifyNotPotentiallySlow() {
        left.verifyNotPotentiallySlow();
        right.verifyNotPotentiallySlow();
    }
    
    public QueryExecutionStats getQueryExecutionStats() {
        return left.getQueryExecutionStats();
    }

}
