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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.Result.SizePrecision;
import org.apache.jackrabbit.oak.namepath.JcrPathParser;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.query.ast.AndImpl;
import org.apache.jackrabbit.oak.query.ast.AstVisitorBase;
import org.apache.jackrabbit.oak.query.ast.BindVariableValueImpl;
import org.apache.jackrabbit.oak.query.ast.ChildNodeImpl;
import org.apache.jackrabbit.oak.query.ast.ChildNodeJoinConditionImpl;
import org.apache.jackrabbit.oak.query.ast.ColumnImpl;
import org.apache.jackrabbit.oak.query.ast.ComparisonImpl;
import org.apache.jackrabbit.oak.query.ast.ConstraintImpl;
import org.apache.jackrabbit.oak.query.ast.DescendantNodeImpl;
import org.apache.jackrabbit.oak.query.ast.DescendantNodeJoinConditionImpl;
import org.apache.jackrabbit.oak.query.ast.DynamicOperandImpl;
import org.apache.jackrabbit.oak.query.ast.EquiJoinConditionImpl;
import org.apache.jackrabbit.oak.query.ast.FullTextSearchImpl;
import org.apache.jackrabbit.oak.query.ast.FullTextSearchScoreImpl;
import org.apache.jackrabbit.oak.query.ast.InImpl;
import org.apache.jackrabbit.oak.query.ast.JoinConditionImpl;
import org.apache.jackrabbit.oak.query.ast.JoinImpl;
import org.apache.jackrabbit.oak.query.ast.JoinType;
import org.apache.jackrabbit.oak.query.ast.LengthImpl;
import org.apache.jackrabbit.oak.query.ast.LiteralImpl;
import org.apache.jackrabbit.oak.query.ast.LowerCaseImpl;
import org.apache.jackrabbit.oak.query.ast.NativeFunctionImpl;
import org.apache.jackrabbit.oak.query.ast.NodeLocalNameImpl;
import org.apache.jackrabbit.oak.query.ast.NodeNameImpl;
import org.apache.jackrabbit.oak.query.ast.NotImpl;
import org.apache.jackrabbit.oak.query.ast.OrImpl;
import org.apache.jackrabbit.oak.query.ast.OrderingImpl;
import org.apache.jackrabbit.oak.query.ast.PropertyExistenceImpl;
import org.apache.jackrabbit.oak.query.ast.PropertyInexistenceImpl;
import org.apache.jackrabbit.oak.query.ast.PropertyValueImpl;
import org.apache.jackrabbit.oak.query.ast.SameNodeImpl;
import org.apache.jackrabbit.oak.query.ast.SameNodeJoinConditionImpl;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.ast.SimilarImpl;
import org.apache.jackrabbit.oak.query.ast.SourceImpl;
import org.apache.jackrabbit.oak.query.ast.SpellcheckImpl;
import org.apache.jackrabbit.oak.query.ast.SuggestImpl;
import org.apache.jackrabbit.oak.query.ast.UpperCaseImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.query.index.TraversingIndex;
import org.apache.jackrabbit.oak.query.plan.ExecutionPlan;
import org.apache.jackrabbit.oak.query.plan.SelectorExecutionPlan;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvancedQueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry.Order;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a parsed query.
 */
public class QueryImpl implements Query {
    
    /**
     * The "jcr:path" pseudo-property.
     */
    // TODO jcr:path isn't an official feature, support it?
    public static final String JCR_PATH = "jcr:path";

    /**
     * The "jcr:score" pseudo-property.
     */
    public static final String JCR_SCORE = "jcr:score";

    /**
     * The "rep:excerpt" pseudo-property.
     */
    public static final String REP_EXCERPT = "rep:excerpt";

    /**
     * The "rep:spellcheck" pseudo-property.
     */
    public static final String REP_SPELLCHECK = "rep:spellcheck()";

    /**
     * The "rep:suggest" pseudo-property.
     */
    public static final String REP_SUGGEST = "rep:suggest()";

    private static final Logger LOG = LoggerFactory.getLogger(QueryImpl.class);

    SourceImpl source;
    final String statement;
    final HashMap<String, PropertyValue> bindVariableMap = new HashMap<String, PropertyValue>();
    final HashMap<String, Integer> selectorIndexes = new HashMap<String, Integer>();
    final ArrayList<SelectorImpl> selectors = new ArrayList<SelectorImpl>();
    ConstraintImpl constraint;

    /**
     * Whether fallback to the traversing index is supported if no other index
     * is available. This is enabled by default and can be disabled for testing
     * purposes.
     */
    private boolean traversalEnabled = true;

    private OrderingImpl[] orderings;
    private ColumnImpl[] columns;
    
    /**
     * The columns that make a row distinct. This is all columns
     * except for "jcr:score".
     */
    private boolean[] distinctColumns;
    
    private boolean explain, measure;
    private boolean distinct;
    private long limit = Long.MAX_VALUE;
    private long offset;
    private long size = -1;
    private boolean prepared;
    private ExecutionContext context;

    private final NamePathMapper namePathMapper;
    
    private double estimatedCost;

    private final QueryEngineSettings settings;

    private boolean warnedHidden;

    QueryImpl(String statement, SourceImpl source, ConstraintImpl constraint,
            ColumnImpl[] columns, NamePathMapper mapper, QueryEngineSettings settings) {
        this.statement = statement;
        this.source = source;
        this.constraint = constraint;
        this.columns = columns;
        this.namePathMapper = mapper;
        this.settings = settings;
    }

    @Override
    public void init() {

        final QueryImpl query = this;

        new AstVisitorBase() {

            @Override
            public boolean visit(BindVariableValueImpl node) {
                node.setQuery(query);
                bindVariableMap.put(node.getBindVariableName(), null);
                return true;
            }
            
             @Override
            public boolean visit(ChildNodeImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(ChildNodeJoinConditionImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(ColumnImpl node) {
                node.setQuery(query);
                return true;
            }

            @Override
            public boolean visit(DescendantNodeImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(DescendantNodeJoinConditionImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(EquiJoinConditionImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(FullTextSearchImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return super.visit(node);
            }

            @Override
            public boolean visit(NativeFunctionImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return super.visit(node);
            }
            
            @Override
            public boolean visit(SimilarImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return super.visit(node);
            }
            
            @Override
            public boolean visit(SpellcheckImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return super.visit(node);
            }

            @Override
            public boolean visit(SuggestImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return super.visit(node);
            }

            @Override
            public boolean visit(FullTextSearchScoreImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(LiteralImpl node) {
                node.setQuery(query);
                return true;
            }

            @Override
            public boolean visit(NodeLocalNameImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(NodeNameImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(PropertyExistenceImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }
            
            @Override
            public boolean visit(PropertyInexistenceImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(PropertyValueImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(SameNodeImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(SameNodeJoinConditionImpl node) {
                node.setQuery(query);
                node.bindSelector(source);
                return true;
            }

            @Override
            public boolean visit(SelectorImpl node) {
                String name = node.getSelectorName();
                if (selectorIndexes.put(name, selectors.size()) != null) {
                    throw new IllegalArgumentException("Two selectors with the same name: " + name);
                }
                selectors.add(node);
                node.setQuery(query);
                return true;
            }

            @Override
            public boolean visit(LengthImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }

            @Override
            public boolean visit(UpperCaseImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }

            @Override
            public boolean visit(LowerCaseImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }

            @Override
            public boolean visit(ComparisonImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }

            @Override
            public boolean visit(InImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }
            
            @Override
            public boolean visit(AndImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }

            @Override
            public boolean visit(OrImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }

            @Override
            public boolean visit(NotImpl node) {
                node.setQuery(query);
                return super.visit(node);
            }

        }.visit(this);
        if (constraint != null) {
            constraint = constraint.simplify();
        }
        source.setQueryConstraint(constraint);
        for (ColumnImpl column : columns) {
            column.bindSelector(source);
        }
        distinctColumns = new boolean[columns.length];
        for (int i = 0; i < columns.length; i++) {
            ColumnImpl c = columns[i];
            boolean distinct = true;
            if (JCR_SCORE.equals(c.getPropertyName())) {
                distinct = false;
            }
            distinctColumns[i] = distinct;
        }
    }

    @Override
    public ColumnImpl[] getColumns() {
        return columns;
    }

    public ConstraintImpl getConstraint() {
        return constraint;
    }

    public OrderingImpl[] getOrderings() {
        return orderings;
    }

    public SourceImpl getSource() {
        return source;
    }

    @Override
    public void bindValue(String varName, PropertyValue value) {
        bindVariableMap.put(varName, value);
    }

    @Override
    public void setLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public void setExplain(boolean explain) {
        this.explain = explain;
    }

    @Override
    public void setMeasure(boolean measure) {
        this.measure = measure;
    }
    
    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    @Override
    public ResultImpl executeQuery() {
        return new ResultImpl(this);
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
            LOG.debug("query execute {} ", statement);
            LOG.debug("query plan {}", getPlan());
        }
        RowIterator rowIt = new RowIterator(context.getBaseState());
        Comparator<ResultRowImpl> orderBy;
        boolean sortUsingIndex = false;
        // TODO add issue about order by optimization for multiple selectors
        if (orderings != null && selectors.size() == 1) {
            IndexPlan plan = selectors.get(0).getExecutionPlan().getIndexPlan();
            if (plan != null) {
                List<OrderEntry> list = plan.getSortOrder();
                if (list != null && list.size() == orderings.length) {
                    sortUsingIndex = true;
                    for (int i = 0; i < list.size(); i++) {
                        OrderEntry e = list.get(i);
                        OrderingImpl o = orderings[i];
                        DynamicOperandImpl op = o.getOperand();
                        if (!(op instanceof PropertyValueImpl)) {
                            // ordered by a function: currently not supported
                            sortUsingIndex = false;
                            break;
                        }
                        // we only have one selector, so no need to check that
                        // TODO support joins
                        String pn = ((PropertyValueImpl) op).getPropertyName();
                        if (!pn.equals(e.getPropertyName())) {
                            // ordered by another property
                            sortUsingIndex = false;
                            break;
                        }
                        if (o.isDescending() != (e.getOrder() == Order.DESCENDING)) {
                            // ordered ascending versus descending
                            sortUsingIndex = false;
                            break;
                        }
                    }
                }
            }
        }
        if (sortUsingIndex) {
            orderBy = null;
        } else {
            orderBy = ResultRowImpl.getComparator(orderings);
        }
        Iterator<ResultRowImpl> it = 
                FilterIterators.newCombinedFilter(rowIt, distinct, limit, offset, orderBy, settings);
        if (measure) {
            // run the query
            while (it.hasNext()) {
                it.next();
            }
            columns = new ColumnImpl[] {
                    new ColumnImpl("measure", "selector", "selector"),
                    new ColumnImpl("measure", "scanCount", "scanCount")
            };
            ArrayList<ResultRowImpl> list = new ArrayList<ResultRowImpl>();
            ResultRowImpl r = new ResultRowImpl(this,
                    Tree.EMPTY_ARRAY,
                    new PropertyValue[] {
                            PropertyValues.newString("query"),
                            PropertyValues.newLong(rowIt.getReadCount())
                        },
                    null, null);
            list.add(r);
            for (SelectorImpl selector : selectors) {
                r = new ResultRowImpl(this,
                        Tree.EMPTY_ARRAY,
                        new PropertyValue[] {
                                PropertyValues.newString(selector.getSelectorName()),
                                PropertyValues.newLong(selector.getScanCount()),
                            },
                        null, null);
                list.add(r);
            }
            it = list.iterator();
        }
        return it;
    }
    
    @Override
    public String getPlan() {
        return source.getPlan(context.getBaseState());
    }
    
    @Override
    public double getEstimatedCost() {
        return estimatedCost;
    }

    @Override
    public void prepare() {
        if (prepared) {
            return;
        }
        prepared = true;
        List<SourceImpl> sources = source.getInnerJoinSelectors();
        List<JoinConditionImpl> conditions = source.getInnerJoinConditions();

        if (sources.size() <= 1) {
            // simple case (no join)
            estimatedCost = source.prepare().getEstimatedCost();
            return;
        }

        // use a greedy algorithm
        SourceImpl result = null;
        Set<SourceImpl> available = new HashSet<SourceImpl>();
        while (sources.size() > 0) {
            int bestIndex = 0;
            double bestCost = Double.POSITIVE_INFINITY;
            ExecutionPlan bestPlan = null;
            SourceImpl best = null;
            for (int i = 0; i < sources.size(); i++) {
                SourceImpl test = buildJoin(result, sources.get(i), conditions);
                if (test == null) {
                    // no join condition
                    continue;
                }
                ExecutionPlan testPlan = test.prepare();
                double cost = testPlan.getEstimatedCost();
                if (best == null || cost < bestCost) {
                    bestPlan = testPlan;
                    bestCost = cost;
                    bestIndex = i;
                    best = test;
                }
                test.unprepare();
            }
            available.add(sources.remove(bestIndex));
            result = best;
            best.prepare(bestPlan);
        }
        estimatedCost = result.prepare().getEstimatedCost();
        source = result;
                
    }
    
    private static SourceImpl buildJoin(SourceImpl result, SourceImpl last, List<JoinConditionImpl> conditions) {
        if (result == null) {
            return last;
        }
        List<SourceImpl> selectors = result.getInnerJoinSelectors();
        Set<SourceImpl> oldSelectors = new HashSet<SourceImpl>();
        oldSelectors.addAll(selectors);
        Set<SourceImpl> newSelectors = new HashSet<SourceImpl>();
        newSelectors.addAll(selectors);
        newSelectors.add(last);
        for (JoinConditionImpl j : conditions) {
            // only join conditions can now be evaluated,
            // but couldn't be evaluated before
            if (!j.canEvaluate(oldSelectors) && j.canEvaluate(newSelectors)) {
                JoinImpl join = new JoinImpl(result, last, JoinType.INNER, j);
                return join;
            }
        }
        // no join condition was found
        return null;
    }
 
    /**
     * <b>!Test purpose only! <b>
     * 
     * this creates a filter for the given query
     * 
     */
    Filter createFilter(boolean preparing) {
        return source.createFilter(preparing);
    }

    /**
     * An iterator over result rows.
     */
    class RowIterator implements Iterator<ResultRowImpl> {

        private final NodeState rootState;
        private ResultRowImpl current;
        private boolean started, end;
        private long rowIndex;

        RowIterator(NodeState rootState) {
            this.rootState = rootState;
        }

        public long getReadCount() {
            return rowIndex;
        }

        private void fetchNext() {
            if (end) {
                return;
            }
            if (!started) {
                source.execute(rootState);
                started = true;
            }
            while (true) {
                if (source.next()) {
                    if (constraint == null || constraint.evaluate()) {
                        current = currentRow();
                        rowIndex++;
                        break;
                    }
                } else {
                    current = null;
                    end = true;
                    break;
                }
            }
        }

        @Override
        public boolean hasNext() {
            if (end) {
                return false;
            }
            if (current == null) {
                fetchNext();
            }
            return !end;
        }

        @Override
        public ResultRowImpl next() {
            if (end) {
                return null;
            }
            if (current == null) {
                fetchNext();
            }
            ResultRowImpl r = current;
            current = null;
            return r;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    ResultRowImpl currentRow() {
        int selectorCount = selectors.size();
        Tree[] trees = new Tree[selectorCount];
        for (int i = 0; i < selectorCount; i++) {
            SelectorImpl s = selectors.get(i);
            trees[i] = s.currentTree();
        }
        int columnCount = columns.length;
        PropertyValue[] values = new PropertyValue[columnCount];
        for (int i = 0; i < columnCount; i++) {
            ColumnImpl c = columns[i];
            values[i] = c.currentProperty();
        }
        PropertyValue[] orderValues;
        if (orderings == null) {
            orderValues = null;
        } else {
            int size = orderings.length;
            orderValues = new PropertyValue[size];
            for (int i = 0; i < size; i++) {
                orderValues[i] = orderings[i].getOperand().currentProperty();
            }
        }
        return new ResultRowImpl(this, trees, values, distinctColumns, orderValues);
    }

    @Override
    public int getSelectorIndex(String selectorName) {
        Integer index = selectorIndexes.get(selectorName);
        if (index == null) {
            throw new IllegalArgumentException("Unknown selector: " + selectorName);
        }
        return index;
    }

    @Override
    public int getColumnIndex(String columnName) {
        return getColumnIndex(columns, columnName);
    }
    
    static int getColumnIndex(ColumnImpl[] columns, String columnName) {
        for (int i = 0, size = columns.length; i < size; i++) {
            ColumnImpl c = columns[i];
            String cn = c.getColumnName();
            if (cn != null && cn.equals(columnName)) {
                return i;
            }
        }
        return -1;
    }

    public PropertyValue getBindVariableValue(String bindVariableName) {
        PropertyValue v = bindVariableMap.get(bindVariableName);
        if (v == null) {
            throw new IllegalArgumentException("Bind variable value not set: " + bindVariableName);
        }
        return v;
    }

    @Override
    public String[] getSelectorNames() {
        String[] list = new String[selectors.size()];
        for (int i = 0; i < list.length; i++) {
            list[i] = selectors.get(i).getSelectorName();
        }
        return list;
    }

    @Override
    public List<String> getBindVariableNames() {
        return new ArrayList<String>(bindVariableMap.keySet());
    }

    @Override
    public void setTraversalEnabled(boolean traversalEnabled) {
        this.traversalEnabled = traversalEnabled;
    }

    public SelectorExecutionPlan getBestSelectorExecutionPlan(FilterImpl filter) {
        return getBestSelectorExecutionPlan(context.getBaseState(), filter,
                context.getIndexProvider(), traversalEnabled);
    }

    private SelectorExecutionPlan getBestSelectorExecutionPlan(
            NodeState rootState, FilterImpl filter,
            QueryIndexProvider indexProvider, boolean traversalEnabled) {
        QueryIndex bestIndex = null;
        if (LOG.isDebugEnabled()) {
            LOG.debug("cost using filter " + filter);
        }

        double bestCost = Double.POSITIVE_INFINITY;
        IndexPlan bestPlan = null;
        for (QueryIndex index : indexProvider.getQueryIndexes(rootState)) {
            double cost;
            IndexPlan indexPlan = null;
            if (index instanceof AdvancedQueryIndex) {
                AdvancedQueryIndex advIndex = (AdvancedQueryIndex) index;
                List<OrderEntry> sortOrder = null;
                if (orderings != null) {
                    sortOrder = new ArrayList<OrderEntry>();
                    for (OrderingImpl o : orderings) {
                        DynamicOperandImpl op = o.getOperand();
                        if (!(op instanceof PropertyValueImpl)) {
                            // ordered by a function: currently not supported
                            break;
                        }
                        PropertyValueImpl p = (PropertyValueImpl) op;
                        SelectorImpl s = p.getSelectors().iterator().next();
                        if (!s.equals(filter.getSelector())) {
                            // ordered by a different selector
                            continue;
                        }
                        OrderEntry e = new OrderEntry(
                                p.getPropertyName(), 
                                Type.UNDEFINED, 
                                o.isDescending() ? 
                                OrderEntry.Order.DESCENDING : OrderEntry.Order.ASCENDING);
                        sortOrder.add(e);
                    }
                    if (sortOrder.size() == 0) {
                        sortOrder = null;
                    }
                }
                long maxEntryCount = limit;
                if (offset > 0) {
                    if (offset + limit < 0) {
                        // long overflow
                        maxEntryCount = Long.MAX_VALUE;
                    } else {
                        maxEntryCount = offset + limit;
                    }
                }
                List<IndexPlan> ipList = advIndex.getPlans(
                        filter, sortOrder, rootState);
                cost = Double.POSITIVE_INFINITY;
                for (IndexPlan p : ipList) {
                    // TODO limit is after all conditions
                    long entryCount = Math.min(maxEntryCount, p.getEstimatedEntryCount());
                    double c = p.getCostPerExecution() + entryCount * p.getCostPerEntry();
                    if (c < cost) {
                        cost = c;
                        indexPlan = p;
                    }
                }
            } else {
                cost = index.getCost(filter, rootState);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("cost for " + index.getIndexName() + " is " + cost);
            }
            if (cost < 0) {
                LOG.error("cost below 0 for " + index.getIndexName() + " is " + cost);
            }
            if (cost < bestCost) {
                bestCost = cost;
                bestIndex = index;
                bestPlan = indexPlan;
            }
        }

        if (traversalEnabled) {
            QueryIndex traversal = new TraversingIndex();
            double cost = traversal.getCost(filter, rootState);
            if (LOG.isDebugEnabled()) {
                LOG.debug("cost for " + traversal.getIndexName() + " is " + cost);
            }
            if (cost < bestCost || bestCost == Double.POSITIVE_INFINITY) {
                bestCost = cost;
                bestPlan = null;
                bestIndex = traversal;
            }
        }
        return new SelectorExecutionPlan(filter.getSelector(), bestIndex, bestPlan, bestCost);
    }

    @Override
    public void setExecutionContext(ExecutionContext context) {
        this.context = context;
    }

    @Override
    public void setOrderings(OrderingImpl[] orderings) {
        this.orderings = orderings;
    }

    public NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    @Override
    public Tree getTree(String path) {
        if (NodeStateUtils.isHiddenPath(path)) {
            if (!warnedHidden) {
                warnedHidden = true;
                LOG.warn("Hidden tree traversed: {}", path);
            }
            return null;
        }
        return context.getRoot().getTree(path);
    }

    @Override
    public boolean isMeasureOrExplainEnabled() {
        return explain || measure;
    }

    /**
     * Validate the path is syntactically correct, and convert it to an Oak
     * internal path (including namespace remapping if needed).
     * 
     * @param path the path
     * @return the the converted path
     */
    public String getOakPath(String path) {
        if (path == null) {
            return null;
        }
        if (!JcrPathParser.validate(path)) {
            throw new IllegalArgumentException("Invalid path: " + path);
        }
        String p = namePathMapper.getOakPath(path);
        if (p == null) {
            throw new IllegalArgumentException("Invalid path or namespace prefix: " + path);
        }
        return p;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("select ");
        int i = 0;
        for (ColumnImpl c : columns) {
            if (i++ > 0) {
                buff.append(", ");
            }
            buff.append(c);
        }
        buff.append(" from ").append(source);
        if (constraint != null) {
            buff.append(" where ").append(constraint);
        }
        if (orderings != null) {
            buff.append(" order by ");
            i = 0;
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
    public long getSize() {
        return size;
    }
    
    @Override
    public long getSize(SizePrecision precision, long max) {
        // Note: DISTINCT is ignored
        return Math.min(limit, source.getSize(precision, max));
    }

    public String getStatement() {
        return statement;
    }

    public QueryEngineSettings getSettings() {
        return settings;
    }

    public ExecutionContext getExecutionContext() {
        return context;
    }
    
    /**
     * Add two values, but don't let it overflow or underflow.
     * 
     * @param x the first value
     * @param y the second value
     * @return the sum, or Long.MIN_VALUE for underflow, or Long.MAX_VALUE for
     *         overflow
     */
    public static long saturatedAdd(long x, long y) {
        BigInteger min = BigInteger.valueOf(Long.MIN_VALUE);
        BigInteger max = BigInteger.valueOf(Long.MAX_VALUE);
        BigInteger sum = BigInteger.valueOf(x).add(BigInteger.valueOf(y));
        return sum.min(max).max(min).longValue();
    }

}
