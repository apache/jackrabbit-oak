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
package org.apache.jackrabbit.oak.query.ast;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result.SizePrecision;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.LazyValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.plugins.metric.util.StatsProviderUtil;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.query.ExecutionContext;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.query.QueryOptions;
import org.apache.jackrabbit.oak.query.RuntimeNodeTraversalException;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.query.plan.ExecutionPlan;
import org.apache.jackrabbit.oak.query.plan.SelectorExecutionPlan;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.cursor.Cursors;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvancedQueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.PrefetchNodeStore;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jackrabbit.guava.common.collect.Iterables;

/**
 * A selector within a query.
 */
public class SelectorImpl extends SourceImpl {

    private static final Logger LOG = LoggerFactory.getLogger(SelectorImpl.class);

    private static final Boolean TIMER_DISABLED = Boolean.getBoolean("oak.query.timerDisabled");

    // The sample rate. Must be a power of 2.
    private static final Long TIMER_SAMPLE_RATE = Long.getLong("oak.query.timerSampleRate", 0x100);

    private static final long SLOW_QUERY_HISTOGRAM = 1;
    private static final long TOTAL_QUERY_HISTOGRAM = 0;
    private static final String SLOW_QUERY_PERCENTILE_METRICS_NAME = "SLOW_QUERY_PERCENTILE_METRICS";
    private static final String SLOW_QUERY_COUNT_NAME = "SLOW_QUERY_COUNT";

    private static long timerSampleCounter;


    // TODO possibly support using multiple indexes (using index intersection / index merge)
    private SelectorExecutionPlan plan;

    /**
     * The WHERE clause of the query.
     */
    private ConstraintImpl queryConstraint;

    /**
     * The join condition of this selector that can be evaluated at execution
     * time. For the query "select * from nt:base as a inner join nt:base as b
     * on a.x = b.x", the join condition "a.x = b.x" is only set for the
     * selector b, as selector a can't evaluate it if it is executed first
     * (until b is executed).
     */
    private JoinConditionImpl joinCondition;

    /**
     * The node type associated with the {@link #nodeTypeName}
     */
    private final NodeTypeInfo nodeTypeInfo;

    private final String selectorName;

    private final String nodeTypeName;

    private final boolean matchesAllTypes;

    /**
     * All of the matching supertypes, or empty if the {@link #matchesAllTypes}
     * flag is set
     */
    private final Set<String> supertypes;

    /**
     * All of the matching primary subtypes, or empty if the
     * {@link #matchesAllTypes} flag is set
     */
    private final Set<String> primaryTypes;

    /**
     * All of the matching mixin types, or empty if the {@link #matchesAllTypes}
     * flag is set
     */
    private final Set<String> mixinTypes;

    /**
     * Whether this selector is the parent of a descendent or parent-child join.
     * Access rights don't need to be checked in such selectors (unless there
     * are conditions on the selector).
     */
    private boolean isParent;  

    /**
     * Whether this selector is the left hand side of a left outer join.
     * Right outer joins are converted to left outer join.
     */
    private boolean outerJoinLeftHandSide;

    /**
     * Whether this selector is the right hand side of a left outer join.
     * Right outer joins are converted to left outer join.
     */
    private boolean outerJoinRightHandSide;

    /**
     * The list of all join conditions this selector is involved. For the query
     * "select * from nt:base as a inner join nt:base as b on a.x =
     * b.x", the join condition "a.x = b.x" is set for both selectors a and b,
     * so both can check if the property x is set.
     * The join conditions are added during the init phase.
     */
    private ArrayList<JoinConditionImpl> allJoinConditions =
            new ArrayList<JoinConditionImpl>();

    /**
     * The selector constraints can be evaluated when the given selector is
     * evaluated. For example, for the query
     * "select * from nt:base a inner join nt:base b where a.x = 1 and b.y = 2",
     * the condition "a.x = 1" can be evaluated when evaluating selector a. The
     * other part of the condition can't be evaluated until b is available.
     * These constraints are collected during the prepare phase.
     */
    private final List<ConstraintImpl> selectorConstraints = new ArrayList<>();

    private Cursor cursor;
    private IndexRow currentRow;
    private int scanCount;

    private String planIndexName;
    private TimerStats timerDuration;

    private CachedTree cachedTree;

    private boolean updateTotalQueryHistogram = true;

    public SelectorImpl(NodeTypeInfo nodeTypeInfo, String selectorName) {
        this.nodeTypeInfo = requireNonNull(nodeTypeInfo);
        this.selectorName = requireNonNull(selectorName);

        this.nodeTypeName = nodeTypeInfo.getNodeTypeName();
        this.matchesAllTypes = NT_BASE.equals(nodeTypeName);

        if (!this.matchesAllTypes) {
            this.supertypes = nodeTypeInfo.getSuperTypes();
            supertypes.add(nodeTypeName);

            this.primaryTypes = nodeTypeInfo.getPrimarySubTypes();
            this.mixinTypes = nodeTypeInfo.getMixinSubTypes();
            if (nodeTypeInfo.isMixin()) {
                mixinTypes.add(nodeTypeName);
            } else {
                primaryTypes.add(nodeTypeName);
            }
        } else {
            this.supertypes = Set.of();
            this.primaryTypes = Set.of();
            this.mixinTypes = Set.of();
        }
    }

    public String getSelectorName() {
        return selectorName;
    }

    public String getNodeType() {
        return nodeTypeName;
    }

    public boolean matchesAllTypes() {
        return matchesAllTypes;
    }

    /**
     * @return all of the matching supertypes, or empty if the
     *         {@link #matchesAllTypes} flag is set
     */
    @NotNull
    public Set<String> getSupertypes() {
        return supertypes;
    }

    /**
     * @return all of the matching primary subtypes, or empty if the
     *         {@link #matchesAllTypes} flag is set
     */
    @NotNull
    public Set<String> getPrimaryTypes() {
        return primaryTypes;
    }

    /**
     * @return all of the matching mixin types, or empty if the
     *         {@link #matchesAllTypes} flag is set
     */
    @NotNull
    public Set<String> getMixinTypes() {
        return mixinTypes;
    }

    public Iterable<String> getWildcardColumns() {
        return nodeTypeInfo.getNamesSingleValuesProperties();
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return quote(nodeTypeName) + " as " + quote(selectorName);
    }

    public boolean isPrepared() {
        return plan != null;
    }

    @Override
    public void unprepare() {
        plan = null;
        planIndexName = null;
        timerDuration = null;
        selectorConstraints.clear();
        isParent = false;
        joinCondition = null;
        allJoinConditions.clear();
    }

    @Override
    public void prepare(ExecutionPlan p) {
        if (!(p instanceof SelectorExecutionPlan)) {
            throw new IllegalArgumentException("Not a selector plan");
        }
        SelectorExecutionPlan selectorPlan = (SelectorExecutionPlan) p;
        if (selectorPlan.getSelector() != this) {
            throw new IllegalArgumentException("Not a plan for this selector");
        }
        pushDown();
        this.plan = selectorPlan;
    }

    private void pushDown() {
        if (queryConstraint != null) {
            queryConstraint.restrictPushDown(this);
        }
        if (!outerJoinLeftHandSide && !outerJoinRightHandSide) {
            for (JoinConditionImpl c : allJoinConditions) {
                c.restrictPushDown(this);
            }
        }
    }

    @Override
    public ExecutionPlan prepare() {
        if (plan != null) {
            return plan;
        }
        pushDown();
        plan = query.getBestSelectorExecutionPlan(createFilter(true));
        return plan;
    }

    public SelectorExecutionPlan getExecutionPlan() {
        return plan;
    }

    @Override
    public void setQueryConstraint(ConstraintImpl queryConstraint) {
        this.queryConstraint = queryConstraint;
    }

    @Override
    public void setOuterJoin(boolean outerJoinLeftHandSide, boolean outerJoinRightHandSide) {
        this.outerJoinLeftHandSide = outerJoinLeftHandSide;
        this.outerJoinRightHandSide = outerJoinRightHandSide;
    }

    @Override
    public void addJoinCondition(JoinConditionImpl joinCondition, boolean forThisSelector) {
        if (forThisSelector) {
            this.joinCondition = joinCondition;
        }
        allJoinConditions.add(joinCondition);
        if (joinCondition.isParent(this)) {
            isParent = true;
        }
    }

    @Override
    public void execute(NodeState rootState) {
        long start = startTimer();
        try {
            executeInternal(rootState);
        } finally {
            stopTimer(start, true);
        }
    }

    private void executeInternal(NodeState rootState) {
        QueryIndex index = plan.getIndex();
        timerDuration = null;
        if (index == null) {
            cursor = Cursors.newPathCursor(new ArrayList<String>(), query.getSettings());
            planIndexName = "traverse";
            return;
        }
        IndexPlan p = plan.getIndexPlan();
        if (p != null) {
            planIndexName = p.getPlanName();
            p.setFilter(createFilter(false));
            AdvancedQueryIndex adv = (AdvancedQueryIndex) index;
            cursor = adv.query(p, rootState);
        } else {
            FilterImpl f = createFilter(false);
            planIndexName = index.getIndexName(f, rootState);
            cursor = index.query(f, rootState);
        }
        int prefetchCount = query.getQueryOptions().prefetchCount.
                orElse(query.getExecutionContext().getSettings().getPrefetchCount());
        if (prefetchCount > 0) {
            PrefetchNodeStore store = query.getExecutionContext().getPrefetchNodeStore();
            cursor = Cursors.newPrefetchCursor(cursor, store, prefetchCount,
                    rootState, query.getQueryOptions().prefetch);
        }
    }

    private long startTimer() {
        if (TIMER_DISABLED) {
            return -1;
        }
        return System.nanoTime();
    }
    
    private void stopTimer(long start, boolean execute) {
        if (start == -1) {
            return;
        }
        long timeNanos = System.nanoTime() - start;
        if (timeNanos > 1000000) {
            // always measure slow events (slower than 1 ms)
            measure(timeNanos);
        } else if ((timerSampleCounter++ & (TIMER_SAMPLE_RATE - 1)) == 0) {
            // only measure each xth fast event, but multiply by x, so on
            // average measured times are correct
            measure(timeNanos * TIMER_SAMPLE_RATE);
        }
    }

    private void measure(long timeNanos) {
        TimerStats t = timerDuration;
        if (t == null) {
            // reuse the timer (in the normal case)
            // QUERY_DURATION;index=<planIndexName> will be translated as metric name = QUERY_DURATION
            // and index=<planIndexName> as a label by a downstream consumer like prometheus.
            StatsProviderUtil statsProviderUtil = new StatsProviderUtil(query.getSettings().getStatisticsProvider());
            t = timerDuration = statsProviderUtil.getTimerStats().apply("QUERY_DURATION", Collections.singletonMap("index", planIndexName));
        }
        t.update(timeNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public String getPlan(NodeState rootState) {
        StringBuilder buff = new StringBuilder();
        buff.append(toString());
        buff.append(" /* ");
        QueryIndex index = getIndex();
        if (index != null) {
            if (index instanceof AdvancedQueryIndex) {
                AdvancedQueryIndex adv = (AdvancedQueryIndex) index;
                IndexPlan p = plan.getIndexPlan();
                buff.append(adv.getPlanDescription(p, rootState));
            } else {
                buff.append(index.getPlan(createFilter(true), rootState));
            }
        } else {
            buff.append("no-index\n");
        }
        if (!selectorConstraints.isEmpty()) {
            // we could add the selector constraints here,
            // but it turns out this distracts more than it helps -
            // however for the JSON representation it would be useful,
            // that's why I think it makes sense to keep the commented code for now
            // buff.append("    selectorCondition: ").append(new AndImpl(selectorConstraints).toString()).append("\n");
        }
        buff.append(" */");
        return buff.toString();
    }

    @Override
    public String getIndexCostInfo(NodeState rootState) {
        StringBuilder buff = new StringBuilder();
        buff.append(quoteJson(selectorName)).append(": ");
        QueryIndex index = getIndex();
        if (index != null) {
            if (index instanceof AdvancedQueryIndex) {
                IndexPlan p = plan.getIndexPlan();
                buff.append("{ perEntry: ").append(p.getCostPerEntry());
                buff.append(", perExecution: ").append(p.getCostPerExecution());
                buff.append(", count: ").append(p.getEstimatedEntryCount());
                buff.append(" }");
            } else {
                buff.append(index.getCost(createFilter(true), rootState));
            }
        }
        return buff.toString();
    }

    /**
     * Create the filter condition for planning or execution.
     * 
     * @param preparing whether a filter for the prepare phase should be made 
     * @return the filter
     */
    @Override
    public FilterImpl createFilter(boolean preparing) {
        FilterImpl f = new FilterImpl(this, query.getStatement(), query.getSettings());
        f.setPreparing(preparing);
        if (joinCondition != null) {
            joinCondition.restrict(f);
        }
        // rep:excerpt handling: create a (fake) restriction
        // "rep:excerpt is not null" to let the index know that
        // we will need the excerpt
        for (ColumnImpl c : query.getColumns()) {
            if (c.getSelector().equals(this)) {
                String columnName = c.getColumnName();
                if (columnName.equals(QueryConstants.OAK_SCORE_EXPLANATION)) {
                    f.restrictProperty(columnName, Operator.NOT_EQUAL, null);
                } else if (columnName.startsWith(QueryConstants.REP_EXCERPT)) {
                    f.restrictProperty(QueryConstants.REP_EXCERPT, Operator.EQUAL, PropertyValues.newString(columnName));
                } else if (columnName.startsWith(QueryConstants.REP_FACET)) {
                    f.restrictProperty(QueryConstants.REP_FACET, Operator.EQUAL, PropertyValues.newString(columnName));
                }
            }
        }

        // all conditions can be pushed to the selectors -
        // except in some cases to "outer joined" selectors,
        // but the exceptions are handled in the condition
        // itself.
        // An example where it *is* a problem:
        //  "select * from a left outer join b on a.x = b.y
        // where b.y is null" - in this case the selector b
        // must not use an index condition on "y is null"
        // (".. is null" must be written as "not .. is not null").
        if (queryConstraint != null) {
            queryConstraint.restrict(f);
            FullTextExpression ft = queryConstraint.getFullTextConstraint(this);
            f.setFullTextConstraint(ft);
        }
        for (ConstraintImpl constraint : selectorConstraints) {
            constraint.restrict(f);
        }
        QueryOptions options = query.getQueryOptions();
        if (options != null) {
            if (options.indexName != null) {
                f.restrictProperty(IndexConstants.INDEX_NAME_OPTION, 
                        Operator.EQUAL, PropertyValues.newString(options.indexName));
            }
            if (options.indexTag != null) {
                f.restrictProperty(IndexConstants.INDEX_TAG_OPTION, 
                        Operator.EQUAL, PropertyValues.newString(options.indexTag));
            }
        }
        return f;
    }

    @Override
    public boolean next() {
        long start = startTimer();
        try {
            return nextInternal();
        } finally {
            stopTimer(start, true);
        }
    }

    private boolean nextInternal() {
        while (cursor != null && cursor.hasNext()) {
            scanCount++;
            query.getQueryExecutionStats().scan(1, scanCount);
            try {
                totalQueryStats(query.getSettings());
                currentRow = cursor.next();
            } catch (RuntimeNodeTraversalException e) {
                addSlowQueryStats(query.getSettings());
                LOG.warn(e.getMessage() + " for query " + query.getStatement());
                throw e;
            }
            if (isParent) {
                // we must not check whether the _parent_ is readable
                // for joins of type
                // "select [b].[jcr:primaryType]
                // from [nt:base] as [a] 
                // inner join [nt:base] as [b] 
                // on isdescendantnode([b], [a]) 
                // where [b].[jcr:path] = $path"
                // because if we did, we would filter out
                // correct results
            } else if (currentRow.isVirtualRow()) {
                // this is a virtual row and should be selected as is
                return true;
            } else {
                // we must check whether the _child_ is readable
                // (even if no properties are read) for joins of type
                // "select [a].[jcr:primaryType]
                // from [nt:base] as [a] 
                // inner join [nt:base] as [b] 
                // on isdescendantnode([b], [a]) 
                // where [a].[jcr:path] = $path"
                // because not checking would reveal existence
                // of the child node
                if (!getCachedTree(currentRow.getPath()).exists()) {
                    continue;
                }
            }
            if (evaluateCurrentRow()) {
                return true;
            }
        }
        cursor = null;
        currentRow = null;
        return false;
    }

    private void totalQueryStats(QueryEngineSettings queryEngineSettings) {
        if (updateTotalQueryHistogram) {
            updateTotalQueryHistogram = false;
            HistogramStats histogramStats = queryEngineSettings.getStatisticsProvider().getHistogram(SLOW_QUERY_PERCENTILE_METRICS_NAME, StatsOptions.METRICS_ONLY);
            histogramStats.update(TOTAL_QUERY_HISTOGRAM);
        }
    }

    private void addSlowQueryStats(QueryEngineSettings queryEngineSettings) {
        HistogramStats histogramStats = queryEngineSettings.getStatisticsProvider().getHistogram(SLOW_QUERY_PERCENTILE_METRICS_NAME, StatsOptions.METRICS_ONLY);
        histogramStats.update(SLOW_QUERY_HISTOGRAM);
        CounterStats slowQueryCounter = queryEngineSettings.getStatisticsProvider().getCounterStats(SLOW_QUERY_COUNT_NAME, StatsOptions.METRICS_ONLY);
        slowQueryCounter.inc();
    }

    private boolean evaluateCurrentRow() {
        if (currentRow.isVirtualRow()) {
            //null path implies that all checks are already done -- we just need to pass it through
            return true;
        }
        if (!matchesAllTypes && !evaluateTypeMatch()) {
            return false;
        }
        for (ConstraintImpl constraint : selectorConstraints) {
            if (!constraint.evaluate()) {
                if (constraint.evaluateStop()) {
                    // stop processing from now on
                    cursor = null;
                }
                return false;
            }
        }
        if (joinCondition != null && !joinCondition.evaluate()) {
            return false;
        }
        return true;
    }

    private boolean evaluateTypeMatch() {
        CachedTree ct = getCachedTree(currentRow.getPath());
        if (!ct.exists()) {
            return false;
        }

        Tree t = ct.getTree();
        LazyValue<Tree> readOnly = ct.getReadOnlyTree();
        String primaryTypeName = TreeUtil.getPrimaryTypeName(t, readOnly);
        if (primaryTypeName != null && primaryTypes.contains(primaryTypeName)) {
            return true;
        }

        for (String mixinName : TreeUtil.getMixinTypeNames(t, readOnly)) {
            if (mixinTypes.contains(mixinName)) {
                return true;
            }
        }
        // no matches found
        return false; 
    }

    /**
     * Get the current absolute Oak path (normalized).
     *
     * @return the path
     */
    public String currentPath() {
        return cursor == null ? null : currentRow.getPath();
    }

    /**
     * Get the tree at the current path.
     * 
     * @return the current tree, or null
     */
    @Nullable
    public Tree currentTree() {
        String path = currentPath();
        if (path == null) {
            return null;
        }
        return getTree(path);
    }

    @Nullable
    Tree getTree(@NotNull String path) {
        return getCachedTree(path).getTree();
    }

    /**
     * Get the tree at the given path.
     * 
     * @param path the path
     * @return the tree, or null
     */
    @NotNull
    private CachedTree getCachedTree(@NotNull  String path) {
        if (cachedTree == null || !cachedTree.denotes(path)) {
            cachedTree = new CachedTree(path, query);
        }
        return cachedTree;
    }

    /**
     * The value for the given selector for the current node.
     * 
     * @param propertyName the JCR (not normalized) property name
     * @return the property value
     */
    public PropertyValue currentProperty(String propertyName) {
        String pn = normalizePropertyName(propertyName);
        return currentOakProperty(pn);
    }

    /**
     * The value for the given selector for the current node, filtered by
     * property type.
     * 
     * @param propertyName the JCR (not normalized) property name
     * @param propertyType only include properties of this type
     * @return the property value (possibly null)
     */
    public PropertyValue currentProperty(String propertyName, int propertyType) {
        String pn = normalizePropertyName(propertyName);
        return currentOakProperty(pn, propertyType);
    }

    /**
     * Get the property value. The property name may be relative. The special
     * property names "jcr:path", "jcr:score" and "rep:excerpt" are supported.
     * 
     * @param oakPropertyName (must already be normalized)
     * @return the property value or null if not found
     */
    public PropertyValue currentOakProperty(String oakPropertyName) {
        return currentOakProperty(oakPropertyName, null);
    }

    private PropertyValue currentOakProperty(String oakPropertyName, Integer propertyType) {
        boolean asterisk = oakPropertyName.indexOf('*') >= 0;
        if (asterisk) {
            Tree t = currentTree();
            if (t != null) {
                LOG.trace("currentOakProperty() - '*' case. looking for '{}' in '{}'",
                    oakPropertyName, t.getPath());
            }
            ArrayList<PropertyValue> list = new ArrayList<PropertyValue>();
            readOakProperties(list, t, oakPropertyName, propertyType);
            if (list.size() == 0) {
                return null;
            } else if (list.size() == 1) {
                return list.get(0);
            }
            Type<?> type = list.get(0).getType();
            for (int i = 1; i < list.size(); i++) {
                Type<?> t2 = list.get(i).getType();
                if (t2 != type) {
                    // types don't match
                    type = Type.STRING;
                    break;
                }
            }
            if (type == Type.STRING) {
                ArrayList<String> strings = new ArrayList<String>();
                for (PropertyValue p : list) {
                    Iterables.addAll(strings, p.getValue(Type.STRINGS));
                }
                return PropertyValues.newString(strings);
            }
            Type<?> baseType = type.isArray() ? type.getBaseType() : type;
            @SuppressWarnings("unchecked")
            PropertyBuilder<Object> builder = (PropertyBuilder<Object>) PropertyBuilder.array(baseType);
            builder.setName("");
            for (PropertyValue v : list) {
                if (type.isArray()) {
                    for (Object value : (Iterable<?>) v.getValue(type)) {
                        builder.addValue(value);
                    }
                } else {
                    builder.addValue(v.getValue(type));
                }
            }
            PropertyState s = builder.getPropertyState();
            return PropertyValues.create(s);
        }
        boolean relative = !oakPropertyName.startsWith(QueryConstants.REP_FACET + "(")
                && !oakPropertyName.startsWith(QueryConstants.REP_EXCERPT + "(")
                && oakPropertyName.indexOf('/') >= 0;
        Tree t = currentTree();
        if (relative) {
            for (String p : PathUtils.elements(PathUtils.getParentPath(oakPropertyName))) {
                if (t == null) {
                    return null;
                }
                if (p.equals("..")) {
                    t = t.isRoot() ? null : t.getParent();
                } else if (p.equals(".")) {
                    // same node
                } else {
                    t = t.getChild(p);
                }
            }
            oakPropertyName = PathUtils.getName(oakPropertyName);
        }
        return currentOakProperty(t, oakPropertyName, propertyType);
    }

    private PropertyValue currentOakProperty(Tree t, String oakPropertyName, Integer propertyType) {
        PropertyValue result;
        if ((t == null || !t.exists()) && (currentRow == null || !currentRow.isVirtualRow())) {
            return null;
        }
        if (oakPropertyName.equals(QueryConstants.JCR_PATH)) {
            String path = currentPath();
            String local = getLocalPath(path);
            if (local == null) {
                // not a local path
                return null;
            }
            result = PropertyValues.newString(local);
        } else if (oakPropertyName.equals(QueryConstants.JCR_SCORE)) {
            result = currentRow.getValue(QueryConstants.JCR_SCORE);
        } else if (oakPropertyName.equals(QueryConstants.REP_EXCERPT)
                || oakPropertyName.startsWith(QueryConstants.REP_EXCERPT + "(")) {
            result = currentRow.getValue(oakPropertyName);
        } else if (oakPropertyName.equals(QueryConstants.OAK_SCORE_EXPLANATION)) {
            result = currentRow.getValue(QueryConstants.OAK_SCORE_EXPLANATION);
        } else if (oakPropertyName.equals(QueryConstants.REP_SPELLCHECK)) {
            result = currentRow.getValue(QueryConstants.REP_SPELLCHECK);
        } else if (oakPropertyName.equals(QueryConstants.REP_SUGGEST)) {
            result = currentRow.getValue(QueryConstants.REP_SUGGEST);
        } else if (oakPropertyName.startsWith(QueryConstants.REP_FACET + "(")) {
            result = currentRow.getValue(oakPropertyName);
        } else {
            result = PropertyValues.create(t.getProperty(oakPropertyName));
        }
        if (result == null) {
            return null;
        }
        if (propertyType != null && result.getType().tag() != propertyType) {
            return null;
        }
        return result;
    }

    private void readOakProperties(ArrayList<PropertyValue> target, Tree t, String oakPropertyName, Integer propertyType) {
        boolean skipCurrentNode = false;

        while (!skipCurrentNode) {
            if (t == null || !t.exists()) {
                return;
            }
            LOG.trace("readOakProperties() - reading '{}' for '{}'", t.getPath(),
                oakPropertyName);
            int slash = oakPropertyName.indexOf('/');
            if (slash < 0) {
                break;
            }
            String parent = oakPropertyName.substring(0, slash);
            oakPropertyName = oakPropertyName.substring(slash + 1);
            if (parent.equals("..")) {
                t = t.isRoot() ? null : t.getParent();
            } else if (parent.equals(".")) {
                // same node
            } else if (parent.equals("*")) {
                for (Tree child : t.getChildren()) {
                    readOakProperties(target, child, oakPropertyName, propertyType);
                }
                skipCurrentNode = true;
            } else {
                t = t.getChild(parent);
            }
        }
        if (skipCurrentNode) {
            return;
        }
        if (!"*".equals(oakPropertyName)) {
            PropertyValue value = currentOakProperty(t, oakPropertyName, propertyType);
            if (value != null) {
                LOG.trace("readOakProperties() - adding: '{}' from '{}'", value, t.getPath());
                target.add(value);
            }
            return;
        }
        for (PropertyState p : t.getProperties()) {
            if (propertyType == null || p.getType().tag() == propertyType) {
                PropertyValue v = PropertyValues.create(p);
                target.add(v);
            }
        }
    }

    public boolean isVirtualRow() {
        return currentRow != null && currentRow.isVirtualRow();
    }

    @Override
    public SelectorImpl getSelector(String selectorName) {
        if (selectorName.equals(this.selectorName)) {
            return this;
        }
        return null;
    }

    public long getScanCount() {
        return scanCount;
    }

    public void restrictSelector(ConstraintImpl constraint) {
        selectorConstraints.add(constraint);
    }

    public List<ConstraintImpl> getSelectorConstraints() {
        return selectorConstraints;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!(other instanceof SelectorImpl)) {
            return false;
        }
        return selectorName.equals(((SelectorImpl) other).selectorName);
    }

    @Override
    public int hashCode() {
        return selectorName.hashCode();
    }

    QueryIndex getIndex() {
        return plan == null ? null : plan.getIndex();
    }

    @Override
    public ArrayList<SourceImpl> getInnerJoinSelectors() {
        ArrayList<SourceImpl> list = new ArrayList<SourceImpl>();
        list.add(this);
        return list;
    }

    @Override
    public boolean isOuterJoinRightHandSide() {
        return this.outerJoinRightHandSide;
    }

    public QueryImpl getQuery() {
        return query;
    }

    @Override
    public long getSize(NodeState rootState, SizePrecision precision, long max) {
        if (cursor == null) {
            execute(rootState);
        }
        return cursor.getSize(precision, max);
    }

    @Override
    public SourceImpl copyOf() {
        return new SelectorImpl(nodeTypeInfo, selectorName);
    }

    private static final class CachedTree {

        private final String path;
        private final Tree tree;
        private final ExecutionContext ctx;
        private final LazyValue<Tree> readOnlyTree;

        private CachedTree(@NotNull String path, @NotNull QueryImpl query) {
            this.path = path;
            this.tree = query.getTree(path);
            this.ctx = query.getExecutionContext();
            this.readOnlyTree = new LazyValue<Tree>() {
                @Override
                protected Tree createValue() {
                    return new ImmutableRoot(ctx.getBaseState()).getTree(path);
                }
            };
        }

        private boolean denotes(@NotNull String path) {
            return this.path.equals(path);
        }

        private boolean exists() {
            return tree != null && tree.exists();
        }

        @Nullable
        private Tree getTree() {
            return tree;
        }

        @NotNull
        private LazyValue<Tree> getReadOnlyTree() {
            return readOnlyTree;
        }
    }
}
